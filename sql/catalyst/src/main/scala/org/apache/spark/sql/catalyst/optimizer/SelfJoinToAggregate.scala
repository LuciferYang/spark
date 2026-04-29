/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{INNER_LIKE_JOIN, LEFT_SEMI_OR_ANTI_JOIN}
import org.apache.spark.sql.types.AtomicType

/**
 * Rewrites a self-join with a non-equi residual on a non-key column into a GROUP BY HAVING
 * aggregation. Targets the q95 pattern where the self-join is wrapped in a `LeftSemi` or
 * `LeftAnti` join (an `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` subquery) consuming only the
 * equi-key columns.
 *
 * Pattern (post-analyzer / pre-optimizer):
 * {{{
 *   SELECT ... FROM outer
 *   WHERE outer.k IN (
 *     SELECT t1.k FROM t1, t1 t2 WHERE t1.k = t2.k AND t1.c <> t2.c
 *   )
 * }}}
 *
 * is rewritten as:
 * {{{
 *   SELECT ... FROM outer
 *   WHERE outer.k IN (
 *     SELECT k FROM t1
 *     WHERE c IS NOT NULL
 *     GROUP BY k
 *     HAVING count(DISTINCT c) > 1
 *   )
 * }}}
 *
 * The `c IS NOT NULL` filter is required for soundness under three-valued logic: SQL `<>`
 * returns UNKNOWN for any pair containing a NULL, so the original Inner+`<>` join excludes
 * rows whose `c` is NULL. `COUNT(DISTINCT c)` likewise excludes NULLs from the count, but
 * does not filter the underlying row -- without `IS NOT NULL` the rewrite would aggregate
 * over rows with NULL `c` that the original would have excluded.
 *
 * Why scoped to LeftSemi/LeftAnti parent: the rewrite produces ONE row per qualifying group,
 * while the original Inner self-join produces N x (N-1) rows for a partition with N distinct
 * non-key values. Cardinality reduction is sound only when the consumer treats the result as
 * a key set rather than a row multiset. `LeftSemi`/`LeftAnti` joins (the desugared form of
 * `IN`/`NOT IN`/`EXISTS`/`NOT EXISTS`) do exactly this: they only check whether at least one
 * matching row exists. Other parent shapes (e.g. `LIMIT`, plain `Project` consumed by another
 * `Join`) WOULD observe the cardinality difference, so the rule does not fire for them.
 *
 * Why scoped to atomic non-equi column types: SQL `<>` on complex types (struct/array/map)
 * uses field-wise null-propagating comparison, returning UNKNOWN whenever a null appears in
 * any field, while `count(DISTINCT)` and `IsNotNull` see the value as a whole and treat
 * `struct(null, x)` as a non-null distinct value. The semantics diverge for nested null. The
 * rule restricts `c.dataType` to `AtomicType` (the leaf scalar types) to keep `IsNotNull(c)`
 * and `<>` semantically equivalent.
 *
 * Restrictions:
 *   - Outer plan must be a `Join` of type `LeftSemi` or `LeftAnti`.
 *   - The right child of that outer join must be `Project(projectList, Join(left, right,
 *     Inner, Some(cond), _))`.
 *   - `left.sameResult(right)` (same underlying scan modulo ExprIds).
 *   - `cond` splits into one or more equi-key conjuncts plus exactly one non-equi residual of
 *     the form `Not(EqualTo(c1, c2))` where `c1` and `c2` are paired columns and NOT among
 *     the equi keys.
 *   - `c.dataType` is an `AtomicType`.
 *   - Project list references are a subset of the equi-key set (consumer treats result as a
 *     key set).
 *   - `left.stats.sizeInBytes` is at least
 *     `spark.sql.optimizer.selfJoinToAggregate.minTableSizeInBytes` (default 1 GiB). At
 *     smaller sizes the original Inner self-join's cost is dominated by setup
 *     (BroadcastHashJoin, ReusedExchange), and the rewrite's overhead would regress runtime
 *     (see `isLargeEnough`).
 *
 * Default-off (`spark.sql.optimizer.selfJoinToAggregate.enabled`).
 */
object SelfJoinToAggregate extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.selfJoinToAggregateEnabled) {
      plan
    } else {
      plan.transformUpWithPruning(
          _.containsAllPatterns(LEFT_SEMI_OR_ANTI_JOIN, INNER_LIKE_JOIN), ruleId) {
        // LeftSemi/LeftAnti always have the outer (row-source) side on the LEFT and the
        // membership-check side on the RIGHT. The cardinality reduction performed by this
        // rule is only sound for the RIGHT side; matching the LEFT side would silently
        // change the output cardinality of the LeftSemi/LeftAnti.
        case j @ Join(outer, Project(projectList, inner: Join), LeftSemi | LeftAnti, _, _) =>
          rewriteInnerSelfJoin(projectList, inner)
            .map(rewritten => j.withNewChildren(Seq(outer, rewritten)))
            .getOrElse(j)
      }
    }
  }

  /**
   * Try to build the GROUP BY HAVING rewrite for the inner self-join. Returns `None` if any
   * precondition fails; in that case the caller leaves the plan unchanged.
   */
  private def rewriteInnerSelfJoin(
      projectList: Seq[NamedExpression],
      inner: Join): Option[LogicalPlan] = inner match {
    case Join(left, right, Inner, Some(cond), _)
        if left.sameResult(right) && isLargeEnough(left) =>
      buildAggregate(projectList, left, right, cond)
    case _ => None
  }

  /**
   * Cost gate: skip the rewrite when the relation is smaller than
   * `selfJoinToAggregate.minTableSizeInBytes` (default 1 GiB). At smaller sizes the original
   * Inner self-join's cost is dominated by setup (BroadcastHashJoin, ReusedExchange) and the
   * rewrite's `count(DISTINCT)` -- which lowers via `RewriteDistinctAggregates` to an
   * Expand + two-stage Aggregate -- adds overhead that exceeds any saved join cost. SF10 q95
   * empirically regresses 10-22%; the rewrite is designed to win at SF100 and beyond, where
   * the relation outgrows broadcast and the SortMergeJoin shuffle dominates.
   *
   * Missing statistics surface as `defaultSizeInBytes` (very large), which trivially passes.
   */
  private def isLargeEnough(plan: LogicalPlan): Boolean = {
    plan.stats.sizeInBytes >= conf.selfJoinToAggregateMinTableSize
  }

  /**
   * Construct the rewritten plan. All preconditions on attribute pairing, residual shape, and
   * type/cardinality safety are checked here.
   */
  private def buildAggregate(
      projectList: Seq[NamedExpression],
      left: LogicalPlan,
      right: LogicalPlan,
      cond: Expression): Option[LogicalPlan] = {
    if (left.output.length != right.output.length) return None

    // Pair attributes by position, keyed by ExprId. Lookup must be by ExprId because
    // Attribute.equals also compares qualifier, which can differ between left.output and
    // predicate references after SubqueryAlias resolution.
    val leftIdToRight: Map[ExprId, Attribute] =
      left.output.zip(right.output).map { case (l, r) => l.exprId -> r }.toMap
    val rightIdToLeft: Map[ExprId, Attribute] =
      left.output.zip(right.output).map { case (l, r) => r.exprId -> l }.toMap

    // Split into equi-key conjuncts and "everything else". `IsNotNull(k)` predicates on
    // either side of an equi-key are treated as informational and ignored when counting
    // residuals: `InferFiltersFromConstraints` synthesises them in a later optimizer pass and
    // would otherwise prevent the matcher from finding the unique non-equi residual.
    val (equiPairs, residualParts) =
      classifyConjuncts(splitConjunctivePredicates(cond), leftIdToRight, rightIdToLeft)

    if (equiPairs.isEmpty || residualParts.length != 1) return None

    val nonEquiLeftCol: Option[Attribute] = residualParts.head match {
      case Not(EqualTo(a: Attribute, b: Attribute)) =>
        if (leftIdToRight.get(a.exprId).exists(_.exprId == b.exprId)) Some(a)
        else if (rightIdToLeft.get(a.exprId).exists(_.exprId == b.exprId)) Some(b)
        else None
      case _ => None
    }

    nonEquiLeftCol.flatMap { c =>
      // Soundness gate: complex types break the equivalence between `c1 <> c2` and
      // `IsNotNull(c) AND count(DISTINCT c) > 1` (see Scaladoc).
      if (!c.dataType.isInstanceOf[AtomicType]) return None

      val equiLeftIds = equiPairs.map(_._1.exprId).toSet
      if (equiLeftIds.contains(c.exprId)) return None

      val equiLeftCols = equiPairs.map(_._1)
      val equiLeftAttrSet = AttributeSet(equiLeftCols)
      if (projectList.exists(!_.references.subsetOf(equiLeftAttrSet))) return None

      // Build:
      //   Project(projectList,
      //     Filter(_cnt > 1L,
      //       Aggregate(groupBy=[k1, k2, ...],
      //                 aggExprs=[k1, k2, ..., count(distinct c) AS _cnt],
      //                 child=Filter(IsNotNull(c) AND IsNotNull(k1) AND ..., left))))
      //
      // IsNotNull(c) reproduces the original `<>` residual's three-valued-logic
      // exclusion of rows whose `c` is NULL.
      //
      // IsNotNull(k_i) on every equi-key is required for null-aware LeftAnti (desugared
      // `NOT IN`): the original `k1 = k2` excludes NULL-keyed rows via UNKNOWN. Without
      // it, the rewritten Aggregate would emit a NULL-key group, which null-aware LeftAnti
      // would then match against every outer row -- silently dropping outputs.
      val countDistinct = AggregateExpression(
        aggregateFunction = Count(c :: Nil),
        mode = Complete,
        isDistinct = true)
      val cntAlias = Alias(countDistinct, "_self_join_to_aggregate_cnt")()
      val cntAttr = cntAlias.toAttribute

      val notNullPredicates: Seq[Expression] = IsNotNull(c) +: equiLeftCols.map(IsNotNull(_))
      val combinedNotNull = notNullPredicates.reduce(And)
      val withNotNull = Filter(combinedNotNull, left)
      val agg = Aggregate(
        groupingExpressions = equiLeftCols,
        aggregateExpressions = equiLeftCols ++ Seq(cntAlias),
        child = withNotNull)
      val having = Filter(GreaterThan(cntAttr, Literal(1L)), agg)
      Some(Project(projectList, having))
    }
  }

  /**
   * Walk the conjuncts of a join condition and partition them into:
   *   - paired equi-key matches: `EqualTo(l, r)` where (l, r) are positionally-paired
   *     attributes of the self-joined relation
   *   - residuals: everything else, except `IsNotNull(attr)` predicates on either side (which
   *     are treated as informational and dropped from the residual list)
   */
  private def classifyConjuncts(
      conjuncts: Seq[Expression],
      leftIdToRight: Map[ExprId, Attribute],
      rightIdToLeft: Map[ExprId, Attribute]): (Seq[(Attribute, Attribute)], Seq[Expression]) = {
    val equiBuilder = Seq.newBuilder[(Attribute, Attribute)]
    val residualBuilder = Seq.newBuilder[Expression]
    conjuncts.foreach { pred =>
      extractEqui(pred, leftIdToRight) match {
        case Some(pair) => equiBuilder += pair
        case None =>
          if (!isInformationalIsNotNull(pred, leftIdToRight, rightIdToLeft)) {
            residualBuilder += pred
          }
      }
    }
    (equiBuilder.result(), residualBuilder.result())
  }

  /**
   * Returns true if `pred` is `IsNotNull(attr)` where `attr` belongs to either side of the
   * paired self-join. Such predicates are inserted by `InferFiltersFromConstraints` and do
   * not change the semantics of the matcher.
   */
  private def isInformationalIsNotNull(
      pred: Expression,
      leftIdToRight: Map[ExprId, Attribute],
      rightIdToLeft: Map[ExprId, Attribute]): Boolean = pred match {
    case IsNotNull(a: Attribute) =>
      leftIdToRight.contains(a.exprId) || rightIdToLeft.contains(a.exprId)
    case _ => false
  }

  private def extractEqui(
      pred: Expression,
      leftIdToRight: Map[ExprId, Attribute]): Option[(Attribute, Attribute)] = pred match {
    case EqualTo(l: Attribute, r: Attribute)
        if leftIdToRight.get(l.exprId).exists(_.exprId == r.exprId) =>
      Some((l, r))
    case EqualTo(r: Attribute, l: Attribute)
        if leftIdToRight.get(l.exprId).exists(_.exprId == r.exprId) =>
      Some((l, r))
    case _ => None
  }
}
