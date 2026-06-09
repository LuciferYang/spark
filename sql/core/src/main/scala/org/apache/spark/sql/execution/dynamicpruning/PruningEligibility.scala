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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{JoinSelectionHelper, PushDownPredicates, PushPredicateThroughJoin, ReorderJoin}
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2ScanRelationPushDown}

/**
 * Helpers used by `TagPruningVetoCTE` (fork-only Path alpha). Two top-level predicates:
 *
 *   - `hasInBodyDPPOpportunity(body)`: would `PartitionPruning` already fire on
 *     this body? If yes, caching the body is fine -- DPP fires during cache
 *     materialization (`cacheManager.cacheQuery -> executePlan` runs the full
 *     optimizer including `PartitionPruning`).
 *
 *   - `looksLikeMaterializationNotWorthIt(body, output)`: if we inline this body into an outer
 *     query, could outer predicates plausibly drive DPP/DFP on a partitioned/
 *     runtime-filterable scan inside the body?
 *
 * The veto decision is `!hasInBodyDPPOpportunity(shadow) &&
 * looksLikeMaterializationNotWorthIt(shadow, output)`,
 * where `shadow` is `shadowOptimize(body)` -- a small fixed set of rules applied
 * to a clone of the body so joins carry `Some(cond)` and V2 scans become
 * `DataSourceV2ScanRelation`.
 */
private[spark] object PruningEligibility extends PredicateHelper with JoinSelectionHelper {

  /**
   * Cap on shadow-optimize iterations. Production `Operator Optimization` runs
   * `PushDownPredicates`/`PushPredicateThroughJoin` inside a FixedPoint(100)
   * batch; deep CTE bodies (3+ joins with intermediate Projects/Filters) can
   * need 6-10 passes to settle predicate pushdown. 20 leaves a generous margin
   * while still preventing pathological unbounded work.
   */
  private val MaxShadowIterations: Int = 20

  /**
   * Apply a minimal optimizer pass to a CLONE of `body`:
   *   - `PushDownPredicates`, `PushPredicateThroughJoin`: move equi-keys and
   *     selective predicates into joins so `Join(_, _, _, Some(cond), _)` matches.
   *   - `ReorderJoin`: pull the join conditions onto the `Join` nodes of a multi-way join so
   *     `Join(_, _, _, Some(cond), _)` matches at every level. (It is NOT here to turn
   *     comma-FROM into `Inner`: the parser already produces `Inner` for that.)
   *   - `V2ScanRelationPushDown`: convert `DataSourceV2Relation` to
   *     `DataSourceV2ScanRelation` so `ExtractV2Scan` matches and
   *     `SupportsRuntimeV2Filtering` is reachable.
   *
   * We iterate to a fixed point (capped at [[MaxShadowIterations]]) so nested
   * Filter/Join chains stabilise.
   *
   * Pre-screen: skip `V2ScanRelationPushDown` entirely when the body contains no
   * `DataSourceV2Relation`. This avoids paying per-connector scan-build cost
   * (e.g., Lance zonemap stats loading) on bodies that wouldn't benefit anyway.
   */
  def shadowOptimize(body: LogicalPlan): LogicalPlan = {
    val needsV2Pushdown = body.exists(_.isInstanceOf[DataSourceV2Relation])
    val rules: Seq[Rule[LogicalPlan]] = if (needsV2Pushdown) {
      Seq(PushDownPredicates, PushPredicateThroughJoin, ReorderJoin, V2ScanRelationPushDown)
    } else {
      Seq(PushDownPredicates, PushPredicateThroughJoin, ReorderJoin)
    }

    var current = body
    var i = 0
    var changed = true
    while (changed && i < MaxShadowIterations) {
      val next = rules.foldLeft(current)((p, r) => r(p))
      changed = !next.fastEquals(current)
      current = next
      i += 1
    }
    current
  }

  /**
   * True if `PartitionPruning` would inject a `DynamicPruningSubquery` somewhere
   * in this body. Mirrors `PartitionPruning.prune` (lines 231-281) but only ASKS.
   *
   * Matches `_: InnerLike` at the top so a `Cross` join is still examined, but the per-side
   * test inside is `canPruneLeft`/`canPruneRight`, which reject `Cross` exactly as
   * `prune` does. Matching the wider pattern here and narrowing per side keeps the two
   * layers honest: this method answers "is there a join to look at", the inner test answers
   * "would `prune` take it".
   */
  def hasInBodyDPPOpportunity(body: LogicalPlan): Boolean = body.exists {
    case j @ Join(left, right, _: InnerLike, Some(cond), _) =>
      joinHasPruningOpportunity(j, left, right, cond)
    // Outer joins: PartitionPruning supports specific cases via canPruneLeft/Right.
    case j @ Join(left, right, joinType, Some(cond), _)
        if canPruneLeft(joinType) || canPruneRight(joinType) =>
      joinHasPruningOpportunity(j, left, right, cond)
    case _ => false
  }

  private def joinHasPruningOpportunity(
      j: Join, left: LogicalPlan, right: LogicalPlan, cond: Expression): Boolean = {
    val joinType: JoinType = j.joinType
    splitConjunctivePredicates(cond).exists {
      case EqualTo(a: Expression, b: Expression) if fromDifferentSides(a, b, left, right) =>
        val aIsLeft = a.references.subsetOf(left.outputSet) &&
          b.references.subsetOf(right.outputSet)
        val (l, r) = if (aIsLeft) (a, b) else (b, a)
        // Match `PartitionPruning.prune`'s requirement that the filtering side
        // is non-streaming AND has a selective predicate. DPP requires the
        // build side to materialise a finite broadcast key set, which a
        // streaming source never reaches; PartitionPruning therefore skips
        // joins where the filter side is streaming. Inlined here to avoid
        // promoting `hasPartitionPruningFilter` to `private[spark]`.
        //
        // `canPruneLeft`/`canPruneRight` alone, per side. Adding
        // `|| joinType.isInstanceOf[InnerLike]` also admitted `Cross`, which is an
        // `InnerLike` that neither `canPrune*` accepts, so a cross-join body counted as an
        // in-body pruning opportunity that `prune` would never take -- and the veto reads
        // this predicate negated, so that mistake kept a body cacheable that should have
        // been vetoed. It changed nothing for the `Inner` bodies this rule was measured on.
        val pruneLeft = canPruneLeft(joinType) &&
          PartitionPruning.getFilterableTableScan(l, left).isDefined &&
          !right.isStreaming && PartitionPruning.hasSelectivePredicate(right)
        val pruneRight = canPruneRight(joinType) &&
          PartitionPruning.getFilterableTableScan(r, right).isDefined &&
          !left.isStreaming && PartitionPruning.hasSelectivePredicate(left)
        pruneLeft || pruneRight
      case _ => false
    }
  }

  private def fromDifferentSides(
      a: Expression, b: Expression, left: LogicalPlan, right: LogicalPlan): Boolean = {
    def fromLeftRight(x: Expression, y: Expression): Boolean =
      x.references.nonEmpty && x.references.subsetOf(left.outputSet) &&
        y.references.nonEmpty && y.references.subsetOf(right.outputSet)
    fromLeftRight(a, b) || fromLeftRight(b, a)
  }

  // ==========================================================================
  // looksLikeMaterializationNotWorthIt -- forward dataflow analysis with alias-chain tracking
  // ==========================================================================

  /**
   * Heuristic eligibility check for the Auto-CTE veto. Returns true when the
   * body has a partitioned/runtime-filterable fact scan AND some attribute
   * on the non-filterable join side survives to the CTE output.
   *
   * Renamed from `outerDPPFeasible`, which claimed a mechanism this predicate does not
   * measure. Empirical validation shows the partition-side join key rarely survives to CTE
   * output -- outer DPP is NOT what produces the observed wins. The benefit comes from
   * avoiding cache materialization on heavy aggregate CTEs and from the optimizer fusion
   * freedom inlining unlocks; this check happens to identify those CTEs because they share a
   * structural fingerprint (partitioned fact + aggregate + multi-reference). Read it as a
   * shape heuristic, not as a prediction of `PartitionPruning.prune`.
   *
   * Do NOT tighten this to require the partition-side join key to survive --
   * that would reject all empirically beneficial vetoes. A future PR should
   * replace this heuristic with an explicit materialization-vs-recompute cost
   * comparison and rename accordingly.
   *
   * Algorithm (top-down walk with ancestor stack):
   *   1. For each `Join` with an equi-key where one side is a filterable scan
   *      (via `PartitionPruning.getFilterableTableScan`), the OTHER side's
   *      output attributes are the candidate carriers.
   *   2. Walk ancestors. For each operator above the Join, transform the
   *      candidate set per survival rules:
   *        - `Project`, `Aggregate`: track Alias renames.
   *        - `Filter`, `SubqueryAlias`, other `Join`: pass-through.
   *        - `Window`, `Distinct`, `Generate`, `Expand`: barrier.
   *   3. If any final survivor's `ExprId` is in `output.map(_.exprId)`,
   *      return true.
   */
  def looksLikeMaterializationNotWorthIt(body: LogicalPlan, output: Seq[Attribute]): Boolean = {
    val outputIds = output.map(_.exprId).toSet
    if (outputIds.isEmpty) return false

    def check(plan: LogicalPlan, ancestors: List[LogicalPlan]): Boolean = plan match {
      case j @ Join(left, right, joinType, Some(cond), _)
          if joinType.isInstanceOf[InnerLike] ||
             canPruneLeft(joinType) || canPruneRight(joinType) =>
        val hit = splitConjunctivePredicates(cond).exists {
          case EqualTo(a: Expression, b: Expression) if fromDifferentSides(a, b, left, right) =>
            val aIsLeft = a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)
            val (l, r) = if (aIsLeft) (a, b) else (b, a)
            (PartitionPruning.getFilterableTableScan(l, left).isDefined &&
                survivesToOutput(right.outputSet.toSeq.map(_.exprId).toSet,
                  ancestors, outputIds)) ||
            (PartitionPruning.getFilterableTableScan(r, right).isDefined &&
                survivesToOutput(left.outputSet.toSeq.map(_.exprId).toSet,
                  ancestors, outputIds))
          case _ => false
        }
        hit || j.children.exists(c => check(c, j :: ancestors))
      case other =>
        other.children.exists(c => check(c, other :: ancestors))
    }

    check(body, Nil)
  }

  /**
   * Walk ancestors from innermost (newest-first) to outermost (body root),
   * transforming `surviving` per each operator's survival rules. Return true
   * if any final survivor's `ExprId` is in `outputIds`.
   */
  private def survivesToOutput(
      surviving: Set[ExprId],
      ancestors: List[LogicalPlan],
      outputIds: Set[ExprId]): Boolean = {
    var current = surviving
    val it = ancestors.iterator
    while (it.hasNext) {
      current = transformSurvival(it.next(), current)
      if (current.isEmpty) return false
    }
    current.intersect(outputIds).nonEmpty
  }

  /**
   * Given a NamedExpression `ne` (a Project/Aggregate output entry) and the
   * current set of surviving input ExprIds, return the ExprId that `ne`
   * contributes to the survivor set, or None if `ne` does not depend on any
   * surviving input. Covers `Alias` (NEW exprId after rename), `Attribute`
   * (pass-through exprId), and any other resolved `NamedExpression` via
   * `.references` (catches `OuterReference` and other concrete subclasses).
   */
  private def survivor(
      ne: NamedExpression, surviving: Set[ExprId]): Option[ExprId] = ne match {
    // Post-analyzer shadow plans contain only resolved nodes, so Alias and
    // Attribute here are resolved in practice. We rely on the analyzer (not
    // the type system) for this -- `UnresolvedAttribute extends Attribute`
    // and `Alias.resolved` can be false on edge cases (e.g. Generator child),
    // so `.resolved` guards below catch any leakage of unresolved instances
    // whose `exprId` would otherwise throw UnresolvedException.
    case a: Alias if a.resolved &&
        a.child.references.exists(attr => surviving.contains(attr.exprId)) =>
      Some(a.exprId)
    case attr: Attribute if attr.resolved && surviving.contains(attr.exprId) =>
      Some(attr.exprId)
    case other if other.resolved &&
        other.references.exists(attr => surviving.contains(attr.exprId)) =>
      Some(other.exprId)
    case _ => None
  }

  /**
   * Compute the set of ExprIds that survive `op`, given input `surviving`.
   * Project/Aggregate: delegate to [[survivor]] for each output entry.
   * Filter / SubqueryAlias: pass-through.
   * Join: intersect `surviving` with `op.output`. This is correct for all
   *   join types -- Inner/Outer expose both sides so the intersect is the
   *   identity on `surviving`, while LeftSemi/LeftAnti/ExistenceJoin drop the
   *   right-side outputs so right-tracked attributes are correctly dropped.
   * Window / Distinct / Generate / Expand: barrier (empty set).
   */
  private def transformSurvival(op: LogicalPlan, surviving: Set[ExprId]): Set[ExprId] = op match {
    case Project(projectList, _) =>
      projectList.flatMap(survivor(_, surviving)).toSet
    case Aggregate(_, aggregateExprs, _, _) =>
      aggregateExprs.flatMap(survivor(_, surviving)).toSet
    case j: Join =>
      surviving.intersect(j.output.map(_.exprId).toSet)
    case _: Filter | _: SubqueryAlias => surviving
    // `Distinct.output` IS `child.output` (same `Attribute` instances, same ExprIds), so
    // pass-through is the EXACT dataflow, not an approximation -- treating it as a barrier was
    // simply wrong. It also has to be pass-through for this rule to agree with itself across a
    // batch boundary: this rule runs in `preOptimizationBatches`, ahead of
    // `Batch("Replace Operators")`, so a deduplicating `UNION` still appears as
    // `Distinct(Union(...))` here, while one batch later `ReplaceDistinctWithAggregate` rewrites
    // it to `Aggregate(child.output, child.output, child)` -- which the `Aggregate` case above
    // passes through. The barrier therefore made the verdict depend on which side of that batch
    // the plan was inspected from.
    //
    // Measured cost of the barrier on the 3.5.5 fork: TPC-DS q75's body is
    // `Aggregate(GROUP BY d_year, ...) over Distinct(Union(3 join chains))`, and every survival
    // walk died on that `Distinct`, so `d_year` never reached the CTE output and the body was
    // not vetoed. Caching it turned six `SubqueryBroadcast` DPP nodes into zero -- the outer
    // `d_year = 2002` could no longer reach the fact scans through the `InMemoryRelation`
    // barrier -- and the three fact tables went from 195.4e9 pruned rows to 490.3e9 unpruned.
    // First execution went 59.5s -> 118.7s at 100TB.
    case _: Distinct => surviving
    case _: Union | _: Intersect | _: Except =>
      // Set-ops are pass-through here, NOT barriers. Union.mergeChildOutputs
      // reuses the first child's ExprIds when all branches have matching data
      // types, so pass-through is correct for walks that enter from the first
      // branch (which is the common case for UNION ALL of aggregate-shaped
      // sub-queries like Q4's year_total). Walks entering from non-first
      // branches, OR type-mismatched Union/Intersect/Except, would carry
      // stale ExprIds that fail to intersect outputIds at the body root --
      // yielding a false-negative (no veto) rather than a false-positive
      // (regression). Safe direction. Proper per-branch remapping is future
      // work.
      surviving
    case _: Window | _: Generate | _: Expand => Set.empty
    case _ =>
      // Unknown operators: be conservative -- pass through. The body shouldn't
      // contain operators that semantically alter output column survival
      // beyond the cases above; if it does, we may produce false positives.
      surviving
  }
}
