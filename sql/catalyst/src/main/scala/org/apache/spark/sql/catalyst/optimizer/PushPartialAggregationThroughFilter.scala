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

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.PartialAggregationDecomposition.{decompose, supportedAgg}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateBase, Filter, FinalAggregate, LogicalPlan, PartialAggregate}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, FILTER}

/**
 * Push the heavy part of an aggregation through [[Filter]], so the per-row
 * aggregation work runs once on a smaller per-group state under the Filter
 * boundary. Implements Modi et al. §4.2's "push γ below select (σ): extend keys
 * with columns referenced in the predicate."
 *
 * Plan before:
 * {{{
 *   Aggregate(g, aggFns)
 *     Filter(p, child)
 * }}}
 * Plan after:
 * {{{
 *   FinalAggregate(g, aggFns rewired to consume partials)
 *     Filter(p,
 *       PartialAggregate(g ∪ p.references,
 *                        g ∪ p.references ∪ partial_aliases,
 *                        child))
 * }}}
 *
 * The extended grouping is the union of the original grouping with the
 * predicate's referenced attributes. This guarantees that within each
 * (g, p.refs) combination all rows have the same predicate-relevant values,
 * so applying the Filter on partial-aggregated rows is semantically equivalent
 * to applying it on individual child rows. The outer FinalAggregate then
 * re-aggregates partial buffers by `g` only, dropping the extra p.refs grouping.
 *
 * Per-function decomposition is shared with [[PushPartialAggregationThroughUnion]]
 * and [[PushPartialAggregationThroughExpand]] via
 * [[PartialAggregationDecomposition]]; see that object's scaladoc for the
 * function table and the rationale for preserving fields like
 * `Sum.evalContext`, `Average.evalMode`, and `First`/`Last.ignoreNulls`.
 *
 * Eligibility (each implemented in [[isEligible]]):
 *  1. The outer aggregate has at least one [[AggregateExpression]].
 *  2. None is `DISTINCT` and none carries a `FILTER` clause.
 *  3. Every aggregate function is in the supported set defined by
 *     [[PartialAggregationDecomposition.supportedAgg]].
 *  4. Every aggregate expression in `outerAgg.aggregateExpressions` is deterministic.
 *  5. Every grouping expression is an [[Attribute]] (matches the convention in
 *     `PushPartialAggregationThroughJoin` and `PushPartialAggregationThroughExpand`).
 *  6. The Filter predicate is deterministic. A non-deterministic predicate
 *     (e.g., `rand() > 0.5`) evaluated once-per-(g, p.refs) row produces
 *     different rows than once-per-child-row.
 *
 * Cost gate (in [[passesCostGate]]): the rewrite fires only when the *extended*
 * grouping keys (original group-by + predicate refs) achieve a row-reduction
 * ratio at or below
 * [[org.apache.spark.sql.internal.SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO]].
 * If extending the keys with the predicate's columns inflates cardinality past
 * the threshold, the push-down is skipped because the partial aggregation
 * wouldn't compress the data. When stats are unavailable the ratio falls back
 * to `1.0` (matching the other rules' permissive default).
 *
 * Idempotency: when an [[AggregateBase]] is already present directly below the
 * Filter (i.e., a previous run already pushed), the rule does not re-fire.
 */
object PushPartialAggregationThroughFilter extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      plan.transformUpWithPruning(_.containsAllPatterns(AGGREGATE, FILTER)) {
        case outerAgg @ Aggregate(_, _, filter: Filter, _)
            if !filter.child.isInstanceOf[AggregateBase] && isEligible(outerAgg, filter) =>
          val extendedGrouping = computeExtendedGrouping(outerAgg, filter)
          if (passesCostGate(filter.child, extendedGrouping)) {
            rewrite(outerAgg, filter, extendedGrouping)
          } else {
            outerAgg
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Eligibility
  // ---------------------------------------------------------------------------

  private def isEligible(outerAgg: Aggregate, filter: Filter): Boolean = {
    val aggExprs = outerAgg.collectAggregateExprs
    if (aggExprs.isEmpty) return false
    if (aggExprs.exists(ae => ae.isDistinct || ae.filter.isDefined)) return false
    if (!aggExprs.forall(supportedAgg)) return false
    if (!outerAgg.aggregateExpressions.forall(_.deterministic)) return false
    if (!filter.condition.deterministic) return false
    // Restrict to bare-Attribute groupings. Matches `PushPartialAggregationThroughJoin`
    // and `PushPartialAggregationThroughExpand` (which derives `dimensions:
    // Seq[Attribute]` from `expand.child.output`). In Spark's standard
    // optimizer pipeline, `PullOutGroupingExpressions` runs before
    // partial-aggregation push-down so derived groupings are already pulled into
    // a Project below the Aggregate. Defensive guard against test harnesses or
    // future pipeline reorderings that violate this invariant.
    if (!outerAgg.groupingExpressions.forall(_.isInstanceOf[Attribute])) return false
    true
  }

  // ---------------------------------------------------------------------------
  // Extended grouping
  // ---------------------------------------------------------------------------

  /**
   * Extended grouping = original grouping ∪ predicate's referenced attributes,
   * with any attribute already in the original grouping deduplicated out. A
   * well-formed Filter's `condition.references` is by definition a subset of
   * `filter.child.outputSet`, so all extension attributes flow through the
   * Filter unchanged. Order: original groupings first, then predicate-only
   * attributes in `AttributeSet.toSeq` traversal order (canonicalized
   * insertion order; deterministic for a given expression tree, not lexical).
   */
  private def computeExtendedGrouping(outerAgg: Aggregate, filter: Filter): Seq[Attribute] = {
    val originals = outerAgg.groupingExpressions.map(_.asInstanceOf[Attribute])
    val originalIds: Set[ExprId] = originals.map(_.exprId).toSet
    val predRefs = filter.condition.references.toSeq.filterNot(a => originalIds.contains(a.exprId))
    originals ++ predRefs
  }

  // ---------------------------------------------------------------------------
  // Cost gate
  // ---------------------------------------------------------------------------

  /**
   * Push only when grouping `child` by the extended keys would compress rows
   * enough. Reuses [[PushPartialAggregationThroughJoin.pushPartialAggHasBenefit]]
   * which evaluates `aggregatedRowCount / originRowCount ≤ benefitRatio` (with
   * the standard ratio=1.0 fallback when stats are unavailable).
   */
  private def passesCostGate(child: LogicalPlan, extendedGrouping: Seq[Attribute]): Boolean = {
    PushPartialAggregationThroughJoin.pushPartialAggHasBenefit(extendedGrouping, child)
  }

  // ---------------------------------------------------------------------------
  // Rewrite
  // ---------------------------------------------------------------------------

  private def rewrite(
      outerAgg: Aggregate,
      filter: Filter,
      extendedGrouping: Seq[Attribute]): LogicalPlan = {
    val originals = outerAgg.collectAggregateExprs
    val mappings = originals.map(decompose)
    val partialAliases = mappings.flatMap(_.partials)

    // Lower PartialAggregate: groups on extended keys, projects
    // [extended grouping ++ all partial aliases].
    val lower = PartialAggregate(
      groupingExpressions = extendedGrouping,
      aggregateExpressions = extendedGrouping ++ partialAliases,
      child = filter.child)

    // Keep the Filter above the PartialAggregate; its predicate refs are all in
    // `extendedGrouping`, so they're in `lower.output`.
    val newFilter = filter.copy(child = lower)

    // Build the outer aggregate's new aggregateExpressions: for each original
    // AggregateExpression matched by `resultId`, splice in the replacement
    // (which references the partial alias attributes that flow through Filter
    // unchanged).
    val replacements: Map[ExprId, Expression] =
      mappings.map(m => m.original.resultId -> m.replacement).toMap

    val newAggregateExpressions = outerAgg.aggregateExpressions.map { ne =>
      ne.transformDown {
        case ae: AggregateExpression if replacements.contains(ae.resultId) =>
          replacements(ae.resultId)
      }.asInstanceOf[NamedExpression]
    }

    FinalAggregate(
      groupingExpressions = outerAgg.groupingExpressions,
      aggregateExpressions = newAggregateExpressions,
      child = newFilter,
      hint = outerAgg.hint)
  }
}
