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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.PartialAggregationDecomposition.{decompose, supportedAgg}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateBase, FinalAggregate, LogicalPlan, PartialAggregate, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, PROJECT}

/**
 * Push the heavy part of an aggregation through [[Project]] so the per-row
 * aggregation work runs once on the Project's child (before its column
 * massaging). Closes the section 4.2 "push gamma below project" gap described in
 * Modi et al., *New Query Optimization Techniques in the Spark Engine of
 * Azure Synapse*, VLDB 2022.
 *
 * First-iteration scope: only handles [[Project]] whose `projectList`
 * consists entirely of bare [[Attribute]] references (no derived aliases).
 * This is the post-`ColumnPruning` shape: Project is a column-selection
 * layer that survives optimization. Derived aliases (e.g.,
 * `Alias(a + 1, "x")`) are not handled in this iteration -- Synapse section 4.2
 * allows them only when the alias is used purely as a grouping key (not
 * as an aggregation-expression input); a follow-up can add the
 * substitute-grouping logic.
 *
 * Plan before:
 * {{{
 *   Aggregate(g, aggFns)
 *     Project(bareAttributes, child)
 * }}}
 * Plan after:
 * {{{
 *   FinalAggregate(g, aggFns rewired to consume partials)
 *     PartialAggregate(g, g ++ partial_aliases, child)
 * }}}
 *
 * The Project layer is **dropped** in the rewrite: a bare-Attribute Project
 * is structurally a column-selection, and its outputs (a subset of
 * `child.outputSet`) are subsumed by the new `PartialAggregate`'s output
 * `g ++ partial_aliases`. Any extra columns the Project carried that are
 * not in `g` and not in aggregate-function inputs would have been dropped
 * by the outer Aggregate anyway.
 *
 * Per-function decomposition is shared with
 * [[PushPartialAggregationThroughUnion]],
 * [[PushPartialAggregationThroughFilter]], and
 * [[PushPartialAggregationThroughExpand]] via
 * [[PartialAggregationDecomposition]].
 *
 * Eligibility (in [[eligible]]):
 *  1. The outer aggregate has at least one [[AggregateExpression]].
 *  2. None is `DISTINCT` and none carries a `FILTER` clause.
 *  3. Every aggregate function is in the supported set.
 *  4. Every entry of `outerAgg.aggregateExpressions` (the full projection
 *     list -- not just the wrapped [[AggregateExpression]]s) is
 *     deterministic. This also rejects non-aggregate projections that
 *     reference non-deterministic expressions (e.g., `Rand()`).
 *  5. Every grouping expression is a bare [[Attribute]].
 *  6. `projectList` consists entirely of bare [[Attribute]] references --
 *     no derived aliases, no identity aliases.
 *
 * Cost gate ([[passesCostGate]]): the rewrite fires only when grouping
 * `project.child` by the grouping keys would compress rows enough, per
 * the shared `pushPartialAggHasBenefit` (ratio at or below
 * [[org.apache.spark.sql.internal.SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO]]).
 * When stats are unavailable the ratio falls back to `1.0`, so the gate
 * passes only when the configured benefit ratio is at or above `1.0`
 * (i.e., the user opts in to a no-stats best-effort rewrite).
 *
 * Idempotency: when the matched Project's child is already an
 * [[AggregateBase]] (which includes [[Aggregate]], [[PartialAggregate]], and
 * [[FinalAggregate]]), the rule does not re-fire. The batch is `Once` so a
 * single pass suffices for batch-local correctness.
 *
 * Traversal order: `transformUpWithPruning` (bottom-up), matching the
 * sibling rules [[PushPartialAggregationThroughUnion]],
 * [[PushPartialAggregationThroughFilter]], and
 * [[PushPartialAggregationThroughExpand]]. Rewriting the deepest match
 * first is safe because the rewrite never makes an outer match newly
 * eligible (it replaces `Aggregate(_, _, Project(_, _))` with
 * `FinalAggregate(_, _, PartialAggregate(_, _, _))`). The `ruleId`
 * registered with [[RuleIdCollection]] is passed to
 * `transformUpWithPruning` so future re-runs of this rule (e.g., from a
 * non-`Once` enclosing batch) can short-circuit already-visited subtrees.
 */
object PushPartialAggregationThroughProject extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      // The `ruleId` (registered in `RuleIdCollection`) lets
      // `transformUpWithPruning` short-circuit subtrees already visited if
      // this rule is ever re-run (e.g., from a non-`Once` enclosing batch).
      plan.transformUpWithPruning(_.containsAllPatterns(AGGREGATE, PROJECT), ruleId) {
        case outerAgg @ Aggregate(_, _, project: Project, _)
            if !project.child.isInstanceOf[AggregateBase] =>
          eligible(outerAgg, project) match {
            case Some((aggExprs, grouping)) =>
              if (passesCostGate(project.child, grouping)) {
                rewrite(outerAgg, project, grouping, aggExprs)
              } else {
                outerAgg
              }
            case None => outerAgg
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Eligibility
  // ---------------------------------------------------------------------------

  /**
   * Returns the collected `AggregateExpression`s and the typed grouping
   * `Attribute` list if and only if the rule can fire on `(outerAgg, project)`.
   * Folding eligibility, `collectAggregateExprs`, and the grouping-attribute
   * cast into a single pass avoids walking the expression tree twice and
   * keeps the unchecked cast next to the check that justifies it.
   */
  private def eligible(
      outerAgg: Aggregate,
      project: Project): Option[(Seq[AggregateExpression], Seq[Attribute])] = {
    val aggExprs = outerAgg.collectAggregateExprs
    if (aggExprs.isEmpty) return None
    if (aggExprs.exists(ae => ae.isDistinct || ae.filter.isDefined)) return None
    if (!aggExprs.forall(supportedAgg)) return None
    if (!outerAgg.aggregateExpressions.forall(_.deterministic)) return None
    val groupingAttrs = outerAgg.groupingExpressions.collect { case a: Attribute => a }
    if (groupingAttrs.size != outerAgg.groupingExpressions.size) return None
    // First-iteration scope: only bare-Attribute Project. Aliases (renaming or
    // derived) are rejected; a follow-up can handle the section 4.2 "alias used only
    // as grouping key" case by substituting the alias with its underlying
    // expression in the partial-aggregate's grouping.
    if (!project.projectList.forall(_.isInstanceOf[Attribute])) return None
    Some((aggExprs, groupingAttrs))
  }

  // ---------------------------------------------------------------------------
  // Cost gate
  // ---------------------------------------------------------------------------

  /**
   * Inspects stats on `child` (which is `project.child` -- the Project layer
   * is a bare-Attribute column selection that never changes row count, so the
   * Project's child carries the same row stats as the Project itself).
   * Returns true iff grouping `child` by `grouping` would compress rows
   * enough to make the pushed PartialAggregate worth its overhead per
   * `pushPartialAggHasBenefit`. When stats are unavailable the underlying
   * helper falls back to a ratio of `1.0`, so the gate passes only when the
   * configured `benefitRatio` is at or above `1.0`.
   */
  private def passesCostGate(child: LogicalPlan, grouping: Seq[Attribute]): Boolean = {
    PushPartialAggregationThroughJoin.pushPartialAggHasBenefit(grouping, child)
  }

  // ---------------------------------------------------------------------------
  // Rewrite
  // ---------------------------------------------------------------------------

  private def rewrite(
      outerAgg: Aggregate,
      project: Project,
      grouping: Seq[Attribute],
      originals: Seq[AggregateExpression]): LogicalPlan = {
    // `collectAggregateExprs` returns one entry per occurrence of an
    // `AggregateExpression` in the projection list. If a shared instance
    // appears twice (e.g., CSE-introduced `Add(sumAe, sumAe)`), `originals`
    // contains it twice but `decompose` must run once -- otherwise the
    // PartialAggregate gets duplicate partial aliases and the resultId-keyed
    // replacement map silently keeps only one entry. Dedupe by resultId.
    val mappings = originals.distinctBy(_.resultId).map(decompose)
    val partialAliases = mappings.flatMap(_.partials)

    // Drop Project. The bare-Attribute projectList is subsumed by the new
    // PartialAggregate's output (`grouping ++ partial_aliases`). Any
    // projectList attributes not in grouping/agg-fn inputs would have been
    // dropped by the outer Aggregate, so dropping the Project layer entirely
    // preserves semantics.
    val lower = PartialAggregate(
      groupingExpressions = grouping,
      aggregateExpressions = grouping ++ partialAliases,
      child = project.child)

    val replacements: Map[ExprId, Expression] =
      mappings.map(m => m.original.resultId -> m.replacement).toMap

    // `transformDown` recurses into the replacement expression after a match
    // (e.g., `Divide(Cast(Sum(...)), Cast(Sum(...)))` produced for Avg). The
    // recursion is safe because `decompose` produces fresh `AggregateExpression`
    // instances with new `resultId`s that are NOT in `replacements`, so no
    // second-pass rewrite fires.
    val newAggregateExpressions = outerAgg.aggregateExpressions.map { ne =>
      ne.transformDown {
        case ae: AggregateExpression if replacements.contains(ae.resultId) =>
          replacements(ae.resultId)
      }.asInstanceOf[NamedExpression]
    }

    FinalAggregate(
      groupingExpressions = outerAgg.groupingExpressions,
      aggregateExpressions = newAggregateExpressions,
      child = lower,
      hint = outerAgg.hint)
  }
}
