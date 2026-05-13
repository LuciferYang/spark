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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.PartialAggregationDecomposition.{decompose, supportedAgg}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateBase, FinalAggregate, LogicalPlan, PartialAggregate, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, UNION}

/**
 * Push the heavy part of an aggregation through [[Union]], so the per-row
 * aggregation work runs once below each Union child instead of once over the
 * combined union output.
 *
 * Plan before:
 * {{{
 *   Aggregate(grouping, aggFns)
 *     Union(c1, c2, ..., cn)
 * }}}
 * Plan after:
 * {{{
 *   FinalAggregate(grouping, aggFns rewired to consume partials)
 *     Union(
 *       PartialAggregate(grouping_for_c1, partials_for_c1, c1),
 *       PartialAggregate(grouping_for_c2, partials_for_c2, c2),
 *       ...
 *       PartialAggregate(grouping_for_cn, partials_for_cn, cn))
 * }}}
 *
 * Per-function decomposition is shared with [[PushPartialAggregationThroughExpand]]
 * via [[PartialAggregationDecomposition]]. Shares the same gating conf
 * [[org.apache.spark.sql.internal.SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED]]
 * and cost model conf
 * [[org.apache.spark.sql.internal.SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO]].
 *
 * Eligibility (each implemented in [[isEligible]]):
 *  1. The outer aggregate has at least one [[AggregateExpression]].
 *  2. None is `DISTINCT` and none carries a `FILTER` clause.
 *  3. Every aggregate function is in the supported set defined by
 *     [[PartialAggregationDecomposition.supportedAgg]].
 *  4. Every aggregate expression in `outerAgg.aggregateExpressions` is deterministic.
 *  5. Every grouping expression is an [[Attribute]] (matches the convention in
 *     `PushPartialAggregationThroughJoin` and `PushPartialAggregationThroughExpand`).
 *
 * Cost gate (in [[passesCostGate]]): the rewrite fires only when at least one
 * Union child sees a row-count reduction below
 * [[org.apache.spark.sql.internal.SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO]].
 * When stats are unavailable on every child, the ratio falls back to `1.0`,
 * matching the existing rules' permissive default.
 *
 * Idempotency: when a [[PartialAggregate]] (or any [[AggregateBase]]) is already
 * present directly below any Union child, the rule does not re-wrap.
 *
 * Attribute mapping across children: each child has the same positional schema
 * as the Union output (Spark's analyzer inserts casts before Union to align
 * types). We map `union.output(i)` to `child_k.output(i)` per child when
 * constructing each per-child [[PartialAggregate]]. New per-child partial
 * aliases are created with fresh [[ExprId]]s; the new [[Union]]'s output picks
 * up the first child's partial alias attributes (via `Union.mergeChildOutputs`,
 * which preserves first-child ExprIds), and we rebind the outer aggregate's
 * replacement references accordingly.
 */
object PushPartialAggregationThroughUnion extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      plan.transformUpWithPruning(_.containsAllPatterns(AGGREGATE, UNION)) {
        case outerAgg @ Aggregate(_, _, union: Union, _)
            if !union.byName && !union.allowMissingCol &&
              union.children.forall(c => !c.isInstanceOf[AggregateBase]) &&
              isEligible(outerAgg) =>
          if (passesCostGate(outerAgg, union)) {
            rewrite(outerAgg, union)
          } else {
            outerAgg
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Eligibility
  // ---------------------------------------------------------------------------

  private def isEligible(outerAgg: Aggregate): Boolean = {
    val aggExprs = outerAgg.collectAggregateExprs
    if (aggExprs.isEmpty) return false
    if (aggExprs.exists(ae => ae.isDistinct || ae.filter.isDefined)) return false
    if (!aggExprs.forall(supportedAgg)) return false
    if (!outerAgg.aggregateExpressions.forall(_.deterministic)) return false
    // Restrict to bare-Attribute groupings. Matches `PushPartialAggregationThroughJoin`
    // (line 436) and `PushPartialAggregationThroughExpand` (which derives
    // `dimensions: Seq[Attribute]` from `expand.child.output`). In Spark's standard
    // optimizer pipeline, `PullOutGroupingExpressions` runs in the "Finish Analysis"
    // batch before partial-aggregation push-down, so derived groupings (e.g.,
    // `Substring(col, 1, 3)`) have already been pulled into a Project below the
    // Aggregate as Aliases and the Aggregate's `groupingExpressions` reference those
    // Aliases as Attributes. Defensive guard against pipelines where this invariant
    // does not hold (e.g., test harnesses that run only this rule).
    if (!outerAgg.groupingExpressions.forall(_.isInstanceOf[Attribute])) return false
    true
  }

  // ---------------------------------------------------------------------------
  // Cost gate
  // ---------------------------------------------------------------------------

  /**
   * Push if at least one child sees row-count reduction below the configured
   * benefit ratio. Mirrors [[PushPartialAggregationThroughJoin.pushPartialAggHasBenefit]]
   * but per-child, with the outer aggregate's grouping expressions remapped to
   * each child's positional schema.
   */
  private def passesCostGate(outerAgg: Aggregate, union: Union): Boolean = {
    union.children.exists { child =>
      val subst = childSubst(union, child)
      val mappedGrouping = outerAgg.groupingExpressions.map { e =>
        e.transform { case a: Attribute if subst.contains(a.exprId) => subst(a.exprId) }
      }
      PushPartialAggregationThroughJoin.pushPartialAggHasBenefit(mappedGrouping, child)
    }
  }

  // ---------------------------------------------------------------------------
  // Rewrite
  // ---------------------------------------------------------------------------

  /** Map from union output ExprId to the corresponding child output Attribute. */
  private def childSubst(union: Union, child: LogicalPlan): Map[ExprId, Attribute] =
    union.output.zip(child.output).map { case (u, c) => u.exprId -> c }.toMap

  /** Substitute attributes inside an expression using a map. */
  private def substitute(expr: Expression, subst: Map[ExprId, Attribute]): Expression =
    expr.transform { case a: Attribute if subst.contains(a.exprId) => subst(a.exprId) }

  private def rewrite(outerAgg: Aggregate, union: Union): LogicalPlan = {
    val originals = outerAgg.collectAggregateExprs
    val mappings = originals.map(decompose)
    val partialAliasesTemplate = mappings.flatMap(_.partials)

    // For each Union child, build a PartialAggregate by substituting union.output
    // attributes with the child's positional output. Each child gets fresh Alias
    // instances (fresh ExprIds) so the per-child PartialAggregate is structurally
    // independent.
    val newChildren: Seq[LogicalPlan] = union.children.map { child =>
      val subst = childSubst(union, child)

      // `isEligible` guarantees every grouping expression is an Attribute, so
      // substitution yields an Attribute too. The result is safe to use directly
      // both as `groupingExpressions` (Seq[Expression]) and inside
      // `aggregateExpressions` (Seq[NamedExpression]) below.
      val mappedGrouping: Seq[Attribute] = outerAgg.groupingExpressions.map { e =>
        substitute(e, subst).asInstanceOf[Attribute]
      }

      val mappedPartialAliases: Seq[NamedExpression] = partialAliasesTemplate.map { a =>
        val newChildExpr = substitute(a.child, subst)
        // Fresh ExprId via Alias()() so each per-child partial alias is distinct.
        Alias(newChildExpr, a.name)()
      }

      PartialAggregate(
        groupingExpressions = mappedGrouping,
        aggregateExpressions = mappedGrouping ++ mappedPartialAliases,
        child = child)
    }

    val newUnion = Union(newChildren)
    val newUnionOutput = newUnion.output
    val groupingArity = outerAgg.groupingExpressions.size

    // The new Union's output positions for partial aliases are
    // [groupingArity .. groupingArity + partialAliasesTemplate.size). Build a map
    // from the *template* partial-alias attribute ExprId (referenced by each
    // mapping.replacement) to the new Union's output attribute at the matching
    // position.
    val partialAttrMap: Map[ExprId, Attribute] = partialAliasesTemplate.zipWithIndex.map {
      case (alias, i) => alias.toAttribute.exprId -> newUnionOutput(groupingArity + i)
    }.toMap

    // Build the outer aggregate's new aggregateExpressions: for each original
    // AggregateExpression matched by resultId, splice in the replacement (with
    // partial-attribute references rebound to the new Union's output).
    val replacements: Map[ExprId, Expression] = mappings.map { m =>
      m.original.resultId -> m.replacement.transform {
        case a: Attribute if partialAttrMap.contains(a.exprId) => partialAttrMap(a.exprId)
      }
    }.toMap

    val newAggregateExpressions = outerAgg.aggregateExpressions.map { ne =>
      ne.transformDown {
        case ae: AggregateExpression if replacements.contains(ae.resultId) =>
          replacements(ae.resultId)
      }.asInstanceOf[NamedExpression]
    }

    FinalAggregate(
      groupingExpressions = outerAgg.groupingExpressions,
      aggregateExpressions = newAggregateExpressions,
      child = newUnion,
      hint = outerAgg.hint)
  }
}
