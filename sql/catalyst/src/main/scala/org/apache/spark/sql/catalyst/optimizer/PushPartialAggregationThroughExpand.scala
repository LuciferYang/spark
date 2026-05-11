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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Cast, Divide, ExprId, Expression, NamedExpression, NumericEvalContext}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, BitAndAgg, BitOrAgg, BitXorAgg, Complete, Count, First, Last, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateBase, Expand, FinalAggregate, LogicalPlan, PartialAggregate}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.types.{DecimalType, LongType, NumericType}

/**
 * Push the heavy part of an aggregation through [[Expand]] (the operator behind
 * `ROLLUP` / `CUBE` / `GROUPING SETS`), so the per-row aggregation work runs on the
 * original child rows instead of on the n-times-expanded rows.
 *
 * Plan before:
 * {{{
 *   Aggregate(outer)
 *     Expand(N projections, output)
 *       child
 * }}}
 * Plan after:
 * {{{
 *   FinalAggregate(outer)               -- aggregate fns rewired to consume
 *     Expand(N, augmentedOutput)        -- pre-aggregated buffers
 *       PartialAggregate(pre-agg)       -- groups on every non-measure
 *         child                            child column
 * }}}
 *
 * This rule is part of the broader partial-aggregation push-down framework introduced
 * by [[PushPartialAggregationThroughJoin]] and described in Modi et al., "New Query
 * Optimization Techniques in the Spark Engine of Azure Synapse", VLDB 2022. It shares
 * the same gating conf [[SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED]] and the
 * same cost model conf [[SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO]].
 *
 * Per-function decomposition (the partial alias name pattern is `_pushed_<fn>_<refs>`,
 * e.g. `_pushed_sum_v` for `sum(v)`):
 *  - `Sum(x)`     -> outer `Sum(partial)`              inner `Sum(x)`
 *  - `Count(x)`   -> outer `Sum(partial)`              inner `Count(x)`
 *  - `Min(x)`     -> outer `Min(partial)`              inner `Min(x)`
 *  - `Max(x)`     -> outer `Max(partial)`              inner `Max(x)`
 *  - `First(x)`   -> outer `First(partial)`            inner `First(x)`
 *  - `Last(x)`    -> outer `Last(partial)`             inner `Last(x)`
 *  - `BitAndAgg`  -> same shape as `Min`               ditto
 *  - `BitOrAgg`   -> same shape as `Min`               ditto
 *  - `BitXorAgg`  -> same shape as `Min`               ditto
 *  - `Avg(x)`     -> outer `Sum(partial_sum) / Sum(partial_count)`,
 *                   inner produces both `Sum(x)` and `Count(x)`.
 *
 * Eligibility guards (each implemented in [[isEligible]]):
 *  1. The outer aggregate has at least one [[AggregateExpression]].
 *  2. None is `DISTINCT` and none carries a `FILTER` clause.
 *  3. Every aggregate function is in the supported set above.
 *  4. Every aggregate expression in `outerAgg.aggregateExpressions` is deterministic.
 *     Without this, e.g. `sum(rand())` would evaluate `rand()` once per child row
 *     instead of once per Expand-output row, changing the result.
 *  5. Every aggregate function input is a pass-through column from `Expand`'s child
 *     (not an Expand-produced attribute such as the `gid` literal or a null-padded
 *     grouping column). Pre-aggregating against an injected null would change semantics.
 *  6. `Expand`'s child has at least one non-measure attribute, so the pre-aggregation
 *     has something meaningful to group by.
 *
 * Stats-based cost gate (implemented in [[passesCostGate]]): even when the structural
 * guards pass, pre-aggregating only helps when the dimensions actually compress. The
 * estimated row count K' of the pre-aggregation (via [[Aggregate.stats]]) is compared
 * against the input row count R (via the child's `stats.rowCount`); the rewrite fires
 * only when `K' / R <= benefitRatio`. When statistics are unavailable, the ratio falls
 * back to `1.0`, so the rewrite fires only if the configured benefitRatio is also
 * `1.0` (i.e., the cost gate is intentionally disabled).
 *
 * The rule is idempotent - once a `PartialAggregate` (or any other [[AggregateBase]])
 * is in place below `Expand`, the second pass will not rewrite again because the
 * idempotency guard rejects that shape.
 *
 * Pass-through classification is computed via [[AttributeSet.intersect]]
 * (`ExprId`-only semantics) rather than `expand.producedAttributes`, which relies on
 * `Seq.diff`'s full `.equals` - later optimizer passes can adjust an attribute's
 * nullability or metadata while preserving its `ExprId`, breaking value equality but
 * not the identity we care about here.
 */
object PushPartialAggregationThroughExpand extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
        case outerAgg @ Aggregate(_, _, expand: Expand, _)
            if !expand.child.isInstanceOf[AggregateBase] && // idempotency guard
              isEligible(outerAgg, expand) =>
          val dimensions = computeDimensions(outerAgg, expand)
          if (passesCostGate(expand, dimensions)) {
            rewrite(outerAgg, expand, dimensions)
          } else {
            outerAgg
          }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Eligibility
  // ---------------------------------------------------------------------------

  private def isEligible(outerAgg: Aggregate, expand: Expand): Boolean = {
    val aggExprs = outerAgg.collectAggregateExprs
    if (aggExprs.isEmpty) return false
    if (aggExprs.exists(ae => ae.isDistinct || ae.filter.isDefined)) return false
    if (!aggExprs.forall(supportedAgg)) return false

    // Refuse non-deterministic aggregate inputs. Pre-aggregating, e.g., `sum(rand())`
    // would evaluate `rand()` once per child row instead of once per Expand-output row,
    // changing the result. (Same guard as `PushPartialAggregationThroughJoin`.)
    if (!outerAgg.aggregateExpressions.forall(_.deterministic)) return false

    val passThrough = AttributeSet(expand.output).intersect(expand.child.outputSet)
    val measureRefs = AttributeSet(aggExprs.flatMap(_.aggregateFunction.references))
    if (!measureRefs.subsetOf(passThrough)) return false

    val measureExprIds: Set[ExprId] = measureRefs.map(_.exprId).toSet
    expand.child.output.exists(a => !measureExprIds.contains(a.exprId))
  }

  private def supportedAgg(ae: AggregateExpression): Boolean = ae match {
    // Average accepts NumericType and AnsiIntervalType inputs. Decimal Average is
    // intentionally excluded for now: matching `Average.evaluateExpression`'s
    // precision exactly requires applying `DecimalPrecision` coercion to the
    // rewritten Divide, which is not done here. Non-Decimal numeric Avg uses the
    // simple `Sum(x) / Count(x)` decomposition. (TODO: extend to Decimal in a
    // follow-up by mirroring `Average.evaluateExpression` end-to-end.)
    case AggregateExpression(Average(child, _), Complete, false, None, _) =>
      child.dataType match {
        case _: DecimalType => false
        case _: NumericType => true
        case _ => false
      }
    case AggregateExpression(_: Sum | _: Count | _: Min | _: Max |
        _: First | _: Last | _: BitAndAgg | _: BitOrAgg | _: BitXorAgg,
        Complete, false, None, _) => true
    case _ => false
  }

  // ---------------------------------------------------------------------------
  // Cost gate
  // ---------------------------------------------------------------------------

  private def computeDimensions(outerAgg: Aggregate, expand: Expand): Seq[Attribute] = {
    val measureExprIds: Set[ExprId] = outerAgg.collectAggregateExprs
      .flatMap(_.aggregateFunction.references.map(_.exprId)).toSet
    expand.child.output.filterNot(a => measureExprIds.contains(a.exprId))
  }

  /**
   * Mirrors the cost model used by [[PushPartialAggregationThroughJoin]] so the two
   * push-down rules use a consistent benefit threshold. Returns `false` (skip the
   * rewrite) when the estimated row count of the pre-aggregation is greater than
   * `benefitRatio * input row count`. Falls back to a ratio of `1.0` (no compression)
   * when statistics are unavailable, which means the rule fires only if the configured
   * threshold is also `1.0`.
   *
   * K' is bounded by the minimum of two upper bounds:
   *  1. [[AggregateEstimation]]'s cross-product cap: `min(prod_i distinct(d_i), R)`.
   *     This is exact when grouping columns are independent but saturates at `R`
   *     whenever the product exceeds `R`, which masks compression in correlated
   *     OLAP shapes (q22 groups by `i_product_name / i_brand / i_class / i_category`
   *     - all functionally determined by `item`, so true K' is ~ `i_product_name`'s
   *     distinct count, not the product).
   *  2. An exponential-backoff bound: with distinct counts sorted desc
   *     `d1 >= d2 >= ... >= dk`,
   *         K' <= d1 * d2^(1/2) * d3^(1/4) * ... * dk^(1/2^(k-1)),
   *     also capped at `R`. This dampens secondary dimensions in proportion to their
   *     position, which closely tracks the partially-correlated case typical of
   *     ROLLUP/CUBE/GROUPING SETS dimension hierarchies. It is always <= the
   *     cross-product, so taking the min is a strict improvement.
   *
   * (The bound choice is local to this cost gate; [[AggregateEstimation]] itself is
   * unchanged, so downstream planning estimates are unaffected.)
   */
  private def passesCostGate(expand: Expand, dimensions: Seq[Attribute]): Boolean = {
    val originRowCount = expand.child.stats.rowCount
    // Tentative Aggregate built only to drive AggregateEstimation; never planned.
    // Empty `aggregateExpressions` is safe because AggregateEstimation derives the
    // estimate solely from `groupingExpressions`.
    val aggregatedRowCount = Aggregate(dimensions, Nil, expand.child).stats.rowCount

    val expBackoffK: Option[BigInt] = originRowCount.filter(_ > 0).flatMap { rc =>
      val perDimDc: Seq[BigInt] = dimensions.flatMap { d =>
        expand.child.stats.attributeStats.get(d).flatMap(_.distinctCount)
      }
      if (perDimDc.size != dimensions.size || perDimDc.isEmpty) {
        None
      } else {
        // Compute the geometric damping product by halving the exponent each step,
        // rather than recomputing `math.pow(0.5, i)` for every iteration.
        var product = 1.0
        var exponent = 1.0
        perDimDc.sortBy(-_).foreach { dc =>
          product *= math.pow(dc.toDouble, exponent)
          exponent *= 0.5
        }
        Some(BigInt(math.min(product, rc.toDouble).toLong.max(1L)))
      }
    }

    val effectiveK: Option[BigInt] =
      Seq(aggregatedRowCount, expBackoffK).flatten.reduceOption(_ min _)

    val ratio = if (originRowCount.nonEmpty && effectiveK.nonEmpty && originRowCount.get > 0) {
      effectiveK.get.toDouble / originRowCount.get.toDouble
    } else {
      1.0
    }
    ratio <= conf.partialAggregationOptimizationBenefitRatio
  }

  // ---------------------------------------------------------------------------
  // Rewrite
  // ---------------------------------------------------------------------------

  /**
   * For each top-level [[AggregateExpression]] in `outerAgg`, decompose it into:
   *  - one or more partial-aggregate aliases to add to the lower aggregate's output;
   *  - one replacement expression that the outer aggregate uses in place of the
   *    original. Most functions yield a single alias and a single replacement; `Avg`
   *    yields two aliases (sum + count) and a `Sum / Sum` replacement.
   *
   * For functions whose case classes carry semantics-bearing fields (`Sum.evalContext`,
   * `Sum.resultDataType`, `Average.evalMode`, `First.ignoreNulls`, `Last.ignoreNulls`),
   * those fields are propagated to the outer expression so the rewrite preserves the
   * original ANSI / TRY behavior, output type, and null handling.
   */
  private case class AggMapping(
      original: AggregateExpression,
      partials: Seq[NamedExpression],
      replacement: Expression)

  private def decompose(ae: AggregateExpression): AggMapping = ae.aggregateFunction match {
    case sum: Sum =>
      val partial = Alias(ae, freshName("sum", sum.child))()
      // Preserve evalContext (ANSI / TRY) and the resolved resultDataType so the outer
      // `Sum` keeps the original overflow semantics and output type. Mirrors v6's
      // `PushPartialAggregationThroughJoin.replaceAliasName`.
      val outerFn = Sum(partial.toAttribute, sum.evalContext, Some(sum.dataType))
      AggMapping(ae, Seq(partial), AggregateExpression(outerFn, Complete, isDistinct = false))

    case cnt: Count =>
      val name = if (cnt.children.size == 1) freshName("count", cnt.children.head) else "count_star"
      val partial = Alias(ae, name)()
      val outerFn = Sum(partial.toAttribute, resultDataType = Some(LongType))
      AggMapping(ae, Seq(partial), AggregateExpression(outerFn, Complete, isDistinct = false))

    case Min(child) => selfCompose(ae, "min", child, Min(_))
    case Max(child) => selfCompose(ae, "max", child, Max(_))
    case BitAndAgg(child) => selfCompose(ae, "bit_and", child, BitAndAgg(_))
    case BitOrAgg(child) => selfCompose(ae, "bit_or", child, BitOrAgg(_))
    case BitXorAgg(child) => selfCompose(ae, "bit_xor", child, BitXorAgg(_))

    case first: First =>
      val partial = Alias(ae, freshName("first", first.child))()
      AggMapping(ae, Seq(partial),
        AggregateExpression(First(partial.toAttribute, first.ignoreNulls),
          Complete, isDistinct = false))

    case last: Last =>
      val partial = Alias(ae, freshName("last", last.child))()
      AggMapping(ae, Seq(partial),
        AggregateExpression(Last(partial.toAttribute, last.ignoreNulls),
          Complete, isDistinct = false))

    case avg: Average =>
      // Avg(x) = Sum(x) / Count(x). Pre-aggregate both, then recombine in the outer
      // expression with a Divide cast to Avg's public dataType. Decimal Avg is
      // excluded by `supportedAgg` (matching `Average.evaluateExpression` for
      // Decimal requires `DecimalPrecision` coercion and `CheckOverflowInSum`,
      // which is left as a follow-up). For non-Decimal numeric Avg:
      //   - the inner Sum carries `Average.evalMode` (preserves ANSI / TRY) and
      //     `Average.sumDataType` (DoubleType for non-Decimal numeric);
      //   - the outer Sum-of-counts carries `LongType` so it cleanly merges Counts.
      val evalCtx = NumericEvalContext(avg.evalMode)
      val sumPartial = Alias(
        AggregateExpression(
          Sum(avg.child, evalCtx, Some(avg.sumDataType)),
          Complete, isDistinct = false),
        freshName("avg_sum", avg.child))()
      val countPartial = Alias(
        AggregateExpression(Count(Seq(avg.child)), Complete, isDistinct = false),
        freshName("avg_count", avg.child))()
      val sumOuter = AggregateExpression(
        Sum(sumPartial.toAttribute, evalCtx, Some(avg.sumDataType)),
        Complete, isDistinct = false)
      val countOuter = AggregateExpression(
        Sum(countPartial.toAttribute, resultDataType = Some(LongType)),
        Complete, isDistinct = false)
      val replacement = Divide(
        Cast(sumOuter, avg.dataType), Cast(countOuter, avg.dataType))
      AggMapping(ae, Seq(sumPartial, countPartial), replacement)

    case other =>
      // Should not happen; supportedAgg gates this, but keep the failure mode explicit.
      throw new IllegalStateException(
        s"PushPartialAggregationThroughExpand: unexpected aggregate function $other")
  }

  /**
   * Helper for the trivially self-composing aggregate functions: outer fn = inner fn
   * with the same shape, just substituting the input. Covers `Min`, `Max`,
   * `BitAndAgg`, `BitOrAgg`, `BitXorAgg`. (`First` / `Last` need to also propagate
   * `ignoreNulls`, so they're handled separately.)
   */
  private def selfCompose(
      ae: AggregateExpression,
      name: String,
      child: Expression,
      mkOuter: Attribute => AggregateFunction): AggMapping = {
    val partial = Alias(ae, freshName(name, child))()
    AggMapping(ae, Seq(partial),
      AggregateExpression(mkOuter(partial.toAttribute), Complete, isDistinct = false))
  }

  private def freshName(prefix: String, expr: Expression): String = {
    val refNames = expr.references.map(_.name).mkString("_")
    if (refNames.isEmpty) s"_pushed_$prefix" else s"_pushed_${prefix}_$refNames"
  }

  private def rewrite(
      outerAgg: Aggregate,
      expand: Expand,
      dimensions: Seq[Attribute]): LogicalPlan = {
    val originals = outerAgg.collectAggregateExprs
    val mappings = originals.map(decompose)

    // Lower aggregate: groups on dimensions, projects [dims ++ all partial aliases].
    val partialAliases = mappings.flatMap(_.partials)
    val lower = PartialAggregate(
      groupingExpressions = dimensions,
      aggregateExpressions = dimensions ++ partialAliases,
      child = expand.child)

    // Augmented Expand: drop measure-input output positions, append partial-attr
    // outputs (as pass-through across every projection).
    val measureInputs = AttributeSet(originals.flatMap(_.aggregateFunction.references))
    val partialAttrs = partialAliases.map(_.toAttribute)
    val (newProjections, newOutput) = augmentExpand(expand, measureInputs, partialAttrs)
    val newExpand = expand.copy(
      projections = newProjections,
      output = newOutput,
      child = lower)

    // Outer aggregate: substitute each original AggregateExpression (matched by
    // resultId) with its replacement.
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
      child = newExpand,
      hint = outerAgg.hint)
  }

  /**
   * Build the augmented Expand's projections and output schema:
   *  - drop output positions corresponding to measure-input columns;
   *  - preserve all other positions (grouping passthroughs, newInstance grouping
   *    attrs, the `gid` literal, etc.);
   *  - append `partialAttrs` as new output entries, every projection passing them
   *    through unchanged.
   */
  private def augmentExpand(
      expand: Expand,
      measureInputs: AttributeSet,
      partialAttrs: Seq[Attribute]): (Seq[Seq[Expression]], Seq[Attribute]) = {
    val measureExprIds: Set[ExprId] = measureInputs.map(_.exprId).toSet
    val keepIndex: Seq[Int] = expand.output.zipWithIndex.collect {
      case (a, i) if !measureExprIds.contains(a.exprId) => i
    }
    val newOutput: Seq[Attribute] = keepIndex.map(expand.output) ++ partialAttrs
    val newProjections: Seq[Seq[Expression]] = expand.projections.map { proj =>
      keepIndex.map(proj) ++ partialAttrs
    }
    (newProjections, newOutput)
  }
}
