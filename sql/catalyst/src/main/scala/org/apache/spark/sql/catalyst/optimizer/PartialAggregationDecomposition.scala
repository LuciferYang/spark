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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Divide, EvalMode, Expression, NumericEvalContext}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, BitAndAgg, BitOrAgg, BitXorAgg, Complete, Count, First, Last, Max, Min, Sum}
import org.apache.spark.sql.types.{DecimalType, LongType, NumericType}

/**
 * Shared decomposition helpers for the partial-aggregation push-down rules.
 *
 * Both [[PushPartialAggregationThroughExpand]] and
 * [[PushPartialAggregationThroughUnion]] take an outer aggregate and decompose
 * each [[AggregateExpression]] into (1) one or more partial-aggregate aliases
 * pushed below the multi-output operator, and (2) a replacement expression that
 * the outer aggregate uses in place of the original.
 *
 * Per-function decomposition (partial alias name pattern is `_pushed_<fn>_<refs>`):
 *  - `Sum(x)`     -> outer `Sum(partial)`              inner `Sum(x)`
 *  - `Count(x)`   -> outer `Sum(partial)`              inner `Count(x)`
 *  - `Min(x)`     -> outer `Min(partial)`              inner `Min(x)`
 *  - `Max(x)`     -> outer `Max(partial)`              inner `Max(x)`
 *  - `First(x)`   -> outer `First(partial, ign)`       inner `First(x, ign)`
 *  - `Last(x)`    -> outer `Last(partial, ign)`        inner `Last(x, ign)`
 *  - `BitAndAgg`  -> outer `BitAndAgg(partial)`        inner `BitAndAgg(x)`
 *  - `BitOrAgg`   -> outer `BitOrAgg(partial)`         inner `BitOrAgg(x)`
 *  - `BitXorAgg`  -> outer `BitXorAgg(partial)`        inner `BitXorAgg(x)`
 *  - `Avg(x)`     -> outer `Sum(partial_sum) / Sum(partial_count)`,
 *                   inner produces both `Sum(x)` and `Count(x)`.
 *
 * Decimal `Avg` is intentionally excluded: matching `Average.evaluateExpression`
 * end-to-end for Decimal requires `DecimalPrecision` coercion and
 * `CheckOverflowInSum`, which is left as a follow-up. Non-Decimal numeric `Avg`
 * uses the simple `Sum(x) / Count(x)` decomposition.
 *
 * For functions whose case classes carry semantics-bearing fields
 * (`Sum.evalContext`, `Sum.resultDataType`, `Average.evalMode`,
 * `First.ignoreNulls`, `Last.ignoreNulls`), those fields are propagated to the
 * outer expression so the rewrite preserves the original ANSI / TRY behavior,
 * output type, and null handling.
 */
private[optimizer] object PartialAggregationDecomposition {

  case class AggMapping(
      original: AggregateExpression,
      partials: Seq[Alias],
      replacement: Expression)

  def supportedAgg(ae: AggregateExpression): Boolean = ae match {
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

  /**
   * Decompose an [[AggregateExpression]] into one or more partial aliases plus a
   * replacement expression for the outer aggregate.
   *
   * Callers must gate inputs through [[supportedAgg]] first; the `case other`
   * branch throws [[IllegalStateException]] only as a defensive check against a
   * `supportedAgg` regression and is unreachable in normal operation.
   */
  def decompose(ae: AggregateExpression): AggMapping =
    ae.aggregateFunction match {
      case sum: Sum =>
        val partial = Alias(ae, freshName("sum", sum.child))()
        val outerFn = Sum(partial.toAttribute, sum.evalContext, Some(sum.dataType))
        AggMapping(ae, Seq(partial), AggregateExpression(outerFn, Complete, isDistinct = false))

      case cnt: Count =>
        val name = if (cnt.children.size == 1) {
          freshName("count", cnt.children.head)
        } else {
          "count_star"
        }
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
        // Use `EvalMode.LEGACY` to match `Average.evaluateExpression` (see
        // `Average.scala:121`): the original Avg emits a LEGACY-mode Divide so
        // an empty group (count == 0) yields `null` instead of throwing
        // `DIVIDE_BY_ZERO` under ANSI mode. The decomposition must preserve
        // this null-on-empty contract.
        val replacement = Divide(
          Cast(sumOuter, avg.dataType), Cast(countOuter, avg.dataType), EvalMode.LEGACY)
        AggMapping(ae, Seq(sumPartial, countPartial), replacement)

      case other =>
        // Should not happen; `supportedAgg` gates this. Keep the failure mode
        // explicit so a regression in `supportedAgg` surfaces fast. The thrown
        // exception's stack trace identifies the calling rule.
        throw new IllegalStateException(
          s"PartialAggregationDecomposition: unexpected aggregate function $other " +
            s"(supportedAgg out of sync?)")
    }

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
}
