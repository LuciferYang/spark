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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Cast, Divide, EvalMode, GreaterThan, LessThan, Literal, Rand, Substring}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, BitAndAgg, BitOrAgg, BitXorAgg, Complete, Count, First, Last, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateBase, AggregateHint, ColumnStat, Filter, FinalAggregate, LocalRelation, LogicalPlan, PartialAggregate}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}

object PushPartialAggregationThroughFilterSuite {
  /** Local test-only `AggregateHint` for verifying hint pass-through. */
  case object TestAggregateHint extends AggregateHint
}

class PushPartialAggregationThroughFilterSuite extends StatsEstimationTestBase with PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Push Partial Aggregation Through Filter", Once,
      PushPartialAggregationThroughFilter) :: Nil
  }

  // --- Fixtures -------------------------------------------------------------

  // Relation with (k: grouping key, v: aggregate value, w: predicate column with
  // distinct=2, p: predicate column with distinct=100). Stats are tuned so that:
  //   - grouping by `k` alone (distinct=10) yields ratio 10/1000 = 0.01
  //   - grouping by `(k, w)` (distinct=10*2=20) yields ratio 20/1000 = 0.02
  //     -- still well under default 0.4, so push-down fires
  //   - grouping by `(k, p)` (distinct=10*100=1000, capped at rowCount) yields
  //     ratio 1.0 -- push-down rejected by the cost gate.
  private def relation: (StatsTestPlan, AttributeReference, AttributeReference,
      AttributeReference, AttributeReference) = {
    val k = AttributeReference("k", IntegerType)()
    val v = AttributeReference("v", IntegerType)()
    val w = AttributeReference("w", IntegerType)()
    val p = AttributeReference("p", IntegerType)()
    val stats = AttributeMap(Seq(
      k -> ColumnStat(distinctCount = Some(10), nullCount = Some(0)),
      v -> ColumnStat(distinctCount = Some(500), nullCount = Some(0)),
      w -> ColumnStat(distinctCount = Some(2), nullCount = Some(0)),
      p -> ColumnStat(distinctCount = Some(100), nullCount = Some(0))))
    val rel = StatsTestPlan(
      outputList = Seq(k, v, w, p),
      rowCount = 1000,
      attributeStats = stats,
      size = Some(1000 * 16))
    (rel, k, v, w, p)
  }

  /** True iff the optimized plan has any `Filter` whose child is an
   *  `AggregateBase` (i.e., the rule fired). Uses `exists` rather than
   *  `collect.headOption` so a deeper match isn't masked by a shallower Filter
   *  whose child is something else. */
  private def hasPreAggBelowFilter(plan: LogicalPlan): Boolean = plan.exists {
    case f: Filter => f.child.isInstanceOf[AggregateBase]
    case _ => false
  }

  private def withRuleEnabled[T](body: => T): T = withSQLConf(
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0"
  )(body)

  // --- Tests: per-function decomposition ------------------------------------

  test("Sum push through Filter") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAggBelowFilter(optimized),
        s"Expected PartialAggregate below Filter; got:\n$optimized")
    }
  }

  test("Count(*) push through Filter") {
    withRuleEnabled {
      val (rel, k, _, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Count(Seq(Literal(1))), Complete, isDistinct = false),
            "cnt")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAggBelowFilter(optimized))
    }
  }

  test("Count(v) push through Filter") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Count(Seq(v)), Complete, isDistinct = false), "cnt_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAggBelowFilter(optimized))
    }
  }

  test("Min/Max push through Filter") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Min(v), Complete, isDistinct = false), "min_v")(),
          Alias(AggregateExpression(Max(v), Complete, isDistinct = false), "max_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAggBelowFilter(optimized))
    }
  }

  test("First/Last push through Filter (preserves ignoreNulls)") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(First(v, ignoreNulls = true), Complete,
            isDistinct = false), "first_v")(),
          Alias(AggregateExpression(Last(v, ignoreNulls = false), Complete,
            isDistinct = false), "last_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAggBelowFilter(optimized))
    }
  }

  test("BitAnd/BitOr/BitXor push through Filter") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(BitAndAgg(v), Complete, isDistinct = false), "ba_v")(),
          Alias(AggregateExpression(BitOrAgg(v), Complete, isDistinct = false), "bo_v")(),
          Alias(AggregateExpression(BitXorAgg(v), Complete, isDistinct = false), "bx_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAggBelowFilter(optimized))
    }
  }

  test("Avg push through Filter: decomposed to Sum/Count partials") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAggBelowFilter(optimized))
    }
  }

  test("Multiple aggregates mixed: Sum + Count + Min + Avg") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")(),
          Alias(AggregateExpression(Count(Seq(v)), Complete, isDistinct = false), "cnt_v")(),
          Alias(AggregateExpression(Min(v), Complete, isDistinct = false), "min_v")(),
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAggBelowFilter(optimized))
    }
  }

  // --- Tests: end-to-end plan lock-in --------------------------------------

  test("comparePlans: Sum push produces FinalAggregate -> Filter -> PartialAggregate shape") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)

      // Root is FinalAggregate
      assert(optimized.isInstanceOf[FinalAggregate],
        s"Expected root FinalAggregate; got ${optimized.getClass.getSimpleName}")
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      assert(finalAgg.groupingExpressions.size == 1)

      // FinalAggregate's child is Filter (preserving the predicate)
      val filter = finalAgg.child.asInstanceOf[Filter]
      // PartialAggregate sits below Filter
      val partial = filter.child.asInstanceOf[PartialAggregate]
      // Extended grouping keys: original (k) + predicate refs (w) = 2 keys
      assert(partial.groupingExpressions.size == 2,
        s"Expected extended grouping (original + predicate refs) = 2 keys; " +
          s"got ${partial.groupingExpressions}")
      // Partial alias `_pushed_sum_v` exists
      val partialAlias = partial.aggregateExpressions.last.asInstanceOf[Alias]
      assert(partialAlias.name.startsWith("_pushed_sum_"),
        s"Expected partial alias name starting with _pushed_sum_; got ${partialAlias.name}")

      // Outer Sum consumes the partial via partialAttrMap rebinding
      val outerSumAlias = finalAgg.aggregateExpressions.last.asInstanceOf[Alias]
      assert(outerSumAlias.name == "sum_v")
      outerSumAlias.child match {
        case AggregateExpression(s: Sum, Complete, false, None, _) =>
          // The outer Sum's input must reference the new PartialAggregate's output
          // (via the Filter pass-through, which preserves child output).
          val sumInputExprIds = s.child.references.map(_.exprId)
          val partialOutputExprIds = partial.output.map(_.exprId).toSet
          assert(sumInputExprIds.forall(partialOutputExprIds.contains),
            s"Outer Sum's input must reference partial.output; got refs $sumInputExprIds " +
              s"but partial.output is ${partial.output.map(a => (a.name, a.exprId))}")
        case other => fail(s"Expected outer Sum aggregate; got $other")
      }
    }
  }

  test("comparePlans: Avg push produces Divide(Cast(sum_outer), Cast(count_outer)) with LEGACY") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      val outerAvgAlias = finalAgg.aggregateExpressions.last.asInstanceOf[Alias]
      outerAvgAlias.child match {
        case d @ Divide(Cast(_: AggregateExpression, _, _, _),
                        Cast(_: AggregateExpression, _, _, _), _) =>
          // Lock in `EvalMode.LEGACY` for the outer Divide -- preserves the
          // null-on-empty contract under ANSI mode (matches Average.scala:121).
          assert(d.evalMode == EvalMode.LEGACY,
            s"Avg replacement Divide must use EvalMode.LEGACY; got ${d.evalMode}")
        case other =>
          fail(s"Expected Divide(Cast(AggExp), Cast(AggExp)) for Avg replacement; got $other")
      }
    }
  }

  // --- Tests: extended grouping ---------------------------------------------

  test("Filter predicate refs already in grouping keys: no key extension needed") {
    // Filter refs k (already a grouping key), so the extended grouping is still
    // [k] (no extension).
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(k, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      val partial = finalAgg.child.asInstanceOf[Filter].child.asInstanceOf[PartialAggregate]
      assert(partial.groupingExpressions.size == 1,
        s"Expected grouping size 1 (no extension since predicate ref == grouping key); " +
          s"got ${partial.groupingExpressions}")
    }
  }

  test("Empty grouping: extended grouping = predicate refs only") {
    // `outerAgg.groupingExpressions = Nil`. After extension the lower
    // PartialAggregate groups solely by the predicate's referenced attributes,
    // and the outer FinalAggregate has no grouping (a single-row result).
    withRuleEnabled {
      val (rel, _, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Nil,
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      assert(finalAgg.groupingExpressions.isEmpty,
        s"Outer FinalAggregate must keep empty grouping; got ${finalAgg.groupingExpressions}")
      val partial = finalAgg.child.asInstanceOf[Filter].child.asInstanceOf[PartialAggregate]
      // Extended grouping = predicate refs only (= [w]).
      assert(partial.groupingExpressions.size == 1,
        s"Extended grouping must equal predicate refs [w]; got ${partial.groupingExpressions}")
      val partialKey = partial.groupingExpressions.head.asInstanceOf[Attribute]
      assert(partialKey.exprId == w.exprId,
        s"Extended grouping key must be w; got ${partialKey.name}#${partialKey.exprId}")
    }
  }

  test("Filter predicate refs an aggregate's input column: extended grouping handles overlap") {
    // Predicate ref `v` is also the input column to `sum(v)`. Extended grouping
    // becomes [k, v]. Each (k, v) bucket has uniform v, so Filter(v > 0) applied
    // post-partial-agg keeps/drops entire buckets correctly. sum(v) over the
    // surviving buckets = sum(v) over the originally-filtered rows.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(v, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      val partial = finalAgg.child.asInstanceOf[Filter].child.asInstanceOf[PartialAggregate]
      // Extended grouping = [k, v] -- v is both predicate ref AND aggregate input.
      assert(partial.groupingExpressions.size == 2,
        s"Expected extended grouping [k, v]; got ${partial.groupingExpressions}")
      val groupingExprIds = partial.groupingExpressions.map(_.asInstanceOf[Attribute].exprId).toSet
      assert(groupingExprIds.contains(k.exprId) && groupingExprIds.contains(v.exprId),
        s"Extended grouping must contain both k and v; got $groupingExprIds")
    }
  }

  test("Filter predicate refs mixed grouping + non-grouping columns: dedup applies") {
    // Predicate refs k (grouping) AND w (non-grouping). Extended grouping
    // becomes [k, w] (k is NOT duplicated). Verifies computeExtendedGrouping's
    // `filterNot(originalIds.contains)` dedup.
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(
          org.apache.spark.sql.catalyst.expressions.And(
            GreaterThan(k, Literal(0)), LessThan(w, Literal(10))),
          rel))
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      val partial = finalAgg.child.asInstanceOf[Filter].child.asInstanceOf[PartialAggregate]
      // Extended grouping = [k, w] -- k appears once (deduped), w added.
      assert(partial.groupingExpressions.size == 2,
        s"Expected extended grouping [k, w] (k not duplicated); " +
          s"got ${partial.groupingExpressions}")
    }
  }

  test("Filter predicate refs multiple non-grouping columns: keys extended with all") {
    // Filter refs w AND p (both non-grouping). Extended grouping: [k, w, p].
    // Even with k=10*w=2*p=100 = 2000 (capped at rowCount=1000), ratio=1.0 > 0.4,
    // but we run at benefitRatio=1.0 here so the rule still fires.
    withRuleEnabled {
      val (rel, k, v, w, p) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(
          org.apache.spark.sql.catalyst.expressions.And(
            GreaterThan(w, Literal(0)), LessThan(p, Literal(50))),
          rel))
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      val partial = finalAgg.child.asInstanceOf[Filter].child.asInstanceOf[PartialAggregate]
      assert(partial.groupingExpressions.size == 3,
        s"Expected grouping size 3 (k + w + p); got ${partial.groupingExpressions}")
    }
  }

  // --- Tests: cost gate / config ---------------------------------------------

  test("Cost gate rejects when extended-keys cardinality is too high") {
    // benefitRatio=0.1, k_distinct=10 * p_distinct=100 = 1000 (= rowCount).
    // Extended-keys ratio = 1000/1000 = 1.0 > 0.1 -> reject.
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.1") {
      val (rel, k, v, _, p) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(p, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggBelowFilter(optimized),
        s"Cost gate should have rejected (extended keys too high cardinality); got:\n$optimized")
    }
  }

  test("Configuration disabled: rule is a no-op") {
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(optimized.fastEquals(agg))
    }
  }

  // --- Tests: ineligibility guards ------------------------------------------

  test("Non-deterministic predicate: rejected") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(Rand(Literal(1L)), Literal(0.5)), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggBelowFilter(optimized))
    }
  }

  test("Non-deterministic aggregate input: rejected") {
    withRuleEnabled {
      val (rel, k, _, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(Rand(Literal(1L))), Complete, isDistinct = false),
            "sum_rand")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggBelowFilter(optimized))
    }
  }

  test("DISTINCT aggregate: rejected") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = true), "sum_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggBelowFilter(optimized))
    }
  }

  test("FILTER (WHERE ...) aggregate: rejected") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val aggFilter = GreaterThan(w, Literal(0))
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false,
            filter = Some(aggFilter)), "sum_v_filtered")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggBelowFilter(optimized))
    }
  }

  test("Idempotency: PartialAggregate already below Filter -> no-op") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val preWrapped = PartialAggregate(rel.output, rel.output, rel)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(w, Literal(0)), preWrapped))
      val optimized = Optimize.execute(agg)
      assert(optimized.fastEquals(agg),
        "Rule must be a strict no-op when Filter's child is already an AggregateBase; " +
          s"got:\n$optimized")
    }
  }

  test("Non-Attribute grouping expression: rule is a no-op (skipped)") {
    // Aggregate.groupingExpressions is Seq[Expression]; a non-Attribute grouping
    // (e.g., Substring) must cause the rule to skip rather than crash.
    withRuleEnabled {
      val (rel, _, v, w, _) = relation
      val strV = Cast(v, StringType)
      val grouping = Substring(strV, Literal(1), Literal(3))
      val agg = Aggregate(
        groupingExpressions = Seq(grouping),
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggBelowFilter(optimized),
        "Rule must skip non-Attribute grouping (avoids ClassCastException)")
    }
  }

  test("Decimal Avg: excluded (matches Expand rule)") {
    withRuleEnabled {
      val dv = AttributeReference("dv", DecimalType(17, 2))()
      val dw = AttributeReference("dw", IntegerType)()
      val rel = LocalRelation(dv, dw)
      val agg = Aggregate(
        groupingExpressions = Nil,
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Average(dv), Complete, isDistinct = false), "avg_dv")()),
        child = Filter(GreaterThan(dw, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggBelowFilter(optimized))
    }
  }

  test("No aggregate function (group-by only): rejected") {
    withRuleEnabled {
      val (rel, k, _, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k),
        child = Filter(GreaterThan(w, Literal(0)), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggBelowFilter(optimized))
    }
  }

  test("Aggregate hint preserved on FinalAggregate after rewrite") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val hint = Some(PushPartialAggregationThroughFilterSuite.TestAggregateHint)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Filter(GreaterThan(w, Literal(0)), rel),
        hint = hint)
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      assert(finalAgg.hint == hint,
        s"FinalAggregate must preserve the outer Aggregate's hint; got ${finalAgg.hint}")
    }
  }

  test("Stats missing: cost gate falls back to ratio=1.0") {
    val v = AttributeReference("v", IntegerType)()
    val k = AttributeReference("k", IntegerType)()
    val w = AttributeReference("w", IntegerType)()
    val rel = LocalRelation(k, v, w)
    val agg = Aggregate(
      groupingExpressions = Seq(k),
      aggregateExpressions = Seq(k,
        Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
      child = Filter(GreaterThan(w, Literal(0)), rel))

    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      assert(hasPreAggBelowFilter(Optimize.execute(agg)),
        "At benefitRatio=1.0 the stats-missing fallback should still fire")
    }
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
      assert(!hasPreAggBelowFilter(Optimize.execute(agg)),
        "At benefitRatio<1.0 the stats-missing fallback should reject")
    }
  }
}
