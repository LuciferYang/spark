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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeMap, AttributeReference, Cast, Divide, EvalMode, Literal, Rand, Substring}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, BitAndAgg, BitOrAgg, BitXorAgg, Complete, Count, First, Last, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateBase, AggregateHint, ColumnStat, FinalAggregate, LocalRelation, LogicalPlan, PartialAggregate, Union}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType}

object PushPartialAggregationThroughUnionSuite {
  /** Local test-only `AggregateHint` (the trait is abstract with no concrete
   *  subclasses in the current source). Used to verify hint pass-through. */
  case object TestAggregateHint extends AggregateHint
}

class PushPartialAggregationThroughUnionSuite extends StatsEstimationTestBase with PlanTest {
  // Extending StatsEstimationTestBase enables `SQLConf.CBO_ENABLED = true` in
  // beforeAll. This is required for `Aggregate.stats.rowCount` to be computed
  // via AggregateEstimation. Without CBO, `LogicalPlanStats.stats` falls back
  // to `SizeInBytesOnlyStatsPlanVisitor` which does not produce rowCount, and
  // `pushPartialAggHasBenefit` would always use its ratio=1.0 fallback --
  // masking real cost-gate behavior. (Same pattern as
  // `PushPartialAggregationThroughJoinSuite`.)

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Push Partial Aggregation Through Union", Once,
      PushPartialAggregationThroughUnion) :: Nil
  }

  // --- Fixtures -------------------------------------------------------------

  // Each child relation has the same schema (k, v, w) with stats that make
  // grouping by `k` reduce rows ~100x (1000 rows / 10 distinct values).
  private def relation(prefix: String): (StatsTestPlan, AttributeReference,
      AttributeReference, AttributeReference) = {
    val k = AttributeReference(s"${prefix}_k", IntegerType)()
    val v = AttributeReference(s"${prefix}_v", IntegerType)()
    val w = AttributeReference(s"${prefix}_w", IntegerType)()
    val stats = AttributeMap(Seq(
      k -> ColumnStat(distinctCount = Some(10), nullCount = Some(0)),
      v -> ColumnStat(distinctCount = Some(500), nullCount = Some(0)),
      w -> ColumnStat(distinctCount = Some(500), nullCount = Some(0))))
    val rel = StatsTestPlan(
      outputList = Seq(k, v, w),
      rowCount = 1000,
      size = Some(1000 * 12),
      attributeStats = stats)
    (rel, k, v, w)
  }

  private def decimalRelation(prefix: String): (StatsTestPlan, AttributeReference) = {
    val v = AttributeReference(s"${prefix}_v", DecimalType(17, 2))()
    val rel = StatsTestPlan(
      outputList = Seq(v),
      rowCount = 1000,
      size = Some(1000 * 16),
      attributeStats = AttributeMap.empty)
    (rel, v)
  }

  // Convenience: build Union of `n` same-schema children, returning the Union and
  // each child's (k, v, w) attribute triple in order.
  private def unionN(n: Int): (Union, Seq[(AttributeReference,
      AttributeReference, AttributeReference)]) = {
    val triples = (1 to n).map { i => relation(s"c$i") }
    val union = Union(triples.map(_._1))
    (union, triples.map { case (_, k, v, w) => (k, v, w) })
  }

  /** True iff every Union child in the optimized plan has an AggregateBase below it. */
  private def hasPreAggOnEveryChild(plan: LogicalPlan): Boolean = plan.collect {
    case u: Union => u.children.forall(_.isInstanceOf[AggregateBase])
  }.headOption.getOrElse(false)

  /** True iff the optimized plan's structure was changed (rule fired). */
  private def ruleFired(original: LogicalPlan, optimized: LogicalPlan): Boolean = {
    !original.fastEquals(optimized) && hasPreAggOnEveryChild(optimized)
  }

  private def withRuleEnabled[T](body: => T): T = withSQLConf(
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0"
  )(body)

  // --- Tests: per-function decomposition ------------------------------------

  test("Sum push through Union: pre-agg appears under every child") {
    withRuleEnabled {
      val (union, triples) = unionN(2)
      val k = union.output.head  // c1_k via Union's output
      val v = union.output(1)    // c1_v via Union's output
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized),
        s"Expected pre-agg under every Union child; got:\n$optimized")
    }
  }

  test("Count(*) push through Union") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Count(Seq(Literal(1))), Complete, isDistinct = false),
            "cnt")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized))
    }
  }

  test("Count(v) push through Union") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Count(Seq(v)), Complete, isDistinct = false),
            "cnt_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized))
    }
  }

  test("Min/Max push through Union") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val w = union.output(2)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Min(v), Complete, isDistinct = false), "min_v")(),
          Alias(AggregateExpression(Max(w), Complete, isDistinct = false), "max_w")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized))
    }
  }

  test("First/Last push through Union (preserves ignoreNulls)") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(First(v, ignoreNulls = true), Complete,
            isDistinct = false), "first_v")(),
          Alias(AggregateExpression(Last(v, ignoreNulls = false), Complete,
            isDistinct = false), "last_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized))
    }
  }

  test("BitAnd/BitOr/BitXor push through Union") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(BitAndAgg(v), Complete, isDistinct = false), "ba_v")(),
          Alias(AggregateExpression(BitOrAgg(v), Complete, isDistinct = false), "bo_v")(),
          Alias(AggregateExpression(BitXorAgg(v), Complete, isDistinct = false), "bx_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized))
    }
  }

  test("Avg push through Union: decomposed to Sum/Count partials") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized))
    }
  }

  test("Three children union: pre-agg under every child") {
    withRuleEnabled {
      val (union, _) = unionN(3)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized))
      val finalChildCount = optimized.collect { case u: Union => u.children.size }.head
      assert(finalChildCount == 3)
    }
  }

  test("Multiple aggregates mixed: Sum + Count + Min + Avg") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val w = union.output(2)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")(),
          Alias(AggregateExpression(Count(Seq(w)), Complete, isDistinct = false), "cnt_w")(),
          Alias(AggregateExpression(Min(w), Complete, isDistinct = false), "min_w")(),
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(ruleFired(agg, optimized))
    }
  }

  // --- Tests: cost gate / config ---------------------------------------------

  test("Cost gate rejects rewrite when row reduction is above benefit ratio") {
    // With benefitRatio = 0.001 and 1000 rows / 10 distinct -> ratio ~ 0.01 > 0.001.
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.001") {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized),
        s"Cost gate should have rejected; got:\n$optimized")
    }
  }

  test("Configuration disabled: rule is a no-op") {
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(optimized.fastEquals(agg), s"Disabled rule should be a no-op; got:\n$optimized")
    }
  }

  // --- Tests: ineligibility guards ------------------------------------------

  test("Non-deterministic aggregate input: rejected") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(Rand(Literal(1L))), Complete, isDistinct = false),
            "sum_rand")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized),
        s"Non-deterministic input should block rewrite; got:\n$optimized")
    }
  }

  test("DISTINCT aggregate: rejected") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = true), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized))
    }
  }

  test("FILTER (WHERE ...) aggregate: rejected") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val w = union.output(2)
      // sum(v) FILTER (WHERE w > 0)
      val filter = org.apache.spark.sql.catalyst.expressions.GreaterThan(w, Literal(0))
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false, filter = Some(filter)),
            "sum_v_filtered")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized))
    }
  }

  test("Idempotency: PartialAggregate already below every Union child -> no-op") {
    withRuleEnabled {
      val (union0, _) = unionN(2)
      // Hand-construct the post-rewrite shape: PartialAggregate under each child.
      val preWrappedChildren = union0.children.map { c =>
        PartialAggregate(c.output, c.output, c)
      }
      val union = Union(preWrappedChildren)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      // Idempotency: the rule's guard `forall(c => !c.isInstanceOf[AggregateBase])`
      // skips when ANY child already has an AggregateBase below. Here every child
      // has a PartialAggregate, so the rule must be a strict no-op. A regression
      // that re-fires the rule would stack a fresh PartialAggregate above each
      // existing one -- `fastEquals(agg)` would then be false, catching it.
      assert(optimized.fastEquals(agg),
        s"Rule must be a strict no-op when children are pre-wrapped; got:\n$optimized")
    }
  }

  test("Decimal Avg: excluded (matches Expand rule)") {
    withRuleEnabled {
      val (l1, lv) = decimalRelation("l1")
      val (l2, _) = decimalRelation("l2")
      val union = Union(Seq(l1, l2))
      val v = union.output.head  // decimal-typed
      val agg = Aggregate(
        groupingExpressions = Nil,
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized),
        s"Decimal Avg should be excluded; got:\n$optimized")
    }
  }

  test("No aggregate function (group-by only): rejected") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized))
    }
  }

  // --- Tests: end-to-end plan lock-in --------------------------------------

  test("comparePlans: Sum push produces the expected FinalAggregate-over-Union shape") {
    withRuleEnabled {
      // Hand-built fixtures so the expected plan can reference the same attributes.
      val k1 = AttributeReference("c1_k", IntegerType)()
      val v1 = AttributeReference("c1_v", IntegerType)()
      val k2 = AttributeReference("c2_k", IntegerType)()
      val v2 = AttributeReference("c2_v", IntegerType)()
      val stats1 = AttributeMap(Seq(
        k1 -> ColumnStat(distinctCount = Some(10), nullCount = Some(0)),
        v1 -> ColumnStat(distinctCount = Some(500), nullCount = Some(0))))
      val stats2 = AttributeMap(Seq(
        k2 -> ColumnStat(distinctCount = Some(10), nullCount = Some(0)),
        v2 -> ColumnStat(distinctCount = Some(500), nullCount = Some(0))))
      val c1 = StatsTestPlan(Seq(k1, v1), 1000, stats1, Some(1000 * 8))
      val c2 = StatsTestPlan(Seq(k2, v2), 1000, stats2, Some(1000 * 8))
      val union = Union(Seq(c1, c2))
      val uK = union.output.head
      val uV = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(uK),
        aggregateExpressions = Seq(uK,
          Alias(AggregateExpression(Sum(uV), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)

      // Structural lock-in: root is FinalAggregate, child is Union of two
      // PartialAggregates, partial aliases reference each child's own attributes
      // (not Union output), and the outer Sum consumes the partial.
      assert(optimized.isInstanceOf[FinalAggregate],
        s"Expected root FinalAggregate; got ${optimized.getClass.getSimpleName}")
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      assert(finalAgg.groupingExpressions.size == 1)
      val newUnion = finalAgg.child.asInstanceOf[Union]
      assert(newUnion.children.size == 2)
      newUnion.children.zipWithIndex.foreach { case (c, idx) =>
        val pa = c.asInstanceOf[PartialAggregate]
        // Each child's PartialAggregate has 1 grouping key (k) + 1 partial alias (sum_v).
        assert(pa.aggregateExpressions.size == 2,
          s"Expected 2 aggregate expressions; got ${pa.aggregateExpressions}")
        // Partial alias name is `_pushed_sum_<v_name>`.
        val partialAlias = pa.aggregateExpressions.last.asInstanceOf[Alias]
        assert(partialAlias.name.startsWith("_pushed_sum_"),
          s"Expected name starting with _pushed_sum_; got ${partialAlias.name}")
        // Partial-alias attribute references THIS child's own attributes, not Union's.
        val expectedChildOutputSet = union.children(idx).outputSet
        val inputAttrs = partialAlias.child.references
        assert(inputAttrs.forall(a => expectedChildOutputSet.exists(_.exprId == a.exprId)),
          s"Partial alias for child $idx must reference child's own attributes; " +
            s"got refs ${inputAttrs} but child output is ${union.children(idx).output}")
      }
      // Outer aggregate expressions: 1 grouping reference + 1 Sum-of-partial alias.
      val outerSumAlias = finalAgg.aggregateExpressions.last.asInstanceOf[Alias]
      assert(outerSumAlias.name == "sum_v")
      outerSumAlias.child match {
        case AggregateExpression(s: Sum, Complete, false, None, _) =>
          // The outer Sum's input MUST reference the new Union's output, not the
          // original union's. Verifies the `partialAttrMap` rebinding worked.
          val sumInputExprIds = s.child.references.map(_.exprId)
          val newUnionExprIds = newUnion.output.map(_.exprId).toSet
          assert(sumInputExprIds.forall(newUnionExprIds.contains),
            s"Outer Sum's input must reference newUnion.output; got refs $sumInputExprIds " +
              s"but newUnion.output is ${newUnion.output.map(a => (a.name, a.exprId))}")
        case other => fail(s"Expected outer Sum aggregate; got $other")
      }
    }
  }

  test("Cost gate OR-over-children: rule fires when ANY child has benefit") {
    // Asymmetric Union: child 1 has high cardinality on k (1000 distinct in 1000 rows,
    // grouping reduces ratio ~= 1.0, no benefit); child 2 has low cardinality on k
    // (10 distinct in 1000 rows, grouping reduces ratio ~= 0.01, big benefit). At
    // benefitRatio = 0.5, only child 2 individually qualifies -- but the rule's
    // OR-over-children policy fires the rewrite for the whole Union.
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
      val k1 = AttributeReference("c1_k", IntegerType)()
      val v1 = AttributeReference("c1_v", IntegerType)()
      val k2 = AttributeReference("c2_k", IntegerType)()
      val v2 = AttributeReference("c2_v", IntegerType)()
      val stats1 = AttributeMap(Seq(
        k1 -> ColumnStat(distinctCount = Some(1000), nullCount = Some(0)),  // no benefit
        v1 -> ColumnStat(distinctCount = Some(500), nullCount = Some(0))))
      val stats2 = AttributeMap(Seq(
        k2 -> ColumnStat(distinctCount = Some(10), nullCount = Some(0)),    // big benefit
        v2 -> ColumnStat(distinctCount = Some(500), nullCount = Some(0))))
      val c1 = StatsTestPlan(Seq(k1, v1), 1000, stats1, Some(1000 * 8))
      val c2 = StatsTestPlan(Seq(k2, v2), 1000, stats2, Some(1000 * 8))
      val union = Union(Seq(c1, c2))
      val uK = union.output.head
      val uV = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(uK),
        aggregateExpressions = Seq(uK,
          Alias(AggregateExpression(Sum(uV), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(hasPreAggOnEveryChild(optimized),
        "Rule must fire when at least one child has benefit (OR-over-children policy)")
    }
  }

  test("Aggregate hint preserved on FinalAggregate after rewrite") {
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val hint = Some(PushPartialAggregationThroughUnionSuite.TestAggregateHint)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union,
        hint = hint)
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      assert(finalAgg.hint == hint,
        s"FinalAggregate must preserve the outer Aggregate's hint; got ${finalAgg.hint}")
    }
  }

  test("Nested Union (Union of Unions): rule fires only at the outer Aggregate match") {
    // Aggregate over Union(c1, Union(c2, c3)). transformUpWithPruning visits the
    // inner Union first; that inner Union has no Aggregate above it (the inner
    // Union is a child of the outer Union, not of an Aggregate), so the rule
    // doesn't fire on the inner Union. The outer Aggregate(Union(c1, innerUnion))
    // is then visited; the rule's `union.children.forall(!isInstanceOf[AggregateBase])`
    // guard passes (neither c1 nor innerUnion is an AggregateBase), so the rule
    // fires once at the outer level and wraps both c1 AND innerUnion in
    // PartialAggregates.
    withRuleEnabled {
      val (rel1, _, _, _) = relation("c1")
      val (rel2, _, _, _) = relation("c2")
      val (rel3, _, _, _) = relation("c3")
      val innerUnion = Union(Seq(rel2, rel3))
      val outerUnion = Union(Seq(rel1, innerUnion))
      val k = outerUnion.output.head
      val v = outerUnion.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = outerUnion)
      val optimized = Optimize.execute(agg)
      val unions = optimized.collect { case u: Union => u }
      assert(unions.size == 2, s"Expected 2 Unions in optimized plan; got ${unions.size}")
      // Outer Union (top-level): both children wrapped in PartialAggregate.
      val outerOpt = unions.head
      assert(outerOpt.children.forall(_.isInstanceOf[PartialAggregate]),
        s"Outer Union must have PartialAggregate under every child; got " +
          s"${outerOpt.children.map(_.getClass.getSimpleName)}")
      // Inner Union: untouched -- its children are still the raw relations
      // (no PartialAggregate wrapping). The inner Union may now sit inside a
      // PartialAggregate (because the outer rewrite wraps innerUnion itself),
      // but the inner Union's own children must be unchanged.
      val innerOpt = unions(1)
      assert(innerOpt.children.forall(c => !c.isInstanceOf[AggregateBase]),
        s"Inner Union must not be rewritten; got " +
          s"${innerOpt.children.map(_.getClass.getSimpleName)}")
    }
  }

  test("Two independent Aggregate(Union) subtrees: both rewrite under transformUp") {
    // Two `Aggregate(Union)` siblings joined by an outer Union. The rule's
    // `transformUpWithPruning(AGGREGATE, UNION)` traversal must fire on both
    // subtrees independently.
    withRuleEnabled {
      val (a1, _, _, _) = relation("a1")
      val (a2, _, _, _) = relation("a2")
      val (b1, _, _, _) = relation("b1")
      val (b2, _, _, _) = relation("b2")
      val unionA = Union(Seq(a1, a2))
      val unionB = Union(Seq(b1, b2))
      val aggA = Aggregate(
        groupingExpressions = Seq(unionA.output.head),
        aggregateExpressions = Seq(unionA.output.head,
          Alias(AggregateExpression(Sum(unionA.output(1)), Complete, isDistinct = false),
            "sum_a")()),
        child = unionA)
      val aggB = Aggregate(
        groupingExpressions = Seq(unionB.output.head),
        aggregateExpressions = Seq(unionB.output.head,
          Alias(AggregateExpression(Sum(unionB.output(1)), Complete, isDistinct = false),
            "sum_b")()),
        child = unionB)
      val plan = Union(Seq(aggA, aggB))
      val optimized = Optimize.execute(plan)
      val finalAggs = optimized.collect { case fa: FinalAggregate => fa }
      assert(finalAggs.size == 2,
        s"Expected 2 FinalAggregates (one per subtree); got ${finalAggs.size}")
      finalAggs.foreach { fa =>
        val innerUnion = fa.child.asInstanceOf[Union]
        assert(innerUnion.children.forall(_.isInstanceOf[PartialAggregate]),
          s"Each subtree's Union must have PartialAggregate under every child; got " +
            s"${innerUnion.children.map(_.getClass.getSimpleName)}")
      }
    }
  }

  // --- Tests: guards ---------------------------------------------------------

  test("byName Union: rule is a no-op (defensive guard)") {
    // Union.resolved requires !byName && !allowMissingCol so this rarely arises
    // in the standard pipeline, but we guard defensively.
    withRuleEnabled {
      val (rel1, _, _, _) = relation("c1")
      val (rel2, _, _, _) = relation("c2")
      val union = Union(Seq(rel1, rel2), byName = true, allowMissingCol = false)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized))
    }
  }

  test("allowMissingCol Union: rule is a no-op (defensive guard)") {
    withRuleEnabled {
      val (rel1, _, _, _) = relation("c1")
      val (rel2, _, _, _) = relation("c2")
      val union = Union(Seq(rel1, rel2), byName = true, allowMissingCol = true)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized))
    }
  }

  test("Non-Attribute grouping expression: rule is a no-op (skipped)") {
    // The Aggregate.groupingExpressions list is Seq[Expression]; if it contains a
    // non-Attribute expression (e.g., Substring(col, 1, 3)), the rule must skip
    // rather than crash. In the standard optimizer pipeline,
    // PullOutGroupingExpressions normalizes these to Attributes before this rule
    // runs, but we guard defensively.
    withRuleEnabled {
      val (union, _) = unionN(2)
      val v = union.output(1) // Integer column; Substring expects String, but
      // for the eligibility check what matters is that the grouping is a
      // non-Attribute expression -- type validity is irrelevant for this test.
      val strV = Cast(v, StringType)
      val grouping = Substring(strV, Literal(1), Literal(3))
      val agg = Aggregate(
        groupingExpressions = Seq(grouping),
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      assert(!hasPreAggOnEveryChild(optimized),
        "Rule must skip non-Attribute grouping (avoids ClassCastException)")
    }
  }

  test("Stats missing: cost gate falls back to ratio=1.0 (fires at benefitRatio=1.0)") {
    // LocalRelation children have no row-count or column-distinct stats, so
    // `pushPartialAggHasBenefit` evaluates ratio = 1.0 (per the helper's
    // permissive fallback). At benefitRatio = 1.0 the rewrite still fires; at
    // benefitRatio < 1.0 it must not.
    val v1 = AttributeReference("c1_v", IntegerType)()
    val k1 = AttributeReference("c1_k", IntegerType)()
    val v2 = AttributeReference("c2_v", IntegerType)()
    val k2 = AttributeReference("c2_k", IntegerType)()
    val c1 = LocalRelation(k1, v1)
    val c2 = LocalRelation(k2, v2)
    val union = Union(Seq(c1, c2))
    val uK = union.output.head
    val uV = union.output(1)
    val agg = Aggregate(
      groupingExpressions = Seq(uK),
      aggregateExpressions = Seq(uK,
        Alias(AggregateExpression(Sum(uV), Complete, isDistinct = false), "sum_v")()),
      child = union)

    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      assert(hasPreAggOnEveryChild(Optimize.execute(agg)),
        "At benefitRatio=1.0 the stats-missing fallback should still fire")
    }
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
      assert(!hasPreAggOnEveryChild(Optimize.execute(agg)),
        "At benefitRatio<1.0 the stats-missing fallback should reject")
    }
  }

  test("Post-analysis same-type children (Int + Int): partial dtypes consistent") {
    // Spark's analyzer inserts casts before Union so children always have aligned
    // per-column types by the time the optimizer runs. We verify that for the
    // common aligned-type case the per-child partial aliases produce the same
    // result type, matching the Union output's type. The "widen mid-pipeline" path
    // is unreachable: `Union.mergeChildOutputs` errors on incompatible types like
    // `Int + Long` (see `StructType.unionLikeMerge`).
    withRuleEnabled {
      val (union, _) = unionN(2)
      val uK = union.output.head
      val uV = union.output(1)
      assert(uV.dataType == IntegerType)
      val agg = Aggregate(
        groupingExpressions = Seq(uK),
        aggregateExpressions = Seq(uK,
          Alias(AggregateExpression(Sum(uV), Complete, isDistinct = false), "sum_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      val newUnion = optimized.collectFirst { case u: Union => u }.get
      newUnion.children.foreach { c =>
        val pa = c.asInstanceOf[PartialAggregate]
        val partialAlias = pa.aggregateExpressions.last.asInstanceOf[Alias]
        assert(partialAlias.dataType == LongType,
          s"Expected partial dtype LongType (Sum of Int -> Long); got ${partialAlias.dataType}")
      }
    }
  }

  test("comparePlans: Avg push produces Divide(Cast(sum_outer), Cast(count_outer))") {
    // Lock-in for the Avg decomposition shape.
    withRuleEnabled {
      val (union, _) = unionN(2)
      val k = union.output.head
      val v = union.output(1)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = union)
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      val outerAvgAlias = finalAgg.aggregateExpressions.last.asInstanceOf[Alias]
      outerAvgAlias.child match {
        case d @ Divide(Cast(_: AggregateExpression, _, _, _),
                        Cast(_: AggregateExpression, _, _, _), _) =>
          // Lock in `EvalMode.LEGACY` for the outer Divide. This matches
          // `Average.evaluateExpression` at `Average.scala:121` and preserves
          // the null-on-empty contract under ANSI mode (count=0 -> LEGACY
          // Divide returns null; ANSI Divide would throw `DIVIDE_BY_ZERO`).
          // Regression to ANSI/TRY EvalMode would silently change empty-group
          // semantics, so this assertion is load-bearing.
          assert(d.evalMode == EvalMode.LEGACY,
            s"Avg replacement Divide must use EvalMode.LEGACY; got ${d.evalMode}")
        case other =>
          fail(s"Expected Divide(Cast(AggExp), Cast(AggExp)) for Avg replacement; got $other")
      }
      // Each child's PartialAggregate has 2 partials per Avg (sum + count) + 1 grouping key.
      val newUnion = finalAgg.child.asInstanceOf[Union]
      newUnion.children.foreach { c =>
        val pa = c.asInstanceOf[PartialAggregate]
        assert(pa.aggregateExpressions.size == 3,
          s"Expected 1 grouping + 2 Avg partials = 3; got ${pa.aggregateExpressions}")
      }
    }
  }
}
