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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeMap, AttributeReference, Literal, Rand}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Complete, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType}

class PushPartialAggregationThroughExpandSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Push Partial Aggregation Through Expand", Once,
      PushPartialAggregationThroughExpand) :: Nil
  }

  // Build a plan shaped like ResolveGroupingAnalytics produces for
  //   SELECT a, b, sum(v) FROM t GROUP BY ROLLUP(a, b)
  // i.e. an Aggregate over an Expand over a Project that materializes the group-by
  // aliases. The Aggregate's grouping references the newInstance grouping attrs and
  // gid, NOT the original child columns - this is the realistic shape.
  private def rollupAggOverExpand(): (Aggregate, Expand) = {
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val v = AttributeReference("v", IntegerType)()
    val child = LocalRelation(a, b, v)

    // Project that materializes ROLLUP's group-by aliases (simple aliases of a, b).
    val aAlias = Alias(a, "a")()
    val bAlias = Alias(b, "b")()
    val project = Project(Seq(a, b, v, aAlias, bAlias), child)

    // newInstance attrs that go into Expand.output.
    val aPrime = aAlias.toAttribute.newInstance().withNullability(true)
    val bPrime = bAlias.toAttribute.newInstance().withNullability(true)
    val gid = AttributeReference("spark_grouping_id", LongType, nullable = false)()

    // Three projections for ROLLUP(a, b): (a, b), (a), (). Pass-through `[a, b, v]`
    // is shared by every projection; the newInstance positions hold the alias attrs
    // in projections that include them or null otherwise; gid carries the bitmask.
    val projections = Seq(
      Seq(a, b, v, aAlias.toAttribute, bAlias.toAttribute, Literal(0L)),
      Seq(a, b, v, aAlias.toAttribute, Literal(null, IntegerType), Literal(1L)),
      Seq(a, b, v, Literal(null, IntegerType), Literal(null, IntegerType), Literal(3L))
    )
    val expand = Expand(projections, Seq(a, b, v, aPrime, bPrime, gid), project)

    val agg = Aggregate(
      groupingExpressions = Seq(aPrime, bPrime, gid),
      aggregateExpressions = Seq(
        aPrime,
        bPrime,
        Alias(
          AggregateExpression(Sum(v), Complete, isDistinct = false),
          "sum_v")()),
      child = expand)

    (agg, expand)
  }

  // True iff the optimized plan inserts a PartialAggregate (or any AggregateBase)
  // immediately below the Expand.
  private def hasPreAgg(plan: LogicalPlan): Boolean = plan.collect {
    case e: Expand => e.child.isInstanceOf[AggregateBase]
  }.exists(identity)

  // Tests that target structural mechanics without needing real column statistics
  // override the cost gate threshold to 1.0 so the rewrite always fires.
  private def withRuleEnabled[T](body: => T): T = withSQLConf(
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0")(body)

  test("disabled by master switch - does not rewrite") {
    val (agg, _) = rollupAggOverExpand()
    val optimized = withSQLConf(
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false")(
      Optimize.execute(agg))
    assert(!hasPreAgg(optimized))
  }

  test("rewrites Aggregate(Sum) over Expand into FinalAggregate over PartialAggregate") {
    withRuleEnabled {
      val (agg, originalExpand) = rollupAggOverExpand()
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized), "Pre-aggregation should be inserted below Expand")

      // Outer aggregate is now a FinalAggregate.
      assert(optimized.isInstanceOf[FinalAggregate],
        s"Outer aggregate should be FinalAggregate, got ${optimized.getClass.getSimpleName}")

      // The inner is specifically a PartialAggregate.
      val expand = optimized.collect { case e: Expand => e }.head
      assert(expand.child.isInstanceOf[PartialAggregate],
        s"Inner should be PartialAggregate, got ${expand.child.getClass.getSimpleName}")

      // Augmented Expand drops the measure-input column (`v`) and appends the partial
      // sum; final output arity matches original.
      assert(!expand.output.exists(_.name == "v"))
      assert(expand.output.size == originalExpand.output.size)
      expand.projections.foreach { proj =>
        assert(proj.size == expand.output.size,
          s"Projection arity ${proj.size} must equal output arity ${expand.output.size}")
      }
    }
  }

  test("idempotent - second pass does not re-rewrite") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val once = Optimize.execute(agg)
      val twice = Optimize.execute(once)
      val expand = twice.collect { case e: Expand => e }.head
      val grandchild = expand.child.asInstanceOf[AggregateBase].child
      assert(!grandchild.isInstanceOf[AggregateBase],
        "Rule must not stack a second pre-aggregation under the same Expand")
    }
  }

  test("does not rewrite when an aggregate is DISTINCT") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val v = agg.child.asInstanceOf[Expand].output.find(_.name == "v").get
      val withDistinct = agg.copy(aggregateExpressions =
        agg.aggregateExpressions.dropRight(1) :+
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = true), "sum_d_v")())
      assert(!hasPreAgg(Optimize.execute(withDistinct)))
    }
  }

  test("does not rewrite when an aggregate has FILTER") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val v = agg.child.asInstanceOf[Expand].output.find(_.name == "v").get
      val withFilter = agg.copy(aggregateExpressions =
        agg.aggregateExpressions.dropRight(1) :+
          Alias(
            AggregateExpression(Sum(v), Complete, isDistinct = false,
              filter = Some(v > Literal(0))),
            "sum_f_v")())
      assert(!hasPreAgg(Optimize.execute(withFilter)))
    }
  }

  test("does not rewrite when an aggregate is non-deterministic") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val v = agg.child.asInstanceOf[Expand].output.find(_.name == "v").get
      // Sum(v + Rand()) is non-deterministic because Rand is. Pre-aggregating before
      // Expand would evaluate Rand once per child row instead of once per
      // Expand-output row, so the rule must skip.
      val nonDet = agg.copy(aggregateExpressions =
        agg.aggregateExpressions.dropRight(1) :+
          Alias(
            AggregateExpression(
              Sum(Add(v, Rand(Literal(1L)))),
              Complete, isDistinct = false),
            "sum_rand")())
      assert(!hasPreAgg(Optimize.execute(nonDet)),
        "Non-deterministic aggregate must disable the rewrite")
    }
  }

  test("does not rewrite when measure references an Expand-produced attribute (gid)") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val gid = agg.child.asInstanceOf[Expand].output.find(_.name == "spark_grouping_id").get
      val withGidMeasure = agg.copy(aggregateExpressions =
        agg.aggregateExpressions.dropRight(1) :+
          Alias(AggregateExpression(Max(gid), Complete, isDistinct = false), "max_gid")())
      assert(!hasPreAgg(Optimize.execute(withGidMeasure)))
    }
  }

  test("supports Min, Max, Count and Avg") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val v = agg.child.asInstanceOf[Expand].output.find(_.name == "v").get
      val expanded = agg.copy(aggregateExpressions =
        agg.aggregateExpressions.dropRight(1) ++ Seq(
          Alias(AggregateExpression(Min(v), Complete, isDistinct = false), "min_v")(),
          Alias(AggregateExpression(Max(v), Complete, isDistinct = false), "max_v")(),
          Alias(AggregateExpression(Count(Seq(v)), Complete, isDistinct = false), "cnt_v")(),
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()))
      val optimized = Optimize.execute(expanded)
      assert(hasPreAgg(optimized),
        "Min/Max/Count/Avg combination should still be eligible for the rewrite")
    }
  }

  test("does not rewrite when child has no non-measure attrs") {
    withRuleEnabled {
      val v = AttributeReference("v", IntegerType)()
      val gid = AttributeReference("spark_grouping_id", LongType, nullable = false)()
      val expand = Expand(
        projections = Seq(Seq(v, Literal(0L))),
        output = Seq(v, gid),
        child = LocalRelation(v))
      val agg = Aggregate(
        groupingExpressions = Seq(gid),
        aggregateExpressions = Seq(gid,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = expand)
      assert(!hasPreAgg(Optimize.execute(agg)))
    }
  }

  test("cost gate still rejects when both cross-product and exp-backoff saturate at R") {
    // Both bounds saturate at the input row count when per-dim distinct counts and the
    // number of dims combine such that even the dampened product exceeds R. With
    // `distinctCount = 400` on each of dims a / b (combined with the alias copies that
    // `Project` keeps in `expand.child.output`, this becomes six dims with dc = 400) and
    // R = 1000:
    //   cross-product = 400^6 = 4.1x10^15 -> capped at 1000.
    //   exp-backoff   = 400 * 400^(1/2) * 400^(1/4) * 400^(1/8) * 400^(1/16) * 400^(1/32)
    //                 ~= 400 * 20 * 4.47 * 2.11 * 1.45 * 1.20 ~= 130_788 -> capped at 1000.
    // Both saturate at K' = 1000, K'/R = 1.0. Rule should not fire at threshold 0.5.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val v = AttributeReference("v", IntegerType)()
    val rowCount = 1000L
    val statsPlan = StatsTestPlan(
      outputList = Seq(a, b, v),
      rowCount = rowCount,
      attributeStats = AttributeMap(Seq(
        a -> ColumnStat(distinctCount = Some(400), min = Some(1), max = Some(400),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        b -> ColumnStat(distinctCount = Some(400), min = Some(1), max = Some(400),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        v -> ColumnStat(distinctCount = Some(1000), min = Some(1), max = Some(1000),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))))

    val aAlias = Alias(a, "a")()
    val bAlias = Alias(b, "b")()
    val project = Project(Seq(a, b, v, aAlias, bAlias), statsPlan)
    val aPrime = aAlias.toAttribute.newInstance().withNullability(true)
    val bPrime = bAlias.toAttribute.newInstance().withNullability(true)
    val gid = AttributeReference("spark_grouping_id", LongType, nullable = false)()
    val projections = Seq(
      Seq(a, b, v, aAlias.toAttribute, bAlias.toAttribute, Literal(0L)),
      Seq(a, b, v, aAlias.toAttribute, Literal(null, IntegerType), Literal(1L)),
      Seq(a, b, v, Literal(null, IntegerType), Literal(null, IntegerType), Literal(3L)))
    val expand = Expand(projections, Seq(a, b, v, aPrime, bPrime, gid), project)
    val agg = Aggregate(
      groupingExpressions = Seq(aPrime, bPrime, gid),
      aggregateExpressions = Seq(aPrime, bPrime,
        Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
      child = expand)

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
      assert(!hasPreAgg(Optimize.execute(agg)),
        "saturated K' under both bounds - rewrite should still be rejected at 0.5")
    }
  }

  test("cost gate exp-backoff fires when cross-product saturates but backoff doesn't") {
    // The rule's `computeDimensions` uses `expand.child.output.filterNot(measure_refs)`,
    // so with the standard `Project([a, b, c, v, aAlias, bAlias, cAlias], _)` shape the
    // synthetic dimension set has SIX entries (originals + alias copies), each with
    // distinctCount = 20. Then:
    //   cross-product = 20^6 = 64M capped at R = 1000 -> 1000, K'/R = 1.0.
    //   exp-backoff   = 20 * 20^(1/2) * 20^(1/4) * 20^(1/8) * 20^(1/16) * 20^(1/32)
    //                 ~= 20 * 4.47 * 2.11 * 1.45 * 1.20 * 1.10 ~= 365 -> 365, K'/R ~= 0.365.
    // 0.365 < 0.5, so the rewrite fires with the exp-backoff bound active.
    val a = AttributeReference("a", IntegerType)()
    val b = AttributeReference("b", IntegerType)()
    val c = AttributeReference("c", IntegerType)()
    val v = AttributeReference("v", IntegerType)()
    val rowCount = 1000L
    val statsPlan = StatsTestPlan(
      outputList = Seq(a, b, c, v),
      rowCount = rowCount,
      attributeStats = AttributeMap(Seq(
        a -> ColumnStat(distinctCount = Some(20), min = Some(1), max = Some(20),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        b -> ColumnStat(distinctCount = Some(20), min = Some(1), max = Some(20),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        c -> ColumnStat(distinctCount = Some(20), min = Some(1), max = Some(20),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
        v -> ColumnStat(distinctCount = Some(1000), min = Some(1), max = Some(1000),
          nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))))

    val aAlias = Alias(a, "a")()
    val bAlias = Alias(b, "b")()
    val cAlias = Alias(c, "c")()
    val project = Project(Seq(a, b, c, v, aAlias, bAlias, cAlias), statsPlan)
    val aPrime = aAlias.toAttribute.newInstance().withNullability(true)
    val bPrime = bAlias.toAttribute.newInstance().withNullability(true)
    val cPrime = cAlias.toAttribute.newInstance().withNullability(true)
    val gid = AttributeReference("spark_grouping_id", LongType, nullable = false)()
    val projections = Seq(
      Seq(a, b, c, v, aAlias.toAttribute, bAlias.toAttribute, cAlias.toAttribute, Literal(0L)),
      Seq(a, b, c, v, aAlias.toAttribute, bAlias.toAttribute, Literal(null, IntegerType),
        Literal(1L)),
      Seq(a, b, c, v, Literal(null, IntegerType), Literal(null, IntegerType),
        Literal(null, IntegerType), Literal(7L)))
    val expand = Expand(projections, Seq(a, b, c, v, aPrime, bPrime, cPrime, gid), project)
    val agg = Aggregate(
      groupingExpressions = Seq(aPrime, bPrime, cPrime, gid),
      aggregateExpressions = Seq(aPrime, bPrime, cPrime,
        Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
      child = expand)

    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
      assert(hasPreAgg(Optimize.execute(agg)),
        "exp-backoff K' ~= 365 < R = 1000, K'/R ~= 0.37 < 0.5 - rewrite should fire")
    }
  }

  test("cost gate skips when stats are missing and benefitRatio < 1.0") {
    withSQLConf(
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
      val (agg, _) = rollupAggOverExpand()
      assert(!hasPreAgg(Optimize.execute(agg)),
        "LocalRelation has no column stats - cost gate should skip the rewrite")
    }
  }
}
