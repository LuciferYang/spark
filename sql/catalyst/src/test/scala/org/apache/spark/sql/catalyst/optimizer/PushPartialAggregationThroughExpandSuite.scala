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
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Literal, Rand}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Complete, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
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
  // gid, NOT the original child columns — this is the realistic shape.
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

  test("disabled by master switch — does not rewrite") {
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

  test("idempotent — second pass does not re-rewrite") {
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

  test("cost gate skips when stats are missing and benefitRatio < 1.0") {
    withSQLConf(
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
      val (agg, _) = rollupAggOverExpand()
      assert(!hasPreAgg(Optimize.execute(agg)),
        "LocalRelation has no column stats — cost gate should skip the rewrite")
    }
  }
}
