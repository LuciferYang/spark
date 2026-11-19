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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Attribute, AttributeMap, AttributeReference, Expression, ExternalUserDefinedFunction, Literal, PythonUDF, Rand, SessionWindow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Complete, Count, First, Last, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder}
import org.apache.spark.udf.worker.UDFWorkerSpecification

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

  test("rewrites Aggregate(Sum) over Expand into Aggregate over Expand over PartialAggregate") {
    withRuleEnabled {
      val (agg, originalExpand) = rollupAggOverExpand()
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized), "Pre-aggregation should be inserted below Expand")

      // Outer aggregate stays a plain Aggregate, which is planned as partial and final.
      assert(optimized.isInstanceOf[Aggregate],
        s"Outer aggregate should be Aggregate, got ${optimized.getClass.getSimpleName}")

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

      // The pre-aggregation groups on every child column but `v`, and the outer sum reads its
      // partial sum.
      val Seq(a, b, v, aAlias, bAlias) = originalExpand.child.output
      val pre = expand.child.asInstanceOf[PartialAggregate]
      assert(pre.groupingExpressions === Seq(a, b, aAlias, bAlias))
      val Seq(partial) = pre.aggregateExpressions.drop(4)
      assert(partial.collectFirst { case ae: AggregateExpression => ae.aggregateFunction } ===
        Some(Sum(v)))
      assert(optimized.expressions.flatMap(_.collect { case s: Sum => s.child }) ===
        Seq(partial.toAttribute))
      assert(optimized.output === agg.output)
      assert(LogicalPlanIntegrity.validateNoDanglingReferences(optimized).isEmpty)
    }
  }

  test("keeps the hint of the outer aggregate") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val hinted = agg.copy(hint = Some(TestAggregateHint))
      val optimized = Optimize.execute(hinted)
      assert(hasPreAgg(optimized))
      assert(optimized.asInstanceOf[Aggregate].hint === Some(TestAggregateHint))
    }
  }

  test("idempotent - does not rewrite an Expand over an aggregate") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val once = Optimize.execute(agg)
      assert(hasPreAgg(once))
      assert(Optimize.execute(once) === once,
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
      // The count stays non-nullable.
      assert(optimized.output === expanded.output)
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
    // `Project` keeps in `expand.child.output`, this becomes four dims with dc = 400) and
    // R = 1000:
    //   cross-product = 400^4 = 2.6x10^10 -> capped at 1000.
    //   exp-backoff   = 400 * 400^(1/2) * 400^(1/4) * 400^(1/8)
    //                 ~= 400 * 20 * 4.47 * 2.11 ~= 75_600 -> capped at 1000.
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
    // The pre-aggregation groups on every child column but the measure `v`, so with the
    // `Project([a, b, c, v, aAlias, bAlias, cAlias], _)` shape it has SIX dimensions
    // (originals + alias copies), each with distinctCount = 20. Then:
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

  test("a measure that an Expand position also reads stays a dimension") {
    withRuleEnabled {
      val a = AttributeReference("a", IntegerType)()
      val b = AttributeReference("b", IntegerType)()
      val v = AttributeReference("v", IntegerType)()
      val aPrime = a.newInstance().withNullability(true)
      val bPrime = b.newInstance().withNullability(true)
      val gid = AttributeReference("spark_grouping_id", LongType, nullable = false)()
      val nullInt = Literal(null, IntegerType)
      // ROLLUP(a, b) once `RemoveRedundantAliases` has stripped the analyzer's `a AS a` and
      // `b AS b`: the grouping positions read `a` and `b` themselves.
      val expand = Expand(
        Seq(Seq(b, v, a, b, Literal(0L)), Seq(b, v, a, nullInt, Literal(1L)),
          Seq(b, v, nullInt, nullInt, Literal(3L))),
        Seq(b, v, aPrime, bPrime, gid),
        LocalRelation(a, b, v))
      val agg = Aggregate(Seq(aPrime, bPrime, gid),
        Seq(aPrime, bPrime, count(b).as("cnt_b"), sum(v).as("sum_v")), expand)
      val optimized = Optimize.execute(agg)
      assert(LogicalPlanIntegrity.validateNoDanglingReferences(optimized).isEmpty)
      val pre = optimized.collectFirst { case p: PartialAggregate => p }
      assert(pre.map(_.groupingExpressions) === Some(Seq(a, b)))
    }
  }

  test("groups once on a column the Expand child outputs twice") {
    withRuleEnabled {
      val (agg, expand) = rollupAggOverExpand()
      // The shape `RemoveRedundantAliases` leaves when it strips `a AS a` and `b AS b`.
      val Seq(a, b, v, aAlias, bAlias) = expand.child.output
      val child = Project(Seq(a, b, v, a, b), expand.child.children.head)
      val projections = expand.projections.map(_.map {
        case `aAlias` => a
        case `bAlias` => b
        case e => e
      })
      val doubled = agg.copy(child = Expand(projections, expand.output, child))
      val pre = Optimize.execute(doubled).collectFirst { case p: PartialAggregate => p }
      assert(pre.map(_.groupingExpressions) === Some(Seq(a, b)))
    }
  }

  test("an aggregate expression that appears twice is pre-aggregated once") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val sumV = agg.aggregateExpressions.last.asInstanceOf[Alias].child
      val twice = agg.copy(aggregateExpressions = agg.aggregateExpressions :+ sumV.as("again"))
      val pre = Optimize.execute(twice).collectFirst { case p: PartialAggregate => p }.get
      assert(pre.aggregateExpressions.size === pre.groupingExpressions.size + 1)
    }
  }

  test("first and last keep ignoreNulls") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val v = agg.child.asInstanceOf[Expand].output.find(_.name == "v").get
      val withFirstLast = agg.copy(aggregateExpressions =
        agg.aggregateExpressions.dropRight(1) ++ Seq(
          First(v, ignoreNulls = true).toAggregateExpression().as("f"),
          Last(v, ignoreNulls = true).toAggregateExpression().as("l")))
      val optimized = Optimize.execute(withFirstLast)
      def functions(plan: LogicalPlan): Seq[Expression] = plan.expressions.flatMap(_.collect {
        case ae: AggregateExpression => ae.aggregateFunction
      })
      val pre = optimized.collectFirst { case p: PartialAggregate => p }.get
      assert(functions(pre) === Seq(First(v, true), Last(v, true)))
      val Seq(firstPartial, lastPartial) = pre.output.takeRight(2)
      assert(functions(optimized) === Seq(First(firstPartial, true), Last(lastPartial, true)))
    }
  }

  test("does not rewrite when a Python or external UDF is in the outer aggregate") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val sumV = agg.aggregateExpressions.last.asInstanceOf[Alias].child
      val v = agg.child.asInstanceOf[Expand].output.find(_.name == "v").get
      def pythonUDF(e: Expression): Expression =
        PythonUDF("udf", null, e.dataType, Seq(e), PythonEvalType.SQL_BATCHED_UDF, true)
      def externalUDF(e: Expression): Expression = ExternalUserDefinedFunction(
        name = Some("udf"),
        workerSpec = UDFWorkerSpecification.getDefaultInstance,
        payload = Array.emptyByteArray,
        dataType = e.dataType,
        children = Seq(e),
        udfDeterministic = true,
        udfNullable = true)
      for (udf <- Seq(pythonUDF _, externalUDF _)) {
        val gid = agg.groupingExpressions.last
        Seq(
          // The UDF is an aggregate function input, which the rewrite would move below the
          // PartialAggregate.
          agg.copy(aggregateExpressions = agg.aggregateExpressions :+ sum(udf(v)).as("u")),
          agg.copy(aggregateExpressions = agg.aggregateExpressions :+ udf(sumV).as("u")),
          agg.copy(aggregateExpressions = agg.aggregateExpressions :+ udf(gid).as("u")),
          agg.copy(groupingExpressions = agg.groupingExpressions :+ udf(gid))
        ).foreach(withUDF => assert(!hasPreAgg(Optimize.execute(withUDF)), withUDF))
      }
    }
  }

  test("does not rewrite when the outer aggregate groups on a session window") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val marker = new MetadataBuilder().putBoolean(SessionWindow.marker, true).build()
      val session = agg.groupingExpressions.head.asInstanceOf[Attribute].withMetadata(marker)
      val withSession = agg.copy(groupingExpressions = session +: agg.groupingExpressions.tail)
      assert(!hasPreAgg(Optimize.execute(withSession)))
    }
  }

  test("does not rewrite a global aggregate") {
    withRuleEnabled {
      val (agg, _) = rollupAggOverExpand()
      val global = Aggregate(Nil, agg.aggregateExpressions.drop(2), agg.child)
      assert(!hasPreAgg(Optimize.execute(global)))
    }
  }

  // An Aggregate over the Expand of ROLLUP(x, y) whose child has 1000 rows, where the
  // pre-aggregation groups on `x` and `y`.
  private def rollupOverStats(x: ColumnStat, y: Option[ColumnStat]): Aggregate = {
    val xa = AttributeReference("x", IntegerType)()
    val ya = AttributeReference("y", IntegerType)()
    val v = AttributeReference("v", IntegerType)()
    val child = StatsTestPlan(Seq(xa, ya, v), 1000, AttributeMap(Seq(xa -> x) ++ y.map(ya -> _)))
    val xPrime = xa.newInstance().withNullability(true)
    val yPrime = ya.newInstance().withNullability(true)
    val gid = AttributeReference("spark_grouping_id", LongType, nullable = false)()
    val nullInt = Literal(null, IntegerType)
    val expand = Expand(
      Seq(Seq(v, xa, ya, Literal(0L)), Seq(v, xa, nullInt, Literal(1L)),
        Seq(v, nullInt, nullInt, Literal(3L))),
      Seq(v, xPrime, yPrime, gid),
      child)
    Aggregate(Seq(xPrime, yPrime, gid), Seq(xPrime, yPrime, sum(v).as("sum_v")), expand)
  }

  private def columnStat(distinctCount: Long, nullCount: Long = 0): ColumnStat = ColumnStat(
    distinctCount = Some(distinctCount), nullCount = Some(nullCount), avgLen = Some(4),
    maxLen = Some(4))

  private def rewritesAtHalf(agg: Aggregate): Boolean = withSQLConf(
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
    hasPreAgg(Optimize.execute(agg))
  }

  test("cost gate damps the dimensions with fewer distinct values") {
    // Sorted descending, 900 * 2^(1/2) ~= 1273 is capped at R = 1000, so K'/R = 1.0. Sorted
    // ascending it would be 2 * 900^(1/2) = 60.
    assert(!rewritesAtHalf(rollupOverStats(columnStat(900), Some(columnStat(2)))))
  }

  test("cost gate skips when a dimension has no column stats") {
    assert(!rewritesAtHalf(rollupOverStats(columnStat(10), None)))
  }

  test("cost gate counts a dimension with no distinct values as one value") {
    // Distinct count 0 and no null count, as a connector may report for an all-null column.
    val noValues = ColumnStat(distinctCount = Some(0), avgLen = Some(4), maxLen = Some(4))
    assert(!rewritesAtHalf(rollupOverStats(noValues, Some(columnStat(900)))))
  }

  test("cost gate counts null as one more value") {
    // An all-null `x` has one value, not zero: K' = 900, not 1.
    assert(!rewritesAtHalf(rollupOverStats(columnStat(0, 1000), Some(columnStat(900)))))
    // K' = 400 * (1 + 1)^(1/2) ~= 566, not 400.
    assert(!rewritesAtHalf(rollupOverStats(columnStat(400), Some(columnStat(1, 10)))))
  }
}

private case object TestAggregateHint extends AggregateHint
