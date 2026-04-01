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

import org.scalatest.GivenWhenThen

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Cast, CheckOverflow, CheckOverflowInSum, Divide, EvalMode, Expression, If, IsNull, Literal, NumericEvalContext}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Sum}
import org.apache.spark.sql.catalyst.optimizer.customAnalyze._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, LongType}

// Custom Analyzer to exclude DecimalPrecision rule
object ExcludeDecimalPrecisionAnalyzer extends Analyzer(
  new CatalogManager(
    FakeV2SessionCatalog,
    new SessionCatalog(
      new InMemoryCatalog,
      EmptyFunctionRegistry,
      EmptyTableFunctionRegistry) {
      override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {}
    })) {
  override def resolver: Resolver = caseSensitiveResolution

  override def batches: Seq[Batch] = {
    super.batches.map { b =>
      val newRules = b.rules.map {
        case t: TypeCoercion.CombinedTypeCoercionRule =>
          // Exclude DecimalPrecision rules.
          val typeCoercionRules = t.rules.filterNot(_.ruleName.equals(DecimalPrecision.ruleName))
          new TypeCoercion.CombinedTypeCoercionRule(typeCoercionRules)
        case t: AnsiTypeCoercion.CombinedTypeCoercionRule =>
          // Exclude DecimalPrecision rules.
          val typeCoercionRules = t.rules.filterNot(_.ruleName.equals(DecimalPrecision.ruleName))
          new AnsiTypeCoercion.CombinedTypeCoercionRule(typeCoercionRules)
        case r => r
      }
      Batch(b.name, b.strategy, newRules: _*)
    }
  }
}

// Custom Analyzer to exclude DecimalPrecision rule
object customAnalyze { // scalastyle:ignore
  implicit class CustomDslLogicalPlan(val logicalPlan: LogicalPlan) {
    def analyzePlan: LogicalPlan = {
      val analyzed = ExcludeDecimalPrecisionAnalyzer.execute(logicalPlan)
      ExcludeDecimalPrecisionAnalyzer.checkAnalysis(analyzed)
      EliminateSubqueryAliases(analyzed)
    }
  }
}

class PushPartialAggregationThroughJoinSuite
  extends StatsEstimationTestBase with PlanTest with GivenWhenThen {

  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED, true)
    SQLConf.get.setConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO, 1.0)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED)
    SQLConf.get.unsetConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO)
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
        Batch("Finish Analysis", Once,
          PullOutGroupingExpressions) ::
        Batch("Filter Pushdown", FixedPoint(10),
          PullOutGroupingExpressions,
          CombineFilters,
          PushPredicateThroughNonJoin,
          BooleanSimplification,
          PushPredicateThroughJoin,
          ColumnPruning,
          SimplifyCasts,
          CollapseProject) ::
        Batch("PushPartialAggregationThroughJoin", Once,
          PushPartialAggregationThroughJoin) :: Nil
  }

  private val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
    attr("a") -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("b") -> ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("c") -> ColumnStat(distinctCount = Some(1000), min = Some(1), max = Some(1000),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("x") -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("y") -> ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(100),
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
    attr("z") -> ColumnStat(distinctCount = Some(1000), min = Some(1), max = Some(1000),
      nullCount = Some(4), avgLen = Some(4), maxLen = Some(4))))

  private val nameToAttr: Map[String, Attribute] = columnInfo.map(kv => kv._1.name -> kv._1)
  private val nameToColInfo: Map[String, (Attribute, ColumnStat)] =
    columnInfo.map(kv => kv._1.name -> kv)

  val rowCount = 1000
  private val testRelation1 = StatsTestPlan(
    outputList = Seq("a", "b", "c").map(nameToAttr),
    rowCount,
    size = Some(rowCount * (8 + 4)),
    attributeStats = AttributeMap(Seq("a", "b", "c").map(nameToColInfo)))
  private val testRelation2 = StatsTestPlan(
    outputList = Seq("x", "y", "z").map(nameToAttr),
    rowCount,
    size = Some(rowCount * (8 + 4)),
    attributeStats = AttributeMap(Seq("x", "y", "z").map(nameToColInfo)))

  private val testRelation3 =
    LocalRelation($"a".decimal(17, 2), $"b".decimal(17, 2), $"c".decimal(17, 2))
  private val testRelation4 =
    LocalRelation($"x".decimal(17, 2), $"y".decimal(17, 2), $"z".decimal(17, 2))

  private def sumWithDataType(
                               sum: Expression,
                               useAnsiAdd: Boolean = conf.ansiEnabled,
                               dataType: Option[DataType] = None): AggregateExpression = {
    val evalCtx = NumericEvalContext(EvalMode.fromBoolean(useAnsiAdd))
    Sum(sum, evalCtx, resultDataType = dataType)
      .toAggregateExpression()
  }

  test("Push down sum") {
    Given("Push both side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("b"))(sum(Symbol("c")).as("sum_c"))
        .analyze

      val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
        Seq(Symbol("a"), Symbol("b"), sum(Symbol("c")).as("_pushed_sum_c")),
        testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
      val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
        testRelation2.select(Symbol("x"))).as("r")

      val correctAnswer =
        correctLeft.join(correctRight,
            joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
          .select(Symbol("b"), Symbol("_pushed_sum_c"), Symbol("cnt"))
          .groupBy(Symbol("b"))(
            sumWithDataType(Symbol("_pushed_sum_c") * Symbol("cnt"), dataType = Some(LongType))
            .as("sum_c"))
          .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }

    Given("Push right side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.3") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("b"))(sum(Symbol("c")).as("sum_c"))
        .analyze

      val correctLeft = testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c")).as("l")
      val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
        testRelation2.select(Symbol("x"))).as("r")

      val correctAnswer =
        correctLeft.join(correctRight,
            joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
          .select(Symbol("b"), Symbol("c"), Symbol("cnt"))
          .groupBy(
            Symbol("b"))(sumWithDataType(Symbol("c") * Symbol("cnt"), dataType = Some(LongType))
            .as("sum_c"))
          .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }

    Given("Push left side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.3") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("y"))(sum(Symbol("z")).as("sum_z"))
        .analyze

      val correctLeft = PartialAggregate(Seq(Symbol("a")), Seq(Symbol("a"), count(1).as("cnt")),
        testRelation1.select(Symbol("a"))).as("l")
      val correctRight = testRelation2.select(Symbol("x"), Symbol("y"), Symbol("z")).as("r")

      val correctAnswer =
        correctLeft.join(correctRight,
            joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
          .select(Symbol("cnt"), Symbol("y"), Symbol("z"))
          .groupBy(Symbol("y"))(
            sumWithDataType(Symbol("z").cast(LongType) * Symbol("cnt"), dataType = Some(LongType))
            .as("sum_z"))
          .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }
  }

  test("Push down count") {
    Given("Push both side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("b"))(count(Symbol("c")).as("cnt"))
        .analyze

      val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
        Seq(Symbol("a"), Symbol("b"), count(Symbol("c")).as("_pushed_count_c")),
        testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
      val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
        testRelation2.select(Symbol("x"))).as("r")

      val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
          condition = Some(Symbol("a") === Symbol("x")))
        .select(Symbol("b"), $"l._pushed_count_c", $"r.cnt")
        .groupBy(Symbol("b"))(sumWithDataType($"l._pushed_count_c" * $"r.cnt",
          dataType = Some(LongType)).as("cnt"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }

    Given("Push right side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.3") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("b"))(count(Symbol("c")).as("cnt"))
        .analyze

      val correctLeft = testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c")).as("l")
      val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
        testRelation2.select(Symbol("x"))).as("r")

      val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
          condition = Some(Symbol("a") === Symbol("x")))
        .select(Symbol("b"), Symbol("c"), $"r.cnt")
        .groupBy(Symbol("b"))(sumWithDataType(
          If(IsNull(Symbol("c")), Literal(0L, LongType), Literal(1L, LongType)) * $"r.cnt",
          dataType = Some(LongType)).as("cnt"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }

    Given("Push left side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.3") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("y"))(count(Symbol("z")).as("cnt"))
        .analyze

      val correctLeft = PartialAggregate(Seq(Symbol("a")), Seq(Symbol("a"), count(1).as("cnt")),
        testRelation1.select(Symbol("a"))).as("l")
      val correctRight = testRelation2.select(Symbol("x"), Symbol("y"), Symbol("z")).as("r")

      val correctAnswer =
        correctLeft.join(correctRight,
            joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
          .select(Symbol("cnt"), Symbol("y"), Symbol("z"))
          .groupBy(Symbol("y"))(sumWithDataType(
            If(IsNull(Symbol("z")), Literal(0L, LongType), Literal(1L, LongType)) * $"l.cnt",
            dataType = Some(LongType)).as("cnt"))
          .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }
  }

  test("Push down avg") {
    Given("Push both side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("b"))(avg(Symbol("c")).as("avg_c"))
        .analyze

      val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
        Seq(Symbol("a"), Symbol("b"),
          sumWithDataType(Symbol("c"), dataType = Some(DoubleType)).as("_pushed_sum_c"),
          count(Symbol("c")).as("_pushed_count_c")),
        testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
      val correctRight = PartialAggregate(Seq(Symbol("x")),
        Seq(Symbol("x"), count(1).as("cnt")),
        testRelation2.select(Symbol("x"))).as("r")
      val newAvg =
        Divide(Sum($"l._pushed_sum_c" * $"r.cnt".cast(DoubleType),
          resultDataType = Some(DoubleType))
          .toAggregateExpression(),
          Sum($"l._pushed_count_c" * $"r.cnt", resultDataType = Some(LongType))
            .toAggregateExpression().cast(DoubleType),
          EvalMode.fromBoolean(conf.ansiEnabled))

      val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
          condition = Some(Symbol("a") === Symbol("x")))
        .select(Symbol("b"), $"l._pushed_sum_c", $"l._pushed_count_c", $"r.cnt")
        .groupBy(Symbol("b"))(newAvg.as("avg_c"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }

    Given("Push right side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.3") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("b"))(avg(Symbol("c")).as("avg_c"))
        .analyze

      val correctLeft = testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c")).as("l")
      val correctRight = PartialAggregate(Seq(Symbol("x")),
        Seq(Symbol("x"), count(1).as("cnt")),
        testRelation2.select(Symbol("x"))).as("r")
      val newAvg =
        Divide(Sum($"l.c".cast(DoubleType) * $"r.cnt".cast(DoubleType),
          resultDataType = Some(DoubleType)).toAggregateExpression(),
          Sum(If(IsNull(Symbol("c")), Literal(0L, LongType), Literal(1L, LongType)) * $"r.cnt",
            resultDataType = Some(LongType)).toAggregateExpression().cast(DoubleType),
          EvalMode.fromBoolean(conf.ansiEnabled))

      val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
          condition = Some(Symbol("a") === Symbol("x")))
        .select(Symbol("b"), $"l.c", $"r.cnt")
        .groupBy(Symbol("b"))(newAvg.as("avg_c"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }

    Given("Push left side")
    withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.3") {
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("y"))(avg(Symbol("z")).as("avg_z"))
        .analyze

      val correctLeft = PartialAggregate(Seq(Symbol("a")), Seq(Symbol("a"), count(1).as("cnt")),
        testRelation1.select(Symbol("a"))).as("l")
      val correctRight = testRelation2.select(Symbol("x"), Symbol("y"), Symbol("z")).as("r")
      val newAvg =
        Divide(Sum($"r.z".cast(DoubleType) * $"l.cnt".cast(DoubleType),
          resultDataType = Some(DoubleType)).toAggregateExpression(),
          Sum(If(IsNull(Symbol("z")), Literal(0L, LongType), Literal(1L, LongType)) * $"l.cnt",
            resultDataType = Some(LongType)).toAggregateExpression().cast(DoubleType),
          EvalMode.fromBoolean(conf.ansiEnabled))

      val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
          condition = Some(Symbol("a") === Symbol("x")))
        .select($"l.cnt", Symbol("y"), $"r.z")
        .groupBy(Symbol("y"))(newAvg.as("avg_z"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), correctAnswer)
    }
  }

  test("Push down first and last") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b"))(first(Symbol("c")).as("first_c"), last(Symbol("c")).as("last_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
      Seq(Symbol("a"), Symbol("b"),
        first(Symbol("c")).as("_pushed_first_c"), last(Symbol("c")).as("_pushed_last_c")),
      testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
    val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x")),
      testRelation2.select(Symbol("x"))).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
        condition = Some(Symbol("a") === Symbol("x")))
      .select(Symbol("b"), $"l._pushed_first_c", $"l._pushed_last_c")
      .groupBy(Symbol("b"))(first(Symbol("_pushed_first_c")).as("first_c"),
        last(Symbol("_pushed_last_c")).as("last_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push down max and min") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b"))(max(Symbol("c")).as("max_c"), min(Symbol("c")).as("min_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
      Seq(Symbol("a"), Symbol("b"), max(Symbol("c")).as("_pushed_max_c"),
        min(Symbol("c")).as("_pushed_min_c")),
      testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
    val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x")),
      testRelation2.select(Symbol("x"))).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
        condition = Some(Symbol("a") === Symbol("x")))
      .select(Symbol("b"), $"l._pushed_max_c", $"l._pushed_min_c")
      .groupBy(Symbol("b"))(
        max(Symbol("_pushed_max_c")).as("max_c"), min(Symbol("_pushed_min_c")).as("min_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push down sum(2), sum(2.5BD), avg(2), min(2), max(2), first(2) and last(2)") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b"))(
        sum(Literal(2)).as("sum_2"),
        sum(Literal(BigDecimal("2.5"))).as("sum_25"),
        avg(Literal(2)).as("avg_2"),
        min(Literal(2)).as("min_2"), max(Literal(2)).as("max_2"),
        first(Literal(2)).as("first_2"), last(Literal(2)).as("last_2"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
      Seq(Symbol("a"), Symbol("b"), count(1).as("cnt")),
      testRelation1.select(Symbol("a"), Symbol("b"))).as("l")
    val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
      testRelation2.select(Symbol("x"))).as("r")

    val correctAnswer =
      correctLeft.join(correctRight,
          joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .select(Symbol("b"), $"l.cnt", $"r.cnt")
        .groupBy(Symbol("b"))(sumWithDataType(Literal(2).cast(LongType) * ($"l.cnt" * $"r.cnt"),
          dataType = Some(LongType)).as("sum_2"),
          sumWithDataType(CheckOverflow(Literal(BigDecimal("2.5")).cast(DecimalType(12, 1)) *
            ($"l.cnt" * $"r.cnt").cast(DecimalType(12, 1)), DecimalType(12, 1), !conf.ansiEnabled),
            conf.ansiEnabled, dataType = Some(DecimalType(12, 1))).as("sum_25"),
          avg(Literal(2)).as("avg_2"),
          min(Literal(2)).as("min_2"), max(Literal(2)).as("max_2"),
          first(Literal(2)).as("first_2"), last(Literal(2)).as("last_2"))
        .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Push distinct") {
    Seq(-1, 10000).foreach { threshold =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
        Seq(Inner, LeftOuter, RightOuter, FullOuter, Cross).foreach { joinType =>
          val originalQuery = testRelation1
            .join(testRelation2, joinType = joinType,
              condition = Some(Symbol("a") === Symbol("x")))
            .groupBy(Symbol("b"), Symbol("y"))(Symbol("b"), Symbol("y"))
            .analyze

          val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
            Seq(Symbol("a"), Symbol("b")),
            testRelation1.select(Symbol("a"), Symbol("b"))).as("l")
          val correctRight = PartialAggregate(
            Seq(Symbol("x"), Symbol("y")), Seq(Symbol("x"), Symbol("y")),
            testRelation2.select(Symbol("x"), Symbol("y"))).as("r")
          val correctAnswer = correctLeft.join(correctRight, joinType = joinType,
              condition = Some(Symbol("a") === Symbol("x")))
            .select(Symbol("b"), Symbol("y"))
            .groupBy(Symbol("b"), Symbol("y"))(Symbol("b"), Symbol("y"))
            .analyze

          comparePlans(Optimize.execute(originalQuery), correctAnswer)
        }
      }
    }
  }

  test("Push distinct for sum(distinct c) and count(distinct c)") {
    Seq(-1, 10000).foreach { threshold =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
        Seq(Inner, LeftOuter, RightOuter, FullOuter).foreach { joinType =>
          val originalQuery = testRelation1
            .join(testRelation2, joinType = joinType, condition = Some(Symbol("a") === Symbol("x")))
            .groupBy(Symbol("b"))(sumDistinct(Symbol("c")), countDistinct(Symbol("c")))

          val correctLeft =
            PartialAggregate(Seq(Symbol("a"), Symbol("b"), Symbol("c")),
              Seq(Symbol("a"), Symbol("b"), Symbol("c")), testRelation1).as("l")
          val correctRight =
            PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x")),
              testRelation2.select(Symbol("x"))).as("r")

          val correctAnswer = correctLeft.join(correctRight, joinType = joinType,
              condition = Some(Symbol("a") === Symbol("x")))
            .select(Symbol("b"), Symbol("c"))
            .groupBy(Symbol("b"))(sumDistinct(Symbol("c")), countDistinct(Symbol("c")))

          comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
        }
      }
    }
  }

  test("Complex join condition") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner,
        condition = Some(Symbol("a") + 1 === Symbol("x") + 2))
      .groupBy(Symbol("b"))(max(Symbol("c")).as("max_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("_pullout_add_a"), Symbol("b")),
      Seq(Symbol("_pullout_add_a"), Symbol("b"), max(Symbol("c")).as("_pushed_max_c")),
      testRelation1.select(Symbol("b"), Symbol("c"), (Symbol("a") + 1).as("_pullout_add_a")))
      .as("l")
    val correctRight = PartialAggregate(
      Seq(Symbol("_pullout_add_x")), Seq(Symbol("_pullout_add_x")),
      testRelation2.select((Symbol("x") + 2).as("_pullout_add_x"))).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
        condition = Some(Symbol("_pullout_add_a") === Symbol("_pullout_add_x")))
      .select(Symbol("b"), $"l._pushed_max_c")
      .groupBy(Symbol("b"))(max(Symbol("_pushed_max_c")).as("max_c"))
      .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Complex grouping keys") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b") + 1)(max(Symbol("c")).as("max_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("_groupingexpression")),
      Seq(Symbol("a"), Symbol("_groupingexpression"), max(Symbol("c")).as("_pushed_max_c")),
      testRelation1.select(
        Symbol("a"), Symbol("c"), (Symbol("b") + 1).as("_groupingexpression"))).as("l")
    val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x")),
      testRelation2.select(Symbol("x"))).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
        condition = Some(Symbol("x") === Symbol("a")))
      .select($"l._groupingexpression", $"l._pushed_max_c")
      .groupBy(Symbol("_groupingexpression"))(max(Symbol("_pushed_max_c")).as("max_c"))
      .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Complex expressions between Aggregate and Join") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .select((Symbol("a") + 1).as("a1"), Symbol("b"))
      .groupBy(Symbol("a1"))(max(Symbol("b")).as("max_b"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("a1")),
      Seq(Symbol("a"), Symbol("a1"), max(Symbol("b")).as("_pushed_max_b")),
      testRelation1.select(Symbol("a"), (Symbol("a") + 1).as("a1"), Symbol("b"))).as("l")
    val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x")),
      testRelation2.select(Symbol("x"))).as("r")

    val correctAnswer = correctLeft.join(correctRight, joinType = Inner,
        condition = Some(Symbol("x") === Symbol("a")))
      .select(Symbol("a1"), $"l._pushed_max_b")
      .groupBy(Symbol("a1"))(max(Symbol("_pushed_max_b")).as("max_b"))
      .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Decimal type sum") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> s"$ansiEnabled") {
        val originalQuery = testRelation3
          .join(testRelation4, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
          .groupBy(Symbol("b"))(sum(Symbol("c")).as("sum_c"))
          .analyze

        val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
          Seq(Symbol("a"), Symbol("b"), sum(Symbol("c")).as("_pushed_sum_c")),
          testRelation3.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
        val correctRight = PartialAggregate(
          Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
          testRelation4.select(Symbol("x"))).as("r")

        val correctAnswer =
          correctLeft.join(correctRight,
              joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
            .select(Symbol("b"), Symbol("_pushed_sum_c"), Symbol("cnt"))
            .groupBy(Symbol("b"))(
              sumWithDataType(Symbol("_pushed_sum_c") *
                Symbol("cnt").cast(DecimalType.LongDecimal),
              ansiEnabled,
              Some(DecimalType(27, 2))).as("sum_c"))
            .analyzePlan

        comparePlans(Optimize.execute(originalQuery), correctAnswer)
      }
    }
  }

  test("Decimal type count") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> s"$ansiEnabled") {
        val originalQuery = testRelation3
          .join(testRelation4, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
          .groupBy(Symbol("b"))(count(Symbol("c")).as("count_c"))

        val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
          Seq(Symbol("a"), Symbol("b"), count(Symbol("c")).as("_pushed_count_c")),
          testRelation3.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
        val correctRight =
          PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
          testRelation4.select(Symbol("x"))).as("r")

        val correctAnswer =
          correctLeft.join(correctRight,
              joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
            .select(Symbol("b"), Symbol("_pushed_count_c"), Symbol("cnt"))
            .groupBy(Symbol("b"))(
              sumWithDataType(Symbol("_pushed_count_c") * Symbol("cnt"),
                ansiEnabled, Some(LongType))
              .as("count_c"))
            .analyzePlan

        comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer)
      }
    }
  }

  test("Decimal type avg") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> s"$ansiEnabled") {
        val originalQuery = testRelation3
          .join(testRelation4, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
          .groupBy(Symbol("b"))(avg(Symbol("c")).as("avg_c"))
          .analyze

        val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
          Seq(Symbol("a"), Symbol("b"),
            sumWithDataType(Symbol("c"), ansiEnabled,
              Some(DecimalType(27, 2))).as("_pushed_sum_c"),
            count(Symbol("c")).as("_pushed_count_c")),
          testRelation3.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
        val correctRight =
          PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
          testRelation4.select(Symbol("x"))).as("r")

        val context = originalQuery.analyzePlan.asInstanceOf[Aggregate]
          .aggregateExpressions
          .flatMap(_.collect { case average: Average => average }).head.getContextOrNull()

        val correctAnswer =
          correctLeft.join(correctRight,
              joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
            .select(Symbol("b"), Symbol("_pushed_sum_c"), $"l._pushed_count_c", $"r.cnt")
            .groupBy(Symbol("b"))(Cast(Divide(CheckOverflowInSum(
              sumWithDataType($"_pushed_sum_c" * Cast($"r.cnt", DecimalType.LongDecimal),
                ansiEnabled, Some(DecimalType(27, 2))),
              DecimalType(27, 2), !ansiEnabled, context),
              Cast(sumWithDataType($"l._pushed_count_c" * $"r.cnt", ansiEnabled,
                Some(LongType)), DecimalType(20, 0)), EvalMode.fromBoolean(ansiEnabled)),
              DecimalType(21, 6)).as("avg_c"))
            .analyzePlan

        comparePlans(Optimize.execute(originalQuery), correctAnswer)
      }
    }
  }

  test("Add join condition references to split list") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b"))(sum(Symbol("c")).as("sum_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
      Seq(Symbol("a"), Symbol("b"), sum(Symbol("c")).as("_pushed_sum_c")),
      testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c"))).as("l")
    val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
      testRelation2.select(Symbol("x"))).as("r")

    val correctAnswer =
      correctLeft.join(correctRight,
          joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .select(Symbol("b"), Symbol("_pushed_sum_c"), Symbol("cnt"))
        .groupBy(Symbol("b"))(
          sumWithDataType(Symbol("_pushed_sum_c") * Symbol("cnt"),
            dataType = Some(LongType)).as("sum_c"))
        .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Aggregate expression's references from Alias") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .select(Symbol("b"), Symbol("c").as("new_c"))
      .groupBy(Symbol("b"))(sum(Symbol("new_c")).as("sum_new_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("a"), Symbol("b")),
      Seq(Symbol("a"), Symbol("b"), sum(Symbol("new_c")).as("_pushed_sum_new_c")),
      testRelation1.select(Symbol("a"), Symbol("b"), Symbol("c").as("new_c"))).as("l")
    val correctRight = PartialAggregate(Seq(Symbol("x")), Seq(Symbol("x"), count(1).as("cnt")),
      testRelation2.select(Symbol("x"))).as("r")

    val correctAnswer =
      correctLeft.join(correctRight,
          joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
        .select(Symbol("b"), Symbol("_pushed_sum_new_c"), Symbol("cnt"))
        .groupBy(Symbol("b"))(
          sumWithDataType(Symbol("_pushed_sum_new_c") * Symbol("cnt"),
            dataType = Some(LongType))
          .as("sum_new_c"))
        .analyzePlan

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  test("Skip partial aggregate if if can't reduce data") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("a"))(sum(Symbol("c")).as("sum_c"))
      .analyze

    val correctLeft = PartialAggregate(Seq(Symbol("a")), Seq(Symbol("a"),
      sum(Symbol("c")).as("_pushed_sum_c")),
      testRelation1.select(Symbol("a"), Symbol("c"))).as("l")
    val correctRight = PartialAggregate(Seq(Symbol("x")),
      Seq(Symbol("x"), count(1).as("cnt")),
      testRelation2.select(Symbol("x"))).as("r")

    val correctAnswer =
      FinalAggregate(
        Seq(Symbol("a")),
        Seq(sumWithDataType(Symbol("_pushed_sum_c") * Symbol("cnt"),
          dataType = Some(LongType)).as("sum_c")),
        correctLeft.join(
            correctRight, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
          .select(Symbol("a"), Symbol("_pushed_sum_c"), Symbol("cnt")))
        .analyze

    comparePlans(Optimize.execute(originalQuery), correctAnswer)
  }

  // The following tests are unsupported cases

  test("Do not push down count if grouping is empty") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy()(count(1).as("cnt"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down avg if grouping is empty") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy()(avg(Symbol("y")).as("avg_y"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down if the aggregate references from both left and right side") {
    val originalQuery1 = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b"))(sum(Symbol("c") + Symbol("y")).as("sum_c_y"))
      .analyze

    comparePlans(Optimize.execute(originalQuery1), ColumnPruning(originalQuery1))

    val originalQuery2 = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .select(Symbol("b"), (Symbol("c") + Symbol("y")).as("cy"))
      .groupBy(Symbol("b"))(sum(Symbol("cy")).as("sum_c_y"))
      .analyze

    comparePlans(Optimize.execute(originalQuery2), ColumnPruning(originalQuery2))
  }

  test("Do not push down if grouping references from left and right side") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b") + Symbol("y"))(sum(Symbol("z")).as("sum_z"))
      .analyze

    comparePlans(Optimize.execute(originalQuery),
      CollapseProject(ColumnPruning(PullOutGroupingExpressions(originalQuery))))
  }

  test("Do not push down if join condition is empty or contains unequal expression") {
    Seq(None, Some(Symbol("a") > Symbol("x"))).foreach { condition =>
      val originalQuery = testRelation1
        .join(testRelation2, joinType = Inner, condition = condition)
        .groupBy(Symbol("b"))(sum(Symbol("y")).as("sum_y"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
    }
  }

  test("Do not push down aggregate expressions if it's not Inner Join") {
    Seq(LeftOuter, RightOuter, FullOuter).foreach { joinType =>
      val originalQuery = testRelation1
        .join(testRelation2, joinType = joinType, condition = Some(Symbol("a") === Symbol("x")))
        .groupBy(Symbol("b"))(sum(Symbol("c")).as("sum_c"))
        .analyze

      comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
    }
  }

  test("Do not push down aggregate expressions if it's not pushable expression") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b"))(bitAnd(Symbol("c")).as("bitAnd_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down aggregate expressions if the aggregate leaves size exceeds 2") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b"))(
        sum(If(Symbol("y").likeAny("%a%"), Symbol("z") + 1, Symbol("z") + 2)).as("sum_z"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }

  test("Do not push down aggregate expressions if the aggregate filter is not empty") {
    val originalQuery = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(Symbol("a") === Symbol("x")))
      .groupBy(Symbol("b"))(sum(Symbol("c"), Some(Symbol("c") > 1)).as("sum_c"))
      .analyze

    comparePlans(Optimize.execute(originalQuery), ColumnPruning(originalQuery))
  }
}
