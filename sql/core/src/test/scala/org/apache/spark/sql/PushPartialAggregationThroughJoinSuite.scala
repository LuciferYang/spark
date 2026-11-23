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

package org.apache.spark.sql

import org.apache.spark.{SparkArithmeticException, SparkThrowable}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.logical.{FinalAggregate, LogicalPlan, PartialAggregate}
import org.apache.spark.sql.execution.{InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{AggUtils, HashAggregateExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

// Used to check result
class PushPartialAggregationThroughJoinSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    sql(
      """
        |CREATE TABLE store_sales(
        |  ss_item_sk INT,
        |  ss_sales_price DECIMAL(38,4)) USING parquet
      """.stripMargin)
    sql(
      """
        |CREATE TABLE item(
        |  i_item_sk INT,
        |  i_brand_id INT) USING parquet
      """.stripMargin)

    sql(
      """
        |INSERT INTO store_sales VALUES
        |  (1, 9999999999999999999999999999999999.6012),
        |  (1, 9999999999999999999999999999999999.9234),
        |  (1, 9999999999999999999999999999999999.2856),
        |  (2, 6874.6012),
        |  (2, 2828.9223),
        |  (2, 6067.6034),
        |  (2, 6067.6034),
        |  (2, null),
        |  (3, 999999999999999999999999999999999.2812),
        |  (3, 999999999999999999999999999999999.2823)
      """.stripMargin)

    sql(
      """
        |INSERT INTO item VALUES
        |  (1, 7003002),
        |  (1, 7003002),
        |  (2, 10002003),
        |  (2, 10002003),
        |  (2, 10002003),
        |  (3, 10002004),
        |  (3, 10002004),
        |  (3, 10002004),
        |  (3, 10002004)
      """.stripMargin)

    sql("ANALYZE TABLE store_sales COMPUTE STATISTICS FOR ALL COLUMNS")
    sql("ANALYZE TABLE item COMPUTE STATISTICS FOR ALL COLUMNS")

    conf.setConf(SQLConf.CBO_ENABLED, true)
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS store_sales")
      sql("DROP TABLE IF EXISTS item")

      conf.unsetConf(SQLConf.CBO_ENABLED)
    } finally {
      super.afterAll()
    }
  }

  test("sum") {
    Seq(-1, 1000000000L).foreach { broadcastThreshold =>
      Seq(true, false).foreach { pushAgg =>
        Seq(0.1, 0.5, 1.0).foreach { ratio =>
          Seq(true, false).foreach { ansi =>
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$broadcastThreshold",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> s"$pushAgg",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> s"$ratio",
              SQLConf.ANSI_ENABLED.key -> s"$ansi") {
              val df = sql(
                """
                  |SELECT
                  |  item.i_brand_id,
                  |  SUM(ss_sales_price)
                  |FROM store_sales, item
                  |WHERE store_sales.ss_item_sk = item.i_item_sk
                  |GROUP BY item.i_brand_id
                """.stripMargin)

              val optimizedPlan = df.queryExecution.optimizedPlan
              val partialAggCnt = optimizedPlan.collect { case p: PartialAggregate => p }.size
              if (pushAgg) {
                ratio match {
                  case 0.1 => assert(partialAggCnt === 0)
                  case 0.5 => assert(partialAggCnt === 1)
                  case 1.0 => assert(partialAggCnt === 2)
                }
              } else {
                assert(partialAggCnt === 0)
              }
              if (ansi) {
                val error = intercept[SparkArithmeticException] {
                  df.collect()
                }
                assert(error.toString contains "[ARITHMETIC_OVERFLOW] Overflow in sum of decimals")
              } else {
                checkAnswer(
                  df,
                  Row(10002004, BigDecimal("7999999999999999999999999999999994.2540")) ::
                    Row(10002003, BigDecimal("65516.1909")) ::
                    Row(7003002, null) :: Nil)
              }
            }
          }
        }
      }
    }
  }

  test("count") {
    Seq(-1, 1000000000L).foreach { broadcastThreshold =>
      Seq(true, false).foreach { pushAgg =>
        Seq(0.1, 0.5, 1.0).foreach { ratio =>
          Seq(true, false).foreach { ansi =>
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$broadcastThreshold",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> s"$pushAgg",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> s"$ratio",
              SQLConf.ANSI_ENABLED.key -> s"$ansi") {
              val df = sql(
                """
                  |SELECT
                  |  item.i_brand_id,
                  |  count(ss_sales_price),
                  |  count(*),
                  |  count(1),
                  |  count(2)
                  |FROM store_sales, item
                  |WHERE store_sales.ss_item_sk = item.i_item_sk
                  |GROUP BY item.i_brand_id
                """.stripMargin)

              val optimizedPlan = df.queryExecution.optimizedPlan
              val partialAggCnt = optimizedPlan.collect { case p: PartialAggregate => p }.size
              if (pushAgg) {
                ratio match {
                  case 0.1 => assert(partialAggCnt === 0)
                  case 0.5 => assert(partialAggCnt === 1)
                  case 1.0 => assert(partialAggCnt === 2)
                }
              } else {
                assert(partialAggCnt === 0)
              }
              checkAnswer(
                df,
                Row(10002004, 8, 8, 8, 8) ::
                  Row(10002003, 12, 15, 15, 15) ::
                  Row(7003002, 6, 6, 6, 6) :: Nil)
            }
          }
        }
      }
    }
  }

  test("first and last") {
    Seq(-1, 1000000000L).foreach { broadcastThreshold =>
      Seq(true, false).foreach { pushAgg =>
        Seq(0.1, 0.5, 1.0).foreach { ratio =>
          Seq(true, false).foreach { ansi =>
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$broadcastThreshold",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> s"$pushAgg",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> s"$ratio",
              SQLConf.ANSI_ENABLED.key -> s"$ansi") {
              val df = sql(
                """
                  |SELECT
                  |  item.i_brand_id,
                  |  first(ss_sales_price),
                  |  last(ss_sales_price)
                  |FROM store_sales, item
                  |WHERE store_sales.ss_item_sk = item.i_item_sk
                  |GROUP BY item.i_brand_id
                """.stripMargin)

              val optimizedPlan = df.queryExecution.optimizedPlan
              val partialAggCnt = optimizedPlan.collect { case p: PartialAggregate => p }.size
              if (pushAgg) {
                ratio match {
                  case 0.1 => assert(partialAggCnt === 0)
                  case 0.5 => assert(partialAggCnt === 1)
                  case 1.0 => assert(partialAggCnt === 2)
                }
              } else {
                assert(partialAggCnt === 0)
              }
              checkAnswer(
                df,
                Row(10002004, BigDecimal("999999999999999999999999999999999.2812"),
                  BigDecimal("999999999999999999999999999999999.2823")) ::
                  Row(10002003, BigDecimal("6874.6012"), null) ::
                  Row(7003002, BigDecimal("9999999999999999999999999999999999.6012"),
                    BigDecimal("9999999999999999999999999999999999.2856")) :: Nil)
            }
          }
        }
      }
    }
  }

  test("min and max") {
    Seq(-1, 1000000000L).foreach { broadcastThreshold =>
      Seq(true, false).foreach { pushAgg =>
        Seq(0.1, 0.5, 1.0).foreach { ratio =>
          Seq(true, false).foreach { ansi =>
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$broadcastThreshold",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> s"$pushAgg",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> s"$ratio",
              SQLConf.ANSI_ENABLED.key -> s"$ansi") {
              val df = sql(
                """
                  |SELECT
                  |  item.i_brand_id,
                  |  min(ss_sales_price),
                  |  max(ss_sales_price)
                  |FROM store_sales, item
                  |WHERE store_sales.ss_item_sk = item.i_item_sk
                  |GROUP BY item.i_brand_id
                """.stripMargin)

              val optimizedPlan = df.queryExecution.optimizedPlan
              val partialAggCnt = optimizedPlan.collect { case p: PartialAggregate => p }.size
              if (pushAgg) {
                ratio match {
                  case 0.1 => assert(partialAggCnt === 0)
                  case 0.5 => assert(partialAggCnt === 1)
                  case 1.0 => assert(partialAggCnt === 2)
                }
              } else {
                assert(partialAggCnt === 0)
              }
              checkAnswer(
                df,
                Row(10002004, BigDecimal("999999999999999999999999999999999.2812"),
                  BigDecimal("999999999999999999999999999999999.2823")) ::
                  Row(10002003, BigDecimal("2828.9223"), BigDecimal("6874.6012")) ::
                  Row(7003002, BigDecimal("9999999999999999999999999999999999.2856"),
                    BigDecimal("9999999999999999999999999999999999.9234")) :: Nil)
            }
          }
        }
      }
    }
  }

  test("avg") {
    Seq(-1, 1000000000L).foreach { broadcastThreshold =>
      Seq(true, false).foreach { pushAgg =>
        Seq(0.1, 0.5, 1.0).foreach { ratio =>
          Seq(true, false).foreach { ansi =>
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$broadcastThreshold",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> s"$pushAgg",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> s"$ratio",
              SQLConf.ANSI_ENABLED.key -> s"$ansi") {
              val df = sql(
                """
                  |SELECT
                  |  item.i_brand_id,
                  |  avg(ss_sales_price)
                  |FROM store_sales, item
                  |WHERE store_sales.ss_item_sk = item.i_item_sk
                  |GROUP BY item.i_brand_id
                """.stripMargin)

              val optimizedPlan = df.queryExecution.optimizedPlan
              val partialAggCnt = optimizedPlan.collect { case p: PartialAggregate => p }.size
              if (pushAgg) {
                ratio match {
                  case 0.1 => assert(partialAggCnt === 0)
                  case 0.5 => assert(partialAggCnt === 1)
                  case 1.0 => assert(partialAggCnt === 2)
                }
              } else {
                assert(partialAggCnt === 0)
              }
              if (ansi) {
                intercept[SparkArithmeticException] {
                  df.collect()
                }
                // NUMERIC_VALUE_OUT_OF_RANGE
                // [ARITHMETIC_OVERFLOW] Overflow in sum of decimals
              } else {
                checkAnswer(
                  df,
                  Row(10002004, null) ::
                    Row(10002003, BigDecimal("5459.68257500")) ::
                    Row(7003002, null) :: Nil)
              }
            }
          }
        }
      }
    }
  }

  test("aggregate with Literal") {
    Seq(-1, 1000000000L).foreach { broadcastThreshold =>
      Seq(true, false).foreach { pushAgg =>
        Seq(0.1, 0.5, 1.0).foreach { ratio =>
          Seq(true, false).foreach { ansi =>
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$broadcastThreshold",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> s"$pushAgg",
              SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> s"$ratio",
              SQLConf.ANSI_ENABLED.key -> s"$ansi") {
              val df = sql(
                """
                  |SELECT
                  |  item.i_brand_id,
                  |  count(1),
                  |  count(2),
                  |  sum(0),
                  |  sum(1),
                  |  sum(2),
                  |  sum(2.5),
                  |  sum(3.5BD),
                  |  min(1),
                  |  min(2),
                  |  max(1),
                  |  max(2),
                  |  first(1),
                  |  first(2),
                  |  last(1),
                  |  last(2),
                  |  avg(1),
                  |  avg(2)
                  |FROM store_sales, item
                  |WHERE store_sales.ss_item_sk = item.i_item_sk
                  |GROUP BY item.i_brand_id
                """.stripMargin)

              val optimizedPlan = df.queryExecution.optimizedPlan
              val partialAggCnt = optimizedPlan.collect { case p: PartialAggregate => p }.size
              if (pushAgg) {
                ratio match {
                  case 0.1 => assert(partialAggCnt === 0)
                  case 0.5 => assert(partialAggCnt === 1)
                  case 1.0 => assert(partialAggCnt === 2)
                }
              } else {
                assert(partialAggCnt === 0)
              }
              checkAnswer(
                df,
                Row(10002004, 8, 8, 0, 8, 16, 20.0, 28.0, 1, 2, 1, 2, 1, 2, 1, 2, 1.0, 2.0) ::
                  Row(10002003, 15, 15, 0, 15, 30, 37.5, 52.5, 1, 2, 1, 2, 1, 2, 1, 2, 1.0, 2.0) ::
                  Row(7003002, 6, 6, 0, 6, 12, 15.0, 21.0, 1, 2, 1, 2, 1, 2, 1, 2, 1.0, 2.0) :: Nil)
            }
          }
        }
      }
    }
  }

  Seq("count(1)", "count(*)", "sum(2)", "min(2)", "max(2)", "first(2)", "last(2)",
    "avg(2)").foreach { aggExp =>
    test(s"query empty result: $aggExp") {
      Seq(-1, 1000000000L).foreach { broadcastThreshold =>
        Seq(true, false).foreach { pushAgg =>
          Seq(0.1, 0.5, 1.0).foreach { ratio =>
            Seq(true, false).foreach { ansi =>
              withSQLConf(
                SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$broadcastThreshold",
                SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> s"$pushAgg",
                SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> s"$ratio",
                SQLConf.ANSI_ENABLED.key -> s"$ansi") {
                val df = sql(
                  s"""
                     |SELECT
                     |  $aggExp
                     |FROM store_sales, item
                     |WHERE store_sales.ss_item_sk = item.i_item_sk AND ss_item_sk < 0
                  """.stripMargin)

                val optimizedPlan = df.queryExecution.optimizedPlan
                if (aggExp.startsWith("cast(count") || aggExp.startsWith("count")) {
                  assert(!optimizedPlan.exists(_.isInstanceOf[PartialAggregate]))
                  checkAnswer(df, Row(0) :: Nil)
                } else {
                  val optimizedPlan = df.queryExecution.optimizedPlan
                  val partialAggCnt = optimizedPlan.collect { case p: PartialAggregate => p }.size
                  if (broadcastThreshold == -1 && pushAgg) {
                    ratio match {
                      case 0.1 | 0.5 => assert(partialAggCnt === 0)
                      case 1.0 => assert(partialAggCnt === 2)
                    }
                  } else {
                    assert(partialAggCnt === 0)
                  }
                  checkAnswer(df, Row(null) :: Nil)
                }
              }
            }
          }
        }
      }
    }

    test(s"query empty result and cast to string type: $aggExp") {
      Seq(-1, 1000000000L).foreach { broadcastThreshold =>
        Seq(true, false).foreach { pushAgg =>
          Seq(0.1, 0.5, 1.0).foreach { ratio =>
            Seq(true, false).foreach { ansi =>
              withSQLConf(
                SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$broadcastThreshold",
                SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> s"$pushAgg",
                SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> s"$ratio",
                SQLConf.ANSI_ENABLED.key -> s"$ansi") {
                val df = sql(
                  s"""
                     |SELECT
                     |  cast($aggExp as string)
                     |FROM store_sales, item
                     |WHERE store_sales.ss_item_sk = item.i_item_sk AND ss_item_sk < 0
                  """.stripMargin)

                val optimizedPlan = df.queryExecution.optimizedPlan
                if (aggExp.startsWith("count")) {
                  assert(!optimizedPlan.exists(_.isInstanceOf[PartialAggregate]))
                  checkAnswer(df, Row("0") :: Nil)
                } else {
                  val optimizedPlan = df.queryExecution.optimizedPlan
                  val partialAggCnt = optimizedPlan.collect { case p: PartialAggregate => p }.size
                  if (broadcastThreshold == -1 && pushAgg) {
                    ratio match {
                      case 0.1 | 0.5 => assert(partialAggCnt === 0)
                      case 1.0 => assert(partialAggCnt === 2)
                    }
                  } else {
                    assert(partialAggCnt === 0)
                  }
                  checkAnswer(df, Row(null) :: Nil)
                }
              }
            }
          }
        }
      }
    }
  }

  test("adaptive partial aggregation pass-through keeps the results of pushed aggregates") {
    val groupOnly =
      "SELECT i_brand_id FROM store_sales JOIN item ON ss_item_sk = i_item_sk GROUP BY i_brand_id"
    val semi = "SELECT ss_item_sk, ss_sales_price FROM store_sales " +
      "LEFT SEMI JOIN item ON ss_item_sk = i_item_sk"
    val anti = "SELECT ss_item_sk, ss_sales_price FROM store_sales " +
      "LEFT ANTI JOIN item ON ss_item_sk = i_item_sk AND i_brand_id = 7003002"
    // first/last read i_item_sk, which i_brand_id determines, so their result is deterministic.
    val aggregates =
      """
        |SELECT i_brand_id, sum(ss_sales_price), count(ss_sales_price), avg(ss_sales_price),
        |  min(ss_sales_price), max(ss_sales_price), first(i_item_sk), last(i_item_sk)
        |FROM store_sales JOIN item ON ss_item_sk = i_item_sk
        |GROUP BY i_brand_id
      """.stripMargin
    val baseConf = Seq(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0")
    Seq(groupOnly, semi, anti, aggregates).foreach { query =>
      val expected = withSQLConf(baseConf :+
          (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false"): _*) {
        sql(query).collect().toSeq
      }
      Seq(true, false).foreach { codegen =>
        withSQLConf(baseConf ++ Seq(
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString,
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "1",
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key -> "1000000"): _*) {
          val df = sql(query)
          assert(df.queryExecution.optimizedPlan.exists(_.isInstanceOf[PartialAggregate]))
          checkAnswer(df, expected)
          val aggs = collect(df.queryExecution.executedPlan) { case a: HashAggregateExec => a }
          def passedThrough(a: HashAggregateExec): Option[Long] =
            a.metrics.get("numPassthroughRows").map(_.value)
          val (pushed, others) =
            aggs.partition(_.logicalLink.exists(_.isInstanceOf[PartialAggregate]))
          assert(pushed.nonEmpty)
          assert(pushed.forall(a => passedThrough(a).nonEmpty))
          assert(pushed.forall(_.aggregateExpressions.nonEmpty === (query == aggregates)))
          if (query == aggregates) {
            // The Partial aggregate above the join passes rows through. The pushed aggregates
            // follow `spark.sql.optimizer.partialAggregationOptimization.adaptive.enabled`, which
            // skips the first check, so these few rows are not passed through; the next test
            // passes them.
            assert(others.flatMap(passedThrough).sum > 0)
          } else {
            // A pushed aggregate without aggregate functions follows SPARK-58511.
            assert(pushed.flatMap(passedThrough).sum > 0)
          }
        }
      }
    }
  }

  // Two tables of 1000 rows in one file each, read as one partition in row order. `a` repeats
  // every 500 rows, so rows 500 to 699 bring the keys of the first 200 rows again. `w` overflows at
  // key 50, from rows 50 and 550; rows 650 and 651 hold a null `v` and a null `w`. `z` is
  // determined by `y`. Without statistics, the check window of the pushed aggregates is `minRows`.
  private def withNonReducingTables(f: => Unit): Unit = {
    withTable("pagg_t1", "pagg_t2") {
      spark.range(0, 1000, 1, 1).selectExpr(
        "CAST(id % 500 AS INT) a",
        "IF(id % 1000 IN (7, 650), NULL, CAST(id % 7 AS INT)) v",
        "IF(id % 1000 = 651, NULL, IF(id % 500 = 50, 9223372036854775807L, id)) w")
        .write.saveAsTable("pagg_t1")
      spark.range(0, 1000, 1, 1).selectExpr("CAST(id AS INT) x", "CAST(id % 5 AS INT) y",
        "CAST(id % 5 AS INT) * 2 z").write.saveAsTable("pagg_t2")
      f
    }
  }

  private def pushedPassedThrough(df: DataFrame): Seq[Option[Long]] =
    collect(df.queryExecution.executedPlan) {
      case a: HashAggregateExec if a.logicalLink.exists(_.isInstanceOf[PartialAggregate]) =>
        a.metrics.get("numPassthroughRows").map(_.value)
    }

  test("a pushed aggregate that does not reduce its input passes rows through") {
    // The pushed aggregates keep nearly every row, and pass through after the second check, at
    // row 200. A key of the first 200 rows is then split between the map and a passed-through row.
    withNonReducingTables {
      val query = "SELECT y, sum(v), count(v), count(*), avg(v), min(v), max(v), try_sum(w), " +
        "first(z), last(z) FROM pagg_t1 JOIN pagg_t2 ON a = x GROUP BY y"
      val semiQuery = "SELECT a FROM pagg_t1 LEFT SEMI JOIN pagg_t2 ON a = x"
      val baseConf = Seq(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0",
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "100")
      val expected = withSQLConf(baseConf :+
          (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false"): _*) {
        sql(query).collect().toSeq
      }
      for (adaptive <- Seq(true, false); upstream <- Seq(true, false);
          codegen <- Seq(true, false)) {
        withSQLConf(baseConf ++ Seq(
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ADAPTIVE_ENABLED.key -> adaptive.toString,
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> upstream.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString): _*) {
          val df = sql(query)
          withClue(s"adaptive $adaptive, SPARK-58511 $upstream, codegen $codegen") {
            checkAnswer(df, expected)
            // 1000 rows on each side, of which the first 200 are aggregated.
            assert(pushedPassedThrough(df) === Seq.fill(2)(if (adaptive) Some(800L) else None))
            // A copy loses the tag that makes the pushed aggregate eligible.
            val pushed = collect(df.queryExecution.executedPlan) {
              case a: HashAggregateExec if a.logicalLink.exists(_.isInstanceOf[PartialAggregate]) =>
                a
            }
            assert(!pushed.head.copy().metrics.contains("numPassthroughRows"))
            // The deduplicated right side of a semi join has no aggregate functions, so SPARK-58511
            // admits it too and its rule applies: it judges the whole epoch at row 100.
            val semi = sql(semiQuery)
            checkAnswer(semi, spark.range(0, 1000).selectExpr("CAST(id % 500 AS INT)"))
            assert(pushedPassedThrough(semi) ===
              Seq(if (upstream) Some(900L) else if (adaptive) Some(800L) else None))
          }
        }
      }
      // With column statistics the window of pagg's rule is the distinct count of `x`, all 1000
      // rows, so the second check never comes; SPARK-58511's rule keeps its window of `minRows`.
      sql("ANALYZE TABLE pagg_t2 COMPUTE STATISTICS FOR ALL COLUMNS")
      for (upstream <- Seq(true, false); codegen <- Seq(true, false)) {
        withSQLConf(baseConf ++ Seq(
            SQLConf.CBO_ENABLED.key -> "true",
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> upstream.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString): _*) {
          withClue(s"analyzed, SPARK-58511 $upstream, codegen $codegen") {
            val semi = sql(semiQuery)
            checkAnswer(semi, spark.range(0, 1000).selectExpr("CAST(id % 500 AS INT)"))
            val dedup = collect(semi.queryExecution.executedPlan) {
              case a: HashAggregateExec if a.logicalLink.exists(_.isInstanceOf[PartialAggregate]) =>
                a
            }
            assert(dedup.size === 1)
            assert(dedup.head.getTagValue(AggUtils.PUSHED_PARTIAL_AGGREGATE).exists(_ >= 900))
            assert(dedup.head.metrics.get("numPassthroughRows").map(_.value) ===
              Some(if (upstream) 900L else 0L))
          }
        }
      }
    }
  }

  test("a pushed aggregate on the streamed side of a broadcast join passes rows through") {
    // The pushed aggregate on `pagg_t1` runs in the codegen stage of the join and of the partial
    // aggregate above it, which passes rows through as well under SPARK-58511. A join that could be
    // broadcast is pushed through only when its estimate expands its larger side, so each `x`
    // matches 20 rows of `pagg_t2`, and the tables are analyzed.
    withTable("pagg_t1", "pagg_t2") {
      // Two files of 2000 rows. Every (a, g) of a file is distinct, so the pushed aggregate on
      // `pagg_t1` keeps every row, and passes through after its second check.
      spark.range(0, 4000, 1, 2).selectExpr("CAST(id % 500 AS INT) a", "CAST(id % 7 AS INT) g",
        "IF(id % 2000 IN (7, 1650), NULL, CAST(id % 3 AS INT)) v", "id w")
        .write.saveAsTable("pagg_t1")
      spark.range(0, 2000, 1, 1).selectExpr("CAST(id % 100 AS INT) x", "CAST(id % 5 AS INT) y",
        "CAST(id % 5 AS INT) * 2 z").write.saveAsTable("pagg_t2")
      Seq("pagg_t1", "pagg_t2").foreach { t =>
        sql(s"ANALYZE TABLE $t COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      val query = "SELECT /*+ BROADCAST(pagg_t2) */ y, g, sum(v), count(v), count(*), avg(v), " +
        "try_sum(w), first(z) FROM pagg_t1 JOIN pagg_t2 ON a = x GROUP BY y, g"
      val baseConf = Seq(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> (1L << 30).toString,
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0",
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "100",
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key -> "1000000")
      val expected = withSQLConf(baseConf :+
          (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false"): _*) {
        sql(query).collect().toSeq
      }
      for (upstream <- Seq(true, false); codegen <- Seq(true, false)) {
        withSQLConf(baseConf ++ Seq(
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> upstream.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString): _*) {
          val df = sql(query)
          withClue(s"SPARK-58511 $upstream, codegen $codegen") {
            checkAnswer(df, expected)
            val plan = df.queryExecution.executedPlan
            val join = collect(plan) { case j: BroadcastHashJoinExec => j }
            assert(join.size === 1)
            assert(join.head.buildSide === BuildRight)
            val streamed = join.head.left.collectFirst { case a: HashAggregateExec => a }
            assert(streamed.exists(_.logicalLink.exists(_.isInstanceOf[PartialAggregate])))
            // The check window holds the distinct count of `a`, about 500 rows, and the rows after
            // the second check of each partition are passed through.
            val window = streamed.get.getTagValue(AggUtils.PUSHED_PARTIAL_AGGREGATE).get.max(100L)
            assert(window >= 450 && window <= 550)
            assert(streamed.get.metrics.get("numPassthroughRows").map(_.value) ===
              Some(2 * (2000 - 2 * window)))
            // `x` repeats every 100 rows, so the pushed aggregate on `pagg_t2` keeps reducing.
            val build = collect(plan) {
              case a: HashAggregateExec if a.logicalLink.exists(_.isInstanceOf[PartialAggregate]) &&
                  !(a eq streamed.get) => a
            }
            assert(build.map(_.metrics.get("numPassthroughRows").map(_.value)) === Seq(Some(0L)))
            val partial = collect(plan) {
              case a: HashAggregateExec if a.aggregateExpressions.exists(_.mode == Partial) => a
            }
            assert(partial.size === 1)
            // 400 rows of each partition have an `a` below 100 and match one pushed group of
            // `pagg_t2`; the first 100 of them are aggregated.
            assert(partial.head.metrics.get("numPassthroughRows").map(_.value) ===
              (if (upstream) Some(600L) else None))
            if (codegen) {
              def inStage(p: SparkPlan): Seq[SparkPlan] = p +: (p match {
                case _: InputAdapter => Nil
                case _ => p.children.flatMap(inStage)
              })
              val stage = collect(plan) {
                case w: WholeStageCodegenExec if inStage(w.child).exists(_ eq partial.head) => w
              }
              assert(stage.size === 1 && inStage(stage.head.child).exists(_ eq streamed.get))
            }
          }
        }
      }
    }
  }

  test("a broadcast join whose estimate has no join key statistics is not pushed through") {
    // An INT key joined to a BIGINT key is cast, and the join estimate falls back to a cross
    // product, which would make a broadcast join look like it multiplies its larger side. Before
    // ANALYZE the join has no row count at all, which is treated the same way. A join estimated
    // from its keys at less than twice its larger side is not pushed through either.
    withTable("pagg_fact", "pagg_dim_int", "pagg_dim_long", "pagg_dim_few") {
      sql("CREATE TABLE pagg_fact(a INT, b INT, c INT) USING parquet")
      sql("INSERT INTO pagg_fact VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30), (2, 2, 40), " +
        "(1, 1, 50), (2, 2, 60)")
      sql("CREATE TABLE pagg_dim_int(x INT) USING parquet")
      sql("INSERT INTO pagg_dim_int VALUES (1), (1), (1), (2), (2), (2)")
      sql("CREATE TABLE pagg_dim_long(x BIGINT) USING parquet")
      sql("INSERT INTO pagg_dim_long VALUES (1), (1), (1), (2), (2), (2)")
      // Joined with pagg_fact on 2 distinct keys: 6 * 3 / 2 = 9 rows, not twice its 6 rows.
      sql("CREATE TABLE pagg_dim_few(x INT) USING parquet")
      sql("INSERT INTO pagg_dim_few VALUES (1), (1), (2)")
      def check(cases: Seq[(String, String, Boolean)], analyzed: Boolean): Unit = {
        cases.foreach { case (dim, threshold, pushed) =>
          val query = s"SELECT b, sum(c) FROM pagg_fact JOIN $dim ON a = x GROUP BY b"
          val confs = Seq(
            SQLConf.CBO_ENABLED.key -> "true",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold,
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0")
          val expected = withSQLConf(confs :+
              (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false"): _*) {
            sql(query).collect().toSeq
          }
          val on = confs :+ (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true")
          withSQLConf(on: _*) {
            val df = sql(query)
            withClue(s"$query, broadcast threshold $threshold, analyzed $analyzed") {
              checkAnswer(df, expected)
              assert(df.queryExecution.optimizedPlan.exists(_.isInstanceOf[PartialAggregate]) ===
                pushed)
            }
          }
        }
      }
      // Without broadcast the join is pushed, so only the broadcast check stops it.
      check(Seq(("pagg_dim_int", "10MB", false), ("pagg_dim_int", "-1", true)), analyzed = false)
      Seq("pagg_fact", "pagg_dim_int", "pagg_dim_long", "pagg_dim_few").foreach { t =>
        sql(s"ANALYZE TABLE $t COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      check(Seq(("pagg_dim_int", "10MB", true), ("pagg_dim_long", "10MB", false),
        ("pagg_dim_long", "-1", true), ("pagg_dim_few", "10MB", false),
        ("pagg_dim_few", "-1", true)), analyzed = true)
    }
  }

  test("a count in a correlated scalar subquery is 0 for an outer row without matches") {
    // Decorrelation groups the subquery on `id`, and `RewriteCorrelatedScalarSubquery` takes the
    // value of the subquery's aggregate on zero rows for t1.id = 2. By default `OptimizeSubqueries`
    // keeps that aggregate out of the subquery's optimization, and the rule rewrites it only after
    // the subquery is merged into the main plan; with the conf off the rule rewrites it first.
    withTable("pagg_o", "pagg_s", "pagg_k") {
      sql("CREATE TABLE pagg_o(id INT) USING parquet")
      sql("INSERT INTO pagg_o VALUES (1), (2)")
      sql("CREATE TABLE pagg_s(id INT, k INT, w INT) USING parquet")
      sql("INSERT INTO pagg_s VALUES (1, 7, 5), (1, 7, NULL)")
      sql("CREATE TABLE pagg_k(k INT) USING parquet")
      sql("INSERT INTO pagg_k VALUES (7), (7)")
      // A HAVING clause takes the other count-bug compensation of
      // `RewriteCorrelatedScalarSubquery`.
      val subqueries = Seq(
        "count(*)" -> "", "count(w)" -> "", "count(*)" -> " HAVING count(*) = 0")
      for ((count, having) <- subqueries; keepAggregate <- Seq(true, false)) {
        withSQLConf(SQLConf.DECORRELATE_SUBQUERY_PREVENT_CONSTANT_FOLDING_FOR_COUNT_BUG.key ->
            keepAggregate.toString) {
          checkSameResultWithPagg(s"SELECT pagg_o.id, (SELECT $count FROM pagg_s " +
            s"JOIN pagg_k ON pagg_s.k = pagg_k.k WHERE pagg_s.id = pagg_o.id$having) FROM pagg_o")
        }
      }
    }
  }

  test("streaming queries are not rewritten") {
    withTable("pagg_dim") {
      sql("CREATE TABLE pagg_dim(k INT, v INT) USING parquet")
      sql("INSERT INTO pagg_dim VALUES (1, 10), (1, 11), (2, 20)")
      sql("ANALYZE TABLE pagg_dim COMPUTE STATISTICS FOR ALL COLUMNS")
      // The memory stream has no row count, so a join that could be broadcast would not be pushed
      // through for that reason alone.
      withSQLConf(
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
        import testImplicits._
        val input = MemoryStream[Int](0, spark)
        val counts = input.toDF().toDF("k").join(spark.table("pagg_dim"), "k")
          .groupBy("k").agg(count("*"))
        val query = counts.writeStream.format("memory").queryName("pagg_stream")
          .outputMode("complete").start()
        try {
          input.addData(1, 2)
          query.processAllAvailable()
          input.addData(1)
          query.processAllAvailable()
          // The second batch adds two joined rows for k = 1 to the state of the first.
          checkAnswer(spark.table("pagg_stream"), Row(1, 4) :: Row(2, 1) :: Nil)
          val plan =
            query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.optimizedPlan
          assert(!plan.exists {
            case _: PartialAggregate | _: FinalAggregate => true
            case _ => false
          })
        } finally {
          query.stop()
          spark.catalog.dropTempView("pagg_stream")
        }
      }
    }
  }

  private def resultOrError(query: String, confs: (String, String)*): Either[String, DataFrame] =
    withSQLConf(confs: _*) {
      try {
        val df = sql(query)
        Right(spark.createDataFrame(spark.sparkContext.parallelize(df.collect().toSeq), df.schema))
      } catch {
        case e: SparkThrowable =>
          Left(Option(e.getCondition).getOrElse(s"${e.getClass.getName}: ${e.getMessage}"))
      }
    }

  // Each case is (column type, t1 rows as (a, b, c), t2 keys, aggregate over c).
  // With a benefit ratio below 1, the tables are analyzed so that each side is pushed by its own
  // estimated reduction, and `pushedSide`, if set, names an output column of the only pushed side.
  private def checkPushedMatchesUnpushed(
      cases: Seq[(String, Seq[(Int, Int, String)], Seq[Int], String)],
      benefitRatio: Double = 1.0,
      pushedSide: Option[String] = None): Unit = {
    cases.foreach { case (cType, t1Rows, t2Keys, agg) =>
      withTable("pagg_t1", "pagg_t2") {
        sql(s"CREATE TABLE pagg_t1(a INT, b INT, c $cType) USING parquet")
        sql("CREATE TABLE pagg_t2(x INT) USING parquet")
        val values = t1Rows.map { case (a, b, c) => s"($a, $b, CAST($c AS $cType))" }
        // One file per table: a pushed aggregate only aggregates within a partition, so rows
        // spread over several files would not reach the overflow in the pushed aggregate.
        sql("INSERT INTO pagg_t1 SELECT /*+ COALESCE(1) */ * FROM VALUES " +
          s"${values.mkString(", ")} AS v(a, b, c)")
        sql("INSERT INTO pagg_t2 SELECT /*+ COALESCE(1) */ * FROM VALUES " +
          s"${t2Keys.map(k => s"($k)").mkString(", ")} AS v(x)")
        assert(spark.table("pagg_t1").inputFiles.length === 1)
        assert(spark.table("pagg_t2").inputFiles.length === 1)
        if (benefitRatio < 1.0) {
          sql("ANALYZE TABLE pagg_t1 COMPUTE STATISTICS FOR ALL COLUMNS")
          sql("ANALYZE TABLE pagg_t2 COMPUTE STATISTICS FOR ALL COLUMNS")
        }
        val query = s"SELECT b, $agg FROM pagg_t1 JOIN pagg_t2 ON a = x GROUP BY b"
        Seq("true", "false").foreach { ansi =>
          val confs = Seq(
            SQLConf.ANSI_ENABLED.key -> ansi,
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> benefitRatio.toString)
          val on = confs :+ (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true")
          withSQLConf(on: _*) {
            val pushed = sql(query).queryExecution.optimizedPlan.collect {
              case p: PartialAggregate => p
            }
            assert(pushed.nonEmpty)
            pushedSide.foreach { c =>
              assert(pushed.size === 1 && pushed.head.output.exists(_.name == c))
            }
          }
          val expected = resultOrError(query,
            confs :+ (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false"): _*)
          val actual = resultOrError(query, on: _*)
          withClue(s"$cType $agg ansi=$ansi: ") {
            (expected, actual) match {
              case (Right(e), Right(a)) =>
                assert(a.schema === e.schema)
                checkAnswer(a, e.collect().toSeq)
              // Both fail on an overflow. The error class can differ: an overflow can surface in
              // the pushed multiply instead of in the sum.
              case (Left(e), Left(a)) =>
                Seq(e, a).foreach { c =>
                  assert(Seq("ARITHMETIC_OVERFLOW", "NUMERIC_VALUE_OUT_OF_RANGE", "CAST_OVERFLOW")
                    .exists(c.startsWith), c)
                }
              case _ => assert(actual.map(_.collect().toSeq) === expected.map(_.collect().toSeq))
            }
          }
        }
      }
    }
  }

  test("pushed avg over decimals keeps the result of avg") {
    checkPushedMatchesUnpushed(Seq(
      // A group whose values are all null.
      ("DECIMAL(10,2)", Seq((1, 1, "NULL"), (1, 1, "NULL"), (2, 2, "3.00")), Seq(1, 1, 2),
        "avg(c)"),
      // The result scale of avg over DECIMAL(38,4) is 8.
      ("DECIMAL(38,4)", Seq((1, 1, "1.0000"), (1, 1, "0"), (1, 1, "0")), Seq(1, 1), "avg(c)"),
      // The result of avg over DECIMAL(38,0) holds 34 integer digits.
      ("DECIMAL(38,0)", Seq((1, 1, "1e33"), (1, 1, "1e33")), Seq(1), "avg(c)")))
  }

  test("pushed sum over decimals keeps the scale of the sum") {
    checkPushedMatchesUnpushed(Seq(
      ("DECIMAL(20,10)", Seq((1, 1, "0.0000000001")), Seq(1, 1), "sum(c)"),
      ("DECIMAL(20,10)", Seq((1, 1, "0.0000000001")), Seq(1, 1), "try_sum(c)")))
  }

  test("pushed sum and avg return null on overflow like sum and avg") {
    val big = Seq((1, 1, "6e37"), (1, 1, "6e37"), (2, 1, "5"))
    checkPushedMatchesUnpushed(Seq(
      // The pushed sum of a = 1 overflows.
      ("DECIMAL(38,0)", big, Seq(1, 2), "sum(c)"),
      ("DECIMAL(38,0)", big, Seq(1, 2), "try_sum(c)"),
      ("DECIMAL(38,0)", big, Seq(1, 2), "avg(c)"),
      ("BIGINT", Seq((1, 1, "9223372036854775807"), (1, 1, "1"), (2, 1, "5")), Seq(1, 2),
        "try_sum(c)"),
      // The pushed sum of a = 1 fits, its product with the count of x = 1 overflows.
      ("DECIMAL(38,0)", Seq((1, 1, "6e37"), (2, 1, "5")), Seq(1, 1, 2), "sum(c)"),
      // The pushed sum of a = 1 is null because all its values are, which is not an overflow.
      ("DECIMAL(38,0)", Seq((1, 1, "NULL"), (2, 1, "5")), Seq(1, 2), "sum(c)"),
      ("BIGINT", Seq((1, 1, "NULL"), (2, 1, "5")), Seq(1, 2), "try_sum(c)")))
  }

  test("a sum that is not pushed returns null on overflow when the other side is pushed") {
    // (a, b) is unique in pagg_t1, so only pagg_t2 is pushed, and the sum above reads c * cnt.
    // The product for a = 1 overflows.
    checkPushedMatchesUnpushed(Seq(
      ("DECIMAL(38,0)", Seq((1, 1, "6e37"), (2, 1, "5")), Seq(1, 1, 1, 2), "sum(c)"),
      ("DECIMAL(38,0)", Seq((1, 1, "6e37"), (2, 1, "5")), Seq(1, 1, 1, 2), "try_sum(c)"),
      // c is null for a = 1, which is not an overflow.
      ("DECIMAL(38,0)", Seq((1, 1, "NULL"), (2, 1, "5")), Seq(1, 1, 1, 2), "sum(c)"),
      ("BIGINT", Seq((1, 1, "9223372036854775807"), (2, 1, "5")), Seq(1, 1, 1, 2), "try_sum(c)"),
      // Aggregates other than sum read c on the side that is not pushed.
      ("INT", Seq((1, 1, "NULL"), (2, 1, "5")), Seq(1, 1, 1, 2), "count(c)"),
      ("INT", Seq((1, 1, "3"), (2, 1, "5")), Seq(1, 1, 1, 2), "max(c)"),
      ("INT", Seq((1, 1, "3"), (2, 1, "5")), Seq(1, 1, 1, 2), "min(c)")),
      benefitRatio = 0.5, pushedSide = Some("x"))
  }

  test("a pushed sum returns null on overflow when the other side is not pushed") {
    // (a, b) has two values in four rows of pagg_t1, so only pagg_t1 is pushed. The pushed sum
    // for a = 1 overflows.
    val rows = Seq((1, 1, "6e37"), (1, 1, "6e37"), (2, 1, "5"), (2, 1, "5"))
    checkPushedMatchesUnpushed(Seq(
      ("DECIMAL(38,0)", rows, Seq(1, 2), "sum(c)"),
      ("DECIMAL(38,0)", rows, Seq(1, 2), "try_sum(c)"),
      ("DECIMAL(38,0)", rows, Seq(1, 2), "avg(c)"),
      // A literal sum: four joined rows of 4e37 overflow, the rows of a = 2 alone do not.
      ("INT", Seq((1, 1, "1"), (1, 1, "1"), (1, 1, "1"), (2, 1, "1")), Seq(1, 2),
        "sum(CAST('4e37' AS DECIMAL(38,0)))")),
      benefitRatio = 0.5, pushedSide = Some("a"))
  }

  // Checks that pagg changes the plan of `query`, as `rewrite` sees it, but not its result. At
  // a benefit ratio of 1 the benefit check passes for every side.
  private def checkSameResultWithPagg(
      query: String,
      pushed: Boolean = true,
      rewrite: LogicalPlan => Boolean = _.exists {
        case _: PartialAggregate | _: FinalAggregate => true
        case _ => false
      }): Unit = {
    val confs = Seq(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0")
    val expected = withSQLConf(confs :+
        (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false"): _*) {
      sql(query).collect().toSeq
    }
    withSQLConf(confs :+ (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true"): _*) {
      val df = sql(query)
      withClue(query) {
        checkAnswer(df, expected)
        assert(rewrite(df.queryExecution.optimizedPlan) === pushed)
      }
    }
  }

  test("a join side without output columns is not deduplicated") {
    withTable("pagg_l", "pagg_r") {
      sql("CREATE TABLE pagg_l(a INT) USING parquet")
      sql("CREATE TABLE pagg_r(y INT) USING parquet")
      sql("INSERT INTO pagg_l VALUES (1), (2)")
      // Two files, so a global aggregate over pagg_r would output two rows.
      sql("INSERT INTO pagg_r VALUES (1), (2)")
      sql("INSERT INTO pagg_r VALUES (3)")
      Seq(
        "SELECT * FROM pagg_l LEFT ANTI JOIN pagg_r ON pagg_r.y > 100",
        "SELECT * FROM pagg_l LEFT SEMI JOIN pagg_r ON pagg_r.y > 100",
        "SELECT DISTINCT pagg_l.a FROM pagg_l CROSS JOIN pagg_r WHERE pagg_r.y > 100",
        "SELECT * FROM pagg_l WHERE EXISTS (SELECT 1 FROM pagg_r WHERE y > 100 AND pagg_l.a > 0)",
        "SELECT DISTINCT v.c FROM VALUES (1) AS v(c) CROSS JOIN (SELECT y FROM pagg_r WHERE y > 0)"
      ).foreach { q =>
        // A side with output columns may still be deduplicated.
        checkSameResultWithPagg(q, pushed = false, rewrite = _.exists {
          case p: PartialAggregate => p.output.isEmpty
          case _ => false
        })
      }
    }
  }

  test("a final aggregate normalizes floating-point grouping keys") {
    withTable("pagg_t1", "pagg_t2") {
      sql("CREATE TABLE pagg_t1(a INT, b DOUBLE, c INT) USING parquet")
      sql("CREATE TABLE pagg_t2(x INT) USING parquet")
      sql("INSERT INTO pagg_t1 VALUES (1, 0.0, 10), (1, CAST('-0.0' AS DOUBLE), 20)")
      sql("INSERT INTO pagg_t2 VALUES (1)")
      val finalAggregate = (_: LogicalPlan).exists(_.isInstanceOf[FinalAggregate])
      checkSameResultWithPagg(
        "SELECT a, b, sum(c) FROM pagg_t1 JOIN pagg_t2 ON a = x GROUP BY a, b",
        rewrite = finalAggregate)
      checkSameResultWithPagg(
        "SELECT a, named_struct('b', b), sum(c) FROM pagg_t1 JOIN pagg_t2 ON a = x " +
          "GROUP BY a, named_struct('b', b)",
        rewrite = finalAggregate)
    }
  }

  test("distinct is not pushed below a non-deterministic projection") {
    withTable("pagg_t1", "pagg_t2") {
      sql("CREATE TABLE pagg_t1(a INT) USING parquet")
      sql("CREATE TABLE pagg_t2(x INT) USING parquet")
      // One file, so the duplicates meet in one partition of a pushed aggregate.
      sql("INSERT INTO pagg_t1 SELECT /*+ COALESCE(1) */ * FROM VALUES (1), (1) AS v(a)")
      sql("INSERT INTO pagg_t2 VALUES (1)")
      checkSameResultWithPagg(
        "SELECT count(DISTINCT id) FROM (SELECT uuid() AS id FROM pagg_t1 JOIN pagg_t2 ON a = x)",
        pushed = false)
    }
  }

  test("a side with a collated string column is not deduplicated") {
    withTable("pagg_t1", "pagg_t2") {
      sql("CREATE TABLE pagg_t1(code INT) USING parquet")
      sql("CREATE TABLE pagg_t2(s STRING COLLATE UTF8_LCASE) USING parquet")
      sql("INSERT INTO pagg_t1 VALUES (65), (97)")
      sql("INSERT INTO pagg_t2 SELECT /*+ COALESCE(1) */ * FROM VALUES ('a'), ('A') AS v(s)")
      Seq(
        "SELECT * FROM pagg_t1 LEFT SEMI JOIN pagg_t2 ON code = ascii(s)",
        "SELECT DISTINCT ascii(s) FROM pagg_t1 CROSS JOIN pagg_t2"
      ).foreach(q => checkSameResultWithPagg(q, pushed = false))
    }
  }

  test("a session window grouping is not pushed") {
    withTable("pagg_t1", "pagg_t2") {
      sql("CREATE TABLE pagg_t1(k INT, ts TIMESTAMP) USING parquet")
      sql("CREATE TABLE pagg_t2(k INT, v INT) USING parquet")
      sql("INSERT INTO pagg_t1 VALUES (1, TIMESTAMP'2026-01-01 00:00:00'), " +
        "(1, TIMESTAMP'2026-01-01 00:03:00'), (1, TIMESTAMP'2026-01-01 01:00:00')")
      sql("INSERT INTO pagg_t2 VALUES (1, 10), (1, 20)")
      checkSameResultWithPagg(
        "SELECT pagg_t1.k, session_window(ts, '5 minutes'), sum(v) " +
          "FROM pagg_t1 JOIN pagg_t2 ON pagg_t1.k = pagg_t2.k " +
          "GROUP BY pagg_t1.k, session_window(ts, '5 minutes')",
        pushed = false)
    }
  }

  test("grouping by a join key of either side is a final aggregate") {
    Seq("ss_item_sk", "i_item_sk").foreach { key =>
      checkSameResultWithPagg(
        s"SELECT $key, count(*), count(ss_sales_price), max(i_brand_id) " +
          s"FROM store_sales JOIN item ON ss_item_sk = i_item_sk GROUP BY $key",
        rewrite = _.exists(_.isInstanceOf[FinalAggregate]))
    }
  }

  test("a final aggregate on a floating-point join key adds no shuffle") {
    withTable("pagg_t1", "pagg_t2") {
      sql("CREATE TABLE pagg_t1(b DOUBLE, c INT) USING parquet")
      sql("CREATE TABLE pagg_t2(y DOUBLE) USING parquet")
      sql("INSERT INTO pagg_t1 VALUES (0.0, 10), (CAST('-0.0' AS DOUBLE), 20), (1.0, 30)")
      sql("INSERT INTO pagg_t2 VALUES (0.0), (1.0), (1.0)")
      val query = "SELECT b, sum(c) FROM pagg_t1 JOIN pagg_t2 ON b = y GROUP BY b"
      checkSameResultWithPagg(query, rewrite = _.exists(_.isInstanceOf[FinalAggregate]))
      withSQLConf(
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
        val df = sql(query)
        df.collect()
        // The two shuffles of the join, none for the aggregate above it.
        assert(collect(df.queryExecution.executedPlan) { case e: ShuffleExchangeExec => e }
          .size === 2)
      }
    }
  }

  test("a final aggregate over a broadcast join is clustered by the normalized keys") {
    withTable("pagg_t1", "pagg_t2") {
      sql("CREATE TABLE pagg_t1(b DOUBLE, c INT) USING parquet")
      sql("CREATE TABLE pagg_t2(y DOUBLE) USING parquet")
      // Each file is read as one partition, and -0.0 is inserted on its own, so 0.0 and -0.0 start
      // in different partitions of the streamed side, and no coalescing of the shuffle partitions
      // puts them back together.
      // A join that could be broadcast is pushed through only when its estimate expands its larger
      // side, so every value repeats and the tables are analyzed.
      sql("INSERT INTO pagg_t1 SELECT * FROM VALUES (0.0, 10), (1.0, 30), (0.0, 40), (1.0, 50), " +
        "(0.0, 60), (1.0, 70), (0.0, 80), (1.0, 90)")
      sql("INSERT INTO pagg_t1 SELECT CAST('-0.0' AS DOUBLE), id FROM range(4)")
      sql("INSERT INTO pagg_t2 SELECT IF(id % 2 = 0, 0.0, 1.0) FROM range(10)")
      Seq("pagg_t1", "pagg_t2").foreach { t =>
        sql(s"ANALYZE TABLE $t COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      withSQLConf(
          SQLConf.CBO_ENABLED.key -> "true",
          SQLConf.FILES_OPEN_COST_IN_BYTES.key -> (1L << 30).toString,
          SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false") {
        val query =
          "SELECT /*+ BROADCAST(pagg_t2) */ b, sum(c) FROM pagg_t1 JOIN pagg_t2 ON b = y GROUP BY b"
        checkSameResultWithPagg(query, rewrite = _.exists(_.isInstanceOf[FinalAggregate]))
        withSQLConf(SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
          val df = sql(query)
          df.collect()
          // The aggregate needs its own shuffle, because a broadcast join keeps the partitioning
          // of the streamed side.
          val plan = df.queryExecution.executedPlan
          assert(collect(plan) { case j: BroadcastHashJoinExec => j }.size === 1)
          assert(collect(plan) { case e: ShuffleExchangeExec => e }.size === 1)
        }
      }
    }
  }

  test("a semi join with a non-deterministic condition is not deduplicated") {
    withTable("pagg_t1", "pagg_t2") {
      sql("CREATE TABLE pagg_t1(a INT) USING parquet")
      sql("CREATE TABLE pagg_t2(b BOOLEAN) USING parquet")
      sql("INSERT INTO pagg_t1 SELECT id FROM range(100)")
      // One file of 100 equal rows, which a pushed deduplication would reduce to one row.
      sql("INSERT INTO pagg_t2 SELECT /*+ COALESCE(1) */ true FROM range(100)")
      // The subquery rewrite puts rand(0) into the join condition, evaluated once per pair.
      checkSameResultWithPagg(
        "SELECT count(*) FROM pagg_t1 WHERE (rand(0) > 0.5) IN (SELECT b FROM pagg_t2)",
        pushed = false)
    }
  }
}
