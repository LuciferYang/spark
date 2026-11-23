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

import org.apache.spark.sql.catalyst.plans.logical.{AggregateBase, Expand, LogicalPlan, PartialAggregate}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{AggUtils, HashAggregateExec}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for [[org.apache.spark.sql.catalyst.optimizer.
 * PushPartialAggregationThroughExpand]] driven by SQL through the full Analyzer +
 * Optimizer pipeline. These complement the structural unit tests in
 * `PushPartialAggregationThroughExpandSuite` (sql/catalyst).
 */
class PushPartialAggregationThroughExpandQuerySuite
    extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def hasPreAggBelowExpand(plan: LogicalPlan): Boolean = plan.collect {
    case e: Expand => e.child.isInstanceOf[AggregateBase]
  }.exists(identity)

  // Checks that the rule changes the plan of `query` as `pushed` says, and never its result. The
  // rows are read as one partition, so the pre-aggregation sees every duplicate. The cost gate
  // is set to always fire; the cost-gate tests below exercise the stats path.
  private def checkSameResult(query: String, pushed: Boolean = true): Unit = {
    val confs = Seq(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1")
    val expected = withSQLConf(confs :+
        (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false"): _*) {
      spark.sql(query).collect().toSeq
    }
    withSQLConf(confs ++ Seq(
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0"): _*) {
      withClue(query) {
        // A rewrite that changes an output type fails the optimizer's plan validation here.
        checkAnswer(spark.sql(query), expected)
        assert(hasPreAggBelowExpand(spark.sql(query).queryExecution.optimizedPlan) === pushed)
      }
    }
  }

  test("ROLLUP -- sum produces same result with and without optimization") {
    withTempView("t") {
      Seq((1, 10, Some(100)), (1, 10, Some(200)), (1, 20, Some(50)), (2, 30, Some(7)),
          (3, 40, None), (3, 40, None))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")
      checkSameResult("SELECT a, b, SUM(v) AS sv FROM t GROUP BY ROLLUP(a, b)")
      checkSameResult(
        "SELECT a, b, SUM(CAST(v AS DECIMAL(10, 2))) AS sv FROM t GROUP BY ROLLUP(a, b)")
    }
  }

  test("CUBE -- min and max produce same result with and without optimization") {
    withTempView("t") {
      Seq((1, 10, Some(5)), (1, 10, Some(9)), (1, 20, Some(1)), (2, 30, Some(7)),
          (2, 30, Some(4)), (3, 40, None), (3, 40, None))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")
      checkSameResult("SELECT a, b, MIN(v) AS mn, MAX(v) AS mx FROM t GROUP BY CUBE(a, b)")
    }
  }

  test("GROUPING SETS -- sum and the bit aggregates compose correctly") {
    withTempView("t") {
      // Overlapping bits, so that swapping any two of BIT_AND, BIT_OR, BIT_XOR and SUM changes the
      // result of some group.
      Seq((1, 10, Some(3L)), (1, 10, Some(5L)), (1, 20, Some(6L)), (2, 30, Some(3L)),
          (2, 30, Some(6L)), (3, 40, None))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")
      checkSameResult(
        "SELECT a, b, SUM(v) AS sv, BIT_AND(v) AS ba, BIT_OR(v) AS bo, BIT_XOR(v) AS bx " +
          "FROM t GROUP BY GROUPING SETS ((a), (a, b), ())")
    }
  }

  test("ROLLUP -- count and avg produce same result with and without optimization") {
    withTempView("t") {
      Seq((1, 10, Some(100)), (1, 10, Some(200)), (1, 20, Some(50)), (2, 30, Some(7)),
          (2, 30, Some(13)), (2, 30, None), (3, 40, None), (3, 40, None))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")
      checkSameResult(
        "SELECT a, b, COUNT(v) AS cv, COUNT(*) AS c, COUNT(a, v) AS cav, AVG(v) AS av " +
          "FROM t GROUP BY ROLLUP(a, b)")
    }
  }

  test("AVG over a Decimal is pushed only once DecimalAggregates has rewritten it") {
    withTempView("t") {
      // Matching `Average.evaluateExpression` for Decimal requires `DecimalPrecision`
      // coercion + `CheckOverflowInSum`, which is a separate follow-up. Until then
      // the rule cleanly skips Decimal Avg rather than producing a result with
      // slightly different precision than the unoptimized plan. A narrow decimal is
      // averaged as its unscaled long value, which the rule pushes.
      val rows = Seq(
        (1, 10, BigDecimal("12345678.99")),
        (1, 10, BigDecimal("11111111.11")),
        (2, 30, BigDecimal("99999999.99")))
      rows.toDF("a", "b", "v").createOrReplaceTempView("t")
      checkSameResult("SELECT a, b, AVG(v) AS av FROM t GROUP BY ROLLUP(a, b)", pushed = false)
      checkSameResult(
        "SELECT a, b, AVG(CAST(v AS DECIMAL(10, 2))) AS av FROM t GROUP BY ROLLUP(a, b)")
    }
  }

  test("unsupported shapes are not rewritten") {
    withTempView("t") {
      Seq((1, 10, 100), (1, 10, 200), (1, 20, 50), (2, 30, 7))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")
      Seq(
        "SELECT a, b, SUM(DISTINCT v) AS sv FROM t GROUP BY ROLLUP(a, b)",
        "SELECT a, b, STDDEV(v) AS sd FROM t GROUP BY ROLLUP(a, b)",
        "SELECT a, b, AVG(make_dt_interval(v)) AS av FROM t GROUP BY ROLLUP(a, b)",
        "SELECT a, b FROM t GROUP BY ROLLUP(a, b)",
        // The Expand of RewriteDistinctAggregates feeds the regular aggregate fresh attributes.
        "SELECT a, COUNT(DISTINCT b), COUNT(DISTINCT v), SUM(v) FROM t GROUP BY a"
      ).foreach(q => checkSameResult(q, pushed = false))
    }
  }

  test("cost gate fires when CBO stats show good compression") {
    withTempView("t") {
      // 100 rows; only 2 distinct values of `a` and 3 distinct values of `b`. With CBO
      // enabled, AggregateEstimation estimates `distinct(a) * distinct(b) = 6` rows for the
      // pre-aggregation and the exp-backoff estimate gives 3 * 2^(1/2) ~= 4, so
      // K'/R = 4/100 = 0.04 < 0.5.
      val rows = (0 until 100).map(i => ((i % 2) + 1, ((i % 3) + 1) * 10, i))
      rows.toDF("a", "b", "v").createOrReplaceTempView("t")

      val sql = "SELECT a, b, SUM(v) AS sv FROM t GROUP BY ROLLUP(a, b)"

      withSQLConf(
          SQLConf.CBO_ENABLED.key -> "true",
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
        // ANALYZE TABLE COMPUTE STATISTICS FOR COLUMNS only works on temp views once
        // they are cached.
        spark.catalog.cacheTable("t")
        try {
          spark.sql("ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS")
          val plan = spark.sql(sql).queryExecution.optimizedPlan
          assert(hasPreAggBelowExpand(plan),
            "Cost gate should fire when stats show dimensions compress under the ratio")
        } finally {
          spark.catalog.uncacheTable("t")
        }
      }
    }
  }

  test("cost gate skips when stats are missing under benefitRatio < 1.0") {
    withTempView("t") {
      // No ANALYZE call; column stats are unavailable, so the cost gate denies.
      Seq((1, 10, 100), (1, 20, 50), (2, 30, 7))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")

      val sql = "SELECT a, b, SUM(v) AS sv FROM t GROUP BY ROLLUP(a, b)"

      withSQLConf(
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
        val plan = spark.sql(sql).queryExecution.optimizedPlan
        assert(!hasPreAggBelowExpand(plan),
          "Cost gate should be conservative and skip the rewrite when stats are missing")
      }
    }
  }

  test("an aggregate over a grouping column keeps that column below the Expand") {
    withTable("t") {
      sql("CREATE TABLE t(a INT, b INT, v INT) USING parquet")
      sql("INSERT INTO t VALUES (1, 1, 1), (1, 2, 2), (1, 2, 3), (1, NULL, 4)")
      Seq(
        "SELECT a, b, count(b) FROM t GROUP BY ROLLUP(a, b)",
        "SELECT a, b, sum(a * v) FROM t GROUP BY ROLLUP(a, b)",
        "SELECT a, b, sum(a), max(v) FROM t GROUP BY CUBE(a, b)"
      ).foreach(q => checkSameResult(q))
    }
  }

  test("a sliding window keeps its time column below the Expand") {
    // The Expand computes the window from the measure `ts`.
    withTable("e") {
      sql("CREATE TABLE e(k INT, ts TIMESTAMP) USING parquet")
      sql("INSERT INTO e VALUES (1, TIMESTAMP'2026-01-01 12:01:00'), " +
        "(1, TIMESTAMP'2026-01-01 12:03:00'), (1, TIMESTAMP'2026-01-01 12:07:00')")
      checkSameResult(
        "SELECT k, window, max(ts) FROM e GROUP BY k, window(ts, '10 minutes', '5 minutes')")
    }
  }

  test("UNPIVOT keeps a grouping column that is also a measure below the Expand") {
    // The outer aggregate groups on the measure `id`, which the Expand passes on.
    withTable("u") {
      sql("CREATE TABLE u(id INT, q1 INT, q2 INT) USING parquet")
      sql("INSERT INTO u VALUES (1, 10, NULL), (1, 11, 12), (2, NULL, NULL)")
      Seq(
        "SELECT id, val, sum(id) FROM u UNPIVOT INCLUDE NULLS (val FOR q IN (q1, q2)) " +
          "GROUP BY id, val",
        "SELECT id, sum(id) FROM u UNPIVOT INCLUDE NULLS (val FOR q IN (q1, q2)) GROUP BY id"
      ).foreach(q => checkSameResult(q))
    }
  }

  test("a sum that returns null on overflow still returns null after the rewrite") {
    Seq(
      "SELECT a, b, try_sum(v) FROM VALUES (1, 1, 9223372036854775807L), (1, 1, 1L), " +
        "(1, 2, 5L) AS t(a, b, v) GROUP BY ROLLUP(a, b)",
      // Only the sum of the partial sums overflows.
      "SELECT a, b, try_sum(v) FROM VALUES (1, 1, 9223372036854775807L), (1, 2, 1L) " +
        "AS t(a, b, v) GROUP BY ROLLUP(a, b)",
      // A group of nulls is not an overflow: (2, NULL) is 7.
      "SELECT a, b, try_sum(v) FROM VALUES (1, 1, 9223372036854775807L), (1, 1, 1L), " +
        "(1, 2, 5L), (2, 3, CAST(NULL AS BIGINT)), (2, 4, 7L) AS t(a, b, v) " +
        "GROUP BY ROLLUP(a, b)",
      "SELECT a, b, try_sum(v) FROM VALUES (1, 1, INTERVAL '178956970' YEAR), " +
        "(1, 1, INTERVAL '178956970' YEAR), (1, 2, INTERVAL '1' MONTH) AS t(a, b, v) " +
        "GROUP BY ROLLUP(a, b)"
    ).foreach(q => checkSameResult(q))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkSameResult(
        "SELECT a, b, sum(v) FROM VALUES " +
          "(1, 1, CAST('90000000000000000000000000000000000000' AS DECIMAL(38,0))), " +
          "(1, 1, CAST('90000000000000000000000000000000000000' AS DECIMAL(38,0))), " +
          "(1, 2, CAST(1 AS DECIMAL(38,0))) AS t(a, b, v) GROUP BY ROLLUP(a, b)")
    }
  }

  test("an ANSI sum that overflows still fails after the rewrite") {
    // Only the sum of the partial sums overflows.
    val query = "SELECT a, b, sum(v) FROM VALUES (1, 1, 9223372036854775807L), (1, 2, 1L) " +
      "AS t(a, b, v) GROUP BY ROLLUP(a, b)"
    Seq(false, true).foreach { pagg =>
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> pagg.toString,
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
        assert(hasPreAggBelowExpand(sql(query).queryExecution.optimizedPlan) === pagg)
        val e = intercept[org.apache.spark.SparkArithmeticException](sql(query).collect())
        assert(e.getCondition === "ARITHMETIC_OVERFLOW")
      }
    }
  }

  test("a count in a correlated scalar subquery is 0 for an outer row without matches") {
    // Decorrelation groups the subquery on `id`, and `RewriteCorrelatedScalarSubquery` takes the
    // value of the subquery's aggregate on zero rows for t1.id = 2. By default `OptimizeSubqueries`
    // keeps that aggregate out of the subquery's optimization, and the rule rewrites it only after
    // the subquery is merged into the main plan; with the conf off the rule rewrites it first.
    withTable("t1", "t2") {
      sql("CREATE TABLE t1(id INT) USING parquet")
      sql("INSERT INTO t1 VALUES (1), (2)")
      sql("CREATE TABLE t2(id INT, w INT, q1 INT, q2 INT) USING parquet")
      sql("INSERT INTO t2 VALUES (1, 5, 10, 20), (1, NULL, 11, NULL)")
      // A HAVING clause takes the other count-bug compensation of
      // `RewriteCorrelatedScalarSubquery`.
      val subqueries = Seq(
        "count(*)" -> "", "count(w)" -> "", "count(*)" -> " HAVING count(*) = 0")
      for ((count, having) <- subqueries; keepAggregate <- Seq(true, false)) {
        withSQLConf(SQLConf.DECORRELATE_SUBQUERY_PREVENT_CONSTANT_FOLDING_FOR_COUNT_BUG.key ->
            keepAggregate.toString) {
          checkSameResult(s"SELECT t1.id, (SELECT $count FROM t2 UNPIVOT INCLUDE NULLS " +
            s"(val FOR q IN (q1, q2)) u WHERE u.id = t1.id$having) FROM t1")
        }
      }
    }
  }

  test("streaming aggregates are not rewritten") {
    withSQLConf(
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      val input = MemoryStream[(Int, Int)](0, spark)
      val sums = input.toDF().toDF("a", "v").rollup("a").agg(sum("v"))
      val query = sums.writeStream.format("memory").queryName("pagg_expand_stream")
        .outputMode("complete").start()
      try {
        input.addData((1, 10), (1, 20), (2, 5))
        query.processAllAvailable()
        input.addData((1, 1))
        query.processAllAvailable()
        // The second batch adds to the state of the first.
        checkAnswer(spark.table("pagg_expand_stream"),
          Row(1, 31) :: Row(2, 5) :: Row(null, 36) :: Nil)
        val plan =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.optimizedPlan
        assert(!hasPreAggBelowExpand(plan))
      } finally {
        query.stop()
        spark.catalog.dropTempView("pagg_expand_stream")
      }
    }
  }

  test("adaptive partial aggregation keeps the results of the rewritten aggregates") {
    withTable("t") {
      sql("CREATE TABLE t(a INT, b INT, v INT) USING parquet")
      sql("INSERT INTO t VALUES (1, 1, 1), (1, 1, 2), (1, 2, 3), (2, 3, NULL), (2, 3, 5)")
      val query = "SELECT a, b, sum(v), count(v), avg(v), min(v), max(v) FROM t " +
        "GROUP BY ROLLUP(a, b)"
      val expected = withSQLConf(
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false") {
        sql(query).collect().toSeq
      }
      Seq(true, false).foreach { codegen =>
        withSQLConf(
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0",
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString,
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "1",
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key -> "1000000") {
          val df = sql(query)
          checkAnswer(df, expected)
          assert(hasPreAggBelowExpand(df.queryExecution.optimizedPlan))
          // The pre-aggregation may pass rows through under
          // `spark.sql.optimizer.partialAggregationOptimization.adaptive.enabled`, and so does
          // the partial aggregate of the outer aggregate.
          val aggs = collect(df.queryExecution.executedPlan) { case a: HashAggregateExec => a }
          def passedThrough(a: HashAggregateExec): Option[Long] =
            a.metrics.get("numPassthroughRows").map(_.value)
          val (pre, outer) = aggs.partition(_.logicalLink.exists(_.isInstanceOf[PartialAggregate]))
          assert(pre.size === 1 && passedThrough(pre.head).nonEmpty)
          assert(outer.flatMap(passedThrough).sum > 0)
        }
      }
    }
  }

  test("a pre-aggregation that does not reduce its input passes rows through") {
    // Every (a, b) is distinct, so the pre-aggregation keeps every row. The rows are read as one
    // partition, which passes through after the second check, at row 200. The first 100 rows share
    // one `k` and the rest are distinct: at row 200 the rows since the previous check are all new
    // keys, although the whole epoch, 200 rows in 101 keys, still looks reduced, and would not
    // look ineffective before row 2100.
    withTempView("t") {
      spark.range(0, 1000, 1, 1).selectExpr("CAST(id % 10 AS INT) a", "CAST(id AS INT) b",
        "IF(id = 7, NULL, CAST(id % 7 AS INT)) v", "IF(id = 3, 9223372036854775807L, id) w",
        "IF(id < 100, 0, id) k")
        .createOrReplaceTempView("t")
      // `w` overflows the sums of the groups that contain row 3.
      val queries = Seq(
        "SELECT a, b, sum(v), count(v), count(*), avg(v), min(v), max(v), try_sum(w) " +
          "FROM t GROUP BY ROLLUP(a, b)",
        "SELECT k, count(*) FROM t GROUP BY ROLLUP(k)")
      val baseConf = Seq(
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0",
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "100")
      val expected = withSQLConf(baseConf :+
          (SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false"): _*) {
        queries.map(q => q -> sql(q).collect().toSeq).toMap
      }
      for (query <- queries; adaptive <- Seq(true, false); codegen <- Seq(true, false)) {
        withSQLConf(baseConf ++ Seq(
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
            SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ADAPTIVE_ENABLED.key -> adaptive.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString): _*) {
          val df = sql(query)
          withClue(s"$query, adaptive $adaptive, codegen $codegen") {
            checkAnswer(df, expected(query))
            assert(preAggPassedThrough(df) === Seq(if (adaptive) Some(800L) else None))
          }
        }
      }
    }
  }

  test("the check window of a pre-aggregation holds the distinct count of its grouping key") {
    // `k1` cycles through 300 values in row order and `k2` through 100, so every row of the first
    // 300 is a new (k1, k2) and none after. A window of 100 rows sees only new keys at the second
    // check and passes the rest through; a window of the distinct count of `k1` sees none. Every
    // (k2, j) is distinct, and the largest distinct count among them is the 100 values of `k2`.
    withTable("t") {
      spark.range(0, 3000, 1, 1).selectExpr("CAST(id % 300 AS INT) k1",
        "CAST(id % 100 AS INT) k2", "CAST(id DIV 100 AS INT) j",
        "IF(id % 4 = 3, NULL, CAST(id % 3 AS INT)) c", "CAST(id AS INT) v").write.saveAsTable("t")
      val cycling = "SELECT k1, k2, sum(v) FROM t GROUP BY ROLLUP(k1, k2)"
      val distinct = "SELECT k2, j, sum(v) FROM t GROUP BY ROLLUP(k2, j)"
      val withNull = "SELECT c, sum(v) FROM t GROUP BY ROLLUP(c)"
      val queries = Seq(cycling, distinct, withNull)
      val expected = withSQLConf(
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false") {
        queries.map(q => q -> sql(q).collect().toSeq).toMap
      }
      // Runs `query` and returns its pre-aggregations.
      def runAggs(query: String, confs: (String, String)*): Seq[HashAggregateExec] = {
        withSQLConf(confs: _*) {
          val df = sql(query)
          checkAnswer(df, expected(query))
          collect(df.queryExecution.executedPlan) {
            case a: HashAggregateExec if a.logicalLink.exists(_.isInstanceOf[PartialAggregate]) => a
          }
        }
      }
      // The tags, which hold the largest distinct count of a grouping key, and the
      // passed-through rows of the pre-aggregations of `query`.
      def run(query: String, confs: (String, String)*): Seq[(Option[Long], Option[Long])] =
        runAggs(query, confs: _*).map { a =>
          (a.getTagValue(AggUtils.PUSHED_PARTIAL_AGGREGATE),
            a.metrics.get("numPassthroughRows").map(_.value))
        }
      def confs(codegen: Boolean, minRows: Int = 100): Seq[(String, String)] = Seq(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
        SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0",
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> minRows.toString,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString)
      // Without column statistics the window is `minRows`.
      Seq(true, false).foreach { codegen =>
        withClue(s"codegen $codegen") {
          assert(run(cycling, confs(codegen): _*) === Seq((Some(0L), Some(2800L))))
        }
      }
      sql("ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS")
      Seq(true, false).foreach { codegen =>
        withClue(s"codegen $codegen") {
          val Seq((Some(k1Count), passed)) = run(cycling, confs(codegen): _*)
          assert(k1Count >= 270 && k1Count <= 330 && passed === Some(0L))
          // A map that fills before the window ends spills instead of passing through.
          val spilled = runAggs(cycling, confs(codegen) :+
            ("spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 150"): _*)
          assert(spilled.map(_.metrics("numTasksFallBacked").value) === Seq(1L))
          assert(spilled.flatMap(_.metrics.get("numPassthroughRows")).map(_.value) === Seq(0L))
          // Above `minRows`, the distinct count sets the window; below, `minRows` does.
          val Seq((Some(k2Count), keyPassed)) = run(distinct, confs(codegen, minRows = 50): _*)
          assert(k2Count >= 90 && k2Count <= 110 && keyPassed === Some(3000 - 2 * k2Count))
          assert(run(distinct, confs(codegen, minRows = 200): _*) ===
            Seq((Some(k2Count), Some(2600L))))
          // `minRows = 0` turns the periodic check off whatever the window.
          assert(run(distinct, confs(codegen, minRows = 0): _*) ===
            Seq((Some(k2Count), Some(0L))))
          // A null counts as one more value: `c` holds 0, 1, 2 and null.
          assert(run(withNull, confs(codegen): _*).map(_._1) === Seq(Some(4L)))
        }
      }
    }
  }

  // The rows the pre-aggregations passed through, None when they could not.
  private def preAggPassedThrough(df: DataFrame): Seq[Option[Long]] =
    collect(df.queryExecution.executedPlan) {
      case a: HashAggregateExec if a.logicalLink.exists(_.isInstanceOf[PartialAggregate]) =>
        a.metrics.get("numPassthroughRows").map(_.value)
    }
}
