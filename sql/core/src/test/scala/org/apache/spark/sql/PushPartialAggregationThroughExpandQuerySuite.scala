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

import org.apache.spark.sql.catalyst.plans.logical.{AggregateBase, Expand, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for [[org.apache.spark.sql.catalyst.optimizer.
 * PushPartialAggregationThroughExpand]] driven by SQL through the full Analyzer +
 * Optimizer pipeline. These complement the structural unit tests in
 * `PushPartialAggregationThroughExpandSuite` (sql/catalyst).
 */
class PushPartialAggregationThroughExpandQuerySuite
    extends QueryTest with SharedSparkSession {

  import testImplicits._

  private def hasPreAggBelowExpand(plan: LogicalPlan): Boolean = plan.collect {
    case e: Expand => e.child.isInstanceOf[AggregateBase]
  }.exists(identity)

  // Most tests target rewrite output correctness rather than the stats integration,
  // so the cost gate is bumped to 1.0 (always fire when shape matches). The dedicated
  // cost-gate tests at the bottom of this file exercise the stats path explicitly.
  private def withRuleAlwaysOn[T](body: => T): T = withSQLConf(
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0")(body)

  test("ROLLUP — sum produces same result with and without optimization") {
    withTempView("t") {
      Seq((1, 10, 100), (1, 10, 200), (1, 20, 50), (2, 30, 7))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")

      val sql = "SELECT a, b, SUM(v) AS sv FROM t GROUP BY ROLLUP(a, b)"

      val baseline = withSQLConf(
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false") {
        spark.sql(sql).collect()
      }

      withRuleAlwaysOn {
        val optimized = spark.sql(sql)
        checkAnswer(optimized, baseline.toSeq)
        assert(hasPreAggBelowExpand(optimized.queryExecution.optimizedPlan),
          "Optimized plan should contain a pre-aggregation Aggregate below Expand")
      }
    }
  }

  test("CUBE — min and max produce same result with and without optimization") {
    withTempView("t") {
      Seq((1, 10, 5), (1, 10, 9), (1, 20, 1), (2, 30, 7), (2, 30, 4))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")

      val sql = "SELECT a, b, MIN(v) AS mn, MAX(v) AS mx FROM t GROUP BY CUBE(a, b)"

      val baseline = withSQLConf(
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false") {
        spark.sql(sql).collect()
      }

      withRuleAlwaysOn {
        val optimized = spark.sql(sql)
        checkAnswer(optimized, baseline.toSeq)
        assert(hasPreAggBelowExpand(optimized.queryExecution.optimizedPlan),
          "Optimized plan should contain a pre-aggregation Aggregate below Expand")
      }
    }
  }

  test("GROUPING SETS — sum and bit_or compose correctly") {
    withTempView("t") {
      Seq((1, 10, 1L), (1, 10, 2L), (1, 20, 4L), (2, 30, 8L))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")

      val sql =
        "SELECT a, b, SUM(v) AS sv, BIT_OR(v) AS bo FROM t " +
          "GROUP BY GROUPING SETS ((a), (a, b), ())"

      val baseline = withSQLConf(
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false") {
        spark.sql(sql).collect()
      }

      withRuleAlwaysOn {
        val optimized = spark.sql(sql)
        checkAnswer(optimized, baseline.toSeq)
        assert(hasPreAggBelowExpand(optimized.queryExecution.optimizedPlan),
          "Optimized plan should contain a pre-aggregation Aggregate below Expand")
      }
    }
  }

  test("ROLLUP — count and avg produce same result with and without optimization") {
    withTempView("t") {
      Seq((1, 10, 100), (1, 10, 200), (1, 20, 50), (2, 30, 7), (2, 30, 13))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")

      val sql =
        "SELECT a, b, COUNT(v) AS cv, AVG(v) AS av FROM t GROUP BY ROLLUP(a, b)"

      val baseline = withSQLConf(
          SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false") {
        spark.sql(sql).collect()
      }

      withRuleAlwaysOn {
        val optimized = spark.sql(sql)
        checkAnswer(optimized, baseline.toSeq)
        assert(hasPreAggBelowExpand(optimized.queryExecution.optimizedPlan),
          "Optimized plan should contain a pre-aggregation Aggregate below Expand")
      }
    }
  }

  test("AVG over Decimal is intentionally not pushed (precision parity TODO)") {
    withTempView("t") {
      // Matching `Average.evaluateExpression` for Decimal requires `DecimalPrecision`
      // coercion + `CheckOverflowInSum`, which is a separate follow-up. Until then
      // the rule cleanly skips Decimal Avg rather than producing a result with
      // slightly different precision than the unoptimized plan.
      val rows = Seq(
        (1, 10, BigDecimal("12345678.99")),
        (1, 20, BigDecimal("11111111.11")),
        (2, 30, BigDecimal("99999999.99")))
      rows.toDF("a", "b", "v").createOrReplaceTempView("t")

      val sql = "SELECT a, b, AVG(v) AS av FROM t GROUP BY ROLLUP(a, b)"

      withRuleAlwaysOn {
        val plan = spark.sql(sql).queryExecution.optimizedPlan
        assert(!hasPreAggBelowExpand(plan),
          "Decimal AVG should be ineligible until precision parity is implemented")
      }
    }
  }

  test("DISTINCT in the projection prevents the rewrite") {
    withTempView("t") {
      Seq((1, 10, 100), (1, 20, 50))
        .toDF("a", "b", "v")
        .createOrReplaceTempView("t")

      val sql = "SELECT a, b, SUM(DISTINCT v) AS sv FROM t GROUP BY ROLLUP(a, b)"

      withRuleAlwaysOn {
        val plan = spark.sql(sql).queryExecution.optimizedPlan
        assert(!hasPreAggBelowExpand(plan),
          "DISTINCT must disable the rewrite (eligibility guard #2)")
      }
    }
  }

  test("cost gate fires when CBO stats show good compression") {
    withTempView("t") {
      // 100 rows; only 2 distinct values of `a` and 3 distinct values of `b`. With CBO
      // enabled, AggregateEstimation uses `distinct(a) * distinct(b) = 6` as the upper
      // bound on the pre-aggregation row count, so K'/R = 6/100 = 0.06 < 0.5.
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
}
