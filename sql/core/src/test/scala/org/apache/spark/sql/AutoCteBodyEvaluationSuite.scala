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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Measures the BENEFIT of auto-CTE caching by execution, not by plan shape: an
 * accumulator inside the CTE body counts how many times the body's rows are actually
 * computed.
 *
 * This is the mechanism behind every speedup the 100TB cluster runs measured. A CTE
 * referenced N times is evaluated N times when inlined and once when cached, and the
 * accumulator sees exactly that. Wall-clock is not asserted -- at this data size the
 * fixed cost of materialising dominates and the numbers are noise -- but the work
 * avoided is deterministic and is what wall-clock follows from at scale.
 *
 * The two shapes under test mirror the two cluster outcomes:
 *   - collapsing body (aggregate above the join): must cache, must evaluate once
 *   - row-expanding body (q95's `ws_wh` shape): must NOT cache, and must do no more
 *     work than the auto-CTE-off baseline
 */
class AutoCteBodyEvaluationSuite extends QueryTest with SharedSparkSession {

  private def prepare(): Unit = {
    spark.sql("DROP TABLE IF EXISTS cte_eval_a")
    spark.sql("DROP TABLE IF EXISTS cte_eval_b")
    // Small and with UNIQUE join keys on the b side, so the self-joins below stay
    // bounded. cte_eval_a has 4 rows per key, which is what makes the row-expanding
    // shape expand.
    spark.range(200).selectExpr("id", "id % 50 AS k", "id % 4 AS g")
      .write.mode("overwrite").saveAsTable("cte_eval_a")
    spark.range(50).selectExpr("id AS k", "id AS v")
      .write.mode("overwrite").saveAsTable("cte_eval_b")
  }

  /**
   * Runs `sqlText` and returns how many rows the body's `count_row` UDF saw.
   *
   * The UDF is registered fresh per run so its accumulator starts at zero, and it is
   * deterministic (the default), which matters: a non-deterministic body would be
   * declined by `shouldAutoCache` and the comparison would measure nothing.
   */
  private def bodyRowsEvaluated(sqlText: String, autoCte: Boolean): Long = {
    val calls = spark.sparkContext.longAccumulator("bodyRows")
    spark.udf.register("count_row", (v: Long) => {
      calls.add(1)
      v
    })
    spark.sharedState.autoCTECacheManager.clearAll(spark)
    try {
      withSQLConf(
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> autoCte.toString,
          SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1") {
        spark.sql(sqlText).collect()
      }
      calls.value
    } finally {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }

  private def cachedEntries(sqlText: String): Int = {
    spark.sharedState.autoCTECacheManager.clearAll(spark)
    try {
      withSQLConf(
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
          SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1") {
        spark.sql(sqlText).collect()
      }
      spark.sharedState.autoCTECacheManager.numEntries
    } finally {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }

  test("collapsing body: caching evaluates the body once instead of once per reference") {
    prepare()
    // Per-reference predicates. This is the shape that makes caching pay: without
    // them the three inlined copies of the body are identical and `ReuseExchange`
    // already shares one evaluation, so caching has nothing to add (verified: the
    // accumulator reports the same count both ways). With them, predicate pushdown
    // pushes a DIFFERENT filter into each copy, the copies stop being reusable, and
    // the body is evaluated once per reference. `ReplaceCTERefWithCache` caches the
    // PRE-pushdown body precisely so one materialisation serves all three -- see
    // `prePushdownBody`.
    val sqlText =
      """WITH c AS (
        |  SELECT a.k AS k, a.g AS g, count(*) AS n
        |  FROM cte_eval_a a JOIN cte_eval_b b ON a.k = b.k
        |  WHERE count_row(a.id) >= 0
        |  GROUP BY a.k, a.g
        |)
        |SELECT x.k, x.n, y.n, z.n
        |FROM (SELECT * FROM c WHERE g = 0) x
        |JOIN (SELECT * FROM c WHERE g = 1) y ON x.k = y.k
        |JOIN (SELECT * FROM c WHERE g = 2) z ON x.k = z.k""".stripMargin

    val inlined = bodyRowsEvaluated(sqlText, autoCte = false)
    val cached = bodyRowsEvaluated(sqlText, autoCte = true)

    assert(cachedEntries(sqlText) == 1, "the collapsing body must be cached")
    assert(inlined > 0, "baseline must have evaluated the body")
    assert(cached < inlined,
      s"caching must avoid re-evaluating the body (inlined=$inlined, cached=$cached)")
    // Three references, one materialisation.
    assert(inlined >= cached * 2,
      s"with three diverging references the inlined plan should do far more work " +
      s"(inlined=$inlined, cached=$cached)")
  }

  test("row-expanding body: declining it does no more work than the baseline") {
    prepare()
    // q95's shape: a join with nothing above it that bounds the output. Must be
    // declined by BOTH gates, so the plan is the inlined baseline and the work is
    // identical -- no materialisation write, no extra shuffle.
    val sqlText =
      """WITH c AS (
        |  SELECT a.k AS k
        |  FROM cte_eval_a a JOIN cte_eval_a b
        |    ON a.k = b.k AND a.g <> b.g
        |  WHERE count_row(a.id) >= 0
        |)
        |SELECT x.k FROM c x JOIN c y ON x.k = y.k""".stripMargin

    val off = bodyRowsEvaluated(sqlText, autoCte = false)
    val on = bodyRowsEvaluated(sqlText, autoCte = true)

    assert(cachedEntries(sqlText) == 0,
      "a row-expanding body must not be cached; on q95 caching it measured 5.5x " +
      "slower and the repartition fallback 15.3x slower than inlining")
    assert(off > 0, "baseline must have evaluated the body")
    assert(on == off,
      s"declining must reproduce the baseline exactly (off=$off, on=$on). A larger " +
      s"count means the body picked up extra work, which is what the round-robin " +
      s"repartition fallback did on q95.")
  }

  test("results are unchanged whether the body is cached or inlined") {
    prepare()
    val sqlText =
      """WITH c AS (
        |  SELECT a.k AS k, count(*) AS n
        |  FROM cte_eval_a a JOIN cte_eval_b b ON a.k = b.k
        |  GROUP BY a.k
        |)
        |SELECT x.k, x.n + y.n AS total FROM c x JOIN c y ON x.k = y.k
        |ORDER BY x.k""".stripMargin
    var expected: Seq[Row] = Nil
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      expected = spark.sql(sqlText).collect().toSeq
    }
    spark.sharedState.autoCTECacheManager.clearAll(spark)
    try {
      withSQLConf(
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
          SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1") {
        checkAnswer(spark.sql(sqlText), expected)
      }
    } finally {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS cte_eval_a")
      spark.sql("DROP TABLE IF EXISTS cte_eval_b")
    } finally {
      super.afterAll()
    }
  }
}
