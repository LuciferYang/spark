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

import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Correctness tests for auto-CTE caching that cover two scenarios not exercised
 * by [[AutoCTECacheSuite]]:
 *
 *   1. `hasDivergentPredicates` heuristic - making sure we correctly cache when
 *      per-reference predicates are semantically equal (possibly with different
 *      AST orderings) and correctly skip when they're genuinely divergent.
 *
 *   2. Interaction with SPARK-40193 subquery plan merging - making sure the two
 *      optimizations coexist, produce correct results, and auto-CTE still fires
 *      when a CTE body contains a mergeable subquery.
 */
class AutoCTECacheCorrectnessSuite extends QueryTest with SharedSparkSession {

  override protected def afterEach(): Unit = {
    try {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterEach()
    }
  }

  private def prepareData(): Unit = {
    spark.range(10000)
      .selectExpr(
        "id",
        "id % 100 as key",
        "cast(id % 50 as int) as col1",
        "cast(id % 30 as int) as col2",
        "cast(id as double) as value")
      .write.mode("overwrite").saveAsTable("auto_cte_corr_test")
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS auto_cte_corr_test")
    } finally {
      super.afterAll()
    }
  }

  private def cachedEntries: Int =
    spark.sharedState.autoCTECacheManager.numEntries

  private def countInMemoryRelations(sql: String): Int = {
    spark.sql(sql).queryExecution.optimizedPlan.collect {
      case _: InMemoryRelation => 1
    }.sum
  }

  // ---------------------------------------------------------------------------
  // hasDivergentPredicates tests (blocker #2)
  // ---------------------------------------------------------------------------

  test("hasDivergentPredicates: caches when refs have identical predicates") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key, a.total + b.total
          |FROM cte a JOIN cte b ON a.key = b.key
          |WHERE a.key > 10 AND b.key > 10""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Should cache when all references have identical pushable predicates")
    }
  }

  test("hasDivergentPredicates: caches when predicates are semantically equal " +
       "but syntactically reordered") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // This is the reordering bug case: both references have the same conjunction
      // but in different orders. Without canonicalization, semanticHash() of the
      // two predicates differs and hasDivergentPredicates returns true (bug -> skip).
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.total, b.total FROM
          |  (SELECT key, total FROM cte WHERE key > 10 AND total > 100) a
          |  JOIN
          |  (SELECT key, total FROM cte WHERE total > 100 AND key > 10) b
          |  ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Should cache when reordered but semantically equal predicates are used on both refs; " +
        "hasDivergentPredicates must normalize before hashing")
    }
  }

  test("hasDivergentPredicates: caches when refs have non-correlated divergent predicates") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Each reference has a different non-correlated filter. The previous
      // heuristic skipped caching here, but `PushdownPredicatesAndPruneColumnsForCTEDef`
      // already ORs the per-reference filters and pushes the combined predicate
      // into the CTE body, so caching does NOT block pushdown - the per-reference
      // filters sit above the cache. The EMR reference cluster caches q24a/q64
      // which have exactly this shape, and we should match.
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.total, b.total FROM
          |  (SELECT key, total FROM cte WHERE key < 10) a,
          |  (SELECT key, total FROM cte WHERE key > 90) b
          |  WHERE a.key + 100 = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Should cache when references have non-correlated divergent filters; " +
        "the OR-combined predicate is already pushed into the body, so the " +
        "per-reference filters above the cache preserve pushdown semantics.")
    }
  }

  // ---------------------------------------------------------------------------
  // SPARK-40193 interaction tests (blocker #6)
  // ---------------------------------------------------------------------------

  test("SPARK-40193: auto-CTE + subquery merging produce identical results") {
    prepareData()
    // CTE body contains a scalar subquery (>(SELECT AVG...)) that SPARK-40193
    // might want to merge with the other scalar subquery in the outer select.
    val sql =
      """WITH cte AS (
        |  SELECT key, sum(value) as total, rand() as r
        |  FROM auto_cte_corr_test
        |  WHERE value > (SELECT AVG(value) FROM auto_cte_corr_test)
        |  GROUP BY key
        |)
        |SELECT a.key, a.total + b.total,
        |       (SELECT MAX(value) FROM auto_cte_corr_test)
        |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

    val baseline = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      spark.sql(sql).collect().map(_.toString).sorted.toSeq
    }
    val optimized = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(sql).collect().map(_.toString).sorted.toSeq
    }
    assert(baseline == optimized,
      s"Results must be identical with and without auto-CTE caching. " +
      s"baseline.size=${baseline.size} optimized.size=${optimized.size}")
  }

  test("SPARK-40193: auto-CTE still fires when CTE body has a mergeable subquery") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test
          |  WHERE value > (SELECT AVG(value) FROM auto_cte_corr_test)
          |  GROUP BY key
          |)
          |SELECT a.key, a.total + b.total
          |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Auto-CTE should still fire when the CTE body contains a subquery that " +
        "SPARK-40193 could merge; rule ordering in SparkOptimizer must allow it")
    }
  }

  // ---------------------------------------------------------------------------
  // Deterministic CTE caching tests (TPC-DS shape)
  // ---------------------------------------------------------------------------
  // The existing AutoCTECacheSuite uses rand() in every test query to make CTEs
  // non-deterministic, which is the only condition that bypasses InlineCTE's
  // unconditional deterministic-CTE inlining. Real TPC-DS queries are
  // deterministic, so without an InlineCTE carve-out the entire feature is dead
  // code on real workloads. These tests use deterministic CTEs to catch that
  // regression and verify the fix.

  test("deterministic multi-ref CTE with identical filters is cached") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total, count(*) as cnt
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key, a.total, b.cnt
          |FROM agg a JOIN agg b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Deterministic multi-ref CTE with expensive body must be cached, " +
        "not inlined by InlineCTE")
    }
  }

  test("deterministic multi-ref CTE with different filters is cached (q64-shape)") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Mimics q64's cs1/cs2 self-join: same CTE referenced twice with
      // different non-correlated filters on a group-by key.
      val sql =
        """WITH agg AS (
          |  SELECT key, col1, sum(value) as total, count(*) as cnt
          |  FROM auto_cte_corr_test GROUP BY key, col1
          |)
          |SELECT a.key, a.total, b.total FROM
          |  (SELECT key, total FROM agg WHERE col1 = 1) a
          |  JOIN
          |  (SELECT key, total FROM agg WHERE col1 = 2) b
          |  ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Deterministic multi-ref CTE with non-correlated divergent filters " +
        "must be cached. PushdownPredicatesAndPruneColumnsForCTEDef already " +
        "ORs the per-reference filters into the body, so caching does not " +
        "block pushdown.")
    }
  }

  test("nested deterministic CTE (q64-shape: outer CTE references inner CTE)") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Mimics q64's cs_ui (single-ref, inlinable) inside cross_sales
      // (multi-ref, cache-eligible). The inner CTE should be inlined into
      // the outer CTE's body, then the outer CTE should be cached.
      val sql =
        """WITH inner_cte AS (
          |  SELECT key, col1, sum(value) as inner_total
          |  FROM auto_cte_corr_test
          |  GROUP BY key, col1
          |  HAVING sum(value) > 0
          |),
          |outer_cte AS (
          |  SELECT key, col1, inner_total, count(*) as outer_cnt
          |  FROM inner_cte
          |  GROUP BY key, col1, inner_total
          |)
          |SELECT a.key, a.outer_cnt, b.outer_cnt FROM
          |  (SELECT key, outer_cnt FROM outer_cte WHERE col1 = 1) a
          |  JOIN
          |  (SELECT key, outer_cnt FROM outer_cte WHERE col1 = 2) b
          |  ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Nested CTE: inner_cte should be inlined into outer_cte, then " +
        "outer_cte (multi-ref) should be cached. q64 has this exact shape " +
        "with cs_ui inside cross_sales.")
    }
  }

  test("SPARK-40193: cross-query reuse survives subquery merging") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val cteBody =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test
          |  WHERE value > (SELECT AVG(value) FROM auto_cte_corr_test)
          |  GROUP BY key
          |)""".stripMargin

      val q1 = cteBody +
        "\nSELECT a.total + b.total FROM cte a JOIN cte b ON a.key = b.key"
      val q2 = cteBody +
        "\nSELECT a.total * b.total FROM cte a JOIN cte b ON a.key = b.key"

      spark.sql(q1).collect()
      val afterQ1 = cachedEntries
      assert(afterQ1 >= 1, "q1 should populate the auto-CTE cache")

      spark.sql(q2).collect()
      val afterQ2 = cachedEntries
      assert(afterQ2 == afterQ1,
        s"q2 should reuse q1's cache via sameResult() match, not create a new entry. " +
        s"before=$afterQ1 after=$afterQ2 - sameResult() match is broken, " +
        s"possibly due to asymmetric rewriting by SPARK-40193 or another optimizer rule")
    }
  }
}
