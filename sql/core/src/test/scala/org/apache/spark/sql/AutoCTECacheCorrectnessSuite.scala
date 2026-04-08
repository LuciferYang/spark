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

  test("hasDivergentPredicates: skips caching when refs have truly divergent predicates") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Each reference has a clearly different filter. Caching would block
      // per-reference pushdown, so the heuristic should skip.
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
      assert(cachedEntries == 0,
        "Should skip caching when references have genuinely different predicates " +
        "so per-reference pushdown is preserved")
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
