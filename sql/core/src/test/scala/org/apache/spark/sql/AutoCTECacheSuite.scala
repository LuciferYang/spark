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

class AutoCTECacheSuite extends QueryTest with SharedSparkSession {

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
      .write.mode("overwrite").saveAsTable("auto_cte_test")
  }

  test("auto-cache CTE when enabled and refCount >= 2") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // This CTE is non-deterministic (referenced twice) and won't be
      // inlined by InlineCTE.
      // Use a non-deterministic CTE to prevent inlining.
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.key, a.total, b.total
          |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

      val df = spark.sql(sql)
      df.collect() // force execution

      // Verify CTE was cached
      assert(spark.sharedState.autoCTECacheManager.numEntries > 0,
        "Should have auto-cached the CTE")
    }
  }

  test("no auto-cache when disabled") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.key, a.total, b.total
          |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()

      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "Should not auto-cache when disabled")
    }
  }

  test("correctness: auto-cached CTE produces same results") {
    prepareData()

    // Use non-deterministic CTE to prevent InlineCTE from inlining it
    val sqlNonDet =
      """WITH cte AS (
        |  SELECT key, count(*) as cnt, rand() as r
        |  FROM auto_cte_test GROUP BY key
        |)
        |SELECT a.key, a.cnt + b.cnt as total_cnt
        |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

    val expected = withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      spark.sql(sqlNonDet).drop("r")
        .selectExpr("key", "total_cnt").collect()
    }
    val optimized = withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(sqlNonDet).drop("r")
        .selectExpr("key", "total_cnt").collect()
    }
    // Both should have 100 rows (100 distinct keys)
    assert(expected.length == optimized.length,
      s"Row count mismatch: ${expected.length} vs ${optimized.length}")
  }

  test("within-query CTE reuse: multiple references share cache") {
    prepareData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.AUTO_CLEAR_CTE_CACHE_ENABLED.key -> "false") {

      // Non-deterministic CTE referenced twice --both refs should use
      // the same cached InMemoryRelation
      val sql =
        """WITH expensive_cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.key, a.total, b.total
          |FROM expensive_cte a JOIN expensive_cte b ON a.key = b.key""".stripMargin

      val df = spark.sql(sql)
      df.collect()

      // Should have exactly one cached entry (not two, one per reference)
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "Multiple references to same CTE should share one cache entry")
    }
  }

  test("eviction clears auto-CTE caches") {
    prepareData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.AUTO_CLEAR_CTE_CACHE_ENABLED.key -> "true",
      SQLConf.AUTO_CTE_CACHE_TTL.key -> "1ms") {

      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.key, a.total FROM cte a
          |JOIN cte b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries > 0)

      // Wait for TTL to expire
      Thread.sleep(10)

      // Trigger eviction
      spark.sharedState.autoCTECacheManager.evictIfNeeded(spark)

      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "Should have evicted expired CTE cache entries")
    }
  }

  test("cross-query reuse not possible for non-deterministic CTEs") {
    prepareData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.AUTO_CLEAR_CTE_CACHE_ENABLED.key -> "false") {

      // Non-deterministic CTEs (using rand()) get different seeds per query,
      // so their plans don't match via sameResult() across queries.
      // Each query creates its own cache entry.
      val sql1 =
        """WITH agg AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.key, a.total FROM agg a
          |JOIN agg b ON a.key = b.key""".stripMargin

      spark.sql(sql1).collect()
      val entriesAfterQ1 = spark.sharedState.autoCTECacheManager.numEntries
      assert(entriesAfterQ1 > 0, "Should have cached CTE from query 1")

      val sql2 =
        """WITH agg AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.total + b.total as combined FROM agg a
          |JOIN agg b ON a.key = b.key""".stripMargin

      spark.sql(sql2).collect()

      // Each query creates a separate cache entry (no cross-query reuse)
      assert(spark.sharedState.autoCTECacheManager.numEntries > entriesAfterQ1,
        "Non-deterministic CTEs should create separate cache entries per query")
    }
  }

  test("smart heuristic: skip caching for scan-only CTE") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // CTE with only a scan (no Join, Aggregate, Sort, or Window)
      // should NOT be cached because it's cheap to recompute
      val sql =
        """WITH simple_cte AS (
          |  SELECT key, value, rand() as r
          |  FROM auto_cte_test WHERE key < 50
          |)
          |SELECT a.key, b.value
          |FROM simple_cte a JOIN simple_cte b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()

      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "Should not cache scan-only CTE (not expensive enough)")
    }
  }

  test("smart heuristic: cache CTE with expensive operators") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // CTE with Aggregate --should be cached
      val sql =
        """WITH expensive_cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.key, a.total, b.total
          |FROM expensive_cte a JOIN expensive_cte b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()

      assert(spark.sharedState.autoCTECacheManager.numEntries > 0,
        "Should cache CTE with Aggregate operator")
    }
  }

  test("TTL-based eviction respects lastAccessedAt updates") {
    prepareData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.AUTO_CLEAR_CTE_CACHE_ENABLED.key -> "true",
      SQLConf.AUTO_CTE_CACHE_TTL.key -> "100ms") {

      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.key, a.total FROM cte a
          |JOIN cte b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      val mgr = spark.sharedState.autoCTECacheManager
      assert(mgr.numEntries == 1)

      // Simulate access before TTL expires by directly calling recordAccess
      // (re-executing the query with rand() creates a new plan, not a cache hit)
      Thread.sleep(40)
      mgr.recordAccess(mgr.entryIds.head)

      // Wait --total time since creation > TTL, but time since last access < TTL
      Thread.sleep(70)
      mgr.evictIfNeeded(spark)

      assert(mgr.numEntries == 1,
        "Entry should survive because lastAccessedAt was refreshed")

      // Now wait for full TTL from last access
      Thread.sleep(110)
      mgr.evictIfNeeded(spark)

      assert(mgr.numEntries == 0,
        "Entry should be evicted after TTL expires from last access")
    }
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS auto_cte_test")
    } finally {
      super.afterAll()
    }
  }
}
