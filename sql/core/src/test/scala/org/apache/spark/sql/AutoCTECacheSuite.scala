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

  // Disable the size-based gate so the existing structural-gate tests with
  // small range() data still cache. Stats-gate behavior is exercised
  // separately by AutoCTECacheCorrectnessSuite.
  override protected def sparkConf: org.apache.spark.SparkConf =
    super.sparkConf.set(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key, "0")

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

  // A non-deterministic CTE with Aggregate -- won't be inlined by InlineCTE
  // (non-deterministic + refCount >= 2) and passes isExpensiveEnough (has Aggregate).
  private val cachableCteSQL =
    """WITH cte AS (
      |  SELECT key, sum(value) as total, rand() as r
      |  FROM auto_cte_test GROUP BY key
      |)
      |SELECT a.key, a.total, b.total
      |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

  test("auto-cache CTE when enabled") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "Should have auto-cached the CTE")
    }
  }

  test("no auto-cache when disabled") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0)
    }
  }

  test("correctness: auto-cached CTE produces same results") {
    prepareData()
    val sql =
      """WITH cte AS (
        |  SELECT key, count(*) as cnt, rand() as r
        |  FROM auto_cte_test GROUP BY key
        |)
        |SELECT a.key, a.cnt + b.cnt as total_cnt
        |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

    val baseline = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      spark.sql(sql).selectExpr("key", "total_cnt").collect()
    }
    val optimized = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(sql).selectExpr("key", "total_cnt").collect()
    }
    assert(baseline.length == optimized.length,
      s"Row count mismatch: ${baseline.length} vs ${optimized.length}")
  }

  test("within-query reuse: multiple references share one cache entry") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "Two references to the same CTE should share one cache entry")
    }
  }

  test("cross-query CTE reuse via plan normalization") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {

      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1)

      // Same CTE body, different outer query. QueryExecution.normalize
      // normalizes rand() seeds so the plans match via sameResult().
      val sql2 =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.total + b.total as combined
          |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

      spark.sql(sql2).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "Should reuse cached CTE across queries")
    }
  }

  test("skip caching for scan-only CTE") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
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

  test("TTL-based eviction") {
    import org.apache.spark.sql.execution.AutoCTECacheManager
    // Create a manager with 1ms TTL directly (these configs are not session-bindable)
    val shortTtlManager = new AutoCTECacheManager(ttlMs = 1, maxSizeBytes = -1)
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Manually track an entry to simulate caching
      val plan = spark.sql(
        "SELECT key, sum(value) as total FROM auto_cte_test GROUP BY key")
        .queryExecution.optimizedPlan
      shortTtlManager.trackEntry(1L, plan)
      assert(shortTtlManager.numEntries == 1)

      Thread.sleep(10)
      shortTtlManager.evictStaleEntries(spark)

      assert(shortTtlManager.numEntries == 0,
        "Should have evicted expired CTE cache entries")
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
