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

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Expiry must keep working after `spark.sql.auto.reused.cte.enabled` is turned off.
 *
 * `ReplaceCTERefWithCache.apply` is the only trigger for the sweep, so gating the sweep on the
 * flag froze every entry materialised while the flag was on: they stayed registered in
 * `CacheManager` and kept being handed to new plans for the rest of the SparkContext's life --
 * and turning the feature off is exactly when an operator expects that to stop.
 *
 * Its own suite because the TTL is a static conf: `spark.sql.auto.cte.cache.ttl` is fixed when
 * `SharedState` builds the tracker, so it can only be set through `sparkConf`, and a TTL this
 * short would make every other cache test race its own entries away.
 *
 * 5s rather than 1ms: the rule runs several times per query (AQE re-plans per stage), so a 1ms
 * TTL let the first query sweep away its own entry before the test could look at it.
 */
class AutoCteSweepWithFeatureOffSuite extends QueryTest with SharedSparkSession {

  private val ttlMillis = 5000

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.AUTO_CTE_CACHE_TTL.key, s"${ttlMillis}ms")
      .set(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key, "0")

  override protected def afterEach(): Unit = {
    try {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterEach()
    }
  }

  test("an entry materialised with the feature on still expires after it is turned off") {
    withTable("auto_cte_sweep_t") {
      spark.range(200)
        .selectExpr("id", "id % 20 AS key", "cast(id AS double) AS value")
        .write.mode("overwrite").saveAsTable("auto_cte_sweep_t")

      // `rand()` in the CTE keeps `InlineCTE` from inlining a multi-reference body, and the
      // aggregate gets it past the structural gate. The reference list drops `r`, so column
      // pruning leaves the body deterministic by the time the cache rule sees it.
      val sqlText =
        """WITH cte AS (
          |  SELECT key, sum(value) AS total, rand() AS r
          |  FROM auto_cte_sweep_t GROUP BY key
          |)
          |SELECT a.key, a.total, b.total
          |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

      withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
        spark.sql(sqlText).collect()
      }
      assert(!spark.sharedState.cacheManager.isEmpty,
        "the body must be registered in CacheManager for this test to prove anything")

      Thread.sleep(ttlMillis + 500L)

      // Any query will do: the sweep runs before the flag check bails out. Asserted on
      // `CacheManager` rather than on `numEntries`, because what matters is whether
      // `stopReusing` reached the registry.
      withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
        spark.sql("SELECT count(*) FROM auto_cte_sweep_t").collect()
      }
      assert(spark.sharedState.cacheManager.isEmpty,
        "the expired entry must stop being reusable even though the feature is now off")
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "the tracker must be empty after the sweep")
    }
  }
}
