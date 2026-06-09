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

import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Answer-checks every TPC-DS query that auto-CTE caches on the 100TB benchmark against
 * REAL data, on both sides of every gate and under both settings of
 * `partialAggregationOptimization`.
 *
 * Why this suite exists separately from [[AutoCteTpcdsWorkSuite]]: that one runs on
 * `TPCDSSmallDataFixture`, which cannot make q14b/q23b/q24a/q24b/q39a/q39b return any
 * rows -- their filters need cross-column value overlaps that independently generated
 * columns do not have. Comparing two empty results proves nothing, so those six had no
 * effective correctness coverage. See [[RealTpcdsData]] for what the dataset is and why
 * it is sampled on `i_item_sk`.
 *
 * What is compared. For each query and each setting of the conf, the answer with
 * caching ON (twice: the cold execution that materialises, and a warm one that reads
 * the cache back) must equal the answer with caching OFF, row for row and field for
 * field. Rows are normalised by sorting their string forms: several of these queries
 * have no total ordering (`LIMIT` over a partial `ORDER BY`) and caching legitimately
 * changes partitioning, so row ORDER may differ without the answer differing.
 *
 * Both executions matter and for different reasons. Cold exercises materialisation
 * itself -- `prePushdownBody` caches the UNFILTERED body, so a bug there shows up as
 * rows that should have been filtered out. Warm exercises the cross-execution reuse
 * path, where the cache key must match or a stale entry could be read.
 *
 * The dataset is external and optional; every test cancels when it is absent, so this
 * suite is a no-op on a machine that has not generated it. That is a deliberate
 * trade: the alternative is no answer coverage at all for six of the fourteen.
 */
class AutoCteTpcdsRealDataSuite extends QueryTest with SharedSparkSession
  with RealTpcdsData {

  /** Every query app 0027 cached, with what it measured at 100TB. */
  private val queries = Seq(
    "q2" -> "5.8x", "q14a" -> "8.6x", "q14b" -> "8.6x",
    "q23a" -> "4.0x", "q23b" -> "4.5x", "q24a" -> "55.8x", "q24b" -> "54.6x",
    "q39a" -> "2.3x", "q39b" -> "1.8x", "q47" -> "4.1x", "q57" -> "12.7x",
    "q74" -> "0.83x", "q75" -> "1.01x", "q95" -> "0.07x")

  /** Row counts each query returns on the sampled dataset, as a vacuity guard. */
  private val expectedRowCounts = Map(
    "q2" -> 2513, "q14a" -> 100, "q14b" -> 100, "q23a" -> 1, "q23b" -> 100,
    "q24a" -> 176, "q24b" -> 29, "q39a" -> 281, "q39b" -> 4, "q47" -> 100,
    "q57" -> 100, "q74" -> 100, "q75" -> 8, "q95" -> 1)

  private var registered = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (hasRealTpcdsData) {
      registerRealTpcdsTables()
      registered = true
    }
  }

  override def afterAll(): Unit = {
    try {
      if (registered) dropRealTpcdsTables()
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Cluster configuration, minus `injectStats`. `AUTO_CTE_CACHE_MIN_SIZE_BYTES=-1`
   * matches apps 0016/0021/0027; CBO is on so the cost gates see the ANALYZE-computed
   * column statistics of the real rows.
   */
  private def clusterConf(cacheOn: Boolean, pushPartialAgg: Boolean): Seq[(String, String)] =
    Seq(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> cacheOn.toString,
      SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1",
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.PLAN_STATS_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> pushPartialAgg.toString)

  /** Sorted string forms, so a legitimate reordering is not read as a wrong answer. */
  private def norm(rows: Seq[Row]): Seq[String] = rows.map(_.toString).sorted

  for ((name, clusterSpeedup) <- queries; pagg <- Seq(false, true)) {
    test(s"$name: cached answer equals inlined, cold and warm " +
      s"(cluster measured $clusterSpeedup, pushPartialAgg=$pagg)") {
      assume(hasRealTpcdsData,
        s"no TPC-DS dataset at $realTpcdsDataPath; generate it with SampleRealTpcdsData")
      val q = resourceToString(s"tpcds/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

      spark.sharedState.autoCTECacheManager.clearAll(spark)
      // `withSQLConf` returns Unit, so the rows come out through a var.
      var inlined: Seq[Row] = Nil
      withSQLConf(clusterConf(cacheOn = false, pagg): _*) {
        inlined = spark.sql(q).collect().toSeq
      }

      // A comparison against an empty answer proves nothing, and an empty answer here
      // means the dataset was generated with different sampling parameters rather than
      // that the feature broke. Check the count before trusting the comparison.
      assert(inlined.nonEmpty,
        s"$name returned no rows with caching off, so the comparison below would be " +
        s"vacuous. Expected about ${expectedRowCounts(name)} rows on the sampled " +
        s"dataset; regenerate it with SampleRealTpcdsData if the sampling changed.")

      try {
        withSQLConf(clusterConf(cacheOn = true, pagg): _*) {
          val cold = spark.sql(q).collect().toSeq
          assert(norm(cold) == norm(inlined),
            s"$name (pushPartialAgg=$pagg): the answer differs on the COLD cached " +
            s"execution, which is the one that materialises the body " +
            s"(inlined=${inlined.size} rows, cold=${cold.size} rows)")

          val warm = spark.sql(q).collect().toSeq
          assert(norm(warm) == norm(inlined),
            s"$name (pushPartialAgg=$pagg): the answer differs on the WARM cached " +
            s"execution, which reads the materialised blocks back " +
            s"(inlined=${inlined.size} rows, warm=${warm.size} rows)")
        }
      } finally {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
      }
    }
  }

  test("the sampled dataset still produces the row counts the answer checks assume") {
    assume(hasRealTpcdsData,
      s"no TPC-DS dataset at $realTpcdsDataPath; generate it with SampleRealTpcdsData")
    // Pinning the counts catches a regenerated dataset with different parameters, which
    // would otherwise silently weaken every comparison above. Exact, not a lower bound:
    // the sampling is deterministic, so a changed count means changed inputs.
    withSQLConf(clusterConf(cacheOn = false, pushPartialAgg = false): _*) {
      expectedRowCounts.toSeq.sortBy(_._1).foreach { case (name, expected) =>
        val q = resourceToString(s"tpcds/$name.sql",
          classLoader = Thread.currentThread().getContextClassLoader)
        val actual = spark.sql(q).collect().length
        assert(actual == expected,
          s"$name returned $actual rows, expected $expected. The dataset under " +
          s"$realTpcdsDataPath was generated with different sampling parameters than " +
          s"the ones recorded in SampleRealTpcdsData.")
      }
    }
  }
}
