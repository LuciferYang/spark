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

import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SPARK-30628 lifts a scalar-subquery filter on a partition column into
 * `BatchScanExec.runtimeFilters`, so a V2 file source can prune with no join anywhere. The
 * auto-CTE veto cannot see that path -- `hasInBodyDPPOpportunity` and
 * `looksLikeMaterializationNotWorthIt` both only match `Join` -- so a body of this shape is cached
 * without the veto ever weighing the trade.
 *
 * Measured outcome: caching it loses nothing, and these tests pin the three independent facts that
 * make that true. If any one of them changes, the pruning disappears silently.
 *
 *   1. The per-reference predicates are merged with `Or` and that `Or` reaches the scan as ONE
 *      runtime filter. Split into two conjuncts it would prune to the intersection and drop rows.
 *   2. `canInject`'s subquery guard refuses to inject a scalar subquery, so `prePushdownBody`
 *      falls back to the whole merged predicate rather than a body with the predicate stripped.
 *      That fallback is what keeps the driving predicate inside the cached body.
 *   3. The filter's references are a subset of the partition columns, which is what
 *      `DataSourceV2Strategy` requires before lifting it.
 */
class AutoCteJoinFreePruningSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: org.apache.spark.SparkConf =
    super.sparkConf.set(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key, "0")

  private val factRows = 100
  private val numParts = 10
  private val rowsPerPart = factRows / numParts

  private def writeTables(dir: java.io.File): Unit = {
    val factPath = new java.io.File(dir, "fact").getCanonicalPath
    val dimPath = new java.io.File(dir, "dim").getCanonicalPath
    spark.range(factRows).selectExpr("id", s"id % $numParts AS part")
      .write.mode("overwrite").partitionBy("part").parquet(factPath)
    spark.read.parquet(factPath).createOrReplaceTempView("fact")
    spark.range(numParts).selectExpr("id AS dim_id", "id AS dim_val")
      .write.mode("overwrite").parquet(dimPath)
    spark.read.parquet(dimPath).createOrReplaceTempView("dim")
  }

  /**
   * Body is an aggregate over the partitioned fact with NO join. The two references select
   * DIFFERENT partitions (7 and 3) and are combined with `UNION ALL`, so intersection and union
   * are distinguishable -- a join on the partition column, or two subqueries returning the same
   * value, would hide the difference.
   */
  private val cteSQL =
    """WITH body AS (
      |  SELECT part, count(*) AS c FROM fact GROUP BY part
      |)
      |SELECT part, c FROM body WHERE part = (SELECT max(dim_id) FROM dim WHERE dim_val <= 7)
      |UNION ALL
      |SELECT part, c FROM body WHERE part = (SELECT min(dim_id) FROM dim WHERE dim_val >= 3)
      |""".stripMargin

  private def dppV2Conf(autoCte: String): Seq[(String, String)] = Seq(
    SQLConf.USE_V1_SOURCE_LIST.key -> "",
    SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
    SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "2",
    SQLConf.AUTO_REUSED_CTE_ENABLED.key -> autoCte)

  private def factScan(label: String, plan: SparkPlan): BatchScanExec = {
    val scans = collect(plan) { case b: BatchScanExec => b }
    assert(scans.nonEmpty, s"[$label] no V2 scan; the fixture is not on the V2 path")
    scans.maxBy(_.metrics.get("numOutputRows").map(_.value).getOrElse(0L))
  }

  private def answerOf(sqlText: String): Seq[String] =
    spark.sql(sqlText).collect().map(_.toString).sorted.toSeq

  test("caching a join-free, subquery-pruned body keeps both the answer and the pruning") {
    withTempDir { dir =>
      val inlined = withSQLConf(dppV2Conf("false"): _*) {
        spark.catalog.clearCache()
        writeTables(dir)
        answerOf(cteSQL)
      }
      assert(inlined === Seq("[3,10]", "[7,10]"),
        "the fixture must return one row per selected partition")

      withSQLConf(dppV2Conf("true"): _*) {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        spark.catalog.clearCache()
        writeTables(dir)
        val df = spark.sql(cteSQL)
        val cached = df.collect().map(_.toString).sorted.toSeq
        assert(cached === inlined, s"cached answer $cached differs from inlined $inlined")

        val imrs = df.queryExecution.optimizedPlan.collect { case i: InMemoryRelation => i }
        assert(imrs.nonEmpty, "the body must be cached, or this test measures the wrong path")
        assert(spark.sharedState.autoCTECacheManager.numEntries == 1)

        val scan = factScan("cached", imrs.head.cacheBuilder.cachedPlan)
        assert(scan.runtimeFilters.size == 1,
          "the merged predicate must reach the scan as ONE filter; two AND-ed filters would " +
            s"prune to the intersection: ${scan.runtimeFilters}")
        assert(scan.runtimeFilters.head.exists(_.isInstanceOf[Or]),
          s"the single runtime filter must still be the Or of both references: " +
            s"${scan.runtimeFilters.head}")
        assert(scan.metrics("numOutputRows").value == 2 * rowsPerPart,
          s"the cached body must read exactly the two selected partitions " +
            s"(${2 * rowsPerPart} of $factRows rows), got " +
            s"${scan.metrics("numOutputRows").value}")
      }
    }
  }
}
