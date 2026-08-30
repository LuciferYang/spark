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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Pins the assumption `PruningEligibility.hasInBodyDPPOpportunity` rests on: a CTE body that
 * prunes itself is safe to cache, because the pruning still happens when `CacheManager`
 * materialises the body. That path is not the normal execution path -- AQE is off there, and with
 * `spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly` at its default `true` a DPP
 * filter with no reusable broadcast degrades to `DynamicPruningExpression(TrueLiteral)`, which
 * prunes nothing.
 *
 * V2 file sources reached this path only with SPARK-30628, so nothing covered it before.
 *
 * The fixture is built so the assertion cannot pass vacuously: `fact` holds 100 rows in 10
 * partitions of 10, and the dim filter selects exactly one of them, so a pruning scan reads 10
 * rows and a non-pruning scan reads 100. The DPP-disabled case asserts the 100 to prove the
 * measurement can tell the two apart.
 *
 * Note the views have to be created inside the conf block: `spark.read.parquet` resolves the
 * relation when the view is made, so building them under the default `useV1SourceList` yields V1
 * `HadoopFsRelation` plans and every V2 assertion here would silently find nothing to check.
 */
class AutoCteV2DppMaterializationSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: org.apache.spark.SparkConf =
    super.sparkConf.set(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key, "0")

  private val factRows = 100
  private val numParts = 10
  private val rowsPerPart = factRows / numParts

  private def writeFactAndDim(dir: java.io.File): Unit = {
    val factPath = new java.io.File(dir, "fact").getCanonicalPath
    val dimPath = new java.io.File(dir, "dim").getCanonicalPath
    spark.range(factRows).selectExpr("id", s"id % $numParts AS part")
      .write.mode("overwrite").partitionBy("part").parquet(factPath)
    spark.read.parquet(factPath).createOrReplaceTempView("fact")
    spark.range(numParts).selectExpr("id AS dim_id", "id AS dim_val")
      .write.mode("overwrite").parquet(dimPath)
    spark.read.parquet(dimPath).createOrReplaceTempView("dim")
  }

  /** Body with in-body DPP on the fact partition column, referenced twice so it gets cached. */
  private val cteSQL =
    """WITH body AS (
      |  SELECT f.part, count(*) AS c
      |  FROM fact f JOIN dim d ON f.part = d.dim_id
      |  WHERE d.dim_val = 7
      |  GROUP BY f.part
      |)
      |SELECT a.part, a.c, b.c FROM body a JOIN body b ON a.part = b.part""".stripMargin

  /** The fact-side scan, i.e. the only one carrying runtime filters. */
  private def factScan(plan: SparkPlan): BatchScanExec = {
    val scans = collect(plan) { case b: BatchScanExec => b }
    assert(scans.nonEmpty, s"no V2 scan in plan; fixture is not on the V2 path:\n$plan")
    scans.maxBy(_.metrics.get("numOutputRows").map(_.value).getOrElse(0L))
  }

  private def dppConf(reuseBroadcastOnly: String, dpp: String): Seq[(String, String)] = Seq(
    SQLConf.USE_V1_SOURCE_LIST.key -> "",
    SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> dpp,
    SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> reuseBroadcastOnly,
    SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "2",
    SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true")

  private def cachedFactScan(dir: java.io.File, conf: Seq[(String, String)]): BatchScanExec = {
    withSQLConf(conf: _*) {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      spark.catalog.clearCache()
      writeFactAndDim(dir)
      val df = spark.sql(cteSQL)
      df.collect()
      val imrs = df.queryExecution.optimizedPlan.collect { case i: InMemoryRelation => i }
      assert(imrs.nonEmpty, "the body must be cached, or this measures the wrong path")
      factScan(imrs.head.cacheBuilder.cachedPlan)
    }
  }

  Seq("true", "false").foreach { reuse =>
    test(s"in-body DPP prunes while CacheManager materialises the body " +
      s"(reuseBroadcastOnly=$reuse)") {
      withTempDir { dir =>
        val scan = cachedFactScan(dir, dppConf(reuse, dpp = "true"))
        assert(scan.runtimeFilters.nonEmpty,
          "the cached body's fact scan must carry the DPP filter")
        assert(!scan.runtimeFilters.forall(_.children.forall(_.isInstanceOf[Literal])),
          s"the DPP filter degraded to a literal, so it prunes nothing: ${scan.runtimeFilters}")
        assert(scan.metrics("numOutputRows").value == rowsPerPart,
          s"expected one pruned partition ($rowsPerPart rows), got " +
            s"${scan.metrics("numOutputRows").value} of $factRows")
      }
    }
  }

  test("without DPP the same cached body reads every partition") {
    // Proves the row count above can distinguish pruning from no pruning.
    withTempDir { dir =>
      val scan = cachedFactScan(dir, dppConf("true", dpp = "false"))
      assert(scan.metrics("numOutputRows").value == factRows,
        s"expected a full scan ($factRows rows), got ${scan.metrics("numOutputRows").value}")
    }
  }
}
