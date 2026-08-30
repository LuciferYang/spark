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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, EqualTo, Literal}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * `replanWithRuntimeFilters` sends a V2 `FileScan` through
 * `planInputPartitionsWithRuntimeFilters` only when a filter can actually prune. The gate used to
 * be `runtimeFilters.nonEmpty`, which also admits the `DynamicPruningExpression(TrueLiteral)` that
 * `PlanDynamicPruningFilters` substitutes when `reuseBroadcastOnly` is on and no reusable
 * broadcast exists: a full `fileIndex.listFiles` plus `FilePartition` bucketing that cannot drop
 * one file.
 *
 * The avoided listing has no metric on this path, so these tests pin the classification and the
 * unchanged results, not the work saved.
 */
class PushDownUtilsRuntimeFilterGateSuite extends SharedSparkSession {

  test("the placeholder DPP filter is classified as unable to prune") {
    assert(PushDownUtils.isNoOpRuntimeFilter(
      DynamicPruningExpression(Literal.TrueLiteral)),
      "the value PlanDynamicPruningFilters leaves behind must not open the pruning gate")
    assert(PushDownUtils.isNoOpRuntimeFilter(Literal.TrueLiteral))
  }

  test("a real DPP filter is still classified as able to prune") {
    val real = DynamicPruningExpression(
      EqualTo(Literal(1, IntegerType), Literal(2, IntegerType)))
    assert(!PushDownUtils.isNoOpRuntimeFilter(real),
      "a filter with a non-literal-true child must keep going through the FileScan path")
    assert(!PushDownUtils.isNoOpRuntimeFilter(Literal(false)),
      "only TrueLiteral is a no-op; FalseLiteral prunes everything")
  }

  test("a query whose DPP degrades to the placeholder still returns the right rows") {
    // reuseBroadcastOnly=true with no reusable broadcast is what produces the placeholder, so
    // this exercises the skip path end to end.
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "2") {
      withTempDir { dir =>
        val factPath = new java.io.File(dir, "fact").getCanonicalPath
        val dimPath = new java.io.File(dir, "dim").getCanonicalPath
        spark.range(100).selectExpr("id", "id % 10 AS part")
          .write.mode("overwrite").partitionBy("part").parquet(factPath)
        spark.read.parquet(factPath).createOrReplaceTempView("fact")
        spark.range(10).selectExpr("id AS dim_id", "id AS dim_val")
          .write.mode("overwrite").parquet(dimPath)
        spark.read.parquet(dimPath).createOrReplaceTempView("dim")
        val df = spark.sql(
          """SELECT count(*) FROM fact f JOIN dim d ON f.part = d.dim_id
            |WHERE d.dim_val = 7""".stripMargin)
        assert(df.collect() === Array(Row(10L)))
      }
    }
  }
}
