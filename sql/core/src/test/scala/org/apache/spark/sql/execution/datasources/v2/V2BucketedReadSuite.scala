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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for bucket pruning and bucket join via the V2 file read path
 * (BatchScanExec / FileScan). All tests disable AQE and clear USE_V1_SOURCE_LIST
 * so that tables are resolved through the V2 catalog path.
 */
class V2BucketedReadSuite extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private def collectBatchScans(plan: SparkPlan): Seq[BatchScanExec] = {
    collectWithSubqueries(plan) { case b: BatchScanExec => b }
  }

  // Must be called inside a withSQLConf block that clears USE_V1_SOURCE_LIST
  // so the catalog resolves the table as a V2 FileTable.
  private def withBucketedTable(
      tableName: String,
      numBuckets: Int,
      bucketCol: String,
      sortCol: Option[String] = None)(f: => Unit): Unit = {
    withTable(tableName) {
      val writer = spark.range(100)
        .selectExpr("id", "id % 10 as key", "cast(id as string) as value")
        .write
        .bucketBy(numBuckets, bucketCol)
      val sorted = sortCol.map(c => writer.sortBy(c)).getOrElse(writer)
      sorted.saveAsTable(tableName)
      f
    }
  }

  test("SPARK-56231: bucket pruning filters files by bucket ID") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.BUCKETING_ENABLED.key -> "true",
      SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false") {

      withBucketedTable("t1", numBuckets = 8, bucketCol = "key") {
        val df = spark.table("t1").filter("key = 3")
        val plan = df.queryExecution.executedPlan
        val batchScans = collectBatchScans(plan)
        assert(batchScans.nonEmpty, "Expected at least one BatchScanExec in plan")

        val fileScan = batchScans.head.scan.asInstanceOf[FileScan]
        assert(fileScan.bucketedScan, "Expected bucketedScan = true")

        val partitions = fileScan.planInputPartitions()
        val nonEmpty = partitions.count {
          case fp: FilePartition => fp.files.nonEmpty
          case _ => true
        }
        // Only 1 bucket out of 8 should have files for key = 3
        assert(nonEmpty <= 1,
          s"Expected at most 1 non-empty partition for key = 3, but got $nonEmpty")

        checkAnswer(df, spark.range(100)
          .selectExpr("id", "id % 10 as key", "cast(id as string) as value")
          .filter("key = 3"))
      }
    }
  }

  test("SPARK-56231: bucket pruning with IN filter") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.BUCKETING_ENABLED.key -> "true",
      SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false") {

      withBucketedTable("t1", numBuckets = 8, bucketCol = "key") {
        val df = spark.table("t1").filter("key IN (1, 3)")
        val plan = df.queryExecution.executedPlan
        val batchScans = collectBatchScans(plan)
        assert(batchScans.nonEmpty, "Expected at least one BatchScanExec in plan")

        val fileScan = batchScans.head.scan.asInstanceOf[FileScan]
        assert(fileScan.bucketedScan, "Expected bucketedScan = true")

        val partitions = fileScan.planInputPartitions()
        val nonEmpty = partitions.count {
          case fp: FilePartition => fp.files.nonEmpty
          case _ => true
        }
        // At most 2 buckets should be non-empty (one for key=1, one for key=3)
        assert(nonEmpty <= 2,
          s"Expected at most 2 non-empty partitions for key IN (1, 3), but got $nonEmpty")

        checkAnswer(df, spark.range(100)
          .selectExpr("id", "id % 10 as key", "cast(id as string) as value")
          .filter("key IN (1, 3)"))
      }
    }
  }

  test("SPARK-56231: bucketed join avoids shuffle") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.BUCKETING_ENABLED.key -> "true",
      SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      withBucketedTable("t1", numBuckets = 8, bucketCol = "key") {
        withBucketedTable("t2", numBuckets = 8, bucketCol = "key") {
          val df = spark.table("t1").join(spark.table("t2"), "key")
          val plan = df.queryExecution.executedPlan

          val shuffles = collectWithSubqueries(plan) {
            case s: ShuffleExchangeExec => s
          }
          assert(shuffles.isEmpty,
            s"Expected no shuffles but found ${shuffles.size}: ${shuffles.mkString(", ")}")

          val batchScans = collectBatchScans(plan)
          assert(batchScans.size >= 2,
            s"Expected at least 2 BatchScanExec nodes but found ${batchScans.size}")
          batchScans.foreach { scan =>
            assert(scan.outputPartitioning.isInstanceOf[HashPartitioning],
              s"Expected HashPartitioning but got " +
              s"${scan.outputPartitioning.getClass.getSimpleName}")
          }

          val t1Data = spark.range(100)
            .selectExpr("id", "id % 10 as key", "cast(id as string) as value")
          val t2Data = spark.range(100)
            .selectExpr("id", "id % 10 as key", "cast(id as string) as value")
          assert(df.count() == t1Data.join(t2Data, "key").count())
        }
      }
    }
  }

  test("SPARK-56231: disable unnecessary bucketed scan for simple select") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.BUCKETING_ENABLED.key -> "true",
      SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "true") {

      withBucketedTable("t1", numBuckets = 8, bucketCol = "key") {
        val df = spark.table("t1")
        val plan = df.queryExecution.executedPlan
        val batchScans = collectBatchScans(plan)
        assert(batchScans.nonEmpty, "Expected at least one BatchScanExec in plan")

        val fileScan = batchScans.head.scan.asInstanceOf[FileScan]
        assert(!fileScan.bucketedScan,
          "Expected bucketedScan = false for simple SELECT " +
          "(DisableUnnecessaryBucketedScan should disable it)")

        checkAnswer(df, spark.range(100)
          .selectExpr("id", "id % 10 as key", "cast(id as string) as value"))
      }
    }
  }

  test("SPARK-56231: coalesce buckets in join") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.BUCKETING_ENABLED.key -> "true",
      SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true") {

      // Create 8-bucket and 4-bucket tables on the same join key.
      // CoalesceBucketsInJoin should coalesce the 8-bucket side down to 4
      // so that both sides use 4 partitions and no shuffle is needed.
      withBucketedTable("t1", numBuckets = 8, bucketCol = "key") {
        withBucketedTable("t2", numBuckets = 4, bucketCol = "key") {
          val t1 = spark.table("t1")
          val t2 = spark.table("t2")
          val df = t1.join(t2, t1("key") === t2("key"))
          val plan = df.queryExecution.executedPlan

          val batchScans = collectBatchScans(plan)
          assert(batchScans.size >= 2,
            s"Expected at least 2 BatchScanExec nodes but found ${batchScans.size}")
          batchScans.foreach { b =>
            val fs = b.scan.asInstanceOf[FileScan]
            assert(fs.bucketSpec.isDefined,
              s"Expected bucketSpec to be defined. bucketedScan=${fs.bucketedScan}")
          }

          val coalescedScans = batchScans.filter { b =>
            b.scan.isInstanceOf[FileScan] &&
              b.scan.asInstanceOf[FileScan].optionalNumCoalescedBuckets.isDefined
          }
          assert(coalescedScans.nonEmpty,
            "Expected CoalesceBucketsInJoin to coalesce the 8-bucket scan. " +
            s"Plan:\n${df.queryExecution.sparkPlan}")
          assert(coalescedScans.head.scan
            .asInstanceOf[FileScan].optionalNumCoalescedBuckets.get == 4)

          val shuffles = collectWithSubqueries(plan) {
            case s: ShuffleExchangeExec => s
          }
          assert(shuffles.isEmpty,
            s"Expected no shuffles with bucket coalescing but found ${shuffles.size}")
          assert(df.count() > 0)
        }
      }
    }
  }

  test("SPARK-56231: bucketing disabled by config") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.BUCKETING_ENABLED.key -> "false") {

      withBucketedTable("t1", numBuckets = 8, bucketCol = "key") {
        val df = spark.table("t1")
        val plan = df.queryExecution.executedPlan
        val batchScans = collectBatchScans(plan)
        assert(batchScans.nonEmpty, "Expected at least one BatchScanExec in plan")

        val fileScan = batchScans.head.scan.asInstanceOf[FileScan]
        assert(!fileScan.bucketedScan,
          "Expected bucketedScan = false when bucketing is disabled by config")

        checkAnswer(df, spark.range(100)
          .selectExpr("id", "id % 10 as key", "cast(id as string) as value"))
      }
    }
  }
}
