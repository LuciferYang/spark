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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}

class FileStreamV2ReadSuite extends StreamTest with SharedSparkSession {

  // Clear the V1 source list so file streaming reads use the V2 path.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  // Writes data files directly into srcDir (not subdirectories),
  // because streaming file sources list files at the root level.
  private def writeParquetFilesToDir(
      df: DataFrame,
      srcDir: File,
      tmpDir: File): Unit = {
    val tmpOutput = new File(tmpDir, s"tmp_${System.nanoTime()}")
    df.write.parquet(tmpOutput.getPath)
    srcDir.mkdirs()
    tmpOutput.listFiles().filter(_.getName.endsWith(".parquet"))
      .foreach { f =>
        val dest = new File(srcDir, f.getName)
        f.renameTo(dest)
      }
  }

  private def writeJsonFilesToDir(
      df: DataFrame,
      srcDir: File,
      tmpDir: File): Unit = {
    val tmpOutput = new File(tmpDir, s"tmp_${System.nanoTime()}")
    df.write.json(tmpOutput.getPath)
    srcDir.mkdirs()
    tmpOutput.listFiles()
      .filter(f => f.getName.endsWith(".json"))
      .foreach { f =>
        val dest = new File(srcDir, f.getName)
        f.renameTo(dest)
      }
  }

  test("SPARK-56232: basic streaming read from parquet path") {
    withTempDir { srcDir =>
      withTempDir { tmpDir =>
        writeParquetFilesToDir(spark.range(10).toDF(), srcDir, tmpDir)

        val df = spark.readStream
          .schema(spark.range(0).schema)
          .parquet(srcDir.getPath)

        val query = df.writeStream
          .format("memory")
          .queryName("v2_stream_test")
          .start()

        try {
          query.processAllAvailable()
          checkAnswer(
            spark.table("v2_stream_test"),
            spark.range(10).toDF())
        } finally {
          query.stop()
        }
      }
    }
  }

  test("SPARK-56232: discovers new files across batches") {
    withTempDir { srcDir =>
      withTempDir { tmpDir =>
        writeParquetFilesToDir(
          spark.range(10).toDF(), srcDir, tmpDir)

        val df = spark.readStream
          .schema(spark.range(0).schema)
          .parquet(srcDir.getPath)

        val query = df.writeStream
          .format("memory")
          .queryName("v2_discovery_test")
          .start()

        try {
          query.processAllAvailable()
          assert(spark.table("v2_discovery_test").count() == 10)

          // Add more files
          writeParquetFilesToDir(
            spark.range(10, 20).toDF(), srcDir, tmpDir)
          query.processAllAvailable()
          assert(
            spark.table("v2_discovery_test").count() == 20)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("SPARK-56232: maxFilesPerTrigger limits files per batch") {
    withTempDir { srcDir =>
      withTempDir { tmpDir =>
        // Write 5 separate parquet files
        (0 until 5).foreach { i =>
          writeParquetFilesToDir(
            spark.range(i * 10, (i + 1) * 10).coalesce(1).toDF(),
            srcDir, tmpDir)
        }

        val df = spark.readStream
          .schema(spark.range(0).schema)
          .option("maxFilesPerTrigger", "2")
          .parquet(srcDir.getPath)

        val query = df.writeStream
          .format("memory")
          .queryName("v2_rate_test")
          .start()

        try {
          query.processAllAvailable()
          assert(spark.table("v2_rate_test").count() == 50)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("SPARK-56232: checkpoint recovery resumes from last offset") {
    withTempDir { srcDir =>
      withTempDir { tmpDir =>
        withTempDir { checkpointDir =>
          withTempDir { outputDir =>
            writeParquetFilesToDir(
              spark.range(10).toDF(), srcDir, tmpDir)

            val schema = spark.range(0).schema

            // First run
            val df1 = spark.readStream
              .schema(schema)
              .parquet(srcDir.getPath)

            val q1 = df1.writeStream
              .format("parquet")
              .option(
                "checkpointLocation", checkpointDir.getPath)
              .option("path", outputDir.getPath)
              .start()
            q1.processAllAvailable()
            q1.stop()

            val firstCount =
              spark.read.parquet(outputDir.getPath).count()
            assert(firstCount == 10,
              s"Expected 10 rows after first run, got $firstCount")

            // Add more files
            writeParquetFilesToDir(
              spark.range(10, 20).toDF(), srcDir, tmpDir)

            // Second run - should NOT reprocess batch1
            val df2 = spark.readStream
              .schema(schema)
              .parquet(srcDir.getPath)

            val q2 = df2.writeStream
              .format("parquet")
              .option(
                "checkpointLocation", checkpointDir.getPath)
              .option("path", outputDir.getPath)
              .start()
            q2.processAllAvailable()
            q2.stop()

            // Total should be 20 (10 from first + 10 new)
            val totalCount =
              spark.read.parquet(outputDir.getPath).count()
            assert(totalCount == 20,
              s"Expected 20 total rows, got $totalCount")
          }
        }
      }
    }
  }

  test("SPARK-56232: streaming uses V2 path (MicroBatchScanExec)") {
    withTempDir { srcDir =>
      withTempDir { tmpDir =>
        writeParquetFilesToDir(
          spark.range(10).toDF(), srcDir, tmpDir)

        val df = spark.readStream
          .schema(spark.range(0).schema)
          .parquet(srcDir.getPath)

        val query = df.writeStream
          .format("memory")
          .queryName("v2_path_test")
          .start()

        try {
          query.processAllAvailable()

          val lastExec = query
            .asInstanceOf[StreamingQueryWrapper]
            .streamingQuery.lastExecution
          assert(lastExec != null,
            "Expected at least one batch to execute")
          val hasV2Scan = lastExec.executedPlan.collect {
            case _: MicroBatchScanExec => true
          }.nonEmpty
          assert(hasV2Scan,
            "Expected MicroBatchScanExec (V2) in plan, " +
            s"got: ${lastExec.executedPlan.treeString}")
        } finally {
          query.stop()
        }
      }
    }
  }

  test("SPARK-56232: streaming read works with JSON format") {
    withTempDir { srcDir =>
      withTempDir { tmpDir =>
        val data = spark.range(10)
          .selectExpr("id", "cast(id as string) as name")
        writeJsonFilesToDir(data, srcDir, tmpDir)

        val schema = new StructType()
          .add("id", LongType)
          .add("name", StringType)

        val df = spark.readStream
          .schema(schema)
          .json(srcDir.getPath)

        val query = df.writeStream
          .format("memory")
          .queryName("v2_json_test")
          .start()

        try {
          query.processAllAvailable()
          assert(spark.table("v2_json_test").count() == 10)
        } finally {
          query.stop()
        }
      }
    }
  }
}
