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
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSparkSession

class FileStreamV2WriteSuite extends StreamTest with SharedSparkSession {

  // Clear the V1 source list so file streaming writes use the V2 path.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

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

  test("SPARK-56233: basic streaming write to parquet") {
    withTempDir { outputDir =>
      withTempDir { checkpointDir =>
        import testImplicits._
        val input = MemoryStream[Int]
        val df = input.toDF()

        val query = df.writeStream
          .format("parquet")
          .option("checkpointLocation", checkpointDir.getPath)
          .option("path", outputDir.getPath)
          .start()

        try {
          input.addData(1, 2, 3)
          query.processAllAvailable()

          checkAnswer(
            spark.read.parquet(outputDir.getPath),
            Seq(1, 2, 3).toDF())

          // Verify _spark_metadata exists
          val metadataDir = new File(outputDir, "_spark_metadata")
          assert(metadataDir.exists(),
            "Expected _spark_metadata directory for streaming write")
        } finally {
          query.stop()
        }
      }
    }
  }

  test("SPARK-56233: multiple batches accumulate correctly") {
    withTempDir { outputDir =>
      withTempDir { checkpointDir =>
        import testImplicits._
        val input = MemoryStream[Int]
        val df = input.toDF()

        val query = df.writeStream
          .format("parquet")
          .option("checkpointLocation", checkpointDir.getPath)
          .option("path", outputDir.getPath)
          .start()

        try {
          input.addData(1, 2, 3)
          query.processAllAvailable()

          input.addData(4, 5, 6)
          query.processAllAvailable()

          input.addData(7, 8, 9)
          query.processAllAvailable()

          checkAnswer(
            spark.read.parquet(outputDir.getPath),
            (1 to 9).map(i => Tuple1(i)).toDF())
        } finally {
          query.stop()
        }
      }
    }
  }

  test("SPARK-56233: checkpoint recovery after restart") {
    withTempDir { srcDir =>
      withTempDir { tmpDir =>
        withTempDir { outputDir =>
          withTempDir { checkpointDir =>
            val schema = spark.range(0).schema

            // Write initial source files
            writeParquetFilesToDir(
              spark.range(10).toDF(), srcDir, tmpDir)

            // First run
            val df1 = spark.readStream
              .schema(schema)
              .parquet(srcDir.getPath)

            val q1 = df1.writeStream
              .format("parquet")
              .option("checkpointLocation", checkpointDir.getPath)
              .option("path", outputDir.getPath)
              .start()
            q1.processAllAvailable()
            q1.stop()

            val firstCount =
              spark.read.parquet(outputDir.getPath).count()
            assert(firstCount == 10,
              s"Expected 10 rows after first run, got $firstCount")

            // Add more source files
            writeParquetFilesToDir(
              spark.range(10, 20).toDF(), srcDir, tmpDir)

            // Second run with same checkpoint - should NOT reprocess batch 1
            val df2 = spark.readStream
              .schema(schema)
              .parquet(srcDir.getPath)

            val q2 = df2.writeStream
              .format("parquet")
              .option("checkpointLocation", checkpointDir.getPath)
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

  test("SPARK-56233: streaming write works with JSON format") {
    withTempDir { outputDir =>
      withTempDir { checkpointDir =>
        import testImplicits._
        val input = MemoryStream[Int]
        val df = input.toDF()

        val query = df.writeStream
          .format("json")
          .option("checkpointLocation", checkpointDir.getPath)
          .option("path", outputDir.getPath)
          .start()

        try {
          input.addData(10, 20, 30)
          query.processAllAvailable()

          checkAnswer(
            spark.read.json(outputDir.getPath),
            Seq(10, 20, 30).toDF())
        } finally {
          query.stop()
        }
      }
    }
  }
}
