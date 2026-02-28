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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure Parquet lazy materialization performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ParquetLazyMaterializationBenchmark-results.txt".
 * }}}
 */
object ParquetLazyMaterializationBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("ParquetLazyMaterializationBenchmark")
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    // Enable Parquet vectorized reader and WholeStageCodegen by default
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    sparkSession
  }

  // scalastyle:off
  def lazyMaterializationBenchmark(values: Int, width: Int): Unit = {
    val benchmark = new Benchmark(s"Parquet Lazy Materialization (rows=$values, width=$width)",
      values, output = output)

    withTempPath { dir =>
      // Prepare data
      val middle = width / 2
      val selectExpr = (1 to width).map(i => s"cast(id as string) as c$i")
      // Add an ID column for filtering
      val allExpr = Seq("id") ++ selectExpr

      spark.range(values)
        .withColumn("id", (rand() * 1000000000).cast("long"))
        .selectExpr(allExpr: _*)
        .write.parquet(dir.getCanonicalPath)

      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("t1")

      // Scenario 1: Clustered Data (Sorted by ID) - Best case for Lazy Materialization
      // When data is sorted, filtering on ID will skip entire row groups
      // or large consecutive batches.
      val sortedDir = s"${dir.getCanonicalPath}_sorted"
      spark.read.parquet(dir.getCanonicalPath).sort("id").write.parquet(sortedDir)
      spark.read.parquet(sortedDir).createOrReplaceTempView("t1_sorted")

      val highSelectivityFilter = "id < 10000000" // Approx 1%

      benchmark.addCase("Clustered Data - High Selectivity (1%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1_sorted WHERE $highSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Clustered Data - High Selectivity (1%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1_sorted WHERE $highSelectivityFilter").noop()
        }
      }

      val veryHighSelectivityFilter = "id < 1000000" // Approx 0.1%

      benchmark.addCase("Clustered Data - Very High Selectivity (0.1%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1_sorted WHERE $veryHighSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Clustered Data - Very High Selectivity (0.1%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1_sorted WHERE $veryHighSelectivityFilter").noop()
        }
      }

      val ultraHighSelectivityFilter = "id < 100000" // Approx 0.01%

      benchmark.addCase("Clustered Data - Ultra High Selectivity (0.01%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1_sorted WHERE $ultraHighSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Clustered Data - Ultra High Selectivity (0.01%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1_sorted WHERE $ultraHighSelectivityFilter").noop()
        }
      }

      val extremeHighSelectivityFilter = "id < 10000" // Approx 0.001%

      benchmark.addCase("Clustered Data - Extreme High Selectivity (0.001%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1_sorted WHERE $extremeHighSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Clustered Data - Extreme High Selectivity (0.001%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1_sorted WHERE $extremeHighSelectivityFilter").noop()
        }
      }

      // Scenario 2: Random Data - High Selectivity (Filter keeps 1% rows)
      // We filter on `id` which is the first column.
      // If lazy materialization works, c1...cWidth should not be decoded for 99% rows.
      // However, due to batch-level granularity, if one row in a batch matches,
      // the whole batch is decoded. Random distribution makes this scenario challenging.
      benchmark.addCase("Random Data - High Selectivity (1%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(s"SELECT sum(length(c$middle)) FROM t1 WHERE $highSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Random Data - High Selectivity (1%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(s"SELECT sum(length(c$middle)) FROM t1 WHERE $highSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Random Data - Very High Selectivity (0.1%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1 WHERE $veryHighSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Random Data - Very High Selectivity (0.1%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1 WHERE $veryHighSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Random Data - Ultra High Selectivity (0.01%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1 WHERE $ultraHighSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Random Data - Ultra High Selectivity (0.01%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1 WHERE $ultraHighSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Random Data - Extreme High Selectivity (0.001%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1 WHERE $extremeHighSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Random Data - Extreme High Selectivity (0.001%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1 WHERE $extremeHighSelectivityFilter").noop()
        }
      }

      // Scenario 3: Medium Selectivity (Filter keeps 50% rows)
      val mediumSelectivityFilter = "id < 500000000" // 50%

      benchmark.addCase("Medium Selectivity (50%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(s"SELECT sum(length(c$middle)) FROM t1 WHERE $mediumSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Medium Selectivity (50%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(s"SELECT sum(length(c$middle)) FROM t1 WHERE $mediumSelectivityFilter").noop()
        }
      }

      // Scenario 3: Low Selectivity (Filter keeps 100% rows)
      // This tests the overhead of the Lazy wrapper.
      val lowSelectivityFilter = "id is not null"

      benchmark.addCase("Low Selectivity (100%) - Eager") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "false") {
          spark.sql(s"SELECT sum(length(c$middle)) FROM t1 WHERE $lowSelectivityFilter").noop()
        }
      }

      benchmark.addCase("Low Selectivity (100%) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          spark.sql(s"SELECT sum(length(c$middle)) FROM t1 WHERE $lowSelectivityFilter").noop()
        }
      }

      // Scenario 4: Adaptive Fallback Test
      // Simulate a scenario where Lazy starts but should fallback to Eager because selectivity is low.
      // We reuse the low selectivity filter (100% match) which is the worst case for Lazy.
      benchmark.addCase("Adaptive Fallback (Low Selectivity) - Lazy") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_LAZY_MATERIALIZATION_ENABLED.key -> "true") {
          // This should trigger the adaptive fallback after a few batches
          spark.sql(
            s"SELECT sum(length(c$middle)) FROM t1 WHERE $lowSelectivityFilter").noop()
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Run with 20 million rows and 20 string columns
    // This should create enough IO/CPU pressure to make the difference visible.
    runBenchmark("Parquet Lazy Materialization Benchmark") {
      lazyMaterializationBenchmark(1024 * 1024 * 20, 20)
    }
  }
}
