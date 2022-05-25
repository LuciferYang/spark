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

import java.io.File

import scala.util.Random

import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf


/**
 * Benchmark to measure data source read performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/DataSourceReadBenchmark-results.txt".
 * }}}
 */
object PartitionTableReadBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("PartitionTableReadBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")

    sparkSession
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private def prepareTable(
      dir: File,
      df: DataFrame,
      partitions: Seq[String]): Unit = {
    val testDf = df.write.partitionBy("p1", "p2")

    saveAsParquetV1Table(testDf, dir.getCanonicalPath + "/parquetV1")
    saveAsParquetV2Table(testDf, dir.getCanonicalPath + "/parquetV2")
    saveAsOrcTable(testDf, dir.getCanonicalPath + "/orc")
  }

  private def saveAsParquetV1Table(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("parquetV1Table")
  }

  private def saveAsParquetV2Table(df: DataFrameWriter[Row], dir: String): Unit = {
    withSQLConf(ParquetOutputFormat.WRITER_VERSION ->
      ParquetProperties.WriterVersion.PARQUET_2_0.toString) {
      df.mode("overwrite").option("compression", "snappy").parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView("parquetV2Table")
    }
  }

  private def saveAsOrcTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").orc(dir)
    spark.read.orc(dir).createOrReplaceTempView("orcTable")
  }

  private def withParquetVersions(f: String => Unit): Unit = Seq("V1", "V2").foreach(f)

  def partitionTableScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Partitioned Table", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "parquetV1Table", "parquetV2Table", "orcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        val sqlText = "SELECT cast(value % 2 AS STRING) AS p1, " +
          "cast(value % 3 AS STRING) AS p2, value AS id FROM t1"

        prepareTable(dir, spark.sql(sqlText), Seq("p1", "p2"))

        withParquetVersions { version =>
          benchmark.addCase(s"Partition column - Parquet Vectorized: DataPage$version") { _ =>
            spark.sql(s"select p1, p2 from parquet${version}Table").noop()
          }
        }

        benchmark.addCase("Partition column - ORC Vectorized") { _ =>
          spark.sql("SELECT p1, p2 FROM orcTable").noop()
        }

        withParquetVersions { version =>
          benchmark.addCase(s"Both columns - Parquet Vectorized: DataPage$version") { _ =>
            spark.sql(s"select  p1, p2, id from parquet${version}Table").noop()
          }
        }

        benchmark.addCase("Both columns - ORC Vectorized") { _ =>
          spark.sql("SELECT  p1, p2, id FROM orcTable").noop()
        }

        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Partitioned Table Scan") {
      partitionTableScanBenchmark(1024 * 1024 * 100)
    }
  }
}
