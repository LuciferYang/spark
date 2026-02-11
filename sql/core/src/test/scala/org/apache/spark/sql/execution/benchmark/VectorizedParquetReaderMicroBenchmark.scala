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

import scala.jdk.CollectionConverters._

import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.{SparkConf, TestUtils}
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * Micro benchmark to isolate vectorized Parquet reader decode costs.
 * {{ {
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/<this class>-results.txt".
 * } }}
 */
object VectorizedParquetReaderMicroBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("VectorizedParquetReaderMicroBenchmark")
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")

    SparkSession.builder().config(conf).getOrCreate()
  }

  private val rows = 2 * 1000 * 1000

  private def writeParquet(df: DataFrame, dir: File, version: String): File = {
    val out = new File(dir, s"parquet$version")
    val writerVersion = version match {
      case "V1" => ParquetProperties.WriterVersion.PARQUET_1_0
      case "V2" => ParquetProperties.WriterVersion.PARQUET_2_0
      case other => throw new IllegalArgumentException(s"Unknown parquet version: $other")
    }
    withSQLConf(ParquetOutputFormat.WRITER_VERSION -> writerVersion.toString) {
      df.write.mode("overwrite").parquet(out.getCanonicalPath)
    }
    out
  }

  private def runVectorizedReader(
      files: Array[String],
      column: String,
      dataType: DataType,
      enableOffHeap: Boolean,
      batchSize: Int): Unit = {
    var longSum = 0L
    var doubleSum = 0.0
    var stringLenSum = 0L
    var booleanSum = 0L
    var decimalSum = java.math.BigDecimal.ZERO

    val aggregateValue: (ColumnVector, Int) => Unit = dataType match {
      case BooleanType => (col: ColumnVector, i: Int) =>
        if (col.getBoolean(i)) booleanSum += 1L
      case ByteType => (col: ColumnVector, i: Int) =>
        longSum += col.getByte(i)
      case ShortType => (col: ColumnVector, i: Int) =>
        longSum += col.getShort(i)
      case IntegerType => (col: ColumnVector, i: Int) =>
        longSum += col.getInt(i)
      case LongType => (col: ColumnVector, i: Int) =>
        longSum += col.getLong(i)
      case DoubleType => (col: ColumnVector, i: Int) =>
        doubleSum += col.getDouble(i)
      case FloatType => (col: ColumnVector, i: Int) =>
        doubleSum += col.getFloat(i)
      case StringType => (col: ColumnVector, i: Int) =>
        stringLenSum += col.getUTF8String(i).numBytes()
      case BinaryType => (col: ColumnVector, i: Int) =>
        stringLenSum += col.getBinary(i).length
      case _: DecimalType => (col: ColumnVector, i: Int) =>
        decimalSum = decimalSum.add(col.getDecimal(i, 10, 2).toJavaBigDecimal)
      case _ => throw new IllegalArgumentException(s"Unsupported type: $dataType")
    }

    files.foreach { p =>
      val reader = new VectorizedParquetRecordReader(enableOffHeap, batchSize)
      try {
        reader.initialize(p, (column :: Nil).asJava)
        val batch = reader.resultBatch()
        val col = batch.column(0)
        while (reader.nextBatch()) {
          val numRows = batch.numRows()
          var i = 0
          while (i < numRows) {
            if (!col.isNullAt(i)) aggregateValue(col, i)
            i += 1
          }
        }
      } finally {
        reader.close()
      }
    }

    // Prevent dead-code elimination.
    if (longSum == 42L || doubleSum == 42.0 || stringLenSum == 42L ||
        booleanSum == 42L || decimalSum.signum() == 42) {
      throw new IllegalStateException("Unreachable")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmark = new Benchmark("Vectorized Parquet Reader Micro", rows, output = output)

    withTempPath { dir =>
      import spark.implicits._
      val df = spark.range(rows)
        .select(
          (($"id" % 2) === 0).as("b"),
          (($"id" % 128).cast(ByteType)).as("by"),
          (($"id" % 32768).cast(ShortType)).as("sh"),
          ($"id".cast(IntegerType)).as("i"),
          ($"id".cast(LongType)).as("l"),
          ($"id".cast(DoubleType)).as("d"),
          ($"id".cast(FloatType)).as("f"),
          ($"id".cast(StringType)).as("s"),
          ($"id".cast(StringType)).cast(BinaryType).as("bin"),
          (($"id" % 1000000).cast(DecimalType(10, 2))).as("dec"))
        .repartition(1)

      val parquetV1 = writeParquet(df, dir, "V1")
      val parquetV2 = writeParquet(df, dir, "V2")

      val enableOffHeap = spark.sessionState.conf.offHeapColumnVectorEnabled
      val batchSize = spark.sessionState.conf.parquetVectorizedReaderBatchSize

      // Helper function to add benchmark cases for a data type
      def addBenchmarkCases(
          version: String,
          path: File,
          typeConfigs: Seq[(String, String, DataType)]): Unit = {
        val files = TestUtils.listDirectory(path).filter(_.endsWith(".parquet"))
        typeConfigs.foreach { case (displayName, column, dataType) =>
          benchmark.addCase(s"Vectorized Parquet $displayName: DataPage$version") { _ =>
            runVectorizedReader(files, column, dataType, enableOffHeap, batchSize)
          }
        }
      }

      val typeConfigs = Seq(
        ("Boolean", "b", BooleanType),
        ("Byte", "by", ByteType),
        ("Short", "sh", ShortType),
        ("Int", "i", IntegerType),
        ("Long", "l", LongType),
        ("Float", "f", FloatType),
        ("Double", "d", DoubleType),
        ("String", "s", StringType),
        ("Binary", "bin", BinaryType),
        ("Decimal", "dec", DecimalType(10, 2))
      )

      Seq("V1" -> parquetV1, "V2" -> parquetV2).foreach { case (version, path) =>
        addBenchmarkCases(version, path, typeConfigs)
      }

      benchmark.run()
    }
  }
}

