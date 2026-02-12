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

import java.nio.{ByteBuffer, ByteOrder}

import scala.util.Random

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.datasources.parquet.VectorizedPlainValuesReader
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{BooleanType, ByteType}

/**
 * Benchmark for VectorizedPlainValuesReader optimizations.
 *
 * This benchmark directly tests the performance of VectorizedPlainValuesReader methods,
 * which is useful for validating micro-optimizations like:
 *   - readBytes() array fast path optimization
 *   - readBoolean() branch prediction optimization
 *
 * IMPORTANT: VectorizedPlainValuesReader is only used when the Parquet column uses PLAIN encoding.
 * If dictionary encoding is used, VectorizedRleValuesReader is used instead.
 * See VectorizedColumnReader.getValuesReader() for details.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to
 *        "benchmarks/VectorizedPlainValuesReaderBenchmark-results.txt".
 * }}}
 */
object VectorizedPlainValuesReaderBenchmark extends BenchmarkBase {

  /**
   * Benchmark for readBooleans() batch method.
   * This tests the performance of reading boolean values in batch mode.
   */
  private def readBooleansBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark(s"readBooleans ($values values)", values, output = output)

    // Boolean data: each 8 values occupy 1 byte
    val numBytes = (values + 7) / 8
    val data = new Array[Byte](numBytes)
    Random.nextBytes(data)

    benchmark.addCase("VectorizedPlainValuesReader.readBooleans()") { _ =>
      val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      val in = ByteBufferInputStream.wrap(buffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)

      val column = new OnHeapColumnVector(values, BooleanType)
      try {
        reader.readBooleans(values, column, 0)
      } finally {
        column.close()
      }
    }

    benchmark.run()
  }

  /**
   * Benchmark for readBoolean() single value method.
   * This tests the branch prediction optimization in readBoolean().
   *
   * The optimization changes from:
   *   if (bitOffset == 0) updateCurrentByte()  // check before read
   *   read value
   *   if (bitOffset == 8) bitOffset = 0
   *
   * To:
   *   read value
   *   if (bitOffset == 8) { bitOffset = 0; updateCurrentByte() }  // check after read
   *
   * This reduces branch checks from 2 to 1 and improves CPU branch prediction.
   */
  private def readBooleanSingleBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark(s"readBoolean single ($values calls)", values, output = output)

    val numBytes = (values + 7) / 8
    val data = new Array[Byte](numBytes)
    Random.nextBytes(data)

    benchmark.addCase("VectorizedPlainValuesReader.readBoolean() loop") { _ =>
      val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      val in = ByteBufferInputStream.wrap(buffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)

      var sum = 0
      var i = 0
      while (i < values) {
        if (reader.readBoolean()) sum += 1
        i += 1
      }
    }

    benchmark.run()
  }

  /**
   * Benchmark for readBytes() method.
   * This tests the array fast path optimization in readBytes().
   *
   * The optimization adds a fast path when buffer.hasArray() is true:
   * - Direct array access with stride calculation
   * - Single buffer.position() update at the end
   *
   * Instead of:
   * - buffer.get() + buffer.position(position + 3) for each value
   */
  private def readBytesBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark(s"readBytes ($values values)", values, output = output)

    // Byte data: each value occupies 4 bytes (INT32 storage in Parquet)
    val data = new Array[Byte](values * 4)
    Random.nextBytes(data)

    // Test with heap buffer (should use fast path)
    benchmark.addCase("readBytes() - heap buffer (fast path)") { _ =>
      val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      val in = ByteBufferInputStream.wrap(buffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)

      val column = new OnHeapColumnVector(values, ByteType)
      try {
        reader.readBytes(values, column, 0)
      } finally {
        column.close()
      }
    }

    // Test with direct buffer (should use slow path)
    benchmark.addCase("readBytes() - direct buffer (slow path)") { _ =>
      val directBuffer = ByteBuffer.allocateDirect(values * 4).order(ByteOrder.LITTLE_ENDIAN)
      directBuffer.put(data)
      directBuffer.flip()
      val in = ByteBufferInputStream.wrap(directBuffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)

      val column = new OnHeapColumnVector(values, ByteType)
      try {
        reader.readBytes(values, column, 0)
      } finally {
        column.close()
      }
    }

    benchmark.run()
  }

  /**
   * Benchmark comparing readBooleans() batch vs readBoolean() single value loop.
   * This helps understand the overhead of single-value reads.
   */
  private def booleanBatchVsSingleBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark(
      s"Boolean batch vs single ($values values)", values, output = output)

    val numBytes = (values + 7) / 8
    val data = new Array[Byte](numBytes)
    Random.nextBytes(data)

    benchmark.addCase("readBooleans() batch") { _ =>
      val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      val in = ByteBufferInputStream.wrap(buffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)

      val column = new OnHeapColumnVector(values, BooleanType)
      try {
        reader.readBooleans(values, column, 0)
      } finally {
        column.close()
      }
    }

    benchmark.addCase("readBoolean() single loop") { _ =>
      val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      val in = ByteBufferInputStream.wrap(buffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)

      val column = new OnHeapColumnVector(values, BooleanType)
      try {
        var i = 0
        while (i < values) {
          column.putBoolean(i, reader.readBoolean())
          i += 1
        }
      } finally {
        column.close()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("VectorizedPlainValuesReader.readBooleans() batch") {
      Seq(10000, 100000, 1000000, 10000000).foreach(readBooleansBenchmark)
    }

    runBenchmark("VectorizedPlainValuesReader.readBoolean() single value") {
      Seq(10000, 100000, 1000000).foreach(readBooleanSingleBenchmark)
    }

    runBenchmark("VectorizedPlainValuesReader.readBytes()") {
      Seq(10000, 100000, 1000000, 10000000).foreach(readBytesBenchmark)
    }

    runBenchmark("Boolean batch vs single comparison") {
      Seq(10000, 100000, 1000000).foreach(booleanBatchVsSingleBenchmark)
    }
  }
}

