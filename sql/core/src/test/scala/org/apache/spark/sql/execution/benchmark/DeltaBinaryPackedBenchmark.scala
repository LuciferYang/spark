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

import java.nio.ByteBuffer

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.datasources.parquet.{OldVectorizedDeltaBinaryPackedReader, VectorizedDeltaBinaryPackedReader}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.DataTypes

/**
 * Benchmark to measure the performance of VectorizedDeltaBinaryPackedReader.
 * To run this benchmark:
 * 1. Build Spark and run:
 * SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain org.apache.spark.sql.execution.datasources.parquet.DeltaBinaryPackedBenchmark"
 */
object DeltaBinaryPackedBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val batchSize = 1024 * 16 // 16k rows
    val iters = 10000

    runBenchmark("Delta Binary Packed Decoding") {
      val benchmark = new Benchmark(
        s"Read $batchSize Ints",
        iters * batchSize.toLong,
        output = output)

      // 1. Prepare Mock Data (Simulated Delta-Encoded Page)
      // This is a simplified mock. In a real scenario, use a pre-encoded Parquet byte array.
      val mockPageData = generateMockDeltaPage(batchSize)

      benchmark.addCase("Original (Lambda/Functional)") { _ =>
        var i = 0
        while (i < iters) {
          val vector = new OnHeapColumnVector(batchSize, DataTypes.IntegerType)
          val reader = new OldVectorizedDeltaBinaryPackedReader() // The previous implementation
          val bbis = ByteBufferInputStream.wrap(ByteBuffer.wrap(mockPageData))
          reader.initFromPage(batchSize, bbis)
          reader.readIntegers(batchSize, vector, 0)
          vector.close()
          i += 1
        }
      }

      benchmark.addCase("Optimized (Manual Loop / Hoisted)") { _ =>
        var i = 0
        while (i < iters) {
          val vector = new OnHeapColumnVector(batchSize, DataTypes.IntegerType)
          val reader = new VectorizedDeltaBinaryPackedReader() // The new implementation
          val bbis = ByteBufferInputStream.wrap(ByteBuffer.wrap(mockPageData))
          reader.initFromPage(batchSize, bbis)
          reader.readIntegers(batchSize, vector, 0)
          vector.close()
          i += 1
        }
      }

      benchmark.run()
    }
  }

  /**
   * Generates a minimal valid DELTA_BINARY_PACKED header + data for testing.
   * Parquet Format: <block size> <miniblocks> <total count> <first value> <min delta> <bitwidths...> <data>
   */
  private def generateMockDeltaPage(size: Int): Array[Byte] = {
    // This is a placeholder. For a real benchmark, you'd use
    // DeltaBinaryPackingValuesWriter to generate a valid sequence.
    // To keep it simple for this template, assume a pre-recorded byte array is used.
    new Array[Byte](size * 4)
  }
}