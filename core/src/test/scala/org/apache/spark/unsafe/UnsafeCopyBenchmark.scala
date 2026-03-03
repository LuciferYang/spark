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

package org.apache.spark.unsafe

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for Unsafe.copyMemory with different thresholds.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> \
 *        --conf "spark.executor.extraJavaOptions=-Dspark.unsafe.copyMemory.threshold=..." \
 *        <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/UnsafeCopyBenchmark-results.txt".
 * }}}
 */
object UnsafeCopyBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runCopyMemoryBenchmark()
  }

  def runCopyMemoryBenchmark(): Unit = {
    val size = 512L * 1024L * 1024L // 512MB (Reduced from 1GB to run faster in test env)
    val src = Platform.allocateMemory(size)
    val dst = Platform.allocateMemory(size)

    // Fill src with some data
    Platform.setMemory(src, 1.toByte, size)

    try {
      // Pass 'size' as valuesPerIteration, so that the output 'Rate(M/s)' represents 'MB/s'
    val benchmark = new Benchmark("Unsafe Copy Memory (512MB)", size, output = output)

      benchmark.addCase("Platform.copyMemory") { _ =>
        Platform.copyMemory(null, src, null, dst, size)
      }

      benchmark.run()
    } finally {
      Platform.freeMemory(src)
      Platform.freeMemory(dst)
    }
  }
}
