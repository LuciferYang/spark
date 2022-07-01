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

package org.apache.spark

import scala.collection.mutable

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for min function of ArrayBuffer with different buffer size.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/ArrayBufferMinBenchmark-results.txt".
 * }}}
 * */
object ArrayBufferMinBenchmark extends BenchmarkBase {

  private def min(numIters: Int, bufferSize: Int, loops: Int): Unit = {
    val benchmark = new Benchmark("Array Buffer", loops, output = output)
    val buffer = new mutable.ArrayBuffer[Int]()
    (0 until bufferSize).foreach(i => buffer += i)
    benchmark.addCase(s"buffer add $bufferSize items", numIters) { _ =>
      (0 until loops).foreach(_ => buffer.min)
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    val loops = 20000
    runBenchmark("Coalesced RDD , large scale") {
      Seq(1000, 10000, 100000).foreach { bufferSize =>
        min(numIters, bufferSize, loops)
      }
    }
  }
}
