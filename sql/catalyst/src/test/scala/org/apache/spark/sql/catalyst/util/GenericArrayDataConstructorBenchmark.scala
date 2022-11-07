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

package org.apache.spark.sql.catalyst.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for [[GenericArrayData]].
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/GenericArrayDataBenchmark-results.txt".
 * }}}
 */
object GenericArrayDataConstructorBenchmark extends BenchmarkBase {

  // Benchmarks of GenericArrayData's constructors (see SPARK-30413):
  def constructorBenchmark(bufferSize: Int): Unit = {
    val valuesPerIteration: Long = 1000 * 1000 * 10
    val benchmark = new Benchmark(s"constructor with buffer size = $bufferSize",
      valuesPerIteration, output = output)

    val buffer = if (bufferSize == 0) {
      ArrayBuffer.empty[Any]
    } else {
      val ret = new ArrayBuffer[Any](bufferSize)
      (0 until bufferSize).foreach { i =>
        buffer(i) = i
      }
      ret
    }

    benchmark.addCase("toSeq and construct") { _ =>
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(buffer.toSeq)
        n += 1
      }
    }

    benchmark.addCase("construct directly") { _ =>
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(buffer)
        n += 1
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    constructorBenchmark(0)
    constructorBenchmark(10)
    constructorBenchmark(100)
    constructorBenchmark(1000)
    constructorBenchmark(10000)
  }
}
