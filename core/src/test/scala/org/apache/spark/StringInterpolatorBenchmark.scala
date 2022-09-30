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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}


/**
 * Benchmark for SerializationUtils.clone vs Utils.cloneProperties.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/PropertiesCloneBenchmark-results.txt".
 * }}}
 */
object StringInterpolatorBenchmark extends BenchmarkBase {
  /**
   * Benchmark various cases of cloning properties objects
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000000
    runBenchmark("String Interpolator") {
      val benchmark = new Benchmark("String Interpolator", valuesPerIteration, output = output)
      benchmark.addCase("No Interpolator") { _ =>
        for (_ <- 0L until valuesPerIteration) {
          val v = "Delaying batch as number of records available is less than minOffsetsPerTrigger"
        }
      }
      benchmark.addCase("Use Interpolator") { _ =>
        for (_ <- 0L until valuesPerIteration) {
          val v = s"Delaying batch as number of records available is less than minOffsetsPerTrigger"
        }
      }
      benchmark.run()
    }
  }
}
