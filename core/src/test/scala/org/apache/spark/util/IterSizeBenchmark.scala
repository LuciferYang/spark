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
package org.apache.spark.util

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for EnumSet vs HashSet hold enumeration type
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/EnumTypeSetBenchmark-results.txt".
 * }}}
 */
object IterSizeBenchmark extends BenchmarkBase {

  def testIteratorSize(valuesPerIteration: Int, iterator: Iterator[Int]): Unit = {

    val benchmark = new Benchmark(s"Test iterator size ${iterator.size}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use iter size") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        iterator.size
      }
    }

    benchmark.addCase("Use Utils getIteratorSize") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Utils.getIteratorSize(iterator)
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    // Test Contains
    testIteratorSize(valuesPerIteration, Range(0, 100).toIterator)
    testIteratorSize(valuesPerIteration, Range(0, 1000).toIterator)
    testIteratorSize(valuesPerIteration, Range(0, 10000).toIterator)
    testIteratorSize(valuesPerIteration, Range(0, 100000).toIterator)
    testIteratorSize(valuesPerIteration, Range(0, 1000000).toIterator)
    testIteratorSize(valuesPerIteration, Range(0, 10000000).toIterator)
  }
}
