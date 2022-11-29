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
package org.apache.spark.sql.connector.catalog

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
object SizeBenchmark extends BenchmarkBase {

  def testGetFirstElement(
      valuesPerIteration: Int): Unit = {

    val seq = Seq(1, 2, 3)

    val benchmark = new Benchmark(
      s"Test get first element",
        valuesPerIteration,
        output = output)

    benchmark.addCase("Use apply(0)") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val ret = seq(0)
      }
    }

    benchmark.addCase("Use .head") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val ret = seq.head
      }
    }

    benchmark.run()
  }

  def testArraySize(valuesPerIteration: Int, length: Int): Unit = {
    import org.apache.commons.lang3.RandomUtils

    val a = new String(RandomUtils.nextBytes(length))

    val benchmark = new Benchmark(
      s"Test Array size with $length",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use .size") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val ret = a.size
      }
    }

    benchmark.addCase("Use .length") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val ret = a.length
      }
    }
    benchmark.run()
  }

  def testStringSize(valuesPerIteration: Int, length: Int): Unit = {

    val a = (0 until length).toArray

    val benchmark = new Benchmark(
      s"Test String length with $length",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use .size") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val ret = a.size
      }
    }

    benchmark.addCase("Use .length") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val ret = a.length
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    // Test Contains
    testGetFirstElement(valuesPerIteration)

    testStringSize(valuesPerIteration, 5)
    testStringSize(valuesPerIteration, 10)
    testStringSize(valuesPerIteration, 20)
    testStringSize(valuesPerIteration, 50)
    testStringSize(valuesPerIteration, 100)
    testStringSize(valuesPerIteration, 500)

    testArraySize(valuesPerIteration, 5)
    testArraySize(valuesPerIteration, 10)
    testArraySize(valuesPerIteration, 20)
    testArraySize(valuesPerIteration, 50)
    testArraySize(valuesPerIteration, 100)
    testArraySize(valuesPerIteration, 500)
  }
}
