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
 *   2. build/sbt "catalyst/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/test:runMain <this class>"
 *      Results will be written to "benchmarks/URLCodecBenchmark-results.txt".
 * }}}
 */
object URLCodecBenchmark extends BenchmarkBase {

  def testEncode(url: String, valuesPerIteration: Int): Unit = {
    import org.apache.commons.codec.net.URLCodec

    val benchmark = new Benchmark("Test encode", valuesPerIteration, output = output)

    benchmark.addCase("Use java.net.URLEncoder") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        java.net.URLEncoder.encode(url, "UTF-8")
      }
    }

    val codec = new URLCodec()
    benchmark.addCase("Use org.apache.commons.codec.net.URLCodec") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        codec.encode(url)
      }
    }
    benchmark.run()
  }

  def testDecode(value: String, valuesPerIteration: Int): Unit = {
    import org.apache.commons.codec.net.URLCodec

    val benchmark = new Benchmark("Test encode", valuesPerIteration, output = output)

    benchmark.addCase("Use java.net.URLEncoder") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        java.net.URLDecoder.decode(value, "UTF-8")
      }
    }

    val codec = new URLCodec()
    benchmark.addCase("Use org.apache.commons.codec.net.URLCodec") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        codec.decode(value)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    // Test Contains
    testEncode("https://spark.apache.org", valuesPerIteration)
    testEncode("https%3A%2F%2Fspark.apache.org", valuesPerIteration)
  }
}
