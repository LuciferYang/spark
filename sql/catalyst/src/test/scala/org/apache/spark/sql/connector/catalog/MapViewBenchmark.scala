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
object MapViewBenchmark extends BenchmarkBase {

  def testGetValueByKey(loopTimes: Int, keyLength: Int): Unit = {

    val benchmark = new Benchmark(
      s"Get value by key loopTimes = $loopTimes, keyLength = $keyLength",
      loopTimes * keyLength, output = output)

    val input = (0 until keyLength)

    val mapView = input.groupBy(identity).view.mapValues(_.length).mapValues(_ + 1)

    benchmark.addCase("Use MapView") { _: Int =>
      for (_ <- 0L until loopTimes) {
        input.foreach(k => mapView.getOrElse(k, 0))
      }
    }

    var doCompute = true
    var map: Map[Int, Int] = null

    def getMap(): Map[Int, Int] = {
      if (doCompute) {
        map = input.groupBy(identity).transform((_, v) => v.length).transform((_, v) => v + 1)
        doCompute = false
      }
      map
    }

    benchmark.addCase("Use Map") { _: Int =>
      val data = getMap()
      for (_ <- 0L until loopTimes) {
        input.foreach(k => data.getOrElse(k, 0))
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    testGetValueByKey(1, 100000)
    testGetValueByKey(2, 100000)
    testGetValueByKey(5, 100000)
    testGetValueByKey(10, 100000)
    testGetValueByKey(50, 100000)
  }
}
