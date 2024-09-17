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

package org.apache.spark.shuffle

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}


/**
 * Benchmark for Checksum Algorithms used by shuffle.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ChecksumBenchmark-results.txt".
 * }}}
 */
object MapBenchmark extends BenchmarkBase {

  val N = 1024

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val map1 = new collection.mutable.HashMap[String, Object]()
    val map2 = new collection.mutable.AnyRefMap[String, Object]()
    val size = 4000

    Range(0, size).foreach { id =>
      map1.put(id.toString, new Object())
      map2.put(id.toString, new Object())
    }

    val keys = Range(1, size * 20000).map { _ =>
      new Random().nextInt(size + 10).toString
    }

    val benchmark = new Benchmark("Benchmark Map", size, minNumIters = 30)

    benchmark.addCase("AnyRefMap") { _ =>
      keys.foreach { key => map2.getOrElseUpdate(key, new Object()) }
    }

    benchmark.addCase("HashMap") { _ =>
      keys.foreach { key => map1.getOrElseUpdate(key, new Object()) }
    }

    benchmark.run()
  }
}
