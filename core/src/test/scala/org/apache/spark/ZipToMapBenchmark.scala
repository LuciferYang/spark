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

import org.apache.spark.benchmark.BenchmarkBase

object ZipToMapBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    import org.apache.spark.benchmark.Benchmark
    val valuesPerIteration = 100000

    val benchmark = new Benchmark("Test zip and toMap",
      valuesPerIteration, output = output)

    Seq(10, 100, 1000, 10000).foreach { max =>
      val range = 0 until max
      benchmark.addCase(s"$max kv") { _: Int =>
        for (_ <- 0L until valuesPerIteration) {
          val ret = range.zip(range).toMap
        }
      }
      benchmark.addCase(s"$max kv collection.breakOut") { _: Int =>
        for (_ <- 0L until valuesPerIteration) {
          val ret = range.zip(range)(collection.breakOut)
        }
      }
    }

    benchmark.run()
  }
}
