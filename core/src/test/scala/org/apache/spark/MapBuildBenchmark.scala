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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase

object MapBuildBenchmark extends BenchmarkBase {

  private def testBuildMap(mapSize: Int): Unit = {
    val benchmark = new Benchmark(
      s"Test Map build",
      mapSize,
      output = output)

    benchmark.addCase("immutable.Map") { _: Int =>
      val builder = scala.collection.immutable.Map.newBuilder[Test, String]
      var index: Int = 0
      while (index < mapSize) {
        builder += (Test(index, s"$index _ $index"), index.toString).asInstanceOf[(Test, String)]
        index += 1
      }
      builder.result()
    }

    benchmark.addCase("mutable.Map") { _: Int =>
      val map = scala.collection.mutable.Map[Test, String]()
      var index = 0
      while (index < mapSize) {
        map.put(Test(index, s"$index _ $index"), index.toString)
        index += 1
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    testBuildMap(10)
    testBuildMap(100)
    testBuildMap(1000)
    testBuildMap(10000)
    testBuildMap(100000)
    testBuildMap(1000000)
    testBuildMap(10000000)
  }

  case class Test(id: Int, name: String)
}
