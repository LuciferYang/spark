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

package org.apache.spark.sql.execution.benchmark

import test.org.apache.spark.sql.TestApis

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

object StringJoinBenchmark extends BenchmarkBase {

  private def testJoinString(input: Array[String], valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test join String with input size ${input.length}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Arrays.steam api no prefix, suffix") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.joinStreamApiNoPreSuffix(input)
      }
    }

    benchmark.addCase("Use Arrays.steam api with prefix, suffix") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.joinStreamApiWithPreSuffix(input)
      }
    }

    benchmark.addCase("Use String join api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.stringJoinApi(input)
      }
    }

    benchmark.addCase("Use String joiner api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.stringJoinerApi(input)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000

    testJoinString((1L to 1L).map(_.toString).toArray, valuesPerIteration)
    testJoinString((1L to 5L).map(_.toString).toArray, valuesPerIteration)
    testJoinString((1L to 10L).map(_.toString).toArray, valuesPerIteration)
    testJoinString((1L to 20L).map(_.toString).toArray, valuesPerIteration)
    testJoinString((1L to 50L).map(_.toString).toArray, valuesPerIteration)
    testJoinString((1L to 100L).map(_.toString).toArray, valuesPerIteration)
    testJoinString((1L to 500L).map(_.toString).toArray, valuesPerIteration)
    testJoinString((1L to 1000L).map(_.toString).toArray, valuesPerIteration)
    testJoinString((1L to 10000L).map(_.toString).toArray, valuesPerIteration)
  }
}
