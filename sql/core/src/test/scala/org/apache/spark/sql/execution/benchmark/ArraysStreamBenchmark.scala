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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase

object ArraysStreamBenchmark extends BenchmarkBase {

  private def testForeachOrder(input: Array[String], valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test for eachOrder with input size ${input.length}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Arrays.steam api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.foreachOrderUseStreamApi(input)
      }
    }

    benchmark.addCase("Use Loop api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.foreachOrderUseLoopApi(input)
      }
    }
    benchmark.run()
  }

  private def testAnyMatch(input: Array[Long], target: Long, valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test for AnyMatch with input size ${input.length}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Arrays.steam api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.anyMatchUseStreamApi(input, target)
      }
    }

    benchmark.addCase("Use Loop api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.anyMatchUseLoopApi(input, target)
      }
    }
    benchmark.run()
  }

  private def testAllMatch(input: Array[Long], target: Long, valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test for AllMatch with input size ${input.length}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Arrays.steam api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.allMatchUseStreamApi(input, target)
      }
    }

    benchmark.addCase("Use Loop api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.allMatchUseLoopApi(input, target)
      }
    }
    benchmark.run()
  }

  private def testDistinct(input: Array[TestApis.TestObj], valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test for distinct with input size ${input.length}",
      valuesPerIteration,
      output = output)

//    benchmark.addCase("Use Arrays.steam api") { _: Int =>
//      for (_ <- 0L until valuesPerIteration) {
//        TestApis.distinctUseStreamApi(input)
//      }
//    }

    benchmark.addCase("Use Loop and LinkedHashset") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.distinctUseLinkedHashSet(input)
      }
    }

    benchmark.addCase("Use Loop api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.distinctUseLoopApi(input)
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000

//    testAnyMatch((1L to 1L).toArray, 1L, valuesPerIteration)
//    testAnyMatch((1L to 5L).toArray, 4L, valuesPerIteration)
//    testAnyMatch((1L to 10L).toArray, 9L, valuesPerIteration)
//    testAnyMatch((1L to 20L).toArray, 19L, valuesPerIteration)
//    testAnyMatch((1L to 50L).toArray, 49L, valuesPerIteration)
//    testAnyMatch((1L to 100L).toArray, 99L, valuesPerIteration)
//    testAnyMatch((1L to 500L).toArray, 449L, valuesPerIteration)
//    testAnyMatch((1L to 1000L).toArray, 999L, valuesPerIteration)
//    testAnyMatch((1L to 10000L).toArray, 9999L, valuesPerIteration)
//
//    testAllMatch((2L to 2L).toArray, 1L, valuesPerIteration)
//    testAllMatch((2L to 5L).toArray, 1L, valuesPerIteration)
//    testAllMatch((2L to 10L).toArray, 1L, valuesPerIteration)
//    testAllMatch((2L to 20L).toArray, 1L, valuesPerIteration)
//    testAllMatch((2L to 50L).toArray, 1L, valuesPerIteration)
//    testAllMatch((2L to 100L).toArray, 1L, valuesPerIteration)
//    testAllMatch((2L to 500L).toArray, 1L, valuesPerIteration)
//    testAllMatch((2L to 1000L).toArray, 1L, valuesPerIteration)
//    testAllMatch((2L to 10000L).toArray, 1L, valuesPerIteration)

    testDistinct(TestApis.objs(1, 5, 100), valuesPerIteration)
    testDistinct(TestApis.objs(5, 5, 100), valuesPerIteration)
    testDistinct(TestApis.objs(10, 5, 100), valuesPerIteration)
    testDistinct(TestApis.objs(20, 5, 100), valuesPerIteration)
    testDistinct(TestApis.objs(50, 5, 100), valuesPerIteration)
    testDistinct(TestApis.objs(100, 5, 100), valuesPerIteration)
    testDistinct(TestApis.objs(500, 5, 100), valuesPerIteration)
    testDistinct(TestApis.objs(1000, 5, 100), valuesPerIteration)
    testDistinct(TestApis.objs(10000, 5, 100), valuesPerIteration)
  }
}
