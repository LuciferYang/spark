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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase

object SizeBenchmark extends BenchmarkBase {

  def testSizeOfSeq(dataSize: Int, callSizeTimes: Int = 1): Unit = {
    val valuesPerIteration = 100000
    val buffer = new ArrayBuffer[Int]()
    buffer.appendAll((0 until dataSize))
    val benchmark = new Benchmark(s"Test size of Seq with buffer size $dataSize",
      valuesPerIteration, output = output)
    benchmark.addCase("toSeq + Size") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val seq = buffer.toSeq
        (0 until callSizeTimes).foreach { _ =>
          val size = seq.size
        }
      }
    }
    benchmark.addCase("toIndexedSeq + Size") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val seq = buffer.toIndexedSeq
        (0 until callSizeTimes).foreach { _ =>
          val size = seq.size
        }
      }
    }
    benchmark.run()
  }


  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // test toSeq + size one time
    testSizeOfSeq(1)
    testSizeOfSeq(10)
    testSizeOfSeq(100)
    testSizeOfSeq(1000)
    testSizeOfSeq(10000)
    testSizeOfSeq(100000)

    // test toSeq + size multi times
    testSizeOfSeq(1000, 2)
    testSizeOfSeq(1000, 3)
    testSizeOfSeq(1000, 4)
    testSizeOfSeq(1000, 5)
  }
}
