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
package org.apache.spark.status.protobuf

import scala.collection.JavaConverters._

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

object ToSeqBenchmark extends BenchmarkBase {

  def testToSeq(valuesPerIteration: Int, size: Int): Unit = {

    val builder = StoreTypes.RDDPartitionInfo.newBuilder()
    (0 until size).foreach { i =>
      builder.addExecutors(s"executor-$i")
    }
    val info = builder.build()

    val benchmark = new Benchmark(
      s"Test to Seq with size = $size",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use asScala") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val seq = info.getExecutorsList.asScala
      }
    }

    benchmark.addCase("Use asScala + toSeq") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val seq = info.getExecutorsList.asScala.toSeq
      }
    }

    benchmark.addCase("Use WrappedArray") { _: Int =>
      val strings = Array.empty[String]
      for (_ <- 0L until valuesPerIteration) {
        import scala.collection.immutable
        immutable.ArraySeq.unsafeWrapArray(info.getExecutorsList.toArray(strings))
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    // Test Contains
    testToSeq(valuesPerIteration, 1)
    testToSeq(valuesPerIteration, 5)
    testToSeq(valuesPerIteration, 10)
    testToSeq(valuesPerIteration, 20)
    testToSeq(valuesPerIteration, 50)
    testToSeq(valuesPerIteration, 100)
    testToSeq(valuesPerIteration, 1000)
    testToSeq(valuesPerIteration, 3000)
  }
}
