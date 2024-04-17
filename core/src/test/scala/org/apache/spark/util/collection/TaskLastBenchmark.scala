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
package org.apache.spark.util.collection

import scala.collection.mutable

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

object TaskLastBenchmark extends BenchmarkBase {

  def takeLastWithArray(input: Iterator[Int], num: Int): Iterator[Int] = {
    assert(num >= 0)
    if (input.isEmpty || num == 0) {
      Iterator.empty[Int]
    } else {
      val arr = new Array[Int](num)
      var i = 0
      input.foreach { item =>
        arr(i % num) = item
        i += 1
      }
      if (i < num) arr.slice(0, i).iterator
      else arr.slice(i % num, num).iterator ++ arr.slice(0, i % num).iterator
    }
  }

  def takeLastWithSeq[T](input: Iterator[T], num: Int): Iterator[T] = {
    assert(num >= 0)
    if (input.isEmpty || num == 0) {
      Iterator.empty[T]
    } else {
      var last = Seq.empty[T]
      val sliding = input.sliding(num)
      while (sliding.hasNext) {
        last = sliding.next()
      }
      last.iterator
    }
  }

  def takeLastWithQueue[T](input: Iterator[T], num: Int): Iterator[T] = {
    assert(num >= 0)
    if (input.isEmpty || num == 0) {
      Iterator.empty[T]
    } else {
      val queue = mutable.Queue.empty[T]
      input.foreach { item =>
        if (queue.size >= num) {
          queue.dequeue()
        }
        queue.enqueue(item)
      }
      queue.iterator
    }
  }

  def testTakeLast(valuesPerIteration: Int, size: Int, tail: Int): Unit = {

    val data = Range(0, size)

    val benchmark = new Benchmark(
      s"Test takeLast with size $size and tail $tail",
      valuesPerIteration * tail,
      output = output)

    benchmark.addCase("Use takeLast with Seq") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        takeLastWithSeq(data.iterator, tail)
      }
    }

    benchmark.addCase("Use takeLast with Queue") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        takeLastWithQueue(data.iterator, tail)
      }
    }

    benchmark.addCase("Use takeLast with Array") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        takeLastWithQueue(data.iterator, tail)
      }
    }
    benchmark.run()
  }


  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    testTakeLast(valuesPerIteration, 10, 10)
    testTakeLast(valuesPerIteration, 100, 10)
    testTakeLast(valuesPerIteration, 1000, 10)
    testTakeLast(valuesPerIteration, 10000, 10)
    testTakeLast(valuesPerIteration, 100000, 10)
//    testTakeLast(valuesPerIteration, 1000000, 10)

  }
}
