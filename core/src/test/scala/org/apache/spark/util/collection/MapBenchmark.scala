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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase

object MapBenchmark extends BenchmarkBase {

  def testContains(valuesPerIteration: Int, size: Int, testExistKey: Boolean): Unit = {
    val hosts = (9 until size).map(i => s"host-$i")
    val mmap = scala.collection.mutable.HashMap(hosts.map(_ -> 1): _*)
    val immap = scala.collection.immutable.HashMap(hosts.map(_ -> 1): _*)
    val openHashMap: OpenHashMap[String, Int] = {
      val map = new OpenHashMap[String, Int]()
      hosts.map(_ -> 1).foreach { x =>
        map.update(x._1, x._2)
      }
      map
    }

    val keys = if (testExistKey) {
      hosts
    } else {
      hosts.map(h => s"$h-n")
    }

    val benchmark = new Benchmark(
      s"Test Map($size).contains, testExistKey = $testExistKey",
      valuesPerIteration * size,
      output = output)

    benchmark.addCase("mutable.Map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val unit = keys.foreach(mmap.contains)
      }
    }

    benchmark.addCase("immutable.Map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val unit = keys.foreach(immap.contains)
      }
    }

    benchmark.addCase("OpenHashMap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val unit = keys.foreach(openHashMap.contains)
      }
    }
    benchmark.run()
  }

  def testApply(valuesPerIteration: Int, size: Int): Unit = {
    val hosts = (9 until size).map(i => s"host-$i")
    val mmap = scala.collection.mutable.HashMap(hosts.map(_ -> 1): _*)
    val immap = scala.collection.immutable.HashMap(hosts.map(_ -> 1): _*)
    val openHashMap: OpenHashMap[String, Int] = {
      val map = new OpenHashMap[String, Int]()
      hosts.map(_ -> 1).foreach { x =>
        map.update(x._1, x._2)
      }
      map
    }

    val benchmark = new Benchmark(
      s"Test Map($size).apply",
      valuesPerIteration * size,
      output = output)

    benchmark.addCase("mutable.Map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val unit = hosts.foreach(mmap)
      }
    }

    benchmark.addCase("immutable.Map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val unit = hosts.foreach(immap)
      }
    }

    benchmark.addCase("OpenHashMap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val unit = hosts.foreach(openHashMap.apply)
      }
    }
    benchmark.run()
  }

  def testCreateNewMap(valuesPerIteration: Int, size: Int): Unit = {
    val hosts = (9 until size).map(i => s"host-$i")

    val benchmark = new Benchmark(
      s"Test create new Map($size)",
      valuesPerIteration * size,
      output = output)

    benchmark.addCase("mutable.Map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        scala.collection.mutable.HashMap(hosts.map(_ -> 1): _*)
      }
    }

    benchmark.addCase("immutable.Map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        scala.collection.immutable.HashMap(hosts.map(_ -> 1): _*)
      }
    }

    benchmark.addCase("OpenHashMap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val map: OpenHashMap[String, Int] = new OpenHashMap[String, Int]()
        hosts.map(_ -> 1).foreach { x =>
          map.update(x._1, x._2)
        }
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000

    testCreateNewMap(valuesPerIteration, 10)
    testCreateNewMap(valuesPerIteration, 100)
    testCreateNewMap(valuesPerIteration, 1000)
    testCreateNewMap(valuesPerIteration, 10000)

    testApply(valuesPerIteration, 10)
    testApply(valuesPerIteration, 100)
    testApply(valuesPerIteration, 1000)
    testApply(valuesPerIteration, 10000)

    testContains(valuesPerIteration, 10, true)
    testContains(valuesPerIteration, 10, false)
    testContains(valuesPerIteration, 100, true)
    testContains(valuesPerIteration, 100, false)
    testContains(valuesPerIteration, 1000, true)
    testContains(valuesPerIteration, 1000, false)
    testContains(valuesPerIteration, 10000, true)
    testContains(valuesPerIteration, 10000, false)
  }
}
