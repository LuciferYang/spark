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

import java.util.concurrent.CountDownLatch

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.ThreadUtils

object MJacksonBenchmark extends BenchmarkBase {

  def testWriteMapToJson(valuesPerIteration: Int, threads: Int): Unit = {

    val map: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    map.put("mergeDir", "/a/b/c/mergeDirName")
    map.put("attemptId", "yarn_appattempt_1648454518011_994053_000001")


    val benchmark = new Benchmark(s"Test $threads threads write map to json",
      valuesPerIteration, output = output)

    val multi = Array.fill(threads)({
      val ret = new ObjectMapper()
      ret.registerModule(DefaultScalaModule)
      ret
    })

    benchmark.addCase("Test use multi mapper") { _: Int =>
      val latch = new CountDownLatch(valuesPerIteration)
      val executor = ThreadUtils.newDaemonFixedThreadPool(threads, "multi")
      for (i <- 0 until valuesPerIteration) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            val idx = i % threads
            multi(idx).writeValueAsString(map)
            latch.countDown()
          }
        })
      }
      latch.await()
      executor.shutdown()
    }

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val singleton = Array.fill(threads)(mapper)
    benchmark.addCase("Test use singleton mapper") { _: Int =>
      val latch = new CountDownLatch(valuesPerIteration)
      val executor = ThreadUtils.newDaemonFixedThreadPool(threads, "singleton")
      for (i <- 0 until valuesPerIteration) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            val idx = i % threads
            singleton(idx).writeValueAsString(map)
            latch.countDown()
          }
        })
      }
      latch.await()
      executor.shutdown()
    }

    benchmark.run()
  }

  def testReadJsonToMap(valuesPerIteration: Int, threads: Int): Unit = {
    val input =
      """
        |{"mergeDir":"/a/b/c/mergeDirName","attemptId":"appattempt_1648454518011_994053_000001"}
      """.stripMargin

    val benchmark = new Benchmark(s"Test $threads threads read json to map",
      valuesPerIteration, output = output)

    val multi = Array.fill(threads)({
      val ret = new ObjectMapper()
      ret.registerModule(DefaultScalaModule)
      ret
    })

    benchmark.addCase("Test use multi mapper") { _: Int =>
      val latch = new CountDownLatch(valuesPerIteration)
      val executor = ThreadUtils.newDaemonFixedThreadPool(threads, "multi")
      for (i <- 0 until valuesPerIteration) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            val idx = i % threads
            multi(idx).readValue(input, classOf[mutable.HashMap[String, String]])
            latch.countDown()
          }
        })
      }
      latch.await()
      executor.shutdown()
    }

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val singleton = Array.fill(threads)(mapper)

    benchmark.addCase("Test use singleton mapper") { _: Int =>
      val latch = new CountDownLatch(valuesPerIteration)
      val executor = ThreadUtils.newDaemonFixedThreadPool(threads, "singleton")
      for (i <- 0 until valuesPerIteration) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            val idx = i % threads
            singleton(idx).readValue(input, classOf[mutable.HashMap[String, String]])
            latch.countDown()
          }
        })
      }
      latch.await()
      executor.shutdown()
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 10000000

    testReadJsonToMap(valuesPerIteration, 5)
    testReadJsonToMap(valuesPerIteration, 10)
    testReadJsonToMap(valuesPerIteration, 20)
//    testReadJsonToMap(valuesPerIteration, 50)
//    testReadJsonToMap(valuesPerIteration, 100)

    testWriteMapToJson(valuesPerIteration, 5)
    testWriteMapToJson(valuesPerIteration, 10)
    testWriteMapToJson(valuesPerIteration, 20)
//    testWriteMapToJson(valuesPerIteration, 50)
//    testWriteMapToJson(valuesPerIteration, 100)
  }
}
