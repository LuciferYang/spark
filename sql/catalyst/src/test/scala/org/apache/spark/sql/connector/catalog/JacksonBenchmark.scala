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

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase

object JacksonBenchmark extends BenchmarkBase {

  def testReadJsonToMap(valuesPerIteration: Int): Unit = {
    val input =
      """
        |{"mergeDir":"/a/b/c/mergeDirName","attemptId":"appattempt_1648454518011_994053_000001"}
      """.stripMargin

    val benchmark = new Benchmark("Test read json to map",
      valuesPerIteration, output = output)

    benchmark.addCase("Test Multiple") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(input, classOf[mutable.HashMap[String, String]])
      }
    }

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    benchmark.addCase("Test Single") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        mapper.readValue(input, classOf[mutable.HashMap[String, String]])
      }
    }

    benchmark.run()
  }

  def testWriteMapToJson(valuesPerIteration: Int): Unit = {

    val map: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    map.put("mergeDir", "/a/b/c/mergeDirName")
    map.put("attemptId", "yarn_appattempt_1648454518011_994053_000001")


    val benchmark = new Benchmark("Test write map to json",
      valuesPerIteration, output = output)

    benchmark.addCase("Test Multiple") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        mapper.writeValueAsString(map)
      }
    }

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    benchmark.addCase("Test Single") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        mapper.writeValueAsString(map)
      }
    }

    benchmark.run()
  }

  def testCreateObjectMapper(valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark("Test create ObjectMapper",
      valuesPerIteration, output = output)

    benchmark.addCase("Test create ObjectMapper") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 10000

    testCreateObjectMapper(valuesPerIteration = valuesPerIteration)
    testWriteMapToJson(valuesPerIteration = valuesPerIteration)
    testReadJsonToMap(valuesPerIteration = valuesPerIteration)
  }
}
