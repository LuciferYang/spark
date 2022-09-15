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

package org.apache.spark.sql

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

object ZipWithIndexToMapBenchmark extends BenchmarkBase {

  def testZipWithIndexToMap(valuesPerIteration: Int, collectionSize: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test zip with index to map with collectionSize = $collectionSize",
      valuesPerIteration,
      output = output)

    val data = 0 until collectionSize

    benchmark.addCase("Use zipWithIndex + toMap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val map: Map[Int, Int] = data.zipWithIndex.toMap
      }
    }

    benchmark.addCase("Use zipWithIndex + collection.breakOut") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
         val map: Map[Int, Int] =
           data.zipWithIndex(collection.breakOut[IndexedSeq[Int], (Int, Int), Map[Int, Int]])
      }
    }

    benchmark.addCase("Use Manual builder") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val map: Map[Int, Int] = zipToMapUseMapBuilder[Int](data)
      }
    }

    benchmark.addCase("Use Manual map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val map: Map[Int, Int] = zipWithIndexToMapUseMap[Int](data)
      }
    }
    benchmark.run()
  }

  private def zipToMapUseMapBuilder[K](keys: Iterable[K]): Map[K, Int] = {
    import scala.collection.immutable
    val builder = immutable.Map.newBuilder[K, Int]
    val keyIter = keys.iterator
    var idx = 0
    while (keyIter.hasNext) {
      builder += (keyIter.next(), idx).asInstanceOf[(K, Int)]
      idx = idx + 1
    }
    builder.result()
  }

  private def zipWithIndexToMapUseMap[K](keys: Iterable[K]): Map[K, Int] = {
    var elems: Map[K, Int] = Map.empty[K, Int]
    val keyIter = keys.iterator
    var idx = 0
    while (keyIter.hasNext) {
      elems += (keyIter.next().asInstanceOf[K] -> idx)
      idx = idx + 1
    }
    elems
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    testZipWithIndexToMap(valuesPerIteration, 1)
    testZipWithIndexToMap(valuesPerIteration, 5)
    testZipWithIndexToMap(valuesPerIteration, 10)
    testZipWithIndexToMap(valuesPerIteration, 20)
    testZipWithIndexToMap(valuesPerIteration, 50)
    testZipWithIndexToMap(valuesPerIteration, 100)
    testZipWithIndexToMap(valuesPerIteration, 150)
    testZipWithIndexToMap(valuesPerIteration, 200)
    testZipWithIndexToMap(valuesPerIteration, 300)
    testZipWithIndexToMap(valuesPerIteration, 400)
    testZipWithIndexToMap(valuesPerIteration, 500)
    testZipWithIndexToMap(valuesPerIteration, 1000)
    testZipWithIndexToMap(valuesPerIteration, 5000)
    testZipWithIndexToMap(valuesPerIteration, 10000)
    testZipWithIndexToMap(valuesPerIteration, 20000)
  }
}
