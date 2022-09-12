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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

object ZipToMapBenchmark extends BenchmarkBase {

  def testZipToMap(valuesPerIteration: Int, collectionSize: Int): Unit = {

    val capabilities = TableCapability.values()

    val benchmark = new Benchmark(
      s"Test zip to map with collectionSize = $collectionSize",
      valuesPerIteration * capabilities.length,
      output = output)

    val data = 0 until collectionSize

    benchmark.addCase("Use zip + toMap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val map = data.zip(data).toMap
      }
    }

    benchmark.addCase("Use zip + collection.breakOut") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val tuples = data.zip(data)(collection.breakOut)
      }
    }

    benchmark.addCase("Use Manual builder") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val map = zipToMap1(data, data)
      }
    }

    benchmark.addCase("Use Manual map") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val map = zipToMap2(data, data)
      }
    }
    benchmark.run()
  }

  private def zipToMap1[A, B, K, V](keys: Seq[A], values: Seq[B]): Map[K, V] = {
    import scala.collection.immutable
    val builder = immutable.Map.newBuilder[K, V]
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      builder += (keyIter.next(), valueIter.next()).asInstanceOf[(K, V)]
    }
    builder.result()
  }

  private def zipToMap2[A, B, K, V](keys: Seq[A], values: Seq[B]): Map[K, V] = {
    var elems: Map[K, V] = Map.empty[K, V]
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      elems += (keyIter.next().asInstanceOf[K] -> valueIter.next().asInstanceOf[V])
    }
    elems
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    // Test Contains
    testZipToMap(valuesPerIteration, 1)
    testZipToMap(valuesPerIteration, 5)
    testZipToMap(valuesPerIteration, 10)
    testZipToMap(valuesPerIteration, 20)
    testZipToMap(valuesPerIteration, 50)
    testZipToMap(valuesPerIteration, 100)
    testZipToMap(valuesPerIteration, 500)
    testZipToMap(valuesPerIteration, 1000)
    testZipToMap(valuesPerIteration, 5000)
    testZipToMap(valuesPerIteration, 10000)
    testZipToMap(valuesPerIteration, 20000)
  }
}
