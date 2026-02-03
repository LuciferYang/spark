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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.ArrayImplicits._

/**
 * Benchmark for [[GenericArrayData]].
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/GenericArrayDataBenchmark-results.txt".
 * }}}
 */
object GenericArrayDataBenchmark extends BenchmarkBase {

  // Benchmarks of GenericArrayData's constructors (see SPARK-30413):
  def constructorBenchmark(): Unit = {
    val valuesPerIteration: Long = 1000 * 1000 * 10
    val arraySize = 10
    val benchmark = new Benchmark("constructor", valuesPerIteration, output = output)

    benchmark.addCase("arrayOfAny") {
      _ =>
      val arr: Array[Any] = new Array[Any](arraySize)
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(arr)
        n += 1
      }
    }

    benchmark.addCase("arrayOfAnyAsObject") {
      _ =>
      val arr: Object = new Array[Any](arraySize)
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(arr)
        n += 1
      }
    }

    benchmark.addCase("arrayOfAnyAsSeq") {
      _ =>
      val arr: Seq[Any] = new Array[Any](arraySize).toImmutableArraySeq
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(arr)
        n += 1
      }
    }

    benchmark.addCase("arrayOfInt") {
      _ =>
      val arr: Array[Int] = new Array[Int](arraySize)
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(arr)
        n += 1
      }
    }

    benchmark.addCase("arrayOfIntAsObject") {
      _ =>
      val arr: Object = new Array[Int](arraySize)
      var n = 0
      while (n < valuesPerIteration) {
        new GenericArrayData(arr)
        n += 1
      }
    }

    benchmark.run()
  }

  // Benchmarks of GenericArrayData's methods
  def methodBenchmark(): Unit = {
    val valuesPerIteration: Long = 1000 * 1000 * 10
    val arraySize = 10

    runBasicInfoMethodsBenchmark(valuesPerIteration, arraySize)
    runReadMethodsBenchmark(valuesPerIteration, arraySize)
    runModificationMethodsBenchmark(valuesPerIteration, arraySize)
    runCopyMethodsBenchmark(valuesPerIteration, arraySize)
    runStringCompMethodsBenchmark(valuesPerIteration, arraySize)
  }

  private def runBasicInfoMethodsBenchmark(valuesPerIteration: Long, arraySize: Int): Unit = {
    // Basic info methods
    val basicInfoBenchmark = new Benchmark(
      "basic_info_methods", valuesPerIteration, output = output)
    basicInfoBenchmark.addCase("numElements") {
      _ =>
      val arr: Array[Int] = new Array[Int](arraySize)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.numElements()
        n += 1
      }
    }
    basicInfoBenchmark.run()
  }

  private def runReadMethodsBenchmark(valuesPerIteration: Long, arraySize: Int): Unit = {
    // Read methods
    val readBenchmark = new Benchmark("read_methods", valuesPerIteration, output = output)
    readBenchmark.addCase("getInt_arrayOfAny") {
      _ =>
      val arr: Array[Any] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.getInt(n % arraySize)
        n += 1
      }
    }
    readBenchmark.addCase("getInt_arrayOfInt") {
      _ =>
      val arr: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.getInt(n % arraySize)
        n += 1
      }
    }
    readBenchmark.addCase("isNullAt_arrayOfAny") {
      _ =>
      val arr: Array[Any] = Array(1, null, 3, null, 5)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.isNullAt(n % arr.length)
        n += 1
      }
    }
    readBenchmark.addCase("isNullAt_arrayOfInt") {
      _ =>
      val arr: Array[Int] = Array(1, 2, 3, 4, 5)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.isNullAt(n % arr.length)
        n += 1
      }
    }
    readBenchmark.run()
  }

  private def runModificationMethodsBenchmark(valuesPerIteration: Long, arraySize: Int): Unit = {
    // Modification methods
    val modificationBenchmark = new Benchmark(
      "modification_methods", valuesPerIteration, output = output)
    modificationBenchmark.addCase("setNullAt") {
      _ =>
      val arr: Array[Any] = new Array[Any](arraySize)
      var n = 0
      while (n < valuesPerIteration) {
        val gad = new GenericArrayData(arr.clone())
        gad.setNullAt(n % arraySize)
        n += 1
      }
    }
    modificationBenchmark.addCase("update") {
      _ =>
      val arr: Array[Any] = new Array[Any](arraySize)
      var n = 0
      while (n < valuesPerIteration) {
        val gad = new GenericArrayData(arr.clone())
        gad.update(n % arraySize, n)
        n += 1
      }
    }
    modificationBenchmark.run()
  }

  private def runCopyMethodsBenchmark(valuesPerIteration: Long, arraySize: Int): Unit = {
    // Copy methods
    val copyBenchmark = new Benchmark("copy_methods", valuesPerIteration, output = output)
    copyBenchmark.addCase("copy_arrayOfAny") {
      _ =>
      val arr: Array[Any] = new Array[Any](arraySize)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.copy()
        n += 1
      }
    }
    copyBenchmark.addCase("copy_arrayOfInt") {
      _ =>
      val arr: Array[Int] = new Array[Int](arraySize)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.copy()
        n += 1
      }
    }
    copyBenchmark.run()
  }

  private def runStringCompMethodsBenchmark(valuesPerIteration: Long, arraySize: Int): Unit = {
    // String and comparison methods
    val stringCompBenchmark = new Benchmark(
      "string_comparison_methods", valuesPerIteration, output = output)
    stringCompBenchmark.addCase("toString_arrayOfAny") {
      _ =>
      val arr: Array[Any] = Array(1, 2, 3, 4, 5)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.toString()
        n += 1
      }
    }
    stringCompBenchmark.addCase("toString_arrayOfInt") {
      _ =>
      val arr: Array[Int] = Array(1, 2, 3, 4, 5)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.toString()
        n += 1
      }
    }
    stringCompBenchmark.addCase("equals") {
      _ =>
      val arr1: Array[Int] = Array(1, 2, 3, 4, 5)
      val arr2: Array[Int] = Array(1, 2, 3, 4, 5)
      val gad1 = new GenericArrayData(arr1)
      val gad2 = new GenericArrayData(arr2)
      var n = 0
      while (n < valuesPerIteration) {
        gad1.equals(gad2)
        n += 1
      }
    }
    stringCompBenchmark.addCase("hashCode") {
      _ =>
      val arr: Array[Int] = Array(1, 2, 3, 4, 5)
      val gad = new GenericArrayData(arr)
      var n = 0
      while (n < valuesPerIteration) {
        gad.hashCode()
        n += 1
      }
    }
    stringCompBenchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    constructorBenchmark()
    methodBenchmark()
  }
}
