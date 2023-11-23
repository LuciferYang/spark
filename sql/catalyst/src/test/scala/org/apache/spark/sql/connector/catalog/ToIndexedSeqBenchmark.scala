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

import scala.collection.immutable

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.ArrayImplicits._

/**
 * Benchmark for EnumSet vs HashSet hold enumeration type
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/EnumTypeSetBenchmark-results.txt".
 * }}}
 */
object ToIndexedSeqBenchmark extends BenchmarkBase {

  def testInline(valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark(
      s"Test inline vs noinline",
      valuesPerIteration,
      output = output)

    val ints = Array.fill(1000)(1)

    benchmark.addCase("in inline") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val seq = ints.toImmutableArraySeq2
      }
    }

    benchmark.addCase("inline") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val value = ints.toImmutableArraySeq
      }
    }
    benchmark.run()
  }

  def testCreateIndexedSeq2(valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark(
      s"Test create a IndexedSeq",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Array fill") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val seq = Array.fill(1000)(1).toImmutableArraySeq
      }
    }

    benchmark.addCase("Indexed fill") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val value = IndexedSeq.fill(1000)(1)
      }
    }

    benchmark.addCase("new Array toImmutableArraySeq") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val value = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toImmutableArraySeq
      }
    }

    benchmark.addCase("new IndexedSeq toImmutableArraySeq") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val value = IndexedSeq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      }
    }

    benchmark.addCase("new Array empty toImmutableArraySeq") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val value = Array.emptyDoubleArray.toImmutableArraySeq
      }
    }

    benchmark.addCase("new IndexedSeq empty") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val value = IndexedSeq.empty[Double]
      }
    }
    benchmark.run()
  }

  @scala.annotation.nowarn
  def testCreateIndexedSeq(valuesPerIteration: Int, size: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test create a IndexedSeq with size = $size",
      valuesPerIteration,
      output = output)

    def length(values: Seq[Any]): Int = {
      values.size
    }

    benchmark.addCase("Use Array implicits") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val values = new Array[Any](size)
        var i = 0
        while (i < size) {
          values(i) = i
          i += 1
        }
        length(values)
      }
    }

    benchmark.addCase("Use Array.toIndexedSeq") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val values = new Array[Any](size)
        var i = 0
        while (i < size) {
          values(i) = i
          i += 1
        }
        length(values.toIndexedSeq)
      }
    }

    benchmark.addCase("Use ArraySeq.toImmutableArraySeq") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val values = new Array[Any](size)
        var i = 0
        while (i < size) {
          values(i) = i
          i += 1
        }
        length(values.toImmutableArraySeq)
      }
    }

    benchmark.addCase("Use ArraySeq.unsafeWrapArray") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val values = new Array[Any](size)
        var i = 0
        while (i < size) {
          values(i) = i
          i += 1
        }
        length(immutable.ArraySeq.unsafeWrapArray(values))
      }
    }

    benchmark.addCase("Use ArraySeq builder") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val builder = IndexedSeq.newBuilder[Any]
        builder.sizeHint(size)
        var i = 0
        while (i < size) {
          builder += i
          i += 1
        }
        length(builder.result())
      }
    }

    benchmark.addCase("Use ArrayBuffer") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        import scala.collection.mutable.ArrayBuffer
        val values = new ArrayBuffer[Any](size)
        var i = 0
        while (i < size) {
          values += i
          i += 1
        }
        length(values.toIndexedSeq)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000

    testInline(valuesPerIteration)

    testCreateIndexedSeq(valuesPerIteration, 1)
    testCreateIndexedSeq(valuesPerIteration, 2)
    testCreateIndexedSeq(valuesPerIteration, 5)
    testCreateIndexedSeq(valuesPerIteration, 10)
    testCreateIndexedSeq(valuesPerIteration, 20)
    testCreateIndexedSeq(valuesPerIteration, 50)
    testCreateIndexedSeq(valuesPerIteration, 100)
    testCreateIndexedSeq(valuesPerIteration, 200)
    testCreateIndexedSeq(valuesPerIteration, 500)
    testCreateIndexedSeq(valuesPerIteration, 1000)
    testCreateIndexedSeq(valuesPerIteration, 2000)
    testCreateIndexedSeq(valuesPerIteration, 5000)
    testCreateIndexedSeq(valuesPerIteration, 10000)

    testCreateIndexedSeq2(valuesPerIteration)
  }
}
