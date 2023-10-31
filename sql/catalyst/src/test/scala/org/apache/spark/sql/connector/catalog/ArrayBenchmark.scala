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
import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

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
object ArrayBenchmark extends BenchmarkBase {

  def testConcat3D(
      valuesPerIteration: Int,
      dimension1: Int,
      dimension2: Int,
      dimension3: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test Array connect dimension1 = $dimension1, dimension2 = $dimension2, " +
        s"dimension3 = $dimension3",
      1L * dimension1 * dimension2 * dimension3,
      output = output)

    val array3D = Array.fill(dimension1, dimension2, dimension3)(Random.nextInt())

    benchmark.addCase("Use flatten ") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        array3D.flatten
      }
    }

    benchmark.addCase("Use Array connect without unsafe Wrap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Array.concat(array3D: _*)
      }
    }

    benchmark.addCase("Use Array connect with unsafe Wrap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Array.concat(immutable.ArraySeq.unsafeWrapArray(array3D): _*)
      }
    }
    benchmark.run()
  }

  def testConcat2D(
      valuesPerIteration: Int,
      dimension1: Int,
      dimension2: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test Array connect dimension1 = $dimension1, dimension2 = $dimension2",
      1L * dimension1 * dimension2,
      output = output)

    val array2D = Array.fill(dimension1, dimension2)(Random.nextInt())

    benchmark.addCase("Use flatten ") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        array2D.flatten
      }
    }

    benchmark.addCase("Use Array connect without unsafe Wrap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Array.concat(array2D: _*)
      }
    }

    benchmark.addCase("Use Array connect with unsafe Wrap") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Array.concat(immutable.ArraySeq.unsafeWrapArray(array2D): _*)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 100000
    // group 1
    testConcat2D(valuesPerIteration, 10, 2)
    testConcat2D(valuesPerIteration, 100, 2)
    testConcat2D(valuesPerIteration, 1000, 2)
    testConcat2D(valuesPerIteration, 10000, 2)
    testConcat2D(valuesPerIteration, 50000, 2)

    // group 2
    testConcat2D(valuesPerIteration, 2, 10)
    testConcat2D(valuesPerIteration, 2, 100)
    testConcat2D(valuesPerIteration, 2, 1000)
    testConcat2D(valuesPerIteration, 2, 10000)
    testConcat2D(valuesPerIteration, 2, 50000)

    // group 3
    testConcat3D(valuesPerIteration, 10, 5, 2)
    testConcat3D(valuesPerIteration, 100, 5, 2)
    testConcat3D(valuesPerIteration, 1000, 5, 2)
    testConcat3D(valuesPerIteration, 10000, 5, 2)
    testConcat3D(valuesPerIteration, 50000, 5, 2)

    // group 4
    testConcat3D(valuesPerIteration, 5, 10, 2)
    testConcat3D(valuesPerIteration, 5, 100, 2)
    testConcat3D(valuesPerIteration, 5, 1000, 2)
    testConcat3D(valuesPerIteration, 5, 10000, 2)
    testConcat3D(valuesPerIteration, 5, 50000, 2)

    // group 5
    testConcat3D(valuesPerIteration, 5, 2, 10)
    testConcat3D(valuesPerIteration, 5, 2, 100)
    testConcat3D(valuesPerIteration, 5, 2, 1000)
    testConcat3D(valuesPerIteration, 5, 2, 10000)
    testConcat3D(valuesPerIteration, 5, 2, 50000)
  }
}
