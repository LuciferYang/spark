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

package org.apache.spark.sql.execution.benchmark

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

/**
 * Benchmark to measure performance for byte array operators.
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/<this class>-results.txt".
 * }}}
 */
object ByteArrayBenchmark extends BenchmarkBase {
  private val chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  private val randomChar = new Random(0)

  def randomBytes(min: Int, max: Int): Array[Byte] = {
    val len = randomChar.nextInt(max - min) + min
    val bytes = new Array[Byte](len)
    var i = 0
    while (i < len) {
      bytes(i) = chars.charAt(randomChar.nextInt(chars.length())).toByte
      i += 1
    }
    bytes
  }

  def byteArrayComparisons(iters: Long): Unit = {
    val count = 16 * 1000
    val dataTiny = Seq.fill(count)(randomBytes(2, 7)).toArray
    val dataSmall = Seq.fill(count)(randomBytes(8, 16)).toArray
    val dataMedium = Seq.fill(count)(randomBytes(16, 32)).toArray
    val dataLarge = Seq.fill(count)(randomBytes(512, 1024)).toArray
    val dataLargeSlow = Seq.fill(count)(
      Array.tabulate(512) {i => if (i < 511) 0.toByte else 1.toByte}).toArray

    def compareBinary(data: Array[Array[Byte]]) = { _: Int =>
      var sum = 0L
      for (_ <- 0L until iters) {
        var i = 0
        while (i < count) {
          sum += ByteArray.compareBinary(data(i), data((i + 1) % count))
          i += 1
        }
      }
    }

    val benchmark = new Benchmark("Byte Array compareTo", count * iters, 25, output = output)
    benchmark.addCase("2-7 byte")(compareBinary(dataTiny))
    benchmark.addCase("8-16 byte")(compareBinary(dataSmall))
    benchmark.addCase("16-32 byte")(compareBinary(dataMedium))
    benchmark.addCase("512-1024 byte")(compareBinary(dataLarge))
    benchmark.addCase("512 byte slow")(compareBinary(dataLargeSlow))
    benchmark.addCase("2-7 byte")(compareBinary(dataTiny))
    benchmark.run()
  }

  def byteArrayEquals(iters: Long): Unit = {
    def binaryEquals(inputs: Array[BinaryEqualInfo]) = { _: Int =>
      var res = false
      for (_ <- 0L until iters) {
        inputs.foreach { input =>
          res = ByteArrayMethods.arrayEquals(
            input.s1.getBaseObject, input.s1.getBaseOffset,
            input.s2.getBaseObject, input.s2.getBaseOffset + input.deltaOffset,
            input.len)
        }
      }
    }
    val count = 16 * 1000
    val rand = new Random(0)
    val inputs = (0 until count).map { _ =>
      val s1 = UTF8String.fromBytes(randomBytes(1, 16))
      val s2 = UTF8String.fromBytes(randomBytes(1, 16))
      val len = s1.numBytes().min(s2.numBytes())
      val deltaOffset = rand.nextInt(len)
      BinaryEqualInfo(s1, s2, deltaOffset, len)
    }.toArray

    val benchmark = new Benchmark("Byte Array equals", count * iters, 25, output = output)
    benchmark.addCase("Byte Array equals")(binaryEquals(inputs))
    benchmark.run()
  }

  case class BinaryEqualInfo(
      s1: UTF8String,
      s2: UTF8String,
      deltaOffset: Int,
      len: Int)

  def byteArrayPadding(iters: Long): Unit = {
    val count = 16 * 1000

    // Generate test data with different characteristics
    val dataSmall = Seq.fill(count)(randomBytes(2, 8)).toArray
    val dataMedium = Seq.fill(count)(randomBytes(8, 32)).toArray
    val dataLarge = Seq.fill(count)(randomBytes(32, 128)).toArray

    // Different padding patterns
    val padSingle = "X".getBytes
    val padShort = "AB".getBytes
    val padLong = "0123456789".getBytes
    val padEmpty = Array.empty[Byte]

    def lpad(data: Array[Array[Byte]], pad: Array[Byte], targetLen: Int) = { _: Int =>
      var result: Array[Byte] = null
      for (_ <- 0L until iters) {
        var i = 0
        while (i < count) {
          result = ByteArray.lpad(data(i), targetLen, pad)
          i += 1
        }
      }
    }

    def rpad(data: Array[Array[Byte]], pad: Array[Byte], targetLen: Int) = { _: Int =>
      var result: Array[Byte] = null
      for (_ <- 0L until iters) {
        var i = 0
        while (i < count) {
          result = ByteArray.rpad(data(i), targetLen, pad)
          i += 1
        }
      }
    }

    // Benchmark 1: Test different input data sizes with consistent padding
    val dataSizeBenchmark =
      new Benchmark("Byte Array padding - Data Size Impact", count * iters, 25, output = output)
    dataSizeBenchmark.addCase("lpad small data (2-8 bytes)")(lpad(dataSmall, padSingle, 50))
    dataSizeBenchmark.addCase("lpad medium data (8-32 bytes)")(lpad(dataMedium, padSingle, 50))
    dataSizeBenchmark.addCase("lpad large data (32-128 bytes)")(lpad(dataLarge, padSingle, 100))
    dataSizeBenchmark.addCase("rpad small data (2-8 bytes)")(rpad(dataSmall, padSingle, 50))
    dataSizeBenchmark.addCase("rpad medium data (8-32 bytes)")(rpad(dataMedium, padSingle, 50))
    dataSizeBenchmark.addCase("rpad large data (32-128 bytes)")(rpad(dataLarge, padSingle, 100))
    dataSizeBenchmark.run()

    // Benchmark 2: Test different padding patterns with consistent data
    val padPatternBenchmark =
      new Benchmark("Byte Array padding - Padding Pattern Impact",
        count * iters, 25, output = output)
    padPatternBenchmark.addCase("lpad single char pad")(lpad(dataSmall, padSingle, 50))
    padPatternBenchmark.addCase("lpad short pattern pad")(lpad(dataSmall, padShort, 50))
    padPatternBenchmark.addCase("lpad long pattern pad")(lpad(dataSmall, padLong, 50))
    padPatternBenchmark.addCase("lpad empty pad")(lpad(dataSmall, padEmpty, 10))
    padPatternBenchmark.addCase("rpad single char pad")(rpad(dataSmall, padSingle, 50))
    padPatternBenchmark.addCase("rpad short pattern pad")(rpad(dataSmall, padShort, 50))
    padPatternBenchmark.addCase("rpad long pattern pad")(rpad(dataSmall, padLong, 50))
    padPatternBenchmark.addCase("rpad empty pad")(rpad(dataSmall, padEmpty, 10))
    padPatternBenchmark.run()

    // Benchmark 3: Test different target lengths (truncate vs pad scenarios)
    val targetLengthBenchmark =
      new Benchmark("Byte Array padding - Target Length Impact", count * iters, 25, output = output)
    targetLengthBenchmark.addCase("lpad truncate (len=5)")(lpad(dataSmall, padSingle, 5))
    targetLengthBenchmark.addCase("lpad moderate pad (len=20)")(lpad(dataSmall, padSingle, 20))
    targetLengthBenchmark.addCase("lpad heavy pad (len=100)")(lpad(dataSmall, padSingle, 100))
    targetLengthBenchmark.addCase("rpad truncate (len=5)")(rpad(dataSmall, padSingle, 5))
    targetLengthBenchmark.addCase("rpad moderate pad (len=20)")(rpad(dataSmall, padSingle, 20))
    targetLengthBenchmark.addCase("rpad heavy pad (len=100)")(rpad(dataSmall, padSingle, 100))
    targetLengthBenchmark.run()

    // Benchmark 4: Direct comparison of lpad vs rpad
    val lpadVsRpadBenchmark =
      new Benchmark("Byte Array padding - lpad vs rpad", count * iters, 25, output = output)
    lpadVsRpadBenchmark.addCase("lpad small data, single pad")(lpad(dataSmall, padSingle, 50))
    lpadVsRpadBenchmark.addCase("rpad small data, single pad")(rpad(dataSmall, padSingle, 50))
    lpadVsRpadBenchmark.addCase("lpad medium data, single pad")(lpad(dataMedium, padSingle, 50))
    lpadVsRpadBenchmark.addCase("rpad medium data, single pad")(rpad(dataMedium, padSingle, 50))
    lpadVsRpadBenchmark.addCase("lpad small data, long pad")(lpad(dataSmall, padLong, 50))
    lpadVsRpadBenchmark.addCase("rpad small data, long pad")(rpad(dataSmall, padLong, 50))
    lpadVsRpadBenchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("byte array comparisons") {
      byteArrayComparisons(1024 * 4)
    }

    runBenchmark("byte array equals") {
      byteArrayEquals(1000 * 10)
    }

    runBenchmark("byte array padding") {
      byteArrayPadding(1000 * 2)
    }
  }
}
