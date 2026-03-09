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
package org.apache.spark.sql.execution.datasources.parquet

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Random

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types._

/**
 * Benchmark for VectorizedPlainValuesReader decoding performance.
 * Measures each read method with heap-backed and direct ByteBuffers,
 * across OnHeap and OffHeap column vectors.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to
 *        "benchmarks/VectorizedPlainValuesReaderBenchmark-results.txt".
 * }}}
 */
object VectorizedPlainValuesReaderBenchmark extends BenchmarkBase {

  private val random = new Random(42)

  private def makeHeapByteBuffer(size: Int): ByteBuffer = {
    val buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < size) {
      buf.put((random.nextInt() & 0xFF).toByte)
      i += 1
    }
    buf.flip()
    buf
  }

  private def makeDirectByteBuffer(size: Int): ByteBuffer = {
    val heap = makeHeapByteBuffer(size)
    val direct = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN)
    direct.put(heap)
    direct.flip()
    direct
  }

  private def makeReader(buf: ByteBuffer): VectorizedPlainValuesReader = {
    val reader = new VectorizedPlainValuesReader()
    val bbis = new ByteBufferInputStream(buf.duplicate())
    reader.initFromPage(0, bbis)
    reader
  }

  private def withColumnVector(count: Int, dt: DataType, memMode: MemoryMode)
      (f: org.apache.spark.sql.execution.vectorized.WritableColumnVector => Unit): Unit = {
    val col = if (memMode == MemoryMode.OFF_HEAP) {
      new OffHeapColumnVector(count, dt)
    } else {
      new OnHeapColumnVector(count, dt)
    }
    try { f(col) } finally { col.close() }
  }

  private def benchmarkFixedWidth(
      name: String,
      dataType: DataType,
      bytesPerElement: Int,
      readFn: (VectorizedPlainValuesReader, Int,
        org.apache.spark.sql.execution.vectorized.WritableColumnVector, Int) => Unit): Unit = {
    val count = 4096
    val iters = 5000
    val totalBytes = count * bytesPerElement

    val heapBuf = makeHeapByteBuffer(totalBytes)
    val directBuf = makeDirectByteBuffer(totalBytes)

    val benchmark = new Benchmark(
      s"$name ($count values)", count.toLong * iters, output = output)

    for (memMode <- Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP)) {
      val memStr = if (memMode == MemoryMode.ON_HEAP) "OnHeap" else "OffHeap"

      benchmark.addCase(s"Heap buffer, $memStr vector") { _ =>
        withColumnVector(count, dataType, memMode) { col =>
          var n = 0
          while (n < iters) {
            val reader = makeReader(heapBuf)
            readFn(reader, count, col, 0)
            n += 1
          }
        }
      }

      benchmark.addCase(s"Direct buffer, $memStr vector") { _ =>
        withColumnVector(count, dataType, memMode) { col =>
          var n = 0
          while (n < iters) {
            val reader = makeReader(directBuf)
            readFn(reader, count, col, 0)
            n += 1
          }
        }
      }
    }
    benchmark.run()
  }

  private def benchmarkReadIntegers(): Unit = {
    benchmarkFixedWidth("readIntegers", IntegerType, 4,
      (reader, total, col, rowId) => reader.readIntegers(total, col, rowId))
  }

  private def benchmarkReadLongs(): Unit = {
    benchmarkFixedWidth("readLongs", LongType, 8,
      (reader, total, col, rowId) => reader.readLongs(total, col, rowId))
  }

  private def benchmarkReadFloats(): Unit = {
    benchmarkFixedWidth("readFloats", FloatType, 4,
      (reader, total, col, rowId) => reader.readFloats(total, col, rowId))
  }

  private def benchmarkReadDoubles(): Unit = {
    benchmarkFixedWidth("readDoubles", DoubleType, 8,
      (reader, total, col, rowId) => reader.readDoubles(total, col, rowId))
  }

  private def benchmarkReadBooleans(): Unit = {
    val count = 4096
    val iters = 5000
    // Booleans are bit-packed: 1 bit per value, so count/8 bytes needed
    val totalBytes = (count + 7) / 8

    val heapBuf = makeHeapByteBuffer(totalBytes)
    val directBuf = makeDirectByteBuffer(totalBytes)

    val benchmark = new Benchmark(
      s"readBooleans ($count values)", count.toLong * iters, output = output)

    for (memMode <- Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP)) {
      val memStr = if (memMode == MemoryMode.ON_HEAP) "OnHeap" else "OffHeap"

      benchmark.addCase(s"Heap buffer, $memStr vector") { _ =>
        withColumnVector(count, BooleanType, memMode) { col =>
          var n = 0
          while (n < iters) {
            val reader = makeReader(heapBuf)
            reader.readBooleans(count, col, 0)
            n += 1
          }
        }
      }

      benchmark.addCase(s"Direct buffer, $memStr vector") { _ =>
        withColumnVector(count, BooleanType, memMode) { col =>
          var n = 0
          while (n < iters) {
            val reader = makeReader(directBuf)
            reader.readBooleans(count, col, 0)
            n += 1
          }
        }
      }
    }
    benchmark.run()
  }

  private def benchmarkReadBytes(): Unit = {
    // Parquet stores bytes as INT32 (4 bytes each)
    benchmarkFixedWidth("readBytes", ByteType, 4,
      (reader, total, col, rowId) => reader.readBytes(total, col, rowId))
  }

  private def benchmarkReadShorts(): Unit = {
    // Parquet stores shorts as INT32 (4 bytes each)
    benchmarkFixedWidth("readShorts", ShortType, 4,
      (reader, total, col, rowId) => reader.readShorts(total, col, rowId))
  }

  private def benchmarkReadBinary(): Unit = {
    val count = 1024
    val iters = 2000

    // Generate binary data: 4-byte length prefix + variable-length data
    def makeBinaryBuffer(isDirect: Boolean): ByteBuffer = {
      val avgLen = 32
      val totalEstimate = count * (4 + avgLen + 16) // generous estimate
      val tmp = ByteBuffer.allocate(totalEstimate).order(ByteOrder.LITTLE_ENDIAN)
      val rng = new Random(42)
      var i = 0
      while (i < count) {
        val len = rng.nextInt(60) + 4 // 4-64 bytes per value
        tmp.putInt(len)
        var j = 0
        while (j < len) {
          tmp.put((rng.nextInt() & 0xFF).toByte)
          j += 1
        }
        i += 1
      }
      tmp.flip()
      if (isDirect) {
        val direct = ByteBuffer.allocateDirect(tmp.remaining()).order(ByteOrder.LITTLE_ENDIAN)
        direct.put(tmp)
        direct.flip()
        direct
      } else {
        tmp
      }
    }

    val heapBuf = makeBinaryBuffer(isDirect = false)
    val directBuf = makeBinaryBuffer(isDirect = true)

    val benchmark = new Benchmark(
      s"readBinary ($count values)", count.toLong * iters, output = output)

    for (memMode <- Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP)) {
      val memStr = if (memMode == MemoryMode.ON_HEAP) "OnHeap" else "OffHeap"

      benchmark.addCase(s"Heap buffer, $memStr vector") { _ =>
        withColumnVector(count, BinaryType, memMode) { col =>
          var n = 0
          while (n < iters) {
            col.reset()
            val reader = makeReader(heapBuf)
            reader.readBinary(count, col, 0)
            n += 1
          }
        }
      }

      benchmark.addCase(s"Direct buffer, $memStr vector") { _ =>
        withColumnVector(count, BinaryType, memMode) { col =>
          var n = 0
          while (n < iters) {
            col.reset()
            val reader = makeReader(directBuf)
            reader.readBinary(count, col, 0)
            n += 1
          }
        }
      }
    }
    benchmark.run()
  }

  private def benchmarkReadUnsignedIntegers(): Unit = {
    val count = 4096
    val iters = 5000
    val totalBytes = count * 4

    val heapBuf = makeHeapByteBuffer(totalBytes)
    val directBuf = makeDirectByteBuffer(totalBytes)

    val benchmark = new Benchmark(
      s"readUnsignedIntegers ($count values)", count.toLong * iters, output = output)

    for (memMode <- Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP)) {
      val memStr = if (memMode == MemoryMode.ON_HEAP) "OnHeap" else "OffHeap"

      benchmark.addCase(s"Heap buffer, $memStr vector") { _ =>
        withColumnVector(count, LongType, memMode) { col =>
          var n = 0
          while (n < iters) {
            val reader = makeReader(heapBuf)
            reader.readUnsignedIntegers(count, col, 0)
            n += 1
          }
        }
      }

      benchmark.addCase(s"Direct buffer, $memStr vector") { _ =>
        withColumnVector(count, LongType, memMode) { col =>
          var n = 0
          while (n < iters) {
            val reader = makeReader(directBuf)
            reader.readUnsignedIntegers(count, col, 0)
            n += 1
          }
        }
      }
    }
    benchmark.run()
  }

  private def benchmarkReadIntegersWithRebase(): Unit = {
    val count = 4096
    val iters = 5000
    val totalBytes = count * 4

    // All values > lastSwitchJulianDay so no rebase needed (fast path)
    val buf = ByteBuffer.allocate(totalBytes).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < count) {
      buf.putInt(2500000 + i) // modern dates, well above lastSwitchJulianDay
      i += 1
    }
    buf.flip()
    val directBuf = {
      val d = ByteBuffer.allocateDirect(totalBytes).order(ByteOrder.LITTLE_ENDIAN)
      d.put(buf.duplicate())
      d.flip()
      d
    }

    val benchmark = new Benchmark(
      s"readIntegersWithRebase ($count values, no rebase)",
      count.toLong * iters, output = output)

    for (memMode <- Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP)) {
      val memStr = if (memMode == MemoryMode.ON_HEAP) "OnHeap" else "OffHeap"

      benchmark.addCase(s"Heap buffer, $memStr vector") { _ =>
        withColumnVector(count, IntegerType, memMode) { col =>
          var n = 0
          while (n < iters) {
            val reader = makeReader(buf)
            reader.readIntegersWithRebase(count, col, 0, false)
            n += 1
          }
        }
      }

      benchmark.addCase(s"Direct buffer, $memStr vector") { _ =>
        withColumnVector(count, IntegerType, memMode) { col =>
          var n = 0
          while (n < iters) {
            val reader = makeReader(directBuf)
            reader.readIntegersWithRebase(count, col, 0, false)
            n += 1
          }
        }
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("readIntegers") {
      benchmarkReadIntegers()
    }
    runBenchmark("readLongs") {
      benchmarkReadLongs()
    }
    runBenchmark("readFloats") {
      benchmarkReadFloats()
    }
    runBenchmark("readDoubles") {
      benchmarkReadDoubles()
    }
    runBenchmark("readBooleans") {
      benchmarkReadBooleans()
    }
    runBenchmark("readBytes") {
      benchmarkReadBytes()
    }
    runBenchmark("readShorts") {
      benchmarkReadShorts()
    }
    runBenchmark("readBinary") {
      benchmarkReadBinary()
    }
    runBenchmark("readUnsignedIntegers") {
      benchmarkReadUnsignedIntegers()
    }
    runBenchmark("readIntegersWithRebase (no rebase)") {
      benchmarkReadIntegersWithRebase()
    }
  }
}
