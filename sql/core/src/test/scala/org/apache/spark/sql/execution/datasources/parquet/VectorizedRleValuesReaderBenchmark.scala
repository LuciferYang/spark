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

import java.nio.ByteBuffer

import scala.util.Random

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.io.api.Binary

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReaderTestUtils._
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

/**
 * Low-level benchmark for `VectorizedRleValuesReader`. Measures PACKED-mode decode paths in
 * isolation so per-optimization gains can be quantified without IO or decompression noise.
 *
 * Groups:
 *   A. readBooleans PACKED (targets P1).
 *   B. readIntegers PACKED dictionary-id decode across bitWidths (targets P3, P4).
 *   C. readBatch nullable, constant value reader (isolates P0 decoder overhead).
 *   D. readBatch nullable, plain INT32 value reader (P0 with real value decoding).
 *
 * Cold = fresh reader per iteration; inside the timed region we allocate + init + read
 * (exercises P3's cold `currentBuffer` growth). Reused = reader and a warm-up read are done
 * once outside the timed region; inside we only call `initFromPage` + the measured read
 * (steady-state decode cost with buffer already sized).
 *
 * Column vectors are allocated once per case and reused across iterations so ~4 MB of heap
 * allocation doesn't pollute the measured region.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/VectorizedRleValuesReaderBenchmark-results.txt".
 *   3. GHA: `Run benchmarks` workflow, class = `*VectorizedRleValuesReader*`.
 * }}}
 */
object VectorizedRleValuesReaderBenchmark extends BenchmarkBase {

  private val NUM_ROWS = 1024 * 1024
  private val NUM_ITERS = 5
  private val BATCH_SIZE = 4096

  private def toInputStream(bytes: Array[Byte]): ByteBufferInputStream =
    ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))

  // Def-level pattern where most 8-value groups contain mixed values, forcing PACKED mode.
  private def packedFriendlyDefLevels(
      n: Int, nullRatio: Double, clustered: Boolean): Array[Int] = {
    val rng = new Random(42)
    val arr = new Array[Int](n)
    if (clustered) {
      val runLen = 50
      var i = 0
      while (i < n) {
        val end = math.min(n, i + runLen)
        val isNullRun = rng.nextDouble() < nullRatio
        var j = i
        while (j < end) {
          arr(j) = if (isNullRun) 0 else 1
          j += 1
        }
        i = end
      }
    } else {
      var i = 0
      while (i < n) {
        arr(i) = if (rng.nextDouble() < nullRatio) 0 else 1
        i += 1
      }
    }
    // Perturb every 9th element to break long identical runs and keep encoder in PACKED mode.
    // Skip for the pure-RLE control cases (all 0s or all 1s) where we want a single RLE run.
    if (nullRatio > 0.0 && nullRatio < 1.0) {
      var k = 1
      while (k < n) {
        if (arr(k) == arr(k - 1) && (k % 9 == 0)) arr(k) ^= 1
        k += 1
      }
    }
    arr
  }

  private def packedFriendlyBooleans(n: Int, trueRatio: Double): Array[Int] =
    packedFriendlyDefLevels(n, 1.0 - trueRatio, clustered = false)

  private def packedFriendlyDictIds(n: Int, bitWidth: Int): Array[Int] = {
    val rng = new Random(42)
    val max = 1 << bitWidth
    val arr = new Array[Int](n)
    var i = 0
    while (i < n) {
      arr(i) = rng.nextInt(max)
      i += 1
    }
    arr
  }

  private def newReadState(maxDef: Int, valuesInPage: Int): AnyRef = {
    val state = ParquetReadStateTestAccess.newState(intColumnDescriptor(maxDef), maxDef == 0)
    ParquetReadStateTestAccess.resetForNewBatch(state, BATCH_SIZE)
    ParquetReadStateTestAccess.resetForNewPage(state, valuesInPage, 0L)
    state
  }

  // Stub value reader returning zeros; isolates RLE decoder overhead from real value decoding.
  private final class ConstantIntValuesReader extends ValuesReader with VectorizedValuesReader {
    override def initFromPage(valueCount: Int, in: ByteBufferInputStream): Unit = {}
    override def skip(): Unit = {}

    override def readBoolean(): Boolean = throw new UnsupportedOperationException
    override def readByte(): Byte = throw new UnsupportedOperationException
    override def readShort(): Short = throw new UnsupportedOperationException
    override def readInteger(): Int = 0
    override def readLong(): Long = throw new UnsupportedOperationException
    override def readFloat(): Float = throw new UnsupportedOperationException
    override def readDouble(): Double = throw new UnsupportedOperationException
    override def readBinary(len: Int): Binary = throw new UnsupportedOperationException

    override def readBooleans(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readBytes(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readShorts(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readIntegers(total: Int, c: WritableColumnVector, rowId: Int): Unit = {}
    override def readIntegersWithRebase(
        total: Int, c: WritableColumnVector, rowId: Int, failIfRebase: Boolean): Unit =
      throw new UnsupportedOperationException
    override def readUnsignedIntegers(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readUnsignedLongs(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readLongs(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readLongsWithRebase(
        total: Int,
        c: WritableColumnVector,
        rowId: Int,
        failIfRebase: Boolean,
        timeZone: String): Unit = throw new UnsupportedOperationException
    override def readFloats(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readDoubles(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readBinary(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readGeometry(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException
    override def readGeography(total: Int, c: WritableColumnVector, rowId: Int): Unit =
      throw new UnsupportedOperationException

    override def skipBooleans(total: Int): Unit = {}
    override def skipBytes(total: Int): Unit = {}
    override def skipShorts(total: Int): Unit = {}
    override def skipIntegers(total: Int): Unit = {}
    override def skipLongs(total: Int): Unit = {}
    override def skipFloats(total: Int): Unit = {}
    override def skipDoubles(total: Int): Unit = {}
    override def skipBinary(total: Int): Unit = {}
    override def skipFixedLenByteArray(total: Int, len: Int): Unit = {}
  }

  // A factory pattern that pre-allocates per-reader state once and returns a zero-arg closure
  // which reinitializes and yields a ready-to-use reader, avoiding per-iteration large allocations.
  private type ValueReaderFactory = () => VectorizedValuesReader

  private val constantIntFactory: ValueReaderFactory = {
    val singleton = new ConstantIntValuesReader
    () => singleton
  }

  private def plainIntFactory(nonNullCount: Int): ValueReaderFactory = {
    val buf = new Array[Byte](nonNullCount * 4)
    val r = new VectorizedPlainValuesReader
    () => {
      r.initFromPage(nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(buf)))
      r
    }
  }

  private def runBooleanPackedBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "RLE readBooleans PACKED decode", NUM_ROWS.toLong, NUM_ITERS, output = output)
    val vec = new OnHeapColumnVector(NUM_ROWS, BooleanType)

    Seq(0.1, 0.5, 0.9).foreach { trueRatio =>
      val bytes = encodeRle(packedFriendlyBooleans(NUM_ROWS, trueRatio), bitWidth = 1)

      benchmark.addCase(f"cold reader, trueRatio=${trueRatio}%.1f") { _ =>
        val reader = new VectorizedRleValuesReader(1, false)
        reader.initFromPage(NUM_ROWS, toInputStream(bytes))
        reader.readBooleans(NUM_ROWS, vec, 0)
      }

      val warmReader = new VectorizedRleValuesReader(1, false)
      warmReader.initFromPage(NUM_ROWS, toInputStream(bytes))
      warmReader.readBooleans(NUM_ROWS, vec, 0)

      benchmark.addCase(f"reused reader, trueRatio=${trueRatio}%.1f") { _ =>
        warmReader.initFromPage(NUM_ROWS, toInputStream(bytes))
        warmReader.readBooleans(NUM_ROWS, vec, 0)
      }
    }
    benchmark.run()
  }

  private def runIntegerPackedBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "RLE readIntegers PACKED dictionary-id decode",
      NUM_ROWS.toLong, NUM_ITERS, output = output)
    val vec = new OnHeapColumnVector(NUM_ROWS, IntegerType)

    Seq(4, 8, 12, 20).foreach { bitWidth =>
      val bytes = encodeRle(packedFriendlyDictIds(NUM_ROWS, bitWidth), bitWidth)

      benchmark.addCase(s"cold reader, bitWidth=$bitWidth") { _ =>
        val reader = new VectorizedRleValuesReader(bitWidth, false)
        reader.initFromPage(NUM_ROWS, toInputStream(bytes))
        reader.readIntegers(NUM_ROWS, vec, 0)
      }

      val warmReader = new VectorizedRleValuesReader(bitWidth, false)
      warmReader.initFromPage(NUM_ROWS, toInputStream(bytes))
      warmReader.readIntegers(NUM_ROWS, vec, 0)

      benchmark.addCase(s"reused reader, bitWidth=$bitWidth") { _ =>
        warmReader.initFromPage(NUM_ROWS, toInputStream(bytes))
        warmReader.readIntegers(NUM_ROWS, vec, 0)
      }
    }
    benchmark.run()
  }

  private def runNullableBatchBenchmark(
      label: String,
      buildValueReader: Int => ValueReaderFactory): Unit = {
    val benchmark = new Benchmark(label, NUM_ROWS.toLong, NUM_ITERS, output = output)
    val values = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val defLevelsVec = new OnHeapColumnVector(NUM_ROWS, IntegerType)

    val nullRatios = Seq(0.0, 0.1, 0.3, 0.5, 0.9)
    val clusterings = Seq(false, true)

    nullRatios.foreach { nullRatio =>
      clusterings.foreach { clustered =>
        if (!(nullRatio == 0.0 && clustered)) {
          val defLevels = packedFriendlyDefLevels(NUM_ROWS, nullRatio, clustered)
          val nonNullCount = defLevels.count(_ == 1)
          val bytes = encodeRle(defLevels, bitWidth = 1)
          val clusterTag =
            if (nullRatio == 0.0) "n/a" else if (clustered) "clustered" else "random"
          val factory = buildValueReader(nonNullCount)

          // Pre-warm the reader + state so currentBuffer is sized and the JIT has seen the path.
          val reader = new VectorizedRleValuesReader(1, false)
          val state = newReadState(maxDef = 1, valuesInPage = NUM_ROWS)
          reader.initFromPage(NUM_ROWS, toInputStream(bytes))
          runBatches(reader, state, values, defLevelsVec, factory())

          benchmark.addCase(f"nullRatio=${nullRatio}%.1f, $clusterTag") { _ =>
            reader.initFromPage(NUM_ROWS, toInputStream(bytes))
            ParquetReadStateTestAccess.resetForNewPage(state, NUM_ROWS, 0L)
            runBatches(reader, state, values, defLevelsVec, factory())
          }
        }
      }
    }
    benchmark.run()
  }

  private def runBatches(
      reader: VectorizedRleValuesReader,
      state: AnyRef,
      values: WritableColumnVector,
      defLevelsVec: WritableColumnVector,
      valueReader: VectorizedValuesReader): Unit = {
    var produced = 0
    while (produced < NUM_ROWS) {
      val toRead = math.min(BATCH_SIZE, NUM_ROWS - produced)
      ParquetReadStateTestAccess.resetForNewBatch(state, toRead)
      ParquetReadStateTestAccess.readBatch(
        reader, state, values, defLevelsVec, valueReader, integerUpdater)
      produced += toRead
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Boolean PACKED decode") {
      runBooleanPackedBenchmark()
    }
    runBenchmark("Integer PACKED decode") {
      runIntegerPackedBenchmark()
    }
    runBenchmark("Nullable batch decode, constant value reader") {
      runNullableBatchBenchmark(
        "Nullable batch, constant value reader", _ => constantIntFactory)
    }
    runBenchmark("Nullable batch decode, plain INT32 value reader") {
      runNullableBatchBenchmark(
        "Nullable batch, plain INT32 value reader", plainIntFactory)
    }
  }
}
