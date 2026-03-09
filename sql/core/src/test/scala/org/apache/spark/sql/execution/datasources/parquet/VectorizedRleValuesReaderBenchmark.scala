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

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

/**
 * Benchmark to measure VectorizedRleValuesReader performance.
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain \
 *      org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReaderBenchmark"
 *   2. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain \
 *      org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReaderBenchmark"
 *      Results will be written to "benchmarks/VectorizedRleValuesReaderBenchmark-results.txt".
 * }}}
 */
object VectorizedRleValuesReaderBenchmark extends BenchmarkBase {

  class DummyValuesReader extends VectorizedValuesReader {
    override def readBoolean(): Boolean = false
    override def readInteger(): Int = 0
    override def readLong(): Long = 0
    override def readFloat(): Float = 0
    override def readDouble(): Double = 0
    override def readBinary(i: Int): org.apache.parquet.io.api.Binary = null
    override def readIntegers(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readUnsignedIntegers(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readUnsignedLongs(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readIntegersWithRebase(
        i: Int, w: WritableColumnVector, i1: Int, b: Boolean): Unit = {}
    override def readByte(): Byte = 0
    override def readShort(): Short = 0
    override def readBytes(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readShorts(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readLongs(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readLongsWithRebase(
        i: Int, w: WritableColumnVector, i1: Int, b: Boolean, s: String): Unit = {}
    override def readBinary(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readGeometry(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readGeography(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readBooleans(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readFloats(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def readDoubles(i: Int, w: WritableColumnVector, i1: Int): Unit = {}
    override def skipIntegers(i: Int): Unit = {}
    override def skipBooleans(i: Int): Unit = {}
    override def skipBytes(i: Int): Unit = {}
    override def skipShorts(i: Int): Unit = {}
    override def skipLongs(i: Int): Unit = {}
    override def skipFloats(i: Int): Unit = {}
    override def skipDoubles(i: Int): Unit = {}
    override def skipBinary(i: Int): Unit = {}
    override def skipFixedLenByteArray(i: Int, i1: Int): Unit = {}
  }

  private def encodeValues(
      values: Array[Int],
      bitWidth: Int,
      useRle: Boolean): ByteBufferInputStream = {
    val capacity = values.length * 4 + 1024
    val allocator = new org.apache.parquet.bytes.ByteBufferAllocator {
      override def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size)
      override def release(b: ByteBuffer): Unit = {}
      override def isDirect: Boolean = false
    }
    val encoder = new RunLengthBitPackingHybridEncoder(bitWidth, capacity, capacity, allocator)
    for (v <- values) {
      encoder.writeInt(v)
    }
    val buf = encoder.toBytes
    ByteBufferInputStream.wrap(buf.toByteBuffer)
  }

  def benchmarkIntRead(numValues: Int, bitWidth: Int, mode: String): Unit = {
    val benchmark = new Benchmark(
      s"Int Read bitWidth=$bitWidth mode=$mode", numValues, output = output)

    val values = new Array[Int](numValues)
    if (mode == "RLE") {
      java.util.Arrays.fill(values, 1)
    } else {
      val mask = (1 << bitWidth) - 1
      for (i <- 0 until numValues) {
        values(i) = i & mask
        // Avoid long runs to force PACKED
        if (i > 0 && values(i) == values(i - 1)) {
           values(i) = (values(i) + 1) & mask
        }
      }
    }

    val input = encodeValues(values, bitWidth, mode == "RLE")
    // Read all bytes to a byte array to avoid stream overhead during benchmark setup
    val bytes = new Array[Byte](input.available())
    input.read(ByteBuffer.wrap(bytes))

    benchmark.addCase("readBatch") { _ =>
       val reader = new VectorizedRleValuesReader(bitWidth, false)
       reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
       val vector = new OnHeapColumnVector(numValues, IntegerType)

       // Mock ColumnDescriptor
       val typeName = PrimitiveTypeName.INT32
       val primitiveType = Types.required(typeName).named("col")
       val descriptor = new ColumnDescriptor(Array("col"), primitiveType, 0, 1)

       val state = new ParquetReadState(descriptor, true, null)
       state.rowsToReadInBatch = numValues
       state.valuesToReadInPage = numValues
       state.rowId = 0
       state.levelOffset = 0
       state.valueOffset = 0

       // Using IntegerUpdater to read values
       // We pass null for defLevels so it calls readBatchInternal
       val dummyReader = new DummyValuesReader()
       reader.readBatch(
         state, vector, null, dummyReader, new ParquetVectorUpdaterFactory.IntegerUpdater())
    }

    benchmark.run()
  }

  def benchmarkBooleanRead(numValues: Int, mode: String): Unit = {
    val benchmark = new Benchmark(s"Boolean Read mode=$mode", numValues, output = output)

    val values = new Array[Int](numValues)
    if (mode == "RLE") {
      java.util.Arrays.fill(values, 1)
    } else {
       for (i <- 0 until numValues) {
        values(i) = i % 2
        // Avoid long runs
        if (i > 0 && values(i) == values(i - 1)) {
           values(i) = (values(i) + 1) % 2
        }
      }
    }

    val input = encodeValues(values, 1, mode == "RLE")
    val bytes = new Array[Byte](input.available())
    input.read(ByteBuffer.wrap(bytes))

    benchmark.addCase("readBooleans") { _ =>
       val reader = new VectorizedRleValuesReader(1, false)
       reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
       // Booleans are stored as bytes/ints in vector logic
       // Actually BooleanType is backed by Byte array in OnHeapColumnVector
       val vector = new OnHeapColumnVector(numValues, BooleanType)

       reader.readBooleans(numValues, vector, 0)
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    benchmarkIntRead(1024 * 1024 * 10, 4, "PACKED")
    benchmarkIntRead(1024 * 1024 * 10, 4, "RLE")
    benchmarkIntRead(1024 * 1024 * 10, 8, "PACKED")
    benchmarkIntRead(1024 * 1024 * 10, 8, "RLE")
    benchmarkBooleanRead(1024 * 1024 * 10, "PACKED")
    benchmarkBooleanRead(1024 * 1024 * 10, "RLE")
  }
}
