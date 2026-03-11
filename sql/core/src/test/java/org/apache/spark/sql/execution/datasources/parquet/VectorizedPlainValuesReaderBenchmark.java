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
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.bytes.ByteBufferInputStream;

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmark comparing old (baseline) vs new (optimized) VectorizedPlainValuesReader.
 *
 * Run via:
 *   ./build/sbt "sql/Test/runMain \
 *     org.apache.spark.sql.execution.datasources.parquet \
 *     .VectorizedPlainValuesReaderBenchmark"
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Thread)
public class VectorizedPlainValuesReaderBenchmark {

  private static final int BATCH_SIZE = 4096;

  private Random random;

  // --- Heap buffer data for each type ---
  private byte[] booleanData;
  private byte[] byteData;     // 4 bytes per value (Parquet PLAIN encoding for byte)
  private byte[] shortData;    // 4 bytes per value (Parquet PLAIN encoding for short)
  private byte[] intData;      // 4 bytes per value
  private byte[] longData;     // 8 bytes per value
  private byte[] floatData;    // 4 bytes per value
  private byte[] doubleData;   // 8 bytes per value
  private byte[] binaryData;   // variable length: 4-byte length prefix + payload
  private byte[] rebaseIntData; // 4 bytes per value, all > lastSwitchJulianDay (no rebase)

  // --- Direct buffer versions ---
  private ByteBuffer directIntBuffer;
  private ByteBuffer directLongBuffer;

  // --- Column vectors ---
  private WritableColumnVector booleanColumn;
  private WritableColumnVector byteColumn;
  private WritableColumnVector shortColumn;
  private WritableColumnVector intColumn;
  private WritableColumnVector longColumn;
  private WritableColumnVector floatColumn;
  private WritableColumnVector doubleColumn;
  private WritableColumnVector binaryColumn;
  private WritableColumnVector rebaseIntColumn;
  private WritableColumnVector directIntColumn;
  private WritableColumnVector directLongColumn;
  private WritableColumnVector unsignedIntColumn;

  // --- Unsigned integer data ---
  private byte[] unsignedIntData;

  @Setup(Level.Trial)
  public void setup() {
    random = new Random(42);

    // Boolean data: packed bits, ceil(BATCH_SIZE / 8) bytes
    int booleanBytes = (BATCH_SIZE + 7) / 8;
    booleanData = new byte[booleanBytes];
    random.nextBytes(booleanData);

    // Byte data: PLAIN encoded as 4-byte ints
    byteData = new byte[BATCH_SIZE * 4];
    for (int i = 0; i < BATCH_SIZE; i++) {
      byteData[i * 4] = (byte) random.nextInt(256);
      byteData[i * 4 + 1] = 0;
      byteData[i * 4 + 2] = 0;
      byteData[i * 4 + 3] = 0;
    }

    // Short data: PLAIN encoded as 4-byte ints
    shortData = new byte[BATCH_SIZE * 4];
    ByteBuffer shortBuf = ByteBuffer.wrap(shortData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < BATCH_SIZE; i++) {
      shortBuf.putInt(i * 4, random.nextInt(Short.MAX_VALUE));
    }

    // Int data
    intData = new byte[BATCH_SIZE * 4];
    ByteBuffer intBuf = ByteBuffer.wrap(intData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < BATCH_SIZE; i++) {
      intBuf.putInt(i * 4, random.nextInt());
    }

    // Long data
    longData = new byte[BATCH_SIZE * 8];
    ByteBuffer longBuf = ByteBuffer.wrap(longData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < BATCH_SIZE; i++) {
      longBuf.putLong(i * 8, random.nextLong());
    }

    // Float data
    floatData = new byte[BATCH_SIZE * 4];
    ByteBuffer floatBuf = ByteBuffer.wrap(floatData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < BATCH_SIZE; i++) {
      floatBuf.putFloat(i * 4, random.nextFloat());
    }

    // Double data
    doubleData = new byte[BATCH_SIZE * 8];
    ByteBuffer doubleBuf = ByteBuffer.wrap(doubleData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < BATCH_SIZE; i++) {
      doubleBuf.putDouble(i * 8, random.nextDouble());
    }

    // Binary data: each value = 4-byte length prefix + random payload (8-32 bytes)
    int binaryTotalBytes = 0;
    int[] binaryLengths = new int[BATCH_SIZE];
    for (int i = 0; i < BATCH_SIZE; i++) {
      binaryLengths[i] = 8 + random.nextInt(25); // 8 to 32 bytes
      binaryTotalBytes += 4 + binaryLengths[i];
    }
    binaryData = new byte[binaryTotalBytes];
    ByteBuffer binaryBuf = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN);
    int offset = 0;
    for (int i = 0; i < BATCH_SIZE; i++) {
      binaryBuf.putInt(offset, binaryLengths[i]);
      offset += 4;
      for (int j = 0; j < binaryLengths[i]; j++) {
        binaryData[offset + j] = (byte) random.nextInt(256);
      }
      offset += binaryLengths[i];
    }

    // Rebase int data: all values > lastSwitchJulianDay (common case: no rebase needed)
    rebaseIntData = new byte[BATCH_SIZE * 4];
    ByteBuffer rebaseBuf = ByteBuffer.wrap(rebaseIntData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < BATCH_SIZE; i++) {
      // Use large positive values that won't trigger rebase
      rebaseBuf.putInt(i * 4, 2500000 + random.nextInt(100000));
    }

    // Direct buffer versions of int and long data
    directIntBuffer = ByteBuffer.allocateDirect(BATCH_SIZE * 4).order(ByteOrder.LITTLE_ENDIAN);
    directIntBuffer.put(intData);
    directIntBuffer.flip();

    directLongBuffer = ByteBuffer.allocateDirect(BATCH_SIZE * 8).order(ByteOrder.LITTLE_ENDIAN);
    directLongBuffer.put(longData);
    directLongBuffer.flip();

    // Unsigned integer data (stored as 4-byte ints, read as unsigned longs)
    unsignedIntData = new byte[BATCH_SIZE * 4];
    ByteBuffer unsignedBuf = ByteBuffer.wrap(unsignedIntData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < BATCH_SIZE; i++) {
      unsignedBuf.putInt(i * 4, random.nextInt());
    }

    // Allocate column vectors
    booleanColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.BooleanType);
    byteColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.ByteType);
    shortColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.ShortType);
    intColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
    longColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.LongType);
    floatColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.FloatType);
    doubleColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.DoubleType);
    binaryColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.BinaryType);
    rebaseIntColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
    directIntColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
    directLongColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.LongType);
    unsignedIntColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.LongType);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    booleanColumn.close();
    byteColumn.close();
    shortColumn.close();
    intColumn.close();
    longColumn.close();
    floatColumn.close();
    doubleColumn.close();
    binaryColumn.close();
    rebaseIntColumn.close();
    directIntColumn.close();
    directLongColumn.close();
    unsignedIntColumn.close();
  }

  // --- Helper methods ---

  private ByteBufferInputStream wrapHeap(byte[] data) {
    ByteBuffer buf = ByteBuffer.wrap(data);
    return ByteBufferInputStream.wrap(buf);
  }

  private ByteBufferInputStream wrapDirect(ByteBuffer directBuf) {
    directBuf.position(0);
    return ByteBufferInputStream.wrap(directBuf);
  }

  private OldVectorizedPlainValuesReader initOld(ByteBufferInputStream stream) throws IOException {
    OldVectorizedPlainValuesReader reader = new OldVectorizedPlainValuesReader();
    reader.initFromPage(BATCH_SIZE, stream);
    return reader;
  }

  private VectorizedPlainValuesReader initNew(ByteBufferInputStream stream) throws IOException {
    VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
    reader.initFromPage(BATCH_SIZE, stream);
    return reader;
  }

  // ==================== Boolean benchmarks ====================

  @Benchmark
  public void readBooleans_old() throws IOException {
    booleanColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapHeap(booleanData));
    reader.readBooleans(BATCH_SIZE, booleanColumn, 0);
  }

  @Benchmark
  public void readBooleans_new() throws IOException {
    booleanColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapHeap(booleanData));
    reader.readBooleans(BATCH_SIZE, booleanColumn, 0);
  }

  // ==================== Byte benchmarks ====================

  @Benchmark
  public void readBytes_old() throws IOException {
    byteColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapHeap(byteData));
    reader.readBytes(BATCH_SIZE, byteColumn, 0);
  }

  @Benchmark
  public void readBytes_new() throws IOException {
    byteColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapHeap(byteData));
    reader.readBytes(BATCH_SIZE, byteColumn, 0);
  }

  // ==================== Short benchmarks ====================

  @Benchmark
  public void readShorts_old() throws IOException {
    shortColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapHeap(shortData));
    reader.readShorts(BATCH_SIZE, shortColumn, 0);
  }

  @Benchmark
  public void readShorts_new() throws IOException {
    shortColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapHeap(shortData));
    reader.readShorts(BATCH_SIZE, shortColumn, 0);
  }

  // ==================== Unsigned Integer benchmarks ====================

  @Benchmark
  public void readUnsignedIntegers_old() throws IOException {
    unsignedIntColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapHeap(unsignedIntData));
    reader.readUnsignedIntegers(BATCH_SIZE, unsignedIntColumn, 0);
  }

  @Benchmark
  public void readUnsignedIntegers_new() throws IOException {
    unsignedIntColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapHeap(unsignedIntData));
    reader.readUnsignedIntegers(BATCH_SIZE, unsignedIntColumn, 0);
  }

  // ==================== Integer (direct buffer) benchmarks ====================

  @Benchmark
  public void readIntegers_direct_old() throws IOException {
    directIntColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapDirect(directIntBuffer));
    reader.readIntegers(BATCH_SIZE, directIntColumn, 0);
  }

  @Benchmark
  public void readIntegers_direct_new() throws IOException {
    directIntColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapDirect(directIntBuffer));
    reader.readIntegers(BATCH_SIZE, directIntColumn, 0);
  }

  // ==================== Long (direct buffer) benchmarks ====================

  @Benchmark
  public void readLongs_direct_old() throws IOException {
    directLongColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapDirect(directLongBuffer));
    reader.readLongs(BATCH_SIZE, directLongColumn, 0);
  }

  @Benchmark
  public void readLongs_direct_new() throws IOException {
    directLongColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapDirect(directLongBuffer));
    reader.readLongs(BATCH_SIZE, directLongColumn, 0);
  }

  // ==================== Binary benchmarks ====================

  @Benchmark
  public void readBinary_old() throws IOException {
    binaryColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapHeap(binaryData));
    reader.readBinary(BATCH_SIZE, binaryColumn, 0);
  }

  @Benchmark
  public void readBinary_new() throws IOException {
    binaryColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapHeap(binaryData));
    reader.readBinary(BATCH_SIZE, binaryColumn, 0);
  }

  // ==================== Integer with Rebase benchmarks ====================

  @Benchmark
  public void readIntegersWithRebase_old() throws IOException {
    rebaseIntColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapHeap(rebaseIntData));
    reader.readIntegersWithRebase(BATCH_SIZE, rebaseIntColumn, 0, false);
  }

  @Benchmark
  public void readIntegersWithRebase_new() throws IOException {
    rebaseIntColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapHeap(rebaseIntData));
    reader.readIntegersWithRebase(BATCH_SIZE, rebaseIntColumn, 0, false);
  }

  // ==================== Float (direct buffer) benchmarks ====================

  // Reuse directIntBuffer (same size) for float data since both are 4 bytes per value
  @Benchmark
  public void readFloats_direct_old() throws IOException {
    floatColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapDirect(directIntBuffer));
    reader.readFloats(BATCH_SIZE, floatColumn, 0);
  }

  @Benchmark
  public void readFloats_direct_new() throws IOException {
    floatColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapDirect(directIntBuffer));
    reader.readFloats(BATCH_SIZE, floatColumn, 0);
  }

  // ==================== Double (direct buffer) benchmarks ====================

  @Benchmark
  public void readDoubles_direct_old() throws IOException {
    doubleColumn.reset();
    OldVectorizedPlainValuesReader reader = initOld(wrapDirect(directLongBuffer));
    reader.readDoubles(BATCH_SIZE, doubleColumn, 0);
  }

  @Benchmark
  public void readDoubles_direct_new() throws IOException {
    doubleColumn.reset();
    VectorizedPlainValuesReader reader = initNew(wrapDirect(directLongBuffer));
    reader.readDoubles(BATCH_SIZE, doubleColumn, 0);
  }

  // ==================== Main entry point ====================

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(VectorizedPlainValuesReaderBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}
