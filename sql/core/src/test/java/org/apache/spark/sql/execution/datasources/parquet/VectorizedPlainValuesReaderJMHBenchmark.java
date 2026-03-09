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
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;

/**
 * JMH benchmark for VectorizedPlainValuesReader decoding performance.
 *
 * Run with:
 *   java -cp <classpath> \
 *     org.apache.spark.sql.execution.datasources.parquet
 *       .VectorizedPlainValuesReaderJMHBenchmark
 * or via Maven:
 *   ./build/mvn test -pl sql/core -Dtest=none \
 *     -DwildcardSuites=none \
 *     -Dexec.mainClass="org.apache.spark.sql.execution.datasources.parquet
 *       .VectorizedPlainValuesReaderJMHBenchmark"
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class VectorizedPlainValuesReaderJMHBenchmark {

  @Param({"4096"})
  private int count;

  @Param({"HEAP", "DIRECT"})
  private String bufferType;

  @Param({"ON_HEAP", "OFF_HEAP"})
  private String vectorType;

  // ---- Fixed-width numeric buffers (4 bytes per element) ----
  private ByteBuffer int4Buffer;
  // ---- Fixed-width numeric buffers (8 bytes per element) ----
  private ByteBuffer int8Buffer;
  // ---- Boolean buffer ----
  private ByteBuffer boolBuffer;
  // ---- Binary buffer ----
  private ByteBuffer binaryBuffer;
  private int binaryCount;

  // Column vectors
  private WritableColumnVector intCol;
  private WritableColumnVector longCol;
  private WritableColumnVector floatCol;
  private WritableColumnVector doubleCol;
  private WritableColumnVector boolCol;
  private WritableColumnVector byteCol;
  private WritableColumnVector shortCol;
  private WritableColumnVector binaryCol;

  @Setup(Level.Trial)
  public void setup() {
    Random rng = new Random(42);

    // 4-byte element buffer (for int, float, byte, short)
    int size4 = count * 4;
    ByteBuffer heap4 = ByteBuffer.allocate(size4).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < size4; i++) {
      heap4.put((byte) (rng.nextInt() & 0xFF));
    }
    heap4.flip();

    // 8-byte element buffer (for long, double)
    int size8 = count * 8;
    ByteBuffer heap8 = ByteBuffer.allocate(size8).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < size8; i++) {
      heap8.put((byte) (rng.nextInt() & 0xFF));
    }
    heap8.flip();

    // Boolean buffer (bit-packed)
    int boolSize = (count + 7) / 8;
    ByteBuffer heapBool = ByteBuffer.allocate(boolSize).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < boolSize; i++) {
      heapBool.put((byte) (rng.nextInt() & 0xFF));
    }
    heapBool.flip();

    // Binary buffer (4-byte length prefix + variable data)
    binaryCount = 1024;
    rng = new Random(42);
    int binaryEstimate = binaryCount * (4 + 64 + 16);
    ByteBuffer heapBinary = ByteBuffer.allocate(binaryEstimate).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < binaryCount; i++) {
      int len = rng.nextInt(60) + 4;
      heapBinary.putInt(len);
      for (int j = 0; j < len; j++) {
        heapBinary.put((byte) (rng.nextInt() & 0xFF));
      }
    }
    heapBinary.flip();

    if ("DIRECT".equals(bufferType)) {
      int4Buffer = copyToDirect(heap4);
      int8Buffer = copyToDirect(heap8);
      boolBuffer = copyToDirect(heapBool);
      binaryBuffer = copyToDirect(heapBinary);
    } else {
      int4Buffer = heap4;
      int8Buffer = heap8;
      boolBuffer = heapBool;
      binaryBuffer = heapBinary;
    }

    boolean offHeap = "OFF_HEAP".equals(vectorType);
    intCol = offHeap ? new OffHeapColumnVector(count, IntegerType$.MODULE$)
                     : new OnHeapColumnVector(count, IntegerType$.MODULE$);
    longCol = offHeap ? new OffHeapColumnVector(count, LongType$.MODULE$)
                      : new OnHeapColumnVector(count, LongType$.MODULE$);
    floatCol = offHeap ? new OffHeapColumnVector(count, FloatType$.MODULE$)
                       : new OnHeapColumnVector(count, FloatType$.MODULE$);
    doubleCol = offHeap ? new OffHeapColumnVector(count, DoubleType$.MODULE$)
                        : new OnHeapColumnVector(count, DoubleType$.MODULE$);
    boolCol = offHeap ? new OffHeapColumnVector(count, BooleanType$.MODULE$)
                      : new OnHeapColumnVector(count, BooleanType$.MODULE$);
    byteCol = offHeap ? new OffHeapColumnVector(count, ByteType$.MODULE$)
                      : new OnHeapColumnVector(count, ByteType$.MODULE$);
    shortCol = offHeap ? new OffHeapColumnVector(count, ShortType$.MODULE$)
                       : new OnHeapColumnVector(count, ShortType$.MODULE$);
    binaryCol = offHeap ? new OffHeapColumnVector(binaryCount, BinaryType$.MODULE$)
                        : new OnHeapColumnVector(binaryCount, BinaryType$.MODULE$);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    intCol.close();
    longCol.close();
    floatCol.close();
    doubleCol.close();
    boolCol.close();
    byteCol.close();
    shortCol.close();
    binaryCol.close();
  }

  private static ByteBuffer copyToDirect(ByteBuffer heap) {
    ByteBuffer direct = ByteBuffer.allocateDirect(heap.remaining())
        .order(ByteOrder.LITTLE_ENDIAN);
    direct.put(heap.duplicate());
    direct.flip();
    return direct;
  }

  private VectorizedPlainValuesReader makeReader(ByteBuffer buf) throws IOException {
    VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
    ByteBufferInputStream bbis = new ByteBufferInputStream(buf.duplicate());
    reader.initFromPage(0, bbis);
    return reader;
  }

  @Benchmark
  public void readIntegers() throws IOException {
    VectorizedPlainValuesReader reader = makeReader(int4Buffer);
    reader.readIntegers(count, intCol, 0);
  }

  @Benchmark
  public void readLongs() throws IOException {
    VectorizedPlainValuesReader reader = makeReader(int8Buffer);
    reader.readLongs(count, longCol, 0);
  }

  @Benchmark
  public void readFloats() throws IOException {
    VectorizedPlainValuesReader reader = makeReader(int4Buffer);
    reader.readFloats(count, floatCol, 0);
  }

  @Benchmark
  public void readDoubles() throws IOException {
    VectorizedPlainValuesReader reader = makeReader(int8Buffer);
    reader.readDoubles(count, doubleCol, 0);
  }

  @Benchmark
  public void readBooleans() throws IOException {
    VectorizedPlainValuesReader reader = makeReader(boolBuffer);
    reader.readBooleans(count, boolCol, 0);
  }

  @Benchmark
  public void readBytes() throws IOException {
    VectorizedPlainValuesReader reader = makeReader(int4Buffer);
    reader.readBytes(count, byteCol, 0);
  }

  @Benchmark
  public void readShorts() throws IOException {
    VectorizedPlainValuesReader reader = makeReader(int4Buffer);
    reader.readShorts(count, shortCol, 0);
  }

  @Benchmark
  public void readBinary() throws IOException {
    binaryCol.reset();
    VectorizedPlainValuesReader reader = makeReader(binaryBuffer);
    reader.readBinary(binaryCount, binaryCol, 0);
  }

  @Benchmark
  public void readUnsignedIntegers() throws IOException {
    VectorizedPlainValuesReader reader = makeReader(int4Buffer);
    reader.readUnsignedIntegers(count, longCol, 0);
  }

  @Benchmark
  public void readIntegersWithRebase() throws IOException {
    // Use int4Buffer, values are random so some may trigger rebase
    VectorizedPlainValuesReader reader = makeReader(int4Buffer);
    reader.readIntegersWithRebase(count, intCol, 0, false);
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(VectorizedPlainValuesReaderJMHBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}
