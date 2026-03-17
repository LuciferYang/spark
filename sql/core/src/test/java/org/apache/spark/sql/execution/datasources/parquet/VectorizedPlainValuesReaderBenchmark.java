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

import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;

/**
 * JMH benchmark comparing readUnsignedIntegers performance between
 * {@link VectorizedPlainValuesReader} (optimized batch) and
 * {@link OldVectorizedPlainValuesReader} (element-by-element).
 *
 * <p>Covers 4 scenarios:
 * <ul>
 *   <li>HeapBuffer + OnHeapColumnVector</li>
 *   <li>HeapBuffer + OffHeapColumnVector</li>
 *   <li>DirectBuffer + OnHeapColumnVector</li>
 *   <li>DirectBuffer + OffHeapColumnVector</li>
 * </ul>
 *
 * <p>Run from command line:
 * <pre>
 *   java -cp ... org.apache.spark.sql.execution.datasources.parquet
 *     .VectorizedPlainValuesReaderBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class VectorizedPlainValuesReaderBenchmark {

  @Param({"1024", "4096", "8192"})
  private int totalValues;

  @Param({"HEAP", "DIRECT"})
  private String bufferType;

  @Param({"ON_HEAP", "OFF_HEAP"})
  private String columnVectorType;

  /** Raw bytes for the unsigned int data (4 bytes per value, little-endian). */
  private byte[] rawData;

  private VectorizedPlainValuesReader newReader;
  private OldVectorizedPlainValuesReader oldReader;
  private WritableColumnVector columnVector;

  @Setup(Level.Trial)
  public void setupTrial() {
    Random rng = new Random(42);
    rawData = new byte[totalValues * 4];
    // Fill with random unsigned int data in little-endian format.
    ByteBuffer tmp = ByteBuffer.wrap(rawData).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < totalValues; i++) {
      tmp.putInt(rng.nextInt());
    }

    newReader = new VectorizedPlainValuesReader();
    oldReader = new OldVectorizedPlainValuesReader();
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    // Recreate the column vector and reinitialize readers before each invocation
    // so that readUnsignedIntegers always starts from the beginning of the buffer.
    if (columnVector != null) {
      columnVector.close();
    }

    // readUnsignedIntegers writes longs into the column vector (UINT_32 -> LongType)
    if ("ON_HEAP".equals(columnVectorType)) {
      columnVector = new OnHeapColumnVector(totalValues, DataTypes.LongType);
    } else {
      columnVector = new OffHeapColumnVector(totalValues, DataTypes.LongType);
    }

    ByteBuffer buffer;
    if ("HEAP".equals(bufferType)) {
      // Heap buffer: hasArray() returns true
      buffer = ByteBuffer.wrap(rawData.clone());
    } else {
      // Direct buffer: hasArray() returns false
      buffer = ByteBuffer.allocateDirect(rawData.length);
      buffer.put(rawData);
      buffer.flip();
    }

    newReader.initFromPage(totalValues, ByteBufferInputStream.wrap(buffer));

    // Create a separate stream for the old reader
    ByteBuffer buffer2;
    if ("HEAP".equals(bufferType)) {
      buffer2 = ByteBuffer.wrap(rawData.clone());
    } else {
      buffer2 = ByteBuffer.allocateDirect(rawData.length);
      buffer2.put(rawData);
      buffer2.flip();
    }

    oldReader.initFromPage(totalValues, ByteBufferInputStream.wrap(buffer2));
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (columnVector != null) {
      columnVector.close();
      columnVector = null;
    }
  }

  @Benchmark
  public void newReadUnsignedIntegers() {
    newReader.readUnsignedIntegers(totalValues, columnVector, 0);
  }

  @Benchmark
  public void oldReadUnsignedIntegers() {
    oldReader.readUnsignedIntegers(totalValues, columnVector, 0);
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(VectorizedPlainValuesReaderBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}
