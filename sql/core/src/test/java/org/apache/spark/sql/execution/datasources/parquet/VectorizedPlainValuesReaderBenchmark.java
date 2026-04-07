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
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;

/**
 * JMH benchmark comparing
 * {@link VectorizedPlainValuesReader#readBinary(int, WritableColumnVector, int)}
 * (reusable scratch buffer) against
 * {@link OldVectorizedPlainValuesReader#readBinary(int, WritableColumnVector, int)}
 * (per-element allocation).
 *
 * <p>Each plain-encoded binary value is laid out as a 4-byte little-endian length
 * followed by that many bytes of payload.
 *
 * <p>Covers 4 buffer/column-vector combinations:
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
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 20, time = 1)
@Measurement(iterations = 50, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class VectorizedPlainValuesReaderBenchmark {

  @Param({"1024", "4096", "8192"})
  private int totalValues;

  @Param({"16", "64", "256"})
  private int valueLength;

  @Param({"HEAP", "DIRECT"})
  private String bufferType;

  @Param({"ON_HEAP", "OFF_HEAP"})
  private String columnVectorType;

  /** Plain-encoded binary payload: repeated [int length][bytes]. */
  private byte[] rawData;

  private VectorizedPlainValuesReader newReader;
  private OldVectorizedPlainValuesReader oldReader;
  private WritableColumnVector columnVector;

  @Setup(Level.Trial)
  public void setupTrial() {
    Random rng = new Random(42);
    rawData = new byte[totalValues * (4 + valueLength)];
    ByteBuffer tmp = ByteBuffer.wrap(rawData).order(ByteOrder.LITTLE_ENDIAN);
    byte[] payload = new byte[valueLength];
    for (int i = 0; i < totalValues; i++) {
      tmp.putInt(valueLength);
      rng.nextBytes(payload);
      tmp.put(payload);
    }

    newReader = new VectorizedPlainValuesReader();
    oldReader = new OldVectorizedPlainValuesReader();
  }

  @Setup(Level.Invocation)
  public void setupInvocation() throws IOException {
    // Recreate the column vector and reinitialize readers before each invocation
    // so that readBinary always starts from the beginning of the buffer.
    if (columnVector != null) {
      columnVector.close();
    }

    if ("ON_HEAP".equals(columnVectorType)) {
      columnVector = new OnHeapColumnVector(totalValues, DataTypes.BinaryType);
    } else {
      columnVector = new OffHeapColumnVector(totalValues, DataTypes.BinaryType);
    }

    newReader.initFromPage(totalValues, ByteBufferInputStream.wrap(makeBuffer()));
    oldReader.initFromPage(totalValues, ByteBufferInputStream.wrap(makeBuffer()));
  }

  private ByteBuffer makeBuffer() {
    if ("HEAP".equals(bufferType)) {
      // Heap buffer: hasArray() returns true
      return ByteBuffer.wrap(rawData.clone());
    }
    // Direct buffer: hasArray() returns false
    ByteBuffer buffer = ByteBuffer.allocateDirect(rawData.length);
    buffer.put(rawData);
    buffer.flip();
    return buffer;
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (columnVector != null) {
      columnVector.close();
      columnVector = null;
    }
  }

  @Benchmark
  public void newReadBinary() {
    newReader.readBinary(totalValues, columnVector, 0);
  }

  @Benchmark
  public void oldReadBinary() {
    oldReader.readBinary(totalValues, columnVector, 0);
  }

  public static void main(String[] args) throws Exception {
    // Forward CLI args (e.g. -p, -wi, -i, -f) so callers can override the
    // defaults specified by class-level annotations.
    Options opt = new OptionsBuilder()
        .parent(new CommandLineOptions(args))
        .include(VectorizedPlainValuesReaderBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}
