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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class VectorizedRleValuesReaderBenchmark {

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(VectorizedRleValuesReaderBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }

private static final int NUM_VALUES = 1024 * 1024; // 1M values

  @Param({"PACKED", "RLE"})
  public String encodingMode;
  @Param({"0.0", "0.1", "0.5"})
  public double nullDensity;

  private ByteBufferInputStream inputStream;
  private VectorizedRleValuesReader reader;
  private ParquetReadState state;
  private WritableColumnVector valuesVector;
  private WritableColumnVector nullsVector;
  private ParquetVectorUpdater updater;
  
  // Data to reset
  private byte[] encodedBytes;

  private VectorizedValuesReader valueReader;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    // 1. Generate Definition Levels (0 or 1 for simplicity)
    // maxDefinitionLevel = 1.
    // 0 = null, 1 = non-null.
    
    Random random = new Random(42);
    // Use DirectByteBufferAllocator to simulate real usage
    RunLengthBitPackingHybridEncoder encoder = 
        new RunLengthBitPackingHybridEncoder(1, NUM_VALUES, NUM_VALUES, new DirectByteBufferAllocator());

    for (int i = 0; i < NUM_VALUES; i++) {
      int defLevel;
      if (encodingMode.equals("PACKED")) {
        // Random 0 or 1 based on nullDensity
        defLevel = (random.nextDouble() > nullDensity) ? 1 : 0;
      } else {
        // RLE: Runs of 1s or 0s
        // Switch every 1000 values
        boolean isNullRun = (i / 1000) % 2 == 0;
        if (nullDensity == 0.0) isNullRun = false;
        defLevel = isNullRun ? 0 : 1;
      }
      encoder.writeInt(defLevel);
    }
    
    byte[] data = encoder.toBytes().toByteArray();
    
    // Prepend length (4 bytes little endian)
    encodedBytes = new byte[4 + data.length];
    int len = data.length;
    encodedBytes[0] = (byte) (len & 0xff);
    encodedBytes[1] = (byte) ((len >> 8) & 0xff);
    encodedBytes[2] = (byte) ((len >> 16) & 0xff);
    encodedBytes[3] = (byte) ((len >> 24) & 0xff);
    System.arraycopy(data, 0, encodedBytes, 4, data.length);
    

    // 2. Prepare Vectors
    valuesVector = new OnHeapColumnVector(NUM_VALUES, DataTypes.IntegerType);
    nullsVector = new OnHeapColumnVector(NUM_VALUES, DataTypes.IntegerType);

    // 3. Prepare State
    // maxDefinitionLevel = 1
    state = new ParquetReadState(new ColumnDescriptor(new String[]{"col"}, Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("col").asPrimitiveType(), 0, 1), true, null);
    
    // 4. Prepare Updater
    updater = new ParquetVectorUpdaterFactory.IntegerUpdater();
    
    // 5. Prepare Dummy Data Reader
     valueReader = new VectorizedValuesReader() {
       @Override public int readInteger() { return 42; }
       @Override public long readLong() { return 42L; }
       @Override public byte readByte() { return 42; }
       @Override public float readFloat() { return 42.0f; }
       @Override public double readDouble() { return 42.0d; }
       @Override public boolean readBoolean() { return true; }
       @Override public Binary readBinary(int len) { return Binary.fromConstantByteArray(new byte[0]); }
       @Override public void skipIntegers(int total) {}
       @Override public void skipLongs(int total) {}
       @Override public void skipBytes(int total) {}
       @Override public void skipFloats(int total) {}
       @Override public void skipDoubles(int total) {}
       @Override public void skipBooleans(int total) {}
       @Override public void skipBinary(int total) {}
       @Override public void skipFixedLenByteArray(int total, int len) {}
       @Override public void readIntegers(int total, WritableColumnVector c, int rowId) {}
       @Override public void readLongs(int total, WritableColumnVector c, int rowId) {}
       @Override public void readBytes(int total, WritableColumnVector c, int rowId) {}
       @Override public void readFloats(int total, WritableColumnVector c, int rowId) {}
       @Override public void readDoubles(int total, WritableColumnVector c, int rowId) {}
       @Override public void readBooleans(int total, WritableColumnVector c, int rowId) {}
       @Override public void readBinary(int total, WritableColumnVector c, int rowId) {}
       @Override public void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {}
       @Override public void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {}
       @Override public void readIntegersWithRebase(int total, WritableColumnVector c, int rowId, boolean failIfRebase) {}
       @Override public void readLongsWithRebase(int total, WritableColumnVector c, int rowId, boolean failIfRebase, String timeZone) {}
       @Override public void readShorts(int total, WritableColumnVector c, int rowId) {}
       @Override public void skipShorts(int total) {}
       @Override public short readShort() { return 0; }
       @Override public void readGeometry(int total, WritableColumnVector c, int rowId) {}
       @Override public void readGeography(int total, WritableColumnVector c, int rowId) {}
     };
  }

  @Setup(Level.Invocation)
  public void prepareInvocation() throws IOException {
    // Reset Reader
    ByteBuffer buffer = ByteBuffer.wrap(encodedBytes);
    inputStream = ByteBufferInputStream.wrap(buffer);
    reader = new VectorizedRleValuesReader(1); // bitWidth = 1 for def levels
    reader.initFromPage(NUM_VALUES, inputStream);
    
    // Reset State
    state.resetForNewPage(NUM_VALUES, 0);
    state.resetForNewBatch(NUM_VALUES); 
    state.valueOffset = 0;
    
    valuesVector.reset();
    nullsVector.reset();
  }

  @Benchmark
  public void readBatch() {
     // readBatch(state, values, defLevels, valueReader, updater)
     // If defLevels is null, we are reading required column or similar
     // But we set maxDefLevel=1, so we are reading OPTIONAL column?
     // If optional, we should pass defLevels vector.
     // But readBatchInternal is called when defLevels IS NULL.
     // This happens when?
     // When reading dictionary IDs, we use `readIntegers`.
     // When reading Definition Levels for simple optional columns, we might use `readBatch`?
     
     // Let's assume we are reading definition levels and populating nulls in `valuesVector` (which acts as nulls provider in some contexts)
     // Or we pass `nullsVector`.
     
     // The signature: readBatch(state, values, defLevels, valueReader, updater)
     // If defLevels is null: calls readBatchInternal(..., values, values, ...)
     
     // We will use that path for now.
     reader.readBatch(state, valuesVector, null, valueReader, updater);
  }
}
