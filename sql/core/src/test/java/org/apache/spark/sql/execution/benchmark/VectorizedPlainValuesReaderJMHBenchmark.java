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
package org.apache.spark.sql.execution.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.parquet.bytes.ByteBufferInputStream;

import org.apache.spark.sql.execution.datasources.parquet.VectorizedPlainValuesReader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;

// scalastyle:off
/**
 * JMH Benchmark for VectorizedPlainValuesReader comparing:
 * 1. SingleBufferInputStream vs MultiBufferInputStream input
 * 2. Read operations vs Skip operations
 *
 * This benchmark tests all public APIs of VectorizedPlainValuesReader.
 *
 * To run this benchmark:
 * {{{
 *   # Using Maven
 *   cd sql/core
 *   mvn test-compile
 *   java -cp target/test-classes:target/classes:$(mvn dependency:build-classpath -q -DincludeScope=test -Dmdep.outputFile=/dev/stdout) \
 *     org.apache.spark.sql.execution.benchmark.VectorizedPlainValuesReaderJMHBenchmark
 *
 *   # Or using SBT (simpler)
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.VectorizedPlainValuesReaderJMHBenchmark"
 * }}}
 */
// scalastyle:on
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class VectorizedPlainValuesReaderJMHBenchmark {

    // ==================== Parameters ====================

    @Param({"10000", "100000", "1000000"})
    private int numValues;

    @Param({"4096"})  // Buffer size for MultiBufferInputStream
    private int bufferSize;

    // ==================== Test Data ====================

    // Data for different types
    private byte[] intData;
    private byte[] longData;
    private byte[] floatData;
    private byte[] doubleData;
    private byte[] byteData;   // stored as 4-byte int
    private byte[] shortData;  // stored as 4-byte int
    private byte[] booleanData;

    // Column vectors for output
    private WritableColumnVector intColumn;
    private WritableColumnVector longColumn;
    private WritableColumnVector floatColumn;
    private WritableColumnVector doubleColumn;
    private WritableColumnVector byteColumn;
    private WritableColumnVector shortColumn;
    private WritableColumnVector booleanColumn;

    // ==================== Setup ====================

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);

        // Generate test data
        intData = generateIntData(numValues, random);
        longData = generateLongData(numValues, random);
        floatData = generateFloatData(numValues, random);
        doubleData = generateDoubleData(numValues, random);
        byteData = generateByteData(numValues, random);    // 4 bytes per value
        shortData = generateShortData(numValues, random);  // 4 bytes per value
        booleanData = generateBooleanData(numValues, random);

        // Allocate column vectors
        intColumn = new OnHeapColumnVector(numValues, DataTypes.IntegerType);
        longColumn = new OnHeapColumnVector(numValues, DataTypes.LongType);
        floatColumn = new OnHeapColumnVector(numValues, DataTypes.FloatType);
        doubleColumn = new OnHeapColumnVector(numValues, DataTypes.DoubleType);
        byteColumn = new OnHeapColumnVector(numValues, DataTypes.ByteType);
        shortColumn = new OnHeapColumnVector(numValues, DataTypes.ShortType);
        booleanColumn = new OnHeapColumnVector(numValues, DataTypes.BooleanType);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        intColumn.close();
        longColumn.close();
        floatColumn.close();
        doubleColumn.close();
        byteColumn.close();
        shortColumn.close();
        booleanColumn.close();
    }

    // ==================== Data Generation ====================

    private byte[] generateIntData(int count, Random random) {
        ByteBuffer buffer = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putInt(random.nextInt());
        }
        return buffer.array();
    }

    private byte[] generateLongData(int count, Random random) {
        ByteBuffer buffer = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putLong(random.nextLong());
        }
        return buffer.array();
    }

    private byte[] generateFloatData(int count, Random random) {
        ByteBuffer buffer = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putFloat(random.nextFloat());
        }
        return buffer.array();
    }

    private byte[] generateDoubleData(int count, Random random) {
        ByteBuffer buffer = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putDouble(random.nextDouble());
        }
        return buffer.array();
    }

    private byte[] generateByteData(int count, Random random) {
        // Bytes are stored as 4-byte little endian int
        ByteBuffer buffer = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putInt(random.nextInt(256));
        }
        return buffer.array();
    }

    private byte[] generateShortData(int count, Random random) {
        // Shorts are stored as 4-byte little endian int
        ByteBuffer buffer = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putInt(random.nextInt(65536) - 32768);
        }
        return buffer.array();
    }

    private byte[] generateBooleanData(int count, Random random) {
        int numBytes = (count + 7) / 8;
        byte[] data = new byte[numBytes];
        random.nextBytes(data);
        return data;
    }

    // ==================== ByteBufferInputStream Creation ====================

    /**
     * Create a SingleBufferInputStream from data.
     */
    private ByteBufferInputStream createSingleBufferInputStream(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        return ByteBufferInputStream.wrap(buffer);
    }

    /**
     * Create a MultiBufferInputStream by splitting data into multiple small buffers.
     */
    private ByteBufferInputStream createMultiBufferInputStream(byte[] data, int chunkSize) {
        List<ByteBuffer> buffers = new ArrayList<>();
        int offset = 0;
        while (offset < data.length) {
            int length = Math.min(chunkSize, data.length - offset);
            ByteBuffer chunk = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
            chunk.put(data, offset, length);
            chunk.flip();
            buffers.add(chunk);
            offset += length;
        }
        return ByteBufferInputStream.wrap(buffers);
    }

    // ==================== Integer Benchmarks ====================

    @Benchmark
    public void readIntegers_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.readIntegers(numValues, intColumn, 0);
    }

    @Benchmark
    public void readIntegers_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(intData, bufferSize));
        reader.readIntegers(numValues, intColumn, 0);
    }

    @Benchmark
    public void skipIntegers_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.skipIntegers(numValues);
    }

    @Benchmark
    public void skipIntegers_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(intData, bufferSize));
        reader.skipIntegers(numValues);
    }

    // ==================== Long Benchmarks ====================

    @Benchmark
    public void readLongs_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.readLongs(numValues, longColumn, 0);
    }

    @Benchmark
    public void readLongs_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(longData, bufferSize));
        reader.readLongs(numValues, longColumn, 0);
    }

    @Benchmark
    public void skipLongs_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.skipLongs(numValues);
    }

    @Benchmark
    public void skipLongs_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(longData, bufferSize));
        reader.skipLongs(numValues);
    }

    // ==================== Float Benchmarks ====================

    @Benchmark
    public void readFloats_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(floatData));
        reader.readFloats(numValues, floatColumn, 0);
    }

    @Benchmark
    public void readFloats_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(floatData, bufferSize));
        reader.readFloats(numValues, floatColumn, 0);
    }

    @Benchmark
    public void skipFloats_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(floatData));
        reader.skipFloats(numValues);
    }

    @Benchmark
    public void skipFloats_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(floatData, bufferSize));
        reader.skipFloats(numValues);
    }

    // ==================== Double Benchmarks ====================

    @Benchmark
    public void readDoubles_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(doubleData));
        reader.readDoubles(numValues, doubleColumn, 0);
    }

    @Benchmark
    public void readDoubles_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(doubleData, bufferSize));
        reader.readDoubles(numValues, doubleColumn, 0);
    }

    @Benchmark
    public void skipDoubles_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(doubleData));
        reader.skipDoubles(numValues);
    }

    @Benchmark
    public void skipDoubles_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(doubleData, bufferSize));
        reader.skipDoubles(numValues);
    }

    // ==================== Byte Benchmarks ====================

    @Benchmark
    public void readBytes_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(byteData));
        reader.readBytes(numValues, byteColumn, 0);
    }

    @Benchmark
    public void readBytes_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(byteData, bufferSize));
        reader.readBytes(numValues, byteColumn, 0);
    }

    @Benchmark
    public void skipBytes_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(byteData));
        reader.skipBytes(numValues);
    }

    @Benchmark
    public void skipBytes_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(byteData, bufferSize));
        reader.skipBytes(numValues);
    }

    // ==================== Short Benchmarks ====================

    @Benchmark
    public void readShorts_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(shortData));
        reader.readShorts(numValues, shortColumn, 0);
    }

    @Benchmark
    public void readShorts_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(shortData, bufferSize));
        reader.readShorts(numValues, shortColumn, 0);
    }

    @Benchmark
    public void skipShorts_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(shortData));
        reader.skipShorts(numValues);
    }

    @Benchmark
    public void skipShorts_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(shortData, bufferSize));
        reader.skipShorts(numValues);
    }

    // ==================== Boolean Benchmarks ====================

    @Benchmark
    public void readBooleans_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(booleanData));
        reader.readBooleans(numValues, booleanColumn, 0);
    }

    @Benchmark
    public void readBooleans_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(booleanData, bufferSize));
        reader.readBooleans(numValues, booleanColumn, 0);
    }

    @Benchmark
    public void skipBooleans_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(booleanData));
        reader.skipBooleans(numValues);
    }

    @Benchmark
    public void skipBooleans_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(booleanData, bufferSize));
        reader.skipBooleans(numValues);
    }

    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VectorizedPlainValuesReaderJMHBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

