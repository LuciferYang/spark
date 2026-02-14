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
import org.apache.spark.sql.execution.datasources.parquet.VectorizedSingleBufferPlainValuesReader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;

/**
 * JMH Benchmark for comparing VectorizedPlainValuesReader and VectorizedSingleBufferPlainValuesReader.
 *
 * Test scenarios:
 * 1. VectorizedPlainValuesReader with SingleBufferInputStream
 * 2. VectorizedPlainValuesReader with MultiBufferInputStream
 * 3. VectorizedSingleBufferPlainValuesReader with SingleBufferInputStream (optimized)
 *
 * APIs tested (all public APIs except skip() which throws SparkUnsupportedOperationException):
 * - Batch read: readBooleans, readIntegers, readLongs, readFloats, readDoubles, readBytes, readShorts
 * - Batch skip: skipBooleans, skipIntegers, skipLongs, skipFloats, skipDoubles, skipBytes, skipShorts
 * - Unsigned: readUnsignedIntegers, readUnsignedLongs
 * - Rebase: readIntegersWithRebase, readLongsWithRebase
 * - Single value: readBoolean, readInteger, readLong, readByte, readShort, readFloat, readDouble
 * - Binary: readBinary (batch and single), skipBinary, skipFixedLenByteArray
 * - Full skip scenario: new Reader + initFromPage + skip all data
 *
 * To run:
 * {{{
 *   build/mvn test-compile -pl sql/core -DskipTests
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.VectorizedPlainValuesReaderJMHBenchmark"
 * }}}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class VectorizedPlainValuesReaderJMHBenchmark {

    // ==================== Parameters ====================

    @Param({"100000", "1000000"})
    private int numValues;

    @Param({"4096"})
    private int bufferSize;

    // ==================== Test Data ====================

    private byte[] intData;
    private byte[] longData;
    private byte[] floatData;
    private byte[] doubleData;
    private byte[] byteData;
    private byte[] shortData;
    private byte[] booleanData;
    private byte[] binaryData;
    private byte[] fixedLenData;

    private static final int FIXED_LEN = 16;
    private static final int BINARY_AVG_LEN = 32;

    private WritableColumnVector intColumn;
    private WritableColumnVector longColumn;
    private WritableColumnVector floatColumn;
    private WritableColumnVector doubleColumn;
    private WritableColumnVector byteColumn;
    private WritableColumnVector shortColumn;
    private WritableColumnVector booleanColumn;
    private WritableColumnVector binaryColumn;

    // ==================== Setup ====================

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);

        intData = generateIntData(numValues, random);
        longData = generateLongData(numValues, random);
        floatData = generateFloatData(numValues, random);
        doubleData = generateDoubleData(numValues, random);
        byteData = generateByteData(numValues, random);
        shortData = generateShortData(numValues, random);
        booleanData = generateBooleanData(numValues, random);
        binaryData = generateBinaryData(numValues, random, BINARY_AVG_LEN);
        fixedLenData = generateFixedLenData(numValues, FIXED_LEN, random);

        intColumn = new OnHeapColumnVector(numValues, DataTypes.IntegerType);
        longColumn = new OnHeapColumnVector(numValues, DataTypes.LongType);
        floatColumn = new OnHeapColumnVector(numValues, DataTypes.FloatType);
        doubleColumn = new OnHeapColumnVector(numValues, DataTypes.DoubleType);
        byteColumn = new OnHeapColumnVector(numValues, DataTypes.ByteType);
        shortColumn = new OnHeapColumnVector(numValues, DataTypes.ShortType);
        booleanColumn = new OnHeapColumnVector(numValues, DataTypes.BooleanType);
        binaryColumn = new OnHeapColumnVector(numValues, DataTypes.BinaryType);
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
        binaryColumn.close();
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
        ByteBuffer buffer = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putInt(random.nextInt(256));
        }
        return buffer.array();
    }

    private byte[] generateShortData(int count, Random random) {
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

    private byte[] generateBinaryData(int count, Random random, int avgLen) {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        ByteBuffer lenBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            int len = avgLen;
            lenBuffer.clear();
            lenBuffer.putInt(len);
            baos.write(lenBuffer.array(), 0, 4);
            byte[] data = new byte[len];
            random.nextBytes(data);
            baos.write(data, 0, len);
        }
        return baos.toByteArray();
    }

    private byte[] generateFixedLenData(int count, int len, Random random) {
        byte[] data = new byte[count * len];
        random.nextBytes(data);
        return data;
    }

    // ==================== ByteBufferInputStream Creation ====================

    private ByteBufferInputStream createSingleBufferInputStream(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        return ByteBufferInputStream.wrap(buffer);
    }

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

    // ====================================================================================
    // 1. readBooleans / skipBooleans
    // ====================================================================================

    @Benchmark
    public void readBooleans_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(booleanData));
        reader.readBooleans(numValues, booleanColumn, 0);
    }

    @Benchmark
    public void readBooleans_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
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
    public void skipBooleans_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(booleanData));
        reader.skipBooleans(numValues);
    }

    @Benchmark
    public void skipBooleans_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(booleanData, bufferSize));
        reader.skipBooleans(numValues);
    }

    // ====================================================================================
    // 2. readIntegers / skipIntegers
    // ====================================================================================

    @Benchmark
    public void readIntegers_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.readIntegers(numValues, intColumn, 0);
    }

    @Benchmark
    public void readIntegers_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
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
    public void skipIntegers_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.skipIntegers(numValues);
    }

    @Benchmark
    public void skipIntegers_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(intData, bufferSize));
        reader.skipIntegers(numValues);
    }

    // ====================================================================================
    // 3. readUnsignedIntegers
    // ====================================================================================

    @Benchmark
    public void readUnsignedIntegers_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.readUnsignedIntegers(numValues, longColumn, 0);
    }

    @Benchmark
    public void readUnsignedIntegers_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.readUnsignedIntegers(numValues, longColumn, 0);
    }

    @Benchmark
    public void readUnsignedIntegers_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(intData, bufferSize));
        reader.readUnsignedIntegers(numValues, longColumn, 0);
    }

    // ====================================================================================
    // 4. readIntegersWithRebase
    // ====================================================================================

    @Benchmark
    public void readIntegersWithRebase_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.readIntegersWithRebase(numValues, intColumn, 0, false);
    }

    @Benchmark
    public void readIntegersWithRebase_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.readIntegersWithRebase(numValues, intColumn, 0, false);
    }

    @Benchmark
    public void readIntegersWithRebase_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(intData, bufferSize));
        reader.readIntegersWithRebase(numValues, intColumn, 0, false);
    }

    // ====================================================================================
    // 5. readLongs / skipLongs
    // ====================================================================================

    @Benchmark
    public void readLongs_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.readLongs(numValues, longColumn, 0);
    }

    @Benchmark
    public void readLongs_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
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
    public void skipLongs_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.skipLongs(numValues);
    }

    @Benchmark
    public void skipLongs_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(longData, bufferSize));
        reader.skipLongs(numValues);
    }

    // ====================================================================================
    // 6. readUnsignedLongs
    // ====================================================================================

    @Benchmark
    public void readUnsignedLongs_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.readUnsignedLongs(numValues, binaryColumn, 0);
    }

    @Benchmark
    public void readUnsignedLongs_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.readUnsignedLongs(numValues, binaryColumn, 0);
    }

    @Benchmark
    public void readUnsignedLongs_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(longData, bufferSize));
        reader.readUnsignedLongs(numValues, binaryColumn, 0);
    }

    // ====================================================================================
    // 7. readLongsWithRebase
    // ====================================================================================

    @Benchmark
    public void readLongsWithRebase_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.readLongsWithRebase(numValues, longColumn, 0, false, "UTC");
    }

    @Benchmark
    public void readLongsWithRebase_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.readLongsWithRebase(numValues, longColumn, 0, false, "UTC");
    }

    @Benchmark
    public void readLongsWithRebase_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(longData, bufferSize));
        reader.readLongsWithRebase(numValues, longColumn, 0, false, "UTC");
    }

    // ====================================================================================
    // 8. readFloats / skipFloats
    // ====================================================================================

    @Benchmark
    public void readFloats_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(floatData));
        reader.readFloats(numValues, floatColumn, 0);
    }

    @Benchmark
    public void readFloats_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
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
    public void skipFloats_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(floatData));
        reader.skipFloats(numValues);
    }

    @Benchmark
    public void skipFloats_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(floatData, bufferSize));
        reader.skipFloats(numValues);
    }

    // ====================================================================================
    // 9. readDoubles / skipDoubles
    // ====================================================================================

    @Benchmark
    public void readDoubles_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(doubleData));
        reader.readDoubles(numValues, doubleColumn, 0);
    }

    @Benchmark
    public void readDoubles_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
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
    public void skipDoubles_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(doubleData));
        reader.skipDoubles(numValues);
    }

    @Benchmark
    public void skipDoubles_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(doubleData, bufferSize));
        reader.skipDoubles(numValues);
    }

    // ====================================================================================
    // 10. readBytes / skipBytes
    // ====================================================================================

    @Benchmark
    public void readBytes_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(byteData));
        reader.readBytes(numValues, byteColumn, 0);
    }

    @Benchmark
    public void readBytes_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
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
    public void skipBytes_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(byteData));
        reader.skipBytes(numValues);
    }

    @Benchmark
    public void skipBytes_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(byteData, bufferSize));
        reader.skipBytes(numValues);
    }

    // ====================================================================================
    // 11. readShorts / skipShorts
    // ====================================================================================

    @Benchmark
    public void readShorts_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(shortData));
        reader.readShorts(numValues, shortColumn, 0);
    }

    @Benchmark
    public void readShorts_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
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
    public void skipShorts_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(shortData));
        reader.skipShorts(numValues);
    }

    @Benchmark
    public void skipShorts_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(shortData, bufferSize));
        reader.skipShorts(numValues);
    }

    // ====================================================================================
    // 12. readBinary (batch) / skipBinary
    // ====================================================================================

    @Benchmark
    public void readBinaryBatch_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(binaryData));
        reader.readBinary(numValues, binaryColumn, 0);
    }

    @Benchmark
    public void readBinaryBatch_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(binaryData));
        reader.readBinary(numValues, binaryColumn, 0);
    }

    @Benchmark
    public void readBinaryBatch_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(binaryData, bufferSize));
        reader.readBinary(numValues, binaryColumn, 0);
    }

    @Benchmark
    public void skipBinary_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(binaryData));
        reader.skipBinary(numValues);
    }

    @Benchmark
    public void skipBinary_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(binaryData));
        reader.skipBinary(numValues);
    }

    @Benchmark
    public void skipBinary_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(binaryData, bufferSize));
        reader.skipBinary(numValues);
    }

    // ====================================================================================
    // 13. skipFixedLenByteArray
    // ====================================================================================

    @Benchmark
    public void skipFixedLenByteArray_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(fixedLenData));
        reader.skipFixedLenByteArray(numValues, FIXED_LEN);
    }

    @Benchmark
    public void skipFixedLenByteArray_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(fixedLenData));
        reader.skipFixedLenByteArray(numValues, FIXED_LEN);
    }

    @Benchmark
    public void skipFixedLenByteArray_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(fixedLenData, bufferSize));
        reader.skipFixedLenByteArray(numValues, FIXED_LEN);
    }

    // ====================================================================================
    // 14. Single value: readBoolean
    // ====================================================================================

    @Benchmark
    public void readBooleanSingle_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(booleanData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readBoolean();
        }
    }

    @Benchmark
    public void readBooleanSingle_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(booleanData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readBoolean();
        }
    }

    // ====================================================================================
    // 15. Single value: readInteger
    // ====================================================================================

    @Benchmark
    public void readIntegerSingle_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readInteger();
        }
    }

    @Benchmark
    public void readIntegerSingle_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readInteger();
        }
    }

    // ====================================================================================
    // 16. Single value: readLong
    // ====================================================================================

    @Benchmark
    public void readLongSingle_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readLong();
        }
    }

    @Benchmark
    public void readLongSingle_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readLong();
        }
    }

    // ====================================================================================
    // 17. Single value: readByte
    // ====================================================================================

    @Benchmark
    public void readByteSingle_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readByte();
        }
    }

    @Benchmark
    public void readByteSingle_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readByte();
        }
    }

    // ====================================================================================
    // 18. Single value: readShort
    // ====================================================================================

    @Benchmark
    public void readShortSingle_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readShort();
        }
    }

    @Benchmark
    public void readShortSingle_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readShort();
        }
    }

    // ====================================================================================
    // 19. Single value: readFloat
    // ====================================================================================

    @Benchmark
    public void readFloatSingle_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(floatData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readFloat();
        }
    }

    @Benchmark
    public void readFloatSingle_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(floatData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readFloat();
        }
    }

    // ====================================================================================
    // 20. Single value: readDouble
    // ====================================================================================

    @Benchmark
    public void readDoubleSingle_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(doubleData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readDouble();
        }
    }

    @Benchmark
    public void readDoubleSingle_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(doubleData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readDouble();
        }
    }

    // ====================================================================================
    // 21. Single value: readBinary(int len)
    // ====================================================================================

    @Benchmark
    public void readBinarySingle_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(fixedLenData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readBinary(FIXED_LEN);
        }
    }

    @Benchmark
    public void readBinarySingle_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(fixedLenData));
        int count = Math.min(numValues, 10000);
        for (int i = 0; i < count; i++) {
            reader.readBinary(FIXED_LEN);
        }
    }

    // ====================================================================================
    // 22. Full skip scenario: new Reader + initFromPage + skip all Integers
    // ====================================================================================

    @Benchmark
    public void initAndSkipAllIntegers_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.skipIntegers(numValues);
    }

    @Benchmark
    public void initAndSkipAllIntegers_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(intData));
        reader.skipIntegers(numValues);
    }

    @Benchmark
    public void initAndSkipAllIntegers_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(intData, bufferSize));
        reader.skipIntegers(numValues);
    }

    // ====================================================================================
    // 23. Full skip scenario: new Reader + initFromPage + skip all Longs
    // ====================================================================================

    @Benchmark
    public void initAndSkipAllLongs_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.skipLongs(numValues);
    }

    @Benchmark
    public void initAndSkipAllLongs_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(longData));
        reader.skipLongs(numValues);
    }

    @Benchmark
    public void initAndSkipAllLongs_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(longData, bufferSize));
        reader.skipLongs(numValues);
    }

    // ====================================================================================
    // 24. Full skip scenario: new Reader + initFromPage + skip all Doubles
    // ====================================================================================

    @Benchmark
    public void initAndSkipAllDoubles_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(doubleData));
        reader.skipDoubles(numValues);
    }

    @Benchmark
    public void initAndSkipAllDoubles_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(doubleData));
        reader.skipDoubles(numValues);
    }

    @Benchmark
    public void initAndSkipAllDoubles_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(doubleData, bufferSize));
        reader.skipDoubles(numValues);
    }

    // ====================================================================================
    // 25. Full skip scenario: new Reader + initFromPage + skip all Binary
    // ====================================================================================

    @Benchmark
    public void initAndSkipAllBinary_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(binaryData));
        reader.skipBinary(numValues);
    }

    @Benchmark
    public void initAndSkipAllBinary_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(binaryData));
        reader.skipBinary(numValues);
    }

    @Benchmark
    public void initAndSkipAllBinary_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(binaryData, bufferSize));
        reader.skipBinary(numValues);
    }

    // ====================================================================================
    // 26. Full skip scenario: new Reader + initFromPage + skip all FixedLenByteArray
    // ====================================================================================

    @Benchmark
    public void initAndSkipAllFixedLen_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(fixedLenData));
        reader.skipFixedLenByteArray(numValues, FIXED_LEN);
    }

    @Benchmark
    public void initAndSkipAllFixedLen_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(fixedLenData));
        reader.skipFixedLenByteArray(numValues, FIXED_LEN);
    }

    @Benchmark
    public void initAndSkipAllFixedLen_MultiBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createMultiBufferInputStream(fixedLenData, bufferSize));
        reader.skipFixedLenByteArray(numValues, FIXED_LEN);
    }

    // ====================================================================================
    // 27. Full skip scenario: new Reader + initFromPage + skip all Booleans
    // ====================================================================================

    @Benchmark
    public void initAndSkipAllBooleans_SingleBuffer() throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(booleanData));
        reader.skipBooleans(numValues);
    }

    @Benchmark
    public void initAndSkipAllBooleans_SingleBuffer_Optimized() throws IOException {
        VectorizedSingleBufferPlainValuesReader reader = new VectorizedSingleBufferPlainValuesReader();
        reader.initFromPage(numValues, createSingleBufferInputStream(booleanData));
        reader.skipBooleans(numValues);
    }

    @Benchmark
    public void initAndSkipAllBooleans_MultiBuffer() throws IOException {
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

