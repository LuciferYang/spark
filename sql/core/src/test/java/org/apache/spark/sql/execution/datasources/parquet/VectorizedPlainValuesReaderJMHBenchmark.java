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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.parquet.bytes.ByteBufferInputStream;

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
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class VectorizedPlainValuesReaderJMHBenchmark {

    // ==================== Parameters ====================

    @Param({"10000000"})
    private int numValues;

    // ==================== Test Data ====================

    private byte[] intData;
    private static final int BATCH_SIZE = 4096;

    private OldVectorizedPlainValuesReader oldSingleBufferOnHeapReader;
    private OldVectorizedPlainValuesReader oldSingleBufferOffHeapReader;

    @State(Scope.Thread)
    public static class OnHeapColumnVectorState {
        public WritableColumnVector intColumn;
        @Setup(Level.Iteration)
        public void setup() {
            intColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
        }
        @TearDown(Level.Iteration)
        public void tearDown() {
            intColumn.close();
        }
        @Setup(Level.Invocation)
        public void reset() {
            intColumn.reset();
        }
    }

    // ==================== Setup ====================

    @Setup(Level.Trial)
    public void setupTrial() {
        Random random = new Random(42);
        intData = generateIntData(numValues, random);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        oldSingleBufferOnHeapReader = new OldVectorizedPlainValuesReader();
        oldSingleBufferOnHeapReader.initFromPage(numValues, createSingleBufferInputStream(intData));
        oldSingleBufferOffHeapReader = new OldVectorizedPlainValuesReader();
        oldSingleBufferOffHeapReader.initFromPage(numValues, createDirectSingleBufferInputStream(intData));
    }

    // ==================== Data Generation ====================
    private byte[] generateIntData(int count, Random random) {
        ByteBuffer buffer = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putInt(random.nextInt()); // full unsigned int range
        }
        return buffer.array();
    }

    // ==================== ByteBufferInputStream Creation ====================

    private ByteBufferInputStream createSingleBufferInputStream(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        return ByteBufferInputStream.wrap(buffer);
    }

    private ByteBuffer createDirectBuffer(byte[] data) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(data.length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    private ByteBufferInputStream createDirectSingleBufferInputStream(byte[] data) {
        ByteBuffer buffer = createDirectBuffer(data);
        return ByteBufferInputStream.wrap(buffer);
    }

    // ====================================================================================
    // readUnsignedIntegers OnHeap
    // ====================================================================================

    @Benchmark
    public void readUnsignedIntegers_onHeap_Old(OnHeapColumnVectorState state) throws IOException {
        for (int i = 0; i < numValues; i += BATCH_SIZE) {
            oldSingleBufferOnHeapReader.readUnsignedIntegers(Math.min(BATCH_SIZE, numValues - i), state.intColumn, 0);
        }
    }

    // ====================================================================================
    // readUnsignedIntegers offHeap
    // ====================================================================================

    @Benchmark
    public void readUnsignedIntegers_offHeap_Old(OnHeapColumnVectorState state) throws IOException {
        for (int i = 0; i < numValues; i += BATCH_SIZE) {
            oldSingleBufferOffHeapReader.readUnsignedIntegers(Math.min(BATCH_SIZE, numValues - i), state.intColumn, 0);
        }
    }

    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        String filter = args.length > 0 ? args[0] : VectorizedPlainValuesReaderJMHBenchmark.class.getSimpleName();
        Options opt = new OptionsBuilder()
                .include(filter)
                .build();

        new Runner(opt).run();
    }
}