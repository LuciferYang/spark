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
package org.apache.spark.sql.execution.vectorized;

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


import org.apache.spark.sql.types.DataTypes;


/**
 * JMH Benchmark for comparing VectorizedPlainValuesReader readUnsignedLongs performance.
 *
 * UINT64 in Parquet is mapped to DecimalType(20, 0) in Spark.
 * The column vector must be created with DecimalType(20, 0), not LongType,
 * because readUnsignedLongs writes BigInteger bytes into childColumns via arrayData().
 *
 * To run:
 * {{{
 *   build/mvn test-compile -pl sql/core -DskipTests
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.OnHeapColumnVectorJMHBenchmark"
 * }}}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class OnHeapColumnVectorJMHBenchmark {


    // ==================== Parameters ====================


    @Param({"10000000"})
    private int numValues;


    // ==================== Test Data ====================


    private byte[] longData;
    private static final int BATCH_SIZE = 4096;


    private OldVectorizedPlainValuesReader oldSingleBufferOnHeapReader;
    private OldVectorizedPlainValuesReader oldSingleBufferOffHeapReader;
    private VectorizedPlainValuesReader newSingleBufferOnHeapReader;
    private VectorizedPlainValuesReader newSingleBufferOffHeapReader;


    // ==================== State Classes ====================


    /**
     * Column vector state using DecimalType(20, 0), which is the correct type for UINT64.
     * Parquet UINT_64 logical type is mapped to DecimalType(20, 0) in Spark.
     * Using LongType would cause NullPointerException because readUnsignedLongs
     * calls arrayData() which requires childColumns, only initialized for DecimalType.
     */
    @State(Scope.Thread)
    public static class DecimalColumnVectorState {
        public WritableColumnVector decimalColumn;


        @Setup(Level.Iteration)
        public void setup() {
            // UINT64 -> DecimalType(20, 0): precision=20, scale=0
            decimalColumn = new OnHeapColumnVector(BATCH_SIZE, DataTypes.createDecimalType(20, 0));
        }


        @TearDown(Level.Iteration)
        public void tearDown() {
            decimalColumn.close();
        }


        @Setup(Level.Invocation)
        public void reset() {
            decimalColumn.reset();
        }
    }


    // ==================== Setup ====================


    @Setup(Level.Trial)
    public void setupTrial() {
        Random random = new Random(42);
        longData = generateLongData(numValues, random);
    }


    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        oldSingleBufferOnHeapReader = new OldVectorizedPlainValuesReader();
        oldSingleBufferOnHeapReader.initFromPage(numValues, createSingleBufferInputStream(longData));
        oldSingleBufferOffHeapReader = new OldVectorizedPlainValuesReader();
        oldSingleBufferOffHeapReader.initFromPage(numValues, createDirectSingleBufferInputStream(longData));
        newSingleBufferOnHeapReader = new VectorizedPlainValuesReader();
        newSingleBufferOnHeapReader.initFromPage(numValues, createSingleBufferInputStream(longData));
        newSingleBufferOffHeapReader = new VectorizedPlainValuesReader();
        newSingleBufferOffHeapReader.initFromPage(numValues, createDirectSingleBufferInputStream(longData));
    }


    // ==================== Data Generation ====================


    private byte[] generateLongData(int count, Random random) {
        ByteBuffer buffer = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buffer.putLong(random.nextLong()); // full unsigned long range
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
    // readUnsignedLongs onHeap
    // ====================================================================================


    @Benchmark
    public void readUnsignedLongs_onHeap_Old(DecimalColumnVectorState state) throws IOException {
        for (int i = 0; i < numValues; i += BATCH_SIZE) {
            oldSingleBufferOnHeapReader.readUnsignedLongs(
                    Math.min(BATCH_SIZE, numValues - i), state.decimalColumn, 0);
        }
    }


    @Benchmark
    public void readUnsignedLongs_onHeap_New(DecimalColumnVectorState state) throws IOException {
        for (int i = 0; i < numValues; i += BATCH_SIZE) {
            newSingleBufferOnHeapReader.readUnsignedLongs(
                    Math.min(BATCH_SIZE, numValues - i), state.decimalColumn, 0);
        }
    }

    // ====================================================================================
    // readUnsignedLongs offHeap
    // ====================================================================================


    @Benchmark
    public void readUnsignedLongs_offHeap_Old(DecimalColumnVectorState state) throws IOException {
        for (int i = 0; i < numValues; i += BATCH_SIZE) {
            oldSingleBufferOffHeapReader.readUnsignedLongs(
                    Math.min(BATCH_SIZE, numValues - i), state.decimalColumn, 0);
        }
    }


    @Benchmark
    public void readUnsignedLongs_offHeap_New(DecimalColumnVectorState state) throws IOException {
        for (int i = 0; i < numValues; i += BATCH_SIZE) {
            newSingleBufferOffHeapReader.readUnsignedLongs(
                    Math.min(BATCH_SIZE, numValues - i), state.decimalColumn, 0);
        }
    }

    // ==================== Main Method ====================


    public static void main(String[] args) throws RunnerException {
        String filter = args.length > 0 ?
                args[0] : OnHeapColumnVectorJMHBenchmark.class.getSimpleName();
        Options opt = new OptionsBuilder()
                .include(filter)
                .build();


        new Runner(opt).run();
    }
}