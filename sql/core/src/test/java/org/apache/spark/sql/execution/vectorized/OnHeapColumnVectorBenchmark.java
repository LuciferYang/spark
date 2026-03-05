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

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.spark.sql.types.DataTypes;

/**
 * JMH Benchmark for comparing batch fill operations between
 * OldOnHeapColumnVector (scalar for-loop) and OnHeapColumnVector (Arrays.fill intrinsic).
 *
 * <p>Optimized methods under test:
 * <ul>
 *   <li>{@code putNulls(rowId, count)}       — byte[] nulls fill</li>
 *   <li>{@code putNotNulls(rowId, count)}     — byte[] nulls clear</li>
 *   <li>{@code putBooleans(rowId, count, v)}  — byte[] byteData fill</li>
 *   <li>{@code putBytes(rowId, count, v)}     — byte[] byteData fill</li>
 *   <li>{@code putShorts(rowId, count, v)}    — short[] shortData fill</li>
 *   <li>{@code putInts(rowId, count, v)}      — int[] intData fill</li>
 *   <li>{@code putLongs(rowId, count, v)}     — long[] longData fill</li>
 * </ul>
 *
 * <p>To run:
 * <pre>{@code
 *   build/mvn test-compile -pl sql/core -DskipTests
 *   java -cp "$(build/mvn -pl sql/core dependency:build-classpath -DincludeScope=test \
 *     -Dmdep.outputFile=/dev/stdout -q):sql/core/target/test-classes:sql/core/target/classes" \
 *     org.apache.spark.sql.execution.vectorized.OnHeapColumnVectorBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class OnHeapColumnVectorBenchmark {

    // ==================== Shared base state ====================

    @State(Scope.Thread)
    public static class BaseState {
        @Param({"4096", "8192", "16384"})
        int batchSize;

        OldOnHeapColumnVector oldBooleanVector;
        OldOnHeapColumnVector oldByteVector;
        OldOnHeapColumnVector oldShortVector;
        OldOnHeapColumnVector oldIntVector;
        OldOnHeapColumnVector oldLongVector;

        OnHeapColumnVector newBooleanVector;
        OnHeapColumnVector newByteVector;
        OnHeapColumnVector newShortVector;
        OnHeapColumnVector newIntVector;
        OnHeapColumnVector newLongVector;

        @Setup(Level.Trial)
        public void setup() {
            oldBooleanVector = new OldOnHeapColumnVector(batchSize, DataTypes.BooleanType);
            oldByteVector    = new OldOnHeapColumnVector(batchSize, DataTypes.ByteType);
            oldShortVector   = new OldOnHeapColumnVector(batchSize, DataTypes.ShortType);
            oldIntVector     = new OldOnHeapColumnVector(batchSize, DataTypes.IntegerType);
            oldLongVector    = new OldOnHeapColumnVector(batchSize, DataTypes.LongType);

            newBooleanVector = new OnHeapColumnVector(batchSize, DataTypes.BooleanType);
            newByteVector    = new OnHeapColumnVector(batchSize, DataTypes.ByteType);
            newShortVector   = new OnHeapColumnVector(batchSize, DataTypes.ShortType);
            newIntVector     = new OnHeapColumnVector(batchSize, DataTypes.IntegerType);
            newLongVector    = new OnHeapColumnVector(batchSize, DataTypes.LongType);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            oldBooleanVector.close();
            oldByteVector.close();
            oldShortVector.close();
            oldIntVector.close();
            oldLongVector.close();

            newBooleanVector.close();
            newByteVector.close();
            newShortVector.close();
            newIntVector.close();
            newLongVector.close();
        }
    }

    // ==================== State for putNulls ====================
    // Before each invocation, clear nulls so putNulls has a clean vector to write into.

    @State(Scope.Thread)
    public static class PutNullsState {
        OldOnHeapColumnVector oldVector;
        OnHeapColumnVector newVector;
        int batchSize;

        @Setup(Level.Invocation)
        public void setup(BaseState base) {
            oldVector = base.oldIntVector;
            newVector = base.newIntVector;
            batchSize = base.batchSize;
            // Reset to a clean non-null state so putNulls does real work
            oldVector.reset();
            newVector.reset();
        }
    }

    // ==================== State for putNotNulls ====================
    // Before each invocation, mark all rows as null so putNotNulls has work to do.

    @State(Scope.Thread)
    public static class PutNotNullsState {
        OldOnHeapColumnVector oldVector;
        OnHeapColumnVector newVector;
        int batchSize;

        @Setup(Level.Invocation)
        public void setup(BaseState base) {
            oldVector = base.oldIntVector;
            newVector = base.newIntVector;
            batchSize = base.batchSize;
            // Mark all as null so hasNull()==true and putNotNulls does real work
            oldVector.reset();
            oldVector.putNulls(0, batchSize);
            newVector.reset();
            newVector.putNulls(0, batchSize);
        }
    }


    // ========================== putNulls ==========================

    @Benchmark
    public void putNulls_old(PutNullsState state) {
        state.oldVector.putNulls(0, state.batchSize);
    }

    @Benchmark
    public void putNulls_new(PutNullsState state) {
        state.newVector.putNulls(0, state.batchSize);
    }

    // ========================== putNotNulls ==========================

    @Benchmark
    public void putNotNulls_old(PutNotNullsState state) {
        state.oldVector.putNotNulls(0, state.batchSize);
    }

    @Benchmark
    public void putNotNulls_new(PutNotNullsState state) {
        state.newVector.putNotNulls(0, state.batchSize);
    }

    // ========================== putBooleans ==========================

    @Benchmark
    public void putBooleans_old(BaseState state) {
        state.oldBooleanVector.putBooleans(0, state.batchSize, true);
    }

    @Benchmark
    public void putBooleans_new(BaseState state) {
        state.newBooleanVector.putBooleans(0, state.batchSize, true);
    }

    // ========================== putBytes ==========================

    @Benchmark
    public void putBytes_old(BaseState state) {
        state.oldByteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    @Benchmark
    public void putBytes_new(BaseState state) {
        state.newByteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    // ========================== putShorts ==========================

    @Benchmark
    public void putShorts_old(BaseState state) {
        state.oldShortVector.putShorts(0, state.batchSize, (short) 1234);
    }

    @Benchmark
    public void putShorts_new(BaseState state) {
        state.newShortVector.putShorts(0, state.batchSize, (short) 1234);
    }

    // ========================== putInts ==========================

    @Benchmark
    public void putInts_old(BaseState state) {
        state.oldIntVector.putInts(0, state.batchSize, 123456);
    }

    @Benchmark
    public void putInts_new(BaseState state) {
        state.newIntVector.putInts(0, state.batchSize, 123456);
    }

    // ========================== putLongs ==========================

    @Benchmark
    public void putLongs_old(BaseState state) {
        state.oldLongVector.putLongs(0, state.batchSize, 123456789L);
    }

    @Benchmark
    public void putLongs_new(BaseState state) {
        state.newLongVector.putLongs(0, state.batchSize, 123456789L);
    }


    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OnHeapColumnVectorBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

