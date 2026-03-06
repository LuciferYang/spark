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
 * JMH Benchmark for OffHeapColumnVector batch fill operations.
 *
 * <p>Optimized methods under test:
 * <ul>
 *   <li>{@code putNulls(rowId, count)}       — off-heap byte fill using Platform.setMemory</li>
 *   <li>{@code putNotNulls(rowId, count)}     — off-heap byte clear using Platform.setMemory</li>
 *   <li>{@code putBooleans(rowId, count, v)}  — off-heap byte fill using Platform.setMemory</li>
 *   <li>{@code putBytes(rowId, count, v)}     — off-heap byte fill using Platform.setMemory</li>
 * </ul>
 *
 * <p>Each method is compared against the old implementation that uses element-by-element
 * assignment loops via Platform.putByte.
 *
 * <p>To run:
 * <pre>{@code
 *   build/mvn test-compile -pl sql/core -DskipTests -DskipScala
 *   java -cp "$(build/mvn -pl sql/core dependency:build-classpath -DincludeScope=test \
 *     -Dmdep.outputFile=/dev/stdout -q):sql/core/target/test-classes:sql/core/target/classes" \
 *     org.apache.spark.sql.execution.vectorized.OffHeapColumnVectorBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class OffHeapColumnVectorBenchmark {

    // ==================== Shared base state ====================

    @State(Scope.Thread)
    public static class BaseState {
        @Param({"4096", "8192", "16384"})
        int batchSize;

        OffHeapColumnVector booleanVector;
        OffHeapColumnVector byteVector;
        OffHeapColumnVector shortVector;
        OffHeapColumnVector intVector;
        OffHeapColumnVector longVector;
        OffHeapColumnVector floatVector;
        OffHeapColumnVector doubleVector;

        OldOffHeapColumnVector oldBooleanVector;
        OldOffHeapColumnVector oldByteVector;
        OldOffHeapColumnVector oldShortVector;
        OldOffHeapColumnVector oldIntVector;
        OldOffHeapColumnVector oldLongVector;
        OldOffHeapColumnVector oldFloatVector;
        OldOffHeapColumnVector oldDoubleVector;

        @Setup(Level.Trial)
        public void setup() {
            // 优化后的实现
            booleanVector = new OffHeapColumnVector(batchSize, DataTypes.BooleanType);
            byteVector    = new OffHeapColumnVector(batchSize, DataTypes.ByteType);
            shortVector   = new OffHeapColumnVector(batchSize, DataTypes.ShortType);
            intVector     = new OffHeapColumnVector(batchSize, DataTypes.IntegerType);
            longVector    = new OffHeapColumnVector(batchSize, DataTypes.LongType);
            floatVector   = new OffHeapColumnVector(batchSize, DataTypes.FloatType);
            doubleVector  = new OffHeapColumnVector(batchSize, DataTypes.DoubleType);

            // 优化前的实现（对照组）
            oldBooleanVector = new OldOffHeapColumnVector(batchSize, DataTypes.BooleanType);
            oldByteVector    = new OldOffHeapColumnVector(batchSize, DataTypes.ByteType);
            oldShortVector   = new OldOffHeapColumnVector(batchSize, DataTypes.ShortType);
            oldIntVector     = new OldOffHeapColumnVector(batchSize, DataTypes.IntegerType);
            oldLongVector    = new OldOffHeapColumnVector(batchSize, DataTypes.LongType);
            oldFloatVector   = new OldOffHeapColumnVector(batchSize, DataTypes.FloatType);
            oldDoubleVector  = new OldOffHeapColumnVector(batchSize, DataTypes.DoubleType);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            booleanVector.close();
            byteVector.close();
            shortVector.close();
            intVector.close();
            longVector.close();
            floatVector.close();
            doubleVector.close();

            oldBooleanVector.close();
            oldByteVector.close();
            oldShortVector.close();
            oldIntVector.close();
            oldLongVector.close();
            oldFloatVector.close();
            oldDoubleVector.close();
        }
    }


    // ==================== State for putNulls ====================

    @State(Scope.Thread)
    public static class PutNullsState {
        OffHeapColumnVector vector;
        OldOffHeapColumnVector oldVector;
        int batchSize;

        @Setup(Level.Invocation)
        public void setup(BaseState base) {
            vector = base.booleanVector;
            oldVector = base.oldBooleanVector;
            batchSize = base.batchSize;
            vector.reset();
            oldVector.reset();
            // Set up not nulls first for testing putNulls
            vector.putNotNulls(0, batchSize);
            oldVector.putNotNulls(0, batchSize);
        }
    }

    // ==================== State for putNotNulls ====================

    @State(Scope.Thread)
    public static class PutNotNullsState {
        OffHeapColumnVector vector;
        OldOffHeapColumnVector oldVector;
        int batchSize;

        @Setup(Level.Invocation)
        public void setup(BaseState base) {
            vector = base.byteVector;
            oldVector = base.oldByteVector;
            batchSize = base.batchSize;
            vector.reset();
            oldVector.reset();
            // Set up nulls first for testing putNotNulls
            vector.putNulls(0, batchSize);
            oldVector.putNulls(0, batchSize);
        }
    }

    // ========================== putNulls ==========================

    @Benchmark
    public void putNulls(PutNullsState state) {
        state.vector.putNulls(0, state.batchSize);
    }

    // ========================== putNotNulls ==========================

    @Benchmark
    public void putNotNulls(PutNotNullsState state) {
        state.vector.putNotNulls(0, state.batchSize);
    }

    @Benchmark
    public void putBooleans(BaseState state) {
        state.booleanVector.putBooleans(0, state.batchSize, true);
    }

    // ========================== putBytes ==========================

    @Benchmark
    public void putBytes(BaseState state) {
        state.byteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    // ========================== Old Implementation Benchmarks (Control Group) ===================

    // ========================== putNulls (Old) ==========================

    @Benchmark
    public void putNulls_old(PutNullsState state) {
        state.oldVector.putNulls(0, state.batchSize);
    }

    // ========================== putNotNulls (Old) ==========================

    @Benchmark
    public void putNotNulls_old(PutNotNullsState state) {
        state.oldVector.putNotNulls(0, state.batchSize);
    }

    // ========================== putBooleans (Old) ==========================

    @Benchmark
    public void putBooleans_old(BaseState state) {
        state.oldBooleanVector.putBooleans(0, state.batchSize, true);
    }

    // ========================== putBytes (Old) ==========================

    @Benchmark
    public void putBytes_old(BaseState state) {
        state.oldByteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    // ========================== putShorts (Optimized: seed-and-copy) ==========================

    @Benchmark
    public void putShorts(BaseState state) {
        state.shortVector.putShorts(0, state.batchSize, (short) 42);
    }

    @Benchmark
    public void putShorts_old(BaseState state) {
        state.oldShortVector.putShorts(0, state.batchSize, (short) 42);
    }

    // ========================== putInts (Optimized: seed-and-copy) ==========================

    @Benchmark
    public void putInts(BaseState state) {
        state.intVector.putInts(0, state.batchSize, 42);
    }

    @Benchmark
    public void putInts_old(BaseState state) {
        state.oldIntVector.putInts(0, state.batchSize, 42);
    }

    // ========================== putLongs (Optimized: seed-and-copy) ==========================

    @Benchmark
    public void putLongs(BaseState state) {
        state.longVector.putLongs(0, state.batchSize, 42L);
    }

    @Benchmark
    public void putLongs_old(BaseState state) {
        state.oldLongVector.putLongs(0, state.batchSize, 42L);
    }

    // ========================== putFloats (Optimized: seed-and-copy) ==========================

    @Benchmark
    public void putFloats(BaseState state) {
        state.floatVector.putFloats(0, state.batchSize, 42.0f);
    }

    @Benchmark
    public void putFloats_old(BaseState state) {
        state.oldFloatVector.putFloats(0, state.batchSize, 42.0f);
    }

    // ========================== putDoubles (Optimized: seed-and-copy) ==========================

    @Benchmark
    public void putDoubles(BaseState state) {
        state.doubleVector.putDoubles(0, state.batchSize, 42.0);
    }

    @Benchmark
    public void putDoubles_old(BaseState state) {
        state.oldDoubleVector.putDoubles(0, state.batchSize, 42.0);
    }

    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OffHeapColumnVectorBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

