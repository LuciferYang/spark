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

        OldOffHeapColumnVector oldBooleanVector;
        OldOffHeapColumnVector oldByteVector;

        @Setup(Level.Trial)
        public void setup() {
            // 优化后的实现
            booleanVector = new OffHeapColumnVector(batchSize, DataTypes.BooleanType);
            byteVector    = new OffHeapColumnVector(batchSize, DataTypes.ByteType);

            // 优化前的实现（对照组）
            oldBooleanVector = new OldOffHeapColumnVector(batchSize, DataTypes.BooleanType);
            oldByteVector    = new OldOffHeapColumnVector(batchSize, DataTypes.ByteType);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            booleanVector.close();
            byteVector.close();

            oldBooleanVector.close();
            oldByteVector.close();
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

    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OffHeapColumnVectorBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

