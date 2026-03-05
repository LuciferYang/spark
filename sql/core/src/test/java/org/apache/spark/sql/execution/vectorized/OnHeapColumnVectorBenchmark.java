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
 * JMH Benchmark for OnHeapColumnVector batch fill operations with Platform.setMemory optimization.
 *
 * <p>Optimized methods (using Platform.setMemory for byte array operations):
 * <ul>
 *   <li>{@code putNulls(rowId, count)}       — Platform.setMemory vs for-loop</li>
 *   <li>{@code putNotNulls(rowId, count)}     — Platform.setMemory vs for-loop</li>
 *   <li>{@code putBooleans(rowId, count, v)}  — Platform.setMemory vs for-loop</li>
 *   <li>{@code putBytes(rowId, count, v)}     — Platform.setMemory vs for-loop</li>
 * </ul>
 *
 * <p>Note: putShorts, putInts, putLongs, putFloats, putDoubles implementations are identical
 * between OnHeapColumnVector and OldOnHeapColumnVector (both use for-loop), so they are excluded.
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

        OnHeapColumnVector nullVector;
        OnHeapColumnVector booleanVector;
        OnHeapColumnVector byteVector;

        OldOnHeapColumnVector oldNullVector;
        OldOnHeapColumnVector oldBooleanVector;
        OldOnHeapColumnVector oldByteVector;

        @Setup(Level.Trial)
        public void setup() {
            // 优化后的实现（使用Platform.setMemory）
            nullVector    = new OnHeapColumnVector(batchSize, DataTypes.IntegerType);
            booleanVector = new OnHeapColumnVector(batchSize, DataTypes.BooleanType);
            byteVector    = new OnHeapColumnVector(batchSize, DataTypes.ByteType);

            // 优化前的实现（使用for-loop）
            oldNullVector    = new OldOnHeapColumnVector(batchSize, DataTypes.IntegerType);
            oldBooleanVector = new OldOnHeapColumnVector(batchSize, DataTypes.BooleanType);
            oldByteVector    = new OldOnHeapColumnVector(batchSize, DataTypes.ByteType);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            nullVector.close();
            booleanVector.close();
            byteVector.close();

            oldNullVector.close();
            oldBooleanVector.close();
            oldByteVector.close();
        }
    }

    // ==================== State for putNulls ====================

    @State(Scope.Thread)
    public static class PutNullsState {
        OnHeapColumnVector newVector;
        OldOnHeapColumnVector oldVector;
        int batchSize;

        @Setup(Level.Invocation)
        public void setup(BaseState base) {
            newVector = base.nullVector;
            oldVector = base.oldNullVector;
            batchSize = base.batchSize;
            newVector.reset();
            oldVector.reset();
        }
    }

    // ==================== State for putNotNulls ====================

    @State(Scope.Thread)
    public static class PutNotNullsState {
        OnHeapColumnVector newVector;
        OldOnHeapColumnVector oldVector;
        int batchSize;

        @Setup(Level.Invocation)
        public void setup(BaseState base) {
            newVector = base.nullVector;
            oldVector = base.oldNullVector;
            batchSize = base.batchSize;
            newVector.reset();
            oldVector.reset();
            newVector.putNulls(0, batchSize);
            oldVector.putNulls(0, batchSize);
        }
    }

    // ==================== putNulls Benchmarks ====================

    @Benchmark
    public void putNulls_new(PutNullsState state) {
        state.newVector.putNulls(0, state.batchSize);
    }

    @Benchmark
    public void putNulls_old(PutNullsState state) {
        state.oldVector.putNulls(0, state.batchSize);
    }

    // ==================== putNotNulls Benchmarks ====================

    @Benchmark
    public void putNotNulls_new(PutNotNullsState state) {
        state.newVector.putNotNulls(0, state.batchSize);
    }

    @Benchmark
    public void putNotNulls_old(PutNotNullsState state) {
        state.oldVector.putNotNulls(0, state.batchSize);
    }

    // ==================== putBooleans Benchmarks ====================

    @Benchmark
    public void putBooleans_new(BaseState state) {
        state.booleanVector.putBooleans(0, state.batchSize, true);
    }

    @Benchmark
    public void putBooleans_old(BaseState state) {
        state.oldBooleanVector.putBooleans(0, state.batchSize, true);
    }

    // ==================== putBytes Benchmarks ====================

    @Benchmark
    public void putBytes_new(BaseState state) {
        state.byteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    @Benchmark
    public void putBytes_old(BaseState state) {
        state.oldByteVector.putBytes(0, state.batchSize, (byte) 42);
    }


    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OnHeapColumnVectorBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

