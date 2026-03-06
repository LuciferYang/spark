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
 * Multi-threaded JMH Benchmark for OffHeapColumnVector batch fill operations.
 *
 * <p>Each thread operates on its own independent ColumnVector (Scope.Thread),
 * simulating the real Spark scenario where multiple tasks concurrently fill
 * different column vectors. This measures the impact of memory bandwidth
 * contention and cache pressure under concurrency.
 *
 * <p>Compares {@code Platform.setMemory} (new) vs element-by-element loop (old)
 * for {@code putBytes} under different thread counts.
 *
 * <p>To run:
 * <pre>{@code
 *   build/mvn test-compile -pl sql/core -DskipTests -DskipScala
 *   java -cp "..." org.apache.spark.sql.execution.vectorized.OffHeapColumnVectorMTBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class OffHeapColumnVectorMTBenchmark {

    @State(Scope.Thread)
    public static class ThreadState {
        @Param({"4096", "16384"})
        int batchSize;

        OffHeapColumnVector byteVector;
        OldOffHeapColumnVector oldByteVector;

        @Setup(Level.Trial)
        public void setup() {
            byteVector = new OffHeapColumnVector(batchSize, DataTypes.ByteType);
            oldByteVector = new OldOffHeapColumnVector(batchSize, DataTypes.ByteType);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            byteVector.close();
            oldByteVector.close();
        }
    }

    // ==================== 1 thread ====================

    @Benchmark
    @Threads(1)
    public void putBytes_new_t1(ThreadState state) {
        state.byteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    @Benchmark
    @Threads(1)
    public void putBytes_old_t1(ThreadState state) {
        state.oldByteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    // ==================== 4 threads ====================

    @Benchmark
    @Threads(4)
    public void putBytes_new_t4(ThreadState state) {
        state.byteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    @Benchmark
    @Threads(4)
    public void putBytes_old_t4(ThreadState state) {
        state.oldByteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    // ==================== 8 threads ====================

    @Benchmark
    @Threads(8)
    public void putBytes_new_t8(ThreadState state) {
        state.byteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    @Benchmark
    @Threads(8)
    public void putBytes_old_t8(ThreadState state) {
        state.oldByteVector.putBytes(0, state.batchSize, (byte) 42);
    }

    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OffHeapColumnVectorMTBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

