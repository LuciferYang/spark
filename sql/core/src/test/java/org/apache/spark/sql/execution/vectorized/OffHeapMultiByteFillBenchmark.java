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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.spark.unsafe.Platform;

/**
 * JMH Benchmark for OffHeapColumnVector multi-byte fill operations (putInts, putLongs).
 *
 * <p>Compares three strategies:
 * <ul>
 *   <li>{@code loop} — current element-by-element Platform.putInt/putLong loop</li>
 *   <li>{@code seedCopy} — seed one element then doubling copyMemory</li>
 *   <li>{@code tempArray} — Arrays.fill a small temp array then batch copyMemory</li>
 * </ul>
 *
 * <p>This benchmark determines whether optimization 3 (seed-and-copy strategy) is worthwhile
 * across Java 17, 21 and 25, given that Java 21+ C2 may auto-vectorize the simple loop.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class OffHeapMultiByteFillBenchmark {

    // ==================== State ====================

    @State(Scope.Thread)
    public static class IntState {
        @Param({"4096", "8192", "16384"})
        int batchSize;

        long data;

        @Setup(Level.Trial)
        public void setup() {
            data = Platform.allocateMemory((long) batchSize * 4);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Platform.freeMemory(data);
        }
    }

    @State(Scope.Thread)
    public static class LongState {
        @Param({"4096", "8192", "16384"})
        int batchSize;

        long data;

        @Setup(Level.Trial)
        public void setup() {
            data = Platform.allocateMemory((long) batchSize * 8);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            Platform.freeMemory(data);
        }
    }

    // ==================== putInts: loop (current) ====================

    @Benchmark
    public void putInts_loop(IntState state) {
        long offset = state.data;
        int count = state.batchSize;
        for (int i = 0; i < count; ++i, offset += 4) {
            Platform.putInt(null, offset, 42);
        }
    }

    // ==================== putInts: seed-and-doubling-copy ====================

    @Benchmark
    public void putInts_seedCopy(IntState state) {
        long offset = state.data;
        int count = state.batchSize;
        long totalBytes = count * 4L;

        // Seed first element
        Platform.putInt(null, offset, 42);
        long filled = 4L;

        // Doubling copy
        while (filled < totalBytes) {
            long toCopy = Math.min(filled, totalBytes - filled);
            Platform.copyMemory(null, offset, null, offset + filled, toCopy);
            filled += toCopy;
        }
    }

    // ==================== putInts: temp array + batch copy ====================

    private static final int INT_FILL_BATCH = 64;

    @Benchmark
    public void putInts_tempArray(IntState state) {
        long offset = state.data;
        int count = state.batchSize;

        int[] temp = new int[Math.min(count, INT_FILL_BATCH)];
        Arrays.fill(temp, 42);

        int remaining = count;
        while (remaining > 0) {
            int batch = Math.min(remaining, INT_FILL_BATCH);
            Platform.copyMemory(temp, Platform.INT_ARRAY_OFFSET,
                null, offset, batch * 4L);
            offset += batch * 4L;
            remaining -= batch;
        }
    }

    // ==================== putLongs: loop (current) ====================

    @Benchmark
    public void putLongs_loop(LongState state) {
        long offset = state.data;
        int count = state.batchSize;
        for (int i = 0; i < count; ++i, offset += 8) {
            Platform.putLong(null, offset, 42L);
        }
    }

    // ==================== putLongs: seed-and-doubling-copy ====================

    @Benchmark
    public void putLongs_seedCopy(LongState state) {
        long offset = state.data;
        int count = state.batchSize;
        long totalBytes = count * 8L;

        Platform.putLong(null, offset, 42L);
        long filled = 8L;

        while (filled < totalBytes) {
            long toCopy = Math.min(filled, totalBytes - filled);
            Platform.copyMemory(null, offset, null, offset + filled, toCopy);
            filled += toCopy;
        }
    }

    // ==================== putLongs: temp array + batch copy ====================

    private static final int LONG_FILL_BATCH = 64;

    @Benchmark
    public void putLongs_tempArray(LongState state) {
        long offset = state.data;
        int count = state.batchSize;

        long[] temp = new long[Math.min(count, LONG_FILL_BATCH)];
        Arrays.fill(temp, 42L);

        int remaining = count;
        while (remaining > 0) {
            int batch = Math.min(remaining, LONG_FILL_BATCH);
            Platform.copyMemory(temp, Platform.LONG_ARRAY_OFFSET,
                null, offset, batch * 8L);
            offset += batch * 8L;
            remaining -= batch;
        }
    }

    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OffHeapMultiByteFillBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

