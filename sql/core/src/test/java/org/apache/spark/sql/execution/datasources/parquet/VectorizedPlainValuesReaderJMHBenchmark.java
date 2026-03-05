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

import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;

/**
 * JMH Benchmark for comparing readIntegersWithRebase and readLongsWithRebase between
 * OldVectorizedPlainValuesReader (full-scan + all-or-nothing rebase) and the optimized
 * VectorizedPlainValuesReader (early-exit scan + partial bulk-write + per-value rebase).
 *
 * Data scenarios:
 * - allModern: all values >= rebase threshold (the dominant real-world case, no rebase needed)
 * - mixed: ~1% ancient values scattered among modern values (rebase needed for a few)
 *
 * Buffer scenarios:
 * - heap (hasArray=true):   tests the array-backed bulk-write path
 * - direct (hasArray=false): tests the non-array fallback path
 *
 * To run:
 * {{{
 *   build/mvn test-compile -pl sql/core -DskipTests
 *   java -cp "$(build/mvn -pl sql/core dependency:build-classpath -DincludeScope=test \
 *     -Dmdep.outputFile=/dev/stdout -q):sql/core/target/test-classes:sql/core/target/classes" \
 *     org.apache.spark.sql.execution.datasources.parquet.VectorizedPlainValuesReaderJMHBenchmark
 * }}}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class VectorizedPlainValuesReaderJMHBenchmark {

    // ==================== Parameters ====================

    @Param({"10000000"})
    private int numValues;

    private static final int BATCH_SIZE = 4096;

    // ==================== Test Data ====================

    // int data (4 bytes per value) for readIntegersWithRebase
    private byte[] intDataAllModern;
    private byte[] intDataMixed;

    // long data (8 bytes per value) for readLongsWithRebase
    private byte[] longDataAllModern;
    private byte[] longDataMixed;

    // ==================== Readers ====================
    // readIntegersWithRebase readers: old/new × heap/direct × allModern/mixed
    private OldVectorizedPlainValuesReader oldIntAllModernHeap;
    private OldVectorizedPlainValuesReader oldIntAllModernDirect;
    private OldVectorizedPlainValuesReader oldIntMixedHeap;
    private OldVectorizedPlainValuesReader oldIntMixedDirect;
    private VectorizedPlainValuesReader newIntAllModernHeap;
    private VectorizedPlainValuesReader newIntAllModernDirect;
    private VectorizedPlainValuesReader newIntMixedHeap;
    private VectorizedPlainValuesReader newIntMixedDirect;

    // readLongsWithRebase readers: old/new × heap/direct × allModern/mixed
    private OldVectorizedPlainValuesReader oldLongAllModernHeap;
    private OldVectorizedPlainValuesReader oldLongAllModernDirect;
    private OldVectorizedPlainValuesReader oldLongMixedHeap;
    private OldVectorizedPlainValuesReader oldLongMixedDirect;
    private VectorizedPlainValuesReader newLongAllModernHeap;
    private VectorizedPlainValuesReader newLongAllModernDirect;
    private VectorizedPlainValuesReader newLongMixedHeap;
    private VectorizedPlainValuesReader newLongMixedDirect;

    // ==================== Column Vector States ====================

    @State(Scope.Thread)
    public static class IntColumnState {
        public WritableColumnVector column;
        @Setup(Level.Iteration)
        public void setup() {
            column = new OnHeapColumnVector(BATCH_SIZE, DataTypes.IntegerType);
        }
        @TearDown(Level.Iteration)
        public void tearDown() { column.close(); }
        @Setup(Level.Invocation)
        public void reset() { column.reset(); }
    }

    @State(Scope.Thread)
    public static class LongColumnState {
        public WritableColumnVector column;
        @Setup(Level.Iteration)
        public void setup() {
            column = new OnHeapColumnVector(BATCH_SIZE, DataTypes.LongType);
        }
        @TearDown(Level.Iteration)
        public void tearDown() { column.close(); }
        @Setup(Level.Invocation)
        public void reset() { column.reset(); }
    }

    // ==================== Setup ====================

    @Setup(Level.Trial)
    public void setupTrial() {
        Random random = new Random(42);
        int switchDay = RebaseDateTime.lastSwitchJulianDay();
        long switchTs = RebaseDateTime.lastSwitchJulianTs();

        intDataAllModern = generateIntData(numValues, random, switchDay, 0.0);
        intDataMixed = generateIntData(numValues, new Random(42), switchDay, 0.01);
        longDataAllModern = generateLongData(numValues, random, switchTs, 0.0);
        longDataMixed = generateLongData(numValues, new Random(42), switchTs, 0.01);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        // readIntegersWithRebase readers
        oldIntAllModernHeap = initOld(intDataAllModern, false);
        oldIntAllModernDirect = initOld(intDataAllModern, true);
        oldIntMixedHeap = initOld(intDataMixed, false);
        oldIntMixedDirect = initOld(intDataMixed, true);
        newIntAllModernHeap = initNew(intDataAllModern, false);
        newIntAllModernDirect = initNew(intDataAllModern, true);
        newIntMixedHeap = initNew(intDataMixed, false);
        newIntMixedDirect = initNew(intDataMixed, true);

        // readLongsWithRebase readers
        oldLongAllModernHeap = initOld(longDataAllModern, false);
        oldLongAllModernDirect = initOld(longDataAllModern, true);
        oldLongMixedHeap = initOld(longDataMixed, false);
        oldLongMixedDirect = initOld(longDataMixed, true);
        newLongAllModernHeap = initNew(longDataAllModern, false);
        newLongAllModernDirect = initNew(longDataAllModern, true);
        newLongMixedHeap = initNew(longDataMixed, false);
        newLongMixedDirect = initNew(longDataMixed, true);
    }

    // ==================== Data Generation ====================

    /**
     * Generates int data where each BATCH_SIZE batch contains exactly
     * {@code round(ancientRatio * batchSize)} ancient values (below switchDay),
     * placed at random positions within that batch.
     *
     * <p>This ensures every batch the benchmark reads has the same data distribution,
     * eliminating variance caused by some batches having zero ancient values and
     * others having many when using purely random generation across the whole array.
     */
    private byte[] generateIntData(int count, Random random, int switchDay, double ancientRatio) {
        ByteBuffer buf = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int batchStart = 0; batchStart < count; batchStart += BATCH_SIZE) {
            int batchLen = Math.min(BATCH_SIZE, count - batchStart);
            int numAncient = (int) Math.round(ancientRatio * batchLen);
            // Fill the batch: first numAncient ancient values, then modern values
            int[] values = new int[batchLen];
            for (int i = 0; i < numAncient; i++) {
                values[i] = switchDay - 1 - random.nextInt(500000);
            }
            for (int i = numAncient; i < batchLen; i++) {
                values[i] = random.nextInt(Integer.MAX_VALUE);
            }
            // Fisher-Yates shuffle to randomize positions within the batch
            for (int i = batchLen - 1; i > 0; i--) {
                int j = random.nextInt(i + 1);
                int tmp = values[i];
                values[i] = values[j];
                values[j] = tmp;
            }
            for (int i = 0; i < batchLen; i++) {
                buf.putInt(values[i]);
            }
        }
        return buf.array();
    }

    /**
     * Generates long data where each BATCH_SIZE batch contains exactly
     * {@code round(ancientRatio * batchSize)} ancient values (below switchTs),
     * placed at random positions within that batch.
     *
     * <p>This ensures every batch the benchmark reads has the same data distribution,
     * eliminating variance caused by some batches having zero ancient values and
     * others having many when using purely random generation across the whole array.
     */
    private byte[] generateLongData(int count, Random random, long switchTs, double ancientRatio) {
        ByteBuffer buf = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int batchStart = 0; batchStart < count; batchStart += BATCH_SIZE) {
            int batchLen = Math.min(BATCH_SIZE, count - batchStart);
            int numAncient = (int) Math.round(ancientRatio * batchLen);
            // Fill the batch: first numAncient ancient values, then modern values
            long[] values = new long[batchLen];
            for (int i = 0; i < numAncient; i++) {
                values[i] = switchTs - 1 - (Math.abs(random.nextLong()) % 500_000_000_000L);
            }
            for (int i = numAncient; i < batchLen; i++) {
                values[i] = Math.abs(random.nextLong());
            }
            // Fisher-Yates shuffle to randomize positions within the batch
            for (int i = batchLen - 1; i > 0; i--) {
                int j = random.nextInt(i + 1);
                long tmp = values[i];
                values[i] = values[j];
                values[j] = tmp;
            }
            for (int i = 0; i < batchLen; i++) {
                buf.putLong(values[i]);
            }
        }
        return buf.array();
    }

    // ==================== Reader Initialization Helpers ====================

    private OldVectorizedPlainValuesReader initOld(byte[] data, boolean direct) throws IOException {
        OldVectorizedPlainValuesReader reader = new OldVectorizedPlainValuesReader();
        reader.initFromPage(0, createInputStream(data, direct));
        return reader;
    }

    private VectorizedPlainValuesReader initNew(byte[] data, boolean direct) throws IOException {
        VectorizedPlainValuesReader reader = new VectorizedPlainValuesReader();
        reader.initFromPage(0, createInputStream(data, direct));
        return reader;
    }

    private ByteBufferInputStream createInputStream(byte[] data, boolean direct) {
        if (direct) {
            ByteBuffer buf = ByteBuffer.allocateDirect(data.length).order(ByteOrder.LITTLE_ENDIAN);
            buf.put(data);
            buf.flip();
            return ByteBufferInputStream.wrap(buf);
        } else {
            return ByteBufferInputStream.wrap(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN));
        }
    }

    // ====================================================================================
    // readIntegersWithRebase — allModern (no rebase needed, the common fast path)
    // ====================================================================================

    @Benchmark
    public void readIntWithRebase_allModern_heap_Old(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldIntAllModernHeap.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_allModern_heap_New(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newIntAllModernHeap.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_allModern_direct_Old(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldIntAllModernDirect.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_allModern_direct_New(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newIntAllModernDirect.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    // ====================================================================================
    // readIntegersWithRebase — mixed (~1% ancient values)
    // ====================================================================================

    @Benchmark
    public void readIntWithRebase_mixed_heap_Old(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldIntMixedHeap.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_mixed_heap_New(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newIntMixedHeap.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_mixed_direct_Old(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldIntMixedDirect.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_mixed_direct_New(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newIntMixedDirect.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    // ====================================================================================
    // readLongsWithRebase — allModern (no rebase needed, the common fast path)
    // ====================================================================================

    @Benchmark
    public void readLongWithRebase_allModern_heap_Old(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldLongAllModernHeap.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_allModern_heap_New(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newLongAllModernHeap.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_allModern_direct_Old(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldLongAllModernDirect.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_allModern_direct_New(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newLongAllModernDirect.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    // ====================================================================================
    // readLongsWithRebase — mixed (~1% ancient values)
    // ====================================================================================

    @Benchmark
    public void readLongWithRebase_mixed_heap_Old(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldLongMixedHeap.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_mixed_heap_New(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newLongMixedHeap.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_mixed_direct_Old(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldLongMixedDirect.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_mixed_direct_New(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newLongMixedDirect.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    // ==================== Main Method ====================

    public static void main(String[] args) throws RunnerException {
        String filter = args.length > 0 ?
            args[0] : VectorizedPlainValuesReaderJMHBenchmark.class.getSimpleName();
        Options opt = new OptionsBuilder()
                .include(filter)
                .build();

        new Runner(opt).run();
    }
}
