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
 * - allRebase: all values < rebase threshold (all values need rebase)
 * - allModern: all values >= rebase threshold (no values need rebase)
 * - halfRebase: 50% ancient values mixed with 50% modern values
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
    private byte[] intDataAllRebase;
    private byte[] intDataAllModern;
    private byte[] intDataHalfRebase;

    // long data (8 bytes per value) for readLongsWithRebase
    private byte[] longDataAllRebase;
    private byte[] longDataAllModern;
    private byte[] longDataHalfRebase;

    // ==================== Readers ====================
    // readIntegersWithRebase readers: old/new × heap/direct × allRebase/allModern/halfRebase
    private OldVectorizedPlainValuesReader oldIntAllRebaseHeap;
    private OldVectorizedPlainValuesReader oldIntAllRebaseDirect;
    private OldVectorizedPlainValuesReader oldIntAllModernHeap;
    private OldVectorizedPlainValuesReader oldIntAllModernDirect;
    private OldVectorizedPlainValuesReader oldIntHalfRebaseHeap;
    private OldVectorizedPlainValuesReader oldIntHalfRebaseDirect;
    private VectorizedPlainValuesReader newIntAllRebaseHeap;
    private VectorizedPlainValuesReader newIntAllRebaseDirect;
    private VectorizedPlainValuesReader newIntAllModernHeap;
    private VectorizedPlainValuesReader newIntAllModernDirect;
    private VectorizedPlainValuesReader newIntHalfRebaseHeap;
    private VectorizedPlainValuesReader newIntHalfRebaseDirect;

    // readLongsWithRebase readers: old/new × heap/direct × allRebase/allModern/halfRebase
    private OldVectorizedPlainValuesReader oldLongAllRebaseHeap;
    private OldVectorizedPlainValuesReader oldLongAllRebaseDirect;
    private OldVectorizedPlainValuesReader oldLongAllModernHeap;
    private OldVectorizedPlainValuesReader oldLongAllModernDirect;
    private OldVectorizedPlainValuesReader oldLongHalfRebaseHeap;
    private OldVectorizedPlainValuesReader oldLongHalfRebaseDirect;
    private VectorizedPlainValuesReader newLongAllRebaseHeap;
    private VectorizedPlainValuesReader newLongAllRebaseDirect;
    private VectorizedPlainValuesReader newLongAllModernHeap;
    private VectorizedPlainValuesReader newLongAllModernDirect;
    private VectorizedPlainValuesReader newLongHalfRebaseHeap;
    private VectorizedPlainValuesReader newLongHalfRebaseDirect;

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

        intDataAllRebase = generateIntData(numValues, random, switchDay, 1.0);
        intDataAllModern = generateIntData(numValues, new Random(42), switchDay, 0.0);
        intDataHalfRebase = generateIntData(numValues, new Random(43), switchDay, 0.5);
        longDataAllRebase = generateLongData(numValues, random, switchTs, 1.0);
        longDataAllModern = generateLongData(numValues, new Random(42), switchTs, 0.0);
        longDataHalfRebase = generateLongData(numValues, new Random(43), switchTs, 0.5);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws IOException {
        // readIntegersWithRebase readers
        oldIntAllRebaseHeap = initOld(intDataAllRebase, false);
        oldIntAllRebaseDirect = initOld(intDataAllRebase, true);
        oldIntAllModernHeap = initOld(intDataAllModern, false);
        oldIntAllModernDirect = initOld(intDataAllModern, true);
        oldIntHalfRebaseHeap = initOld(intDataHalfRebase, false);
        oldIntHalfRebaseDirect = initOld(intDataHalfRebase, true);
        newIntAllRebaseHeap = initNew(intDataAllRebase, false);
        newIntAllRebaseDirect = initNew(intDataAllRebase, true);
        newIntAllModernHeap = initNew(intDataAllModern, false);
        newIntAllModernDirect = initNew(intDataAllModern, true);
        newIntHalfRebaseHeap = initNew(intDataHalfRebase, false);
        newIntHalfRebaseDirect = initNew(intDataHalfRebase, true);

        // readLongsWithRebase readers
        oldLongAllRebaseHeap = initOld(longDataAllRebase, false);
        oldLongAllRebaseDirect = initOld(longDataAllRebase, true);
        oldLongAllModernHeap = initOld(longDataAllModern, false);
        oldLongAllModernDirect = initOld(longDataAllModern, true);
        oldLongHalfRebaseHeap = initOld(longDataHalfRebase, false);
        oldLongHalfRebaseDirect = initOld(longDataHalfRebase, true);
        newLongAllRebaseHeap = initNew(longDataAllRebase, false);
        newLongAllRebaseDirect = initNew(longDataAllRebase, true);
        newLongAllModernHeap = initNew(longDataAllModern, false);
        newLongAllModernDirect = initNew(longDataAllModern, true);
        newLongHalfRebaseHeap = initNew(longDataHalfRebase, false);
        newLongHalfRebaseDirect = initNew(longDataHalfRebase, true);
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
    // readIntegersWithRebase — allRebase (all values need rebase)
    // ====================================================================================

    @Benchmark
    public void readIntWithRebase_allRebase_heap_Old(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldIntAllRebaseHeap.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_allRebase_heap_New(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newIntAllRebaseHeap.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_allRebase_direct_Old(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldIntAllRebaseDirect.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_allRebase_direct_New(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newIntAllRebaseDirect.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    // ====================================================================================
    // readIntegersWithRebase — allModern (no rebase needed)
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
    // readIntegersWithRebase — halfRebase (50% values need rebase)
    // ====================================================================================

    @Benchmark
    public void readIntWithRebase_halfRebase_heap_Old(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldIntHalfRebaseHeap.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_halfRebase_heap_New(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newIntHalfRebaseHeap.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_halfRebase_direct_Old(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldIntHalfRebaseDirect.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    @Benchmark
    public void readIntWithRebase_halfRebase_direct_New(IntColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newIntHalfRebaseDirect.readIntegersWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false);
        }
    }

    // ====================================================================================
    // readLongsWithRebase — allRebase (all values need rebase)
    // ====================================================================================

    @Benchmark
    public void readLongWithRebase_allRebase_heap_Old(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldLongAllRebaseHeap.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_allRebase_heap_New(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newLongAllRebaseHeap.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_allRebase_direct_Old(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldLongAllRebaseDirect.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_allRebase_direct_New(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newLongAllRebaseDirect.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    // ====================================================================================
    // readLongsWithRebase — allModern (no rebase needed)
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
    // readLongsWithRebase — halfRebase (50% values need rebase)
    // ====================================================================================

    @Benchmark
    public void readLongWithRebase_halfRebase_heap_Old(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldLongHalfRebaseHeap.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_halfRebase_heap_New(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newLongHalfRebaseHeap.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_halfRebase_direct_Old(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            oldLongHalfRebaseDirect.readLongsWithRebase(
                Math.min(BATCH_SIZE, n - i), state.column, 0, false, "UTC");
        }
    }

    @Benchmark
    public void readLongWithRebase_halfRebase_direct_New(LongColumnState state) {
        int n = numValues;
        for (int i = 0; i < n; i += BATCH_SIZE) {
            newLongHalfRebaseDirect.readLongsWithRebase(
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
