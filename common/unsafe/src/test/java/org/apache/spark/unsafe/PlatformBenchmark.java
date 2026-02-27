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

package org.apache.spark.unsafe;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmarks comparing {@link OldPlatform} (original) against {@link Platform} (optimised).
 *
 * <p>Each benchmark class covers one changed hot-path and contains exactly two methods:
 * {@code old_*} delegates to {@link OldPlatform}, {@code new_*} delegates to {@link Platform}.
 * No logic is duplicated in the benchmark itself; the full call-chains of both classes
 * are exercised as-is.
 *
 * <p>Three benchmark classes:
 * <ol>
 *   <li>{@link AllocateDirectBufferBenchmark} – reflection vs MethodHandle / VarHandle</li>
 *   <li>{@link ReallocateMemoryGrowBenchmark}   – grow: alloc+copy+free vs Unsafe.reallocateMemory</li>
 *   <li>{@link ReallocateMemoryShrinkBenchmark} – shrink: copies oldSize vs in-place realloc</li>
 *   <li>{@link CopyMemoryBenchmark}            – direction logic + UNSAFE_COPY_THRESHOLD</li>
 * </ol>
 *
 * <p>Build and run:
 * <pre>
 *   mvn package -pl common/unsafe -am
 *   java --add-opens java.base/java.nio=ALL-UNNAMED \
 *        --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
 *        -jar target/benchmarks.jar PlatformBenchmark -f 2 -wi 5 -i 10 -rf json
 * </pre>
 */
public class PlatformBenchmark {

  // ---------------------------------------------------------------------------
  // 1. allocateDirectBuffer
  //
  //    OldPlatform uses Constructor.newInstance + Method.invoke + Field.set.
  //    Platform     uses MethodHandle.invoke    + MethodHandle.invoke + MethodHandle.invoke (setter).
  //
  //    Both allocate a 4 KiB DirectByteBuffer backed by native memory and register
  //    a Cleaner.  The native allocation cost is identical; only the dispatch
  //    mechanism differs.
  // ---------------------------------------------------------------------------
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @State(Scope.Thread)
  @Warmup(iterations = 5, time = 1)
  @Measurement(iterations = 10, time = 1)
  @Fork(value = 2, jvmArgsPrepend = {
          "--add-opens", "java.base/java.nio=ALL-UNNAMED",
          "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED"
  })
  public static class AllocateDirectBufferBenchmark {

    private static final int BUFFER_SIZE = 4 * 1024; // 4 KiB

    // lastBuf holds the buffer produced by the current invocation.
    // @TearDown(Invocation) frees its native memory after JMH stops the timer,
    // keeping cleanup cost outside the measurement window.
    // A single field is safe because Scope.Thread guarantees old_reflection and
    // new_methodhandle are never executed concurrently within the same State instance.
    private ByteBuffer lastBuf;

    // Cleaner.clean() and the DirectByteBuffer.cleaner field are resolved once at
    // trial setup so that teardown has zero class-loading or field-lookup overhead.
    private java.lang.reflect.Method cleanMethod;
    private java.lang.reflect.Field  cleanerField;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      try {
        cleanMethod = Class.forName("jdk.internal.ref.Cleaner").getMethod("clean");
      } catch (ClassNotFoundException e) {
        cleanMethod = Class.forName("sun.misc.Cleaner").getMethod("clean");
      }
      cleanMethod.setAccessible(true);
      cleanerField = Class.forName("java.nio.DirectByteBuffer").getDeclaredField("cleaner");
      cleanerField.setAccessible(true);
    }

    private void freeBuffer(ByteBuffer buf) throws Exception {
      if (buf == null) return;
      Object cleaner = cleanerField.get(buf);
      if (cleaner != null) cleanMethod.invoke(cleaner);
    }

    // Runs after JMH stops timing for each invocation; native memory is freed here.
    @TearDown(Level.Invocation)
    public void teardown() throws Exception {
      freeBuffer(lastBuf);
      lastBuf = null;
    }

    @Benchmark
    public ByteBuffer old_reflection(Blackhole bh) {
      lastBuf = OldPlatform.allocateDirectBuffer(BUFFER_SIZE);
      bh.consume(lastBuf);
      return lastBuf;
    }

    @Benchmark
    public ByteBuffer new_methodhandle(Blackhole bh) {
      lastBuf = Platform.allocateDirectBuffer(BUFFER_SIZE);
      bh.consume(lastBuf);
      return lastBuf;
    }
  }

  // ---------------------------------------------------------------------------
  // 2. reallocateMemory
  //
  //    OldPlatform: allocateMemory(newSize) + copyMemory(oldSize) + freeMemory
  //                 → always copies oldSize bytes even when shrinking (bug).
  //    Platform:    Unsafe.reallocateMemory(address, newSize)
  //                 → maps to realloc(3); may resize in-place.
  //
  //    Two scenarios: grow (1 MiB → 2 MiB) and shrink (2 MiB → 1 MiB).
  //    @Setup(Level.Invocation) re-allocates a fresh block before every call so
  //    consecutive reallocations don't accidentally reuse the same address.
  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------
  // 2a. ReallocateMemoryGrowBenchmark  (1 MiB → 2 MiB)
  //
  //    Grow and shrink are split into separate @State classes because JMH only
  //    honours one @Setup(Level.Invocation) per class; having two in the same
  //    class means only the last-declared one is ever called, leaving the other
  //    benchmark methods operating on address=0 and producing corrupt results.
  // ---------------------------------------------------------------------------
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @State(Scope.Thread)
  @Warmup(iterations = 5, time = 1)
  @Measurement(iterations = 10, time = 1)
  @Fork(2)
  public static class ReallocateMemoryGrowBenchmark {

    private static final long SIZE_SMALL = 1L * 1024 * 1024; // 1 MiB
    private static final long SIZE_LARGE = 2L * 1024 * 1024; // 2 MiB

    private long address;

    // Allocate a fresh 1 MiB block before every invocation so that each call
    // starts from the same state and consecutive reallocations cannot reuse the
    // same address (which would make in-place success artificially likely).
    @Setup(Level.Invocation)
    public void setup() {
      address = Platform.allocateMemory(SIZE_SMALL);
    }

    // No @TearDown needed: both benchmark methods consume the returned address
    // (the reallocated block) and the memory is implicitly owned after the call.
    // To avoid leaking on failure paths we free whatever address holds at the end.
    @TearDown(Level.Invocation)
    public void teardown() {
      if (address != 0) {
        Platform.freeMemory(address);
        address = 0;
      }
    }

    @Benchmark
    public long old_grow() {
      // OldPlatform: allocate(2 MiB) + copyMemory(1 MiB) + free(old)
      address = OldPlatform.reallocateMemory(address, SIZE_SMALL, SIZE_LARGE);
      return address;
    }

    @Benchmark
    public long new_grow() {
      // Platform: Unsafe.reallocateMemory → realloc(3), may extend in-place
      address = Platform.reallocateMemory(address, SIZE_SMALL, SIZE_LARGE);
      return address;
    }
  }

  // ---------------------------------------------------------------------------
  // 2b. ReallocateMemoryShrinkBenchmark  (2 MiB → 1 MiB)
  //
  //    OldPlatform copies oldSize (2 MiB) even when shrinking – a bug that
  //    copies twice as many bytes as necessary.
  //    Platform delegates to realloc(3) which is always in-place for shrinks
  //    on glibc / jemalloc / libmalloc (macOS).
  // ---------------------------------------------------------------------------
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @State(Scope.Thread)
  @Warmup(iterations = 5, time = 1)
  @Measurement(iterations = 10, time = 1)
  @Fork(2)
  public static class ReallocateMemoryShrinkBenchmark {

    private static final long SIZE_SMALL = 1L * 1024 * 1024; // 1 MiB
    private static final long SIZE_LARGE = 2L * 1024 * 1024; // 2 MiB

    private long address;

    @Setup(Level.Invocation)
    public void setup() {
      address = Platform.allocateMemory(SIZE_LARGE);
    }

    @TearDown(Level.Invocation)
    public void teardown() {
      if (address != 0) {
        Platform.freeMemory(address);
        address = 0;
      }
    }

    @Benchmark
    public long old_shrink() {
      // OldPlatform: allocate(1 MiB) + copyMemory(2 MiB – copies oldSize, not newSize) + free
      address = OldPlatform.reallocateMemory(address, SIZE_LARGE, SIZE_SMALL);
      return address;
    }

    @Benchmark
    public long new_shrink() {
      // Platform: realloc(3) – shrink is always in-place, zero bytes copied
      address = Platform.reallocateMemory(address, SIZE_LARGE, SIZE_SMALL);
      return address;
    }
  }

  // ---------------------------------------------------------------------------
  // 3. copyMemory
  //
  //    Three scenarios that expose the two distinct optimisations:
  //
  //    a) disjoint_arrays (src != dst)
  //       OldPlatform checks dstOffset < srcOffset and takes the reverse loop because
  //       ARRAY_BASE_OFFSET == ARRAY_BASE_OFFSET (equal, not less-than).
  //       Platform detects src != dst and takes the forward loop unconditionally.
  //
  //    b) same_object_no_overlap
  //       Both src and dst point to the same byte[], with non-overlapping regions.
  //       OldPlatform: dstOffset > srcOffset → reverse loop (unnecessary).
  //       Platform:    detects no overlap → forward loop.
  //
  //    c) large_native_copy (64 MiB, null src/dst = raw addresses)
  //       OldPlatform: UNSAFE_COPY_THRESHOLD = 1 MiB → 64 loop iterations.
  //       Platform:    UNSAFE_COPY_THRESHOLD = 4 MiB → 16 loop iterations.
  // ---------------------------------------------------------------------------
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @State(Scope.Thread)
  @Warmup(iterations = 5, time = 1)
  @Measurement(iterations = 10, time = 1)
  @Fork(2)
  public static class CopyMemoryBenchmark {

    private static final long LARGE_SIZE = 64L * 1024 * 1024; // 64 MiB

    // a) disjoint arrays
    private byte[] srcArray;
    private byte[] dstArray;

    // b) same-object non-overlapping regions (use srcArray, offsets chosen below)
    // src region [ARRAY_BASE_OFFSET, +256), dst region [ARRAY_BASE_OFFSET+512, +768)

    // c) large native copy
    private long nativeSrc;
    private long nativeDst;

    @Setup(Level.Trial)
    public void setup() {
      srcArray  = new byte[1024];
      dstArray  = new byte[1024];
      nativeSrc = Platform.allocateMemory(LARGE_SIZE);
      nativeDst = Platform.allocateMemory(LARGE_SIZE);
    }

    @TearDown(Level.Trial)
    public void teardown() {
      Platform.freeMemory(nativeSrc);
      Platform.freeMemory(nativeDst);
    }

    // ---- a) disjoint arrays ----

    @Benchmark
    public void old_disjoint_arrays(Blackhole bh) {
      OldPlatform.copyMemory(
              srcArray, Platform.BYTE_ARRAY_OFFSET,
              dstArray, Platform.BYTE_ARRAY_OFFSET,
              srcArray.length);
      bh.consume(dstArray);
    }

    @Benchmark
    public void new_disjoint_arrays(Blackhole bh) {
      Platform.copyMemory(
              srcArray, Platform.BYTE_ARRAY_OFFSET,
              dstArray, Platform.BYTE_ARRAY_OFFSET,
              srcArray.length);
      bh.consume(dstArray);
    }

    // ---- b) same object, no overlap ----

    @Benchmark
    public void old_same_no_overlap(Blackhole bh) {
      // src=[base, base+256), dst=[base+512, base+768) – no overlap, but dstOffset > srcOffset
      // OldPlatform takes the reverse loop unconditionally
      OldPlatform.copyMemory(
              srcArray, Platform.BYTE_ARRAY_OFFSET,
              srcArray, Platform.BYTE_ARRAY_OFFSET + 512,
              256);
      bh.consume(srcArray);
    }

    @Benchmark
    public void new_same_no_overlap(Blackhole bh) {
      // Platform detects no overlap and uses the forward loop
      Platform.copyMemory(
              srcArray, Platform.BYTE_ARRAY_OFFSET,
              srcArray, Platform.BYTE_ARRAY_OFFSET + 512,
              256);
      bh.consume(srcArray);
    }

    // ---- c) large native copy ----

    @Benchmark
    public void old_large_native_copy(Blackhole bh) {
      // threshold = 1 MiB → 64 iterations
      OldPlatform.copyMemory(null, nativeSrc, null, nativeDst, LARGE_SIZE);
      bh.consume(nativeDst);
    }

    @Benchmark
    public void new_large_native_copy(Blackhole bh) {
      // threshold = 4 MiB → 16 iterations
      Platform.copyMemory(null, nativeSrc, null, nativeDst, LARGE_SIZE);
      bh.consume(nativeDst);
    }
  }

  public static void main(String[] args) throws RunnerException {
    String filter = args.length > 0 ?
            args[0] : PlatformBenchmark.class.getSimpleName();
    Options opt = new OptionsBuilder()
            .include(filter)
            .build();


    new Runner(opt).run();
  }
}