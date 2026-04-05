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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Platform-dependent memory access backed by the Foreign Function & Memory (FFM) API.
 * <p>
 * This implementation requires Java 25+ and replaces the previous {@code sun.misc.Unsafe}-based
 * implementation. All public method signatures are preserved for backward compatibility.
 * <p>
 * Runtime requirements:
 * <ul>
 *   <li>{@code --enable-native-access=ALL-UNNAMED} (or the appropriate module name)</li>
 * </ul>
 */
@SuppressWarnings("restricted")
public final class Platform {

  // ==================== Native memory access ====================

  /**
   * A MemorySegment covering the entire native address space, equivalent to
   * Unsafe's unrestricted memory access model. Individual accesses are bounds-checked
   * against [0, Long.MAX_VALUE), which is always satisfied for valid malloc'd addresses.
   */
  private static final MemorySegment NATIVE =
    MemorySegment.ofAddress(0).reinterpret(Long.MAX_VALUE);

  // ==================== Array base offset constants ====================

  // These are backward-compatible sentinel values used by all callers.
  // Callers compute: ARRAY_OFFSET + position, then pass to get/put methods.
  // Internally we subtract HEAP_HEADER_BYTES to recover the zero-based position
  // for MemorySegment.ofArray(), which always starts addressing at element 0.
  //
  // The value 16 matches HotSpot's array object header size on 64-bit JVMs
  // (8-byte mark word + 4-byte compressed klass pointer + 4-byte array length),
  // but correctness does NOT depend on this — it is a symmetric add/subtract.
  private static final int HEAP_HEADER_BYTES = 16;

  public static final int BOOLEAN_ARRAY_OFFSET = HEAP_HEADER_BYTES;

  public static final int BYTE_ARRAY_OFFSET = HEAP_HEADER_BYTES;

  public static final int SHORT_ARRAY_OFFSET = HEAP_HEADER_BYTES;

  public static final int INT_ARRAY_OFFSET = HEAP_HEADER_BYTES;

  public static final int LONG_ARRAY_OFFSET = HEAP_HEADER_BYTES;

  public static final int FLOAT_ARRAY_OFFSET = HEAP_HEADER_BYTES;

  public static final int DOUBLE_ARRAY_OFFSET = HEAP_HEADER_BYTES;

  // ==================== ValueLayouts ====================

  // Use UNALIGNED variants for cross-type access patterns
  // (e.g., reading an int from a byte[] at an arbitrary byte offset).
  private static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
  private static final ValueLayout.OfBoolean LAYOUT_BOOLEAN = ValueLayout.JAVA_BOOLEAN;
  private static final ValueLayout.OfShort LAYOUT_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED;
  private static final ValueLayout.OfInt LAYOUT_INT = ValueLayout.JAVA_INT_UNALIGNED;
  private static final ValueLayout.OfLong LAYOUT_LONG = ValueLayout.JAVA_LONG_UNALIGNED;
  private static final ValueLayout.OfFloat LAYOUT_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED;
  private static final ValueLayout.OfDouble LAYOUT_DOUBLE = ValueLayout.JAVA_DOUBLE_UNALIGNED;

  // ==================== Linker handles for malloc / free ====================

  private static final MethodHandle MALLOC;
  private static final MethodHandle FREE;

  static {
    try {
      Linker linker = Linker.nativeLinker();
      SymbolLookup stdlib = linker.defaultLookup();

      MALLOC = linker.downcallHandle(
        stdlib.find("malloc").orElseThrow(),
        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

      FREE = linker.downcallHandle(
        stdlib.find("free").orElseThrow(),
        FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    } catch (Throwable e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ==================== Unaligned access capability ====================

  private static final boolean unaligned;

  static {
    String arch = System.getProperty("os.arch", "");
    if (arch.equals("ppc64le") || arch.equals("ppc64") || arch.equals("s390x")) {
      unaligned = true;
    } else {
      unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64)$");
    }
  }

  // ==================== Public API ====================

  // Visible for testing
  public static boolean cleanerCreateMethodIsDefined() {
    // With FFM-based allocateDirectBuffer, direct buffer allocation
    // with automatic cleanup is always supported.
    return true;
  }

  /**
   * @return true when the underlying system supports unaligned memory access.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  // -------------------- Element access --------------------

  public static int getInt(Object object, long offset) {
    if (object == null) {
      return NATIVE.get(LAYOUT_INT, offset);
    }
    return heapSegment(object).get(LAYOUT_INT, offset - HEAP_HEADER_BYTES);
  }

  public static void putInt(Object object, long offset, int value) {
    if (object == null) {
      NATIVE.set(LAYOUT_INT, offset, value);
    } else {
      heapSegment(object).set(LAYOUT_INT, offset - HEAP_HEADER_BYTES, value);
    }
  }

  public static boolean getBoolean(Object object, long offset) {
    if (object == null) {
      return NATIVE.get(LAYOUT_BOOLEAN, offset);
    }
    // MemorySegment.ofArray(boolean[]) is not available; use direct array access.
    if (object instanceof boolean[] ba) {
      return ba[(int) (offset - BOOLEAN_ARRAY_OFFSET)];
    }
    return heapSegment(object).get(LAYOUT_BOOLEAN, offset - HEAP_HEADER_BYTES);
  }

  public static void putBoolean(Object object, long offset, boolean value) {
    if (object == null) {
      NATIVE.set(LAYOUT_BOOLEAN, offset, value);
      return;
    }
    if (object instanceof boolean[] ba) {
      ba[(int) (offset - BOOLEAN_ARRAY_OFFSET)] = value;
      return;
    }
    heapSegment(object).set(LAYOUT_BOOLEAN, offset - HEAP_HEADER_BYTES, value);
  }

  public static byte getByte(Object object, long offset) {
    if (object == null) {
      return NATIVE.get(LAYOUT_BYTE, offset);
    }
    return heapSegment(object).get(LAYOUT_BYTE, offset - HEAP_HEADER_BYTES);
  }

  public static void putByte(Object object, long offset, byte value) {
    if (object == null) {
      NATIVE.set(LAYOUT_BYTE, offset, value);
    } else {
      heapSegment(object).set(LAYOUT_BYTE, offset - HEAP_HEADER_BYTES, value);
    }
  }

  public static short getShort(Object object, long offset) {
    if (object == null) {
      return NATIVE.get(LAYOUT_SHORT, offset);
    }
    return heapSegment(object).get(LAYOUT_SHORT, offset - HEAP_HEADER_BYTES);
  }

  public static void putShort(Object object, long offset, short value) {
    if (object == null) {
      NATIVE.set(LAYOUT_SHORT, offset, value);
    } else {
      heapSegment(object).set(LAYOUT_SHORT, offset - HEAP_HEADER_BYTES, value);
    }
  }

  public static long getLong(Object object, long offset) {
    if (object == null) {
      return NATIVE.get(LAYOUT_LONG, offset);
    }
    return heapSegment(object).get(LAYOUT_LONG, offset - HEAP_HEADER_BYTES);
  }

  public static void putLong(Object object, long offset, long value) {
    if (object == null) {
      NATIVE.set(LAYOUT_LONG, offset, value);
    } else {
      heapSegment(object).set(LAYOUT_LONG, offset - HEAP_HEADER_BYTES, value);
    }
  }

  public static float getFloat(Object object, long offset) {
    if (object == null) {
      return NATIVE.get(LAYOUT_FLOAT, offset);
    }
    return heapSegment(object).get(LAYOUT_FLOAT, offset - HEAP_HEADER_BYTES);
  }

  public static void putFloat(Object object, long offset, float value) {
    if (object == null) {
      NATIVE.set(LAYOUT_FLOAT, offset, value);
    } else {
      heapSegment(object).set(LAYOUT_FLOAT, offset - HEAP_HEADER_BYTES, value);
    }
  }

  public static double getDouble(Object object, long offset) {
    if (object == null) {
      return NATIVE.get(LAYOUT_DOUBLE, offset);
    }
    return heapSegment(object).get(LAYOUT_DOUBLE, offset - HEAP_HEADER_BYTES);
  }

  public static void putDouble(Object object, long offset, double value) {
    if (object == null) {
      NATIVE.set(LAYOUT_DOUBLE, offset, value);
    } else {
      heapSegment(object).set(LAYOUT_DOUBLE, offset - HEAP_HEADER_BYTES, value);
    }
  }

  // getObjectVolatile / putObjectVolatile: removed.
  // Codebase analysis confirms zero usages outside Platform.java itself.
  // If needed in the future, use java.lang.invoke.VarHandle.

  public static Object getObjectVolatile(Object object, long offset) {
    throw new UnsupportedOperationException(
      "getObjectVolatile is not supported in the FFM-based Platform. Use VarHandle instead.");
  }

  public static void putObjectVolatile(Object object, long offset, Object value) {
    throw new UnsupportedOperationException(
      "putObjectVolatile is not supported in the FFM-based Platform. Use VarHandle instead.");
  }

  // -------------------- Memory allocation --------------------

  public static long allocateMemory(long size) {
    try {
      MemorySegment result = (MemorySegment) MALLOC.invokeExact(size);
      long addr = result.address();
      if (addr == 0) {
        throw new OutOfMemoryError("Unable to allocate " + size + " bytes");
      }
      return addr;
    } catch (OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to allocate memory", t);
    }
  }

  public static void freeMemory(long address) {
    try {
      FREE.invokeExact(MemorySegment.ofAddress(address));
    } catch (Throwable t) {
      throw new RuntimeException("Failed to free memory at address " + address, t);
    }
  }

  public static long reallocateMemory(long address, long oldSize, long newSize) {
    long newMemory = allocateMemory(newSize);
    MemorySegment.copy(NATIVE, address, NATIVE, newMemory, oldSize);
    freeMemory(address);
    return newMemory;
  }

  // -------------------- DirectByteBuffer allocation --------------------

  /**
   * Allocate a DirectByteBuffer, bypassing the JVM's MaxDirectMemorySize limit.
   * Uses {@link Arena#ofAuto()} so the native memory is released when the returned
   * ByteBuffer becomes unreachable (GC-triggered).
   */
  public static ByteBuffer allocateDirectBuffer(int size) {
    Arena arena = Arena.ofAuto();
    MemorySegment segment = arena.allocate(size, 1);
    return segment.asByteBuffer().order(ByteOrder.nativeOrder());
  }

  // -------------------- Bulk memory operations --------------------

  public static void setMemory(Object object, long offset, long size, byte value) {
    if (object == null) {
      NATIVE.asSlice(offset, size).fill(value);
    } else {
      long pos = offset - HEAP_HEADER_BYTES;
      heapSegment(object).asSlice(pos, size).fill(value);
    }
  }

  public static void setMemory(long address, byte value, long size) {
    NATIVE.asSlice(address, size).fill(value);
  }

  public static void copyMemory(
    Object src, long srcOffset, Object dst, long dstOffset, long length) {
    if (length == 0) return;

    MemorySegment srcSeg;
    long srcPos;
    if (src == null) {
      srcSeg = NATIVE;
      srcPos = srcOffset;
    } else {
      srcSeg = heapSegment(src);
      srcPos = srcOffset - HEAP_HEADER_BYTES;
    }

    MemorySegment dstSeg;
    long dstPos;
    if (dst == null) {
      dstSeg = NATIVE;
      dstPos = dstOffset;
    } else {
      dstSeg = heapSegment(dst);
      dstPos = dstOffset - HEAP_HEADER_BYTES;
    }

    // MemorySegment.copy handles overlapping regions correctly
    // (copies as if through a temporary buffer when src and dst overlap).
    MemorySegment.copy(srcSeg, srcPos, dstSeg, dstPos, length);
  }

  // -------------------- Utility --------------------

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   * Uses generics erasure (sneaky throw) instead of Unsafe.throwException.
   */
  public static void throwException(Throwable t) {
    sneakyThrow(t);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable t) throws E {
    throw (E) t;
  }

  // ==================== Internal helpers ====================

  /**
   * Creates a MemorySegment view of the given heap array object.
   * <p>
   * In hot loops, the JIT compiler is expected to:
   * <ol>
   *   <li>Profile the dominant type (typically byte[]) and eliminate dead branches</li>
   *   <li>Scalar-replace the MemorySegment object via escape analysis</li>
   *   <li>Compile the {@code segment.get()} call to a direct array load instruction</li>
   * </ol>
   */
  private static MemorySegment heapSegment(Object obj) {
    if (obj instanceof byte[] ba) return MemorySegment.ofArray(ba);
    if (obj instanceof int[] ia) return MemorySegment.ofArray(ia);
    if (obj instanceof long[] la) return MemorySegment.ofArray(la);
    if (obj instanceof short[] sa) return MemorySegment.ofArray(sa);
    if (obj instanceof float[] fa) return MemorySegment.ofArray(fa);
    if (obj instanceof double[] da) return MemorySegment.ofArray(da);
    if (obj instanceof char[] ca) return MemorySegment.ofArray(ca);
    throw new UnsupportedOperationException(
      "Unsupported heap object type: " + obj.getClass().getName());
  }
}
