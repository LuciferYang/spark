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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public final class Platform {
  private static final Arena GLOBAL_ARENA = Arena.global();

  public static final int BOOLEAN_ARRAY_OFFSET = (int) ValueLayout.JAVA_BOOLEAN.byteAlignment();
  public static final int BYTE_ARRAY_OFFSET = (int) ValueLayout.JAVA_BYTE.byteAlignment();
  public static final int SHORT_ARRAY_OFFSET = (int) ValueLayout.JAVA_SHORT.byteAlignment();
  public static final int INT_ARRAY_OFFSET = (int) ValueLayout.JAVA_INT.byteAlignment();
  public static final int LONG_ARRAY_OFFSET = (int) ValueLayout.JAVA_LONG.byteAlignment();
  public static final int FLOAT_ARRAY_OFFSET = (int) ValueLayout.JAVA_FLOAT.byteAlignment();
  public static final int DOUBLE_ARRAY_OFFSET = (int) ValueLayout.JAVA_DOUBLE.byteAlignment();

  private static final boolean unaligned;

  // Split java.version on non-digit chars:
  private static final int majorVersion =
    Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);

  // Access fields and constructors once and store them, for performance:
  private static final Constructor<?> DBB_CONSTRUCTOR;
  private static final Field DBB_CLEANER_FIELD;
  private static final Method CLEANER_CREATE_METHOD;

  static {
    // At the end of this block, CLEANER_CREATE_METHOD should be non-null iff it's possible to use
    // reflection to invoke it, which is not necessarily possible by default in Java 9+.
    // Code below can test for null to see whether to use it.

    try {
      Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
      Constructor<?> constructor = (majorVersion < 21) ?
        cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE) :
        cls.getDeclaredConstructor(Long.TYPE, Long.TYPE);
      Field cleanerField = cls.getDeclaredField("cleaner");
      if (!constructor.trySetAccessible()) {
        constructor = null;
      }
      if (!cleanerField.trySetAccessible()) {
        cleanerField = null;
      }
      // Have to set these values no matter what:
      DBB_CONSTRUCTOR = constructor;
      DBB_CLEANER_FIELD = cleanerField;

      // no point continuing if the above failed:
      if (DBB_CONSTRUCTOR != null && DBB_CLEANER_FIELD != null) {
        Class<?> cleanerClass = Class.forName("jdk.internal.ref.Cleaner");
        Method createMethod = cleanerClass.getMethod("create", Object.class, Runnable.class);
        // Accessing jdk.internal.ref.Cleaner should actually fail by default in JDK 9+,
        // unfortunately, unless the user has allowed access with something like
        // --add-opens java.base/jdk.internal.ref=ALL-UNNAMED  If not, we can't use the Cleaner
        // hack below. It doesn't break, just means the user might run into the default JVM limit
        // on off-heap memory and increase it or set the flag above. This tests whether it's
        // available:
        try {
          createMethod.invoke(null, null, null);
        } catch (IllegalAccessException e) {
          // Don't throw an exception, but can't log here?
          createMethod = null;
        }
        CLEANER_CREATE_METHOD = createMethod;
      } else {
        CLEANER_CREATE_METHOD = null;
      }
    } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e) {
      // These are all fatal in any Java version - rethrow (have to wrap as this is a static block)
      throw new IllegalStateException(e);
    } catch (InvocationTargetException ite) {
      throw new IllegalStateException(ite.getCause());
    }
  }

  // Visible for testing
  public static boolean cleanerCreateMethodIsDefined() {
    return CLEANER_CREATE_METHOD != null;
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and underlying
   *         system having unaligned-access capability.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  public static int getInt(Object object, long offset) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 4, GLOBAL_ARENA.scope());
    return segment.get(ValueLayout.JAVA_INT, 0);
  }

  public static void putInt(Object object, long offset, int value) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 4, GLOBAL_ARENA.scope());
    segment.set(ValueLayout.JAVA_INT, 0, value);
  }

  public static boolean getBoolean(Object object, long offset) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 1, GLOBAL_ARENA.scope());
    return segment.get(ValueLayout.JAVA_BYTE, 0) != 0;
  }

  public static void putBoolean(Object object, long offset, boolean value) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 1, GLOBAL_ARENA.scope());
    segment.set(ValueLayout.JAVA_BYTE, 0, value ? (byte)1 : (byte)0);
  }

  public static byte getByte(Object object, long offset) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 1, GLOBAL_ARENA.scope());
    return segment.get(ValueLayout.JAVA_BYTE, 0);
  }

  public static void putByte(Object object, long offset, byte value) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 1, GLOBAL_ARENA.scope());
    segment.set(ValueLayout.JAVA_BYTE, 0, value);
  }

  public static short getShort(Object object, long offset) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 2, GLOBAL_ARENA.scope());
    return segment.get(ValueLayout.JAVA_SHORT, 0);
  }

  public static void putShort(Object object, long offset, short value) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 2, GLOBAL_ARENA.scope());
    segment.set(ValueLayout.JAVA_SHORT, 0, value);
  }

  public static long getLong(Object object, long offset) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 8, GLOBAL_ARENA.scope());
    return segment.get(ValueLayout.JAVA_LONG, 0);
  }

  public static void putLong(Object object, long offset, long value) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 8, GLOBAL_ARENA.scope());
    segment.set(ValueLayout.JAVA_LONG, 0, value);
  }

  public static float getFloat(Object object, long offset) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 4, GLOBAL_ARENA.scope());
    return Float.intBitsToFloat(segment.get(ValueLayout.JAVA_INT, 0));
  }

  public static void putFloat(Object object, long offset, float value) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 4, GLOBAL_ARENA.scope());
    segment.set(ValueLayout.JAVA_INT, 0, Float.floatToIntBits(value));
  }

  public static double getDouble(Object object, long offset) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 8, GLOBAL_ARENA.scope());
    return Double.longBitsToDouble(segment.get(ValueLayout.JAVA_LONG, 0));
  }

  public static void putDouble(Object object, long offset, double value) {
    MemorySegment segment = MemorySegment.ofAddress(offset, 8, GLOBAL_ARENA.scope());
    segment.set(ValueLayout.JAVA_LONG, 0, Double.doubleToLongBits(value));
  }

  public static Object getObjectVolatile(Object object, long offset) {
    throw new UnsupportedOperationException("Volatile object access not supported with Foreign Memory API");
  }

  public static void putObjectVolatile(Object object, long offset, Object value) {
    throw new UnsupportedOperationException("Volatile object access not supported with Foreign Memory API");
  }

  public static long allocateMemory(long size) {
    MemorySegment segment = GLOBAL_ARENA.allocate(size);
    return segment.address();
  }

  public static void freeMemory(long address) {
    // Memory is managed by Arena, no explicit free needed
  }

  public static long reallocateMemory(long address, long oldSize, long newSize) {
    MemorySegment oldSegment = MemorySegment.ofAddress(address, oldSize, GLOBAL_ARENA.scope());
    MemorySegment newSegment = GLOBAL_ARENA.allocate(newSize);
    MemorySegment.copy(oldSegment, 0, newSegment, 0, oldSize);
    return newSegment.address();
  }

  /**
   * Allocate a DirectByteBuffer, potentially bypassing the JVM's MaxDirectMemorySize limit.
   */
  public static ByteBuffer allocateDirectBuffer(int size) {
    try {
      if (CLEANER_CREATE_METHOD == null) {
        // Can't set a Cleaner (see comments on field), so need to allocate via normal Java APIs
        try {
          return ByteBuffer.allocateDirect(size);
        } catch (OutOfMemoryError oome) {
          // checkstyle.off: RegexpSinglelineJava
          throw new OutOfMemoryError("Failed to allocate direct buffer (" + oome.getMessage() +
              "); try increasing -XX:MaxDirectMemorySize=... to, for example, your heap size");
          // checkstyle.on: RegexpSinglelineJava
        }
      }
      // Otherwise, use internal JDK APIs to allocate a DirectByteBuffer while ignoring the JVM's
      // MaxDirectMemorySize limit (the default limit is too low and we do not want to
      // require users to increase it).
      long memory = allocateMemory(size);
      ByteBuffer buffer = (ByteBuffer) DBB_CONSTRUCTOR.newInstance(memory, size);
      try {
        DBB_CLEANER_FIELD.set(buffer,
            CLEANER_CREATE_METHOD.invoke(null, buffer, (Runnable) () -> freeMemory(memory)));
      } catch (IllegalAccessException | InvocationTargetException e) {
        freeMemory(memory);
        throw new IllegalStateException(e);
      }
      return buffer;
    } catch (Exception e) {
      throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }

  public static void setMemory(Object object, long offset, long size, byte value) {
    MemorySegment segment = MemorySegment.ofAddress(offset, size, GLOBAL_ARENA.scope());
    segment.fill(value);
  }

  public static void setMemory(long address, byte value, long size) {
    MemorySegment segment = MemorySegment.ofAddress(address, size, GLOBAL_ARENA.scope());
    segment.fill(value);
  }

  public static void copyMemory(
    Object src, long srcOffset, Object dst, long dstOffset, long length) {
    MemorySegment srcSegment = MemorySegment.ofAddress(srcOffset, length, GLOBAL_ARENA.scope());
    MemorySegment dstSegment = MemorySegment.ofAddress(dstOffset, length, GLOBAL_ARENA.scope());
    MemorySegment.copy(srcSegment, 0, dstSegment, 0, length);
  }

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   */
  public static void throwException(Throwable t) {
    try {
      throw t;
    } catch (Throwable e) {
      throw new RuntimeException("Failed to throw exception", e);
    }
  }

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
   * allow safepoint polling during a large copy.
   */
  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  static {
    // Initialize array offsets using MemoryLayout
    BOOLEAN_ARRAY_OFFSET = (int) ValueLayout.JAVA_BOOLEAN.byteAlignment();
    BYTE_ARRAY_OFFSET = (int) ValueLayout.JAVA_BYTE.byteAlignment();
    SHORT_ARRAY_OFFSET = (int) ValueLayout.JAVA_SHORT.byteAlignment();
    INT_ARRAY_OFFSET = (int) ValueLayout.JAVA_INT.byteAlignment();
    LONG_ARRAY_OFFSET = (int) ValueLayout.JAVA_LONG.byteAlignment();
    FLOAT_ARRAY_OFFSET = (int) ValueLayout.JAVA_FLOAT.byteAlignment();
    DOUBLE_ARRAY_OFFSET = (int) ValueLayout.JAVA_DOUBLE.byteAlignment();
  }

  /**
   * Determine if the platform supports unaligned memory access.
   * This uses a simplified approach based on architecture.
   */
  static {
    boolean _unaligned;
    String arch = System.getProperty("os.arch", "");
    // Assume modern architectures support unaligned access
    _unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64|ppc64le|ppc64|s390x)$");
    unaligned = _unaligned;
  }
}
