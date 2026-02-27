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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;

public final class Platform {

  private static final Unsafe _UNSAFE;

  public static final int BOOLEAN_ARRAY_OFFSET;

  public static final int BYTE_ARRAY_OFFSET;

  public static final int SHORT_ARRAY_OFFSET;

  public static final int INT_ARRAY_OFFSET;

  public static final int LONG_ARRAY_OFFSET;

  public static final int FLOAT_ARRAY_OFFSET;

  public static final int DOUBLE_ARRAY_OFFSET;

  private static final boolean unaligned;

  // Split java.version on non-digit chars:
  private static final int majorVersion =
          Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);

  // Access MethodHandles and VarHandle once and store them, for performance:
  // MethodHandle / VarHandle replace the original Constructor / Field / Method fields so that
  // the JIT can inline the call sites in allocateDirectBuffer() as if they were direct calls.
  private static final MethodHandle DBB_CONSTRUCTOR_MH;
  private static final VarHandle    DBB_CLEANER_VH;
  private static final MethodHandle CLEANER_CREATE_MH;

  static {
    // At the end of this block, CLEANER_CREATE_MH should be non-null iff it's possible to use
    // MethodHandles to invoke it, which is not necessarily possible by default in Java 9+.
    // Code below can test for null to see whether to use it.

    MethodHandle constructorMH = null;
    VarHandle    cleanerVH     = null;
    MethodHandle cleanerCreate = null;

    try {
      Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
      java.lang.reflect.Constructor<?> constructor = (majorVersion < 21) ?
              cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE) :
              cls.getDeclaredConstructor(Long.TYPE, Long.TYPE);
      Field cleanerField = cls.getDeclaredField("cleaner");

      if (constructor.trySetAccessible()) {
        // privateLookupIn provides full access to the non-public class so the
        // resulting MethodHandle can be invoked without IllegalAccessException.
        MethodHandles.Lookup lookup =
                MethodHandles.privateLookupIn(cls, MethodHandles.lookup());
        constructorMH = lookup.unreflectConstructor(constructor);
      }

      if (cleanerField.trySetAccessible()) {
        MethodHandles.Lookup lookup =
                MethodHandles.privateLookupIn(cls, MethodHandles.lookup());
        // VarHandle.set() is intrinsified by HotSpot; faster than Field.set().
        cleanerVH = lookup.unreflectVarHandle(cleanerField);
      }

      // no point continuing if the above failed:
      if (constructorMH != null && cleanerVH != null) {
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
          MethodHandles.Lookup cleanerLookup =
                  MethodHandles.privateLookupIn(cleanerClass, MethodHandles.lookup());
          cleanerCreate = cleanerLookup.unreflect(createMethod);
        } catch (IllegalAccessException e) {
          // Don't throw an exception, but can't log here?
          cleanerCreate = null;
        }
      }
    } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException |
             IllegalAccessException e) {
      // These are all fatal in any Java version - rethrow (have to wrap as this is a static block)
      throw new IllegalStateException(e);
    } catch (java.lang.reflect.InvocationTargetException ite) {
      throw new IllegalStateException(ite.getCause());
    }

    DBB_CONSTRUCTOR_MH = constructorMH;
    DBB_CLEANER_VH     = cleanerVH;
    CLEANER_CREATE_MH  = cleanerCreate;
  }

  // Visible for testing
  public static boolean cleanerCreateMethodIsDefined() {
    return CLEANER_CREATE_MH != null;
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and underlying
   *         system having unaligned-access capability.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  public static int getInt(Object object, long offset) {
    return _UNSAFE.getInt(object, offset);
  }

  public static void putInt(Object object, long offset, int value) {
    _UNSAFE.putInt(object, offset, value);
  }

  public static boolean getBoolean(Object object, long offset) {
    return _UNSAFE.getBoolean(object, offset);
  }

  public static void putBoolean(Object object, long offset, boolean value) {
    _UNSAFE.putBoolean(object, offset, value);
  }

  public static byte getByte(Object object, long offset) {
    return _UNSAFE.getByte(object, offset);
  }

  public static void putByte(Object object, long offset, byte value) {
    _UNSAFE.putByte(object, offset, value);
  }

  public static short getShort(Object object, long offset) {
    return _UNSAFE.getShort(object, offset);
  }

  public static void putShort(Object object, long offset, short value) {
    _UNSAFE.putShort(object, offset, value);
  }

  public static long getLong(Object object, long offset) {
    return _UNSAFE.getLong(object, offset);
  }

  public static void putLong(Object object, long offset, long value) {
    _UNSAFE.putLong(object, offset, value);
  }

  public static float getFloat(Object object, long offset) {
    return _UNSAFE.getFloat(object, offset);
  }

  public static void putFloat(Object object, long offset, float value) {
    _UNSAFE.putFloat(object, offset, value);
  }

  public static double getDouble(Object object, long offset) {
    return _UNSAFE.getDouble(object, offset);
  }

  public static void putDouble(Object object, long offset, double value) {
    _UNSAFE.putDouble(object, offset, value);
  }

  public static Object getObjectVolatile(Object object, long offset) {
    return _UNSAFE.getObjectVolatile(object, offset);
  }

  public static void putObjectVolatile(Object object, long offset, Object value) {
    _UNSAFE.putObjectVolatile(object, offset, value);
  }

  public static long allocateMemory(long size) {
    return _UNSAFE.allocateMemory(size);
  }

  public static void freeMemory(long address) {
    _UNSAFE.freeMemory(address);
  }

  public static long reallocateMemory(long address, long oldSize, long newSize) {
    // Delegate to Unsafe.reallocateMemory (maps to realloc(3)) so the allocator can resize
    // in-place when possible, avoiding the unconditional alloc + copy + free of the original.
    // oldSize is retained in the signature for source compatibility with existing call sites.
    return _UNSAFE.reallocateMemory(address, newSize);
  }

  /**
   * Allocate a DirectByteBuffer, potentially bypassing the JVM's MaxDirectMemorySize limit.
   */
  public static ByteBuffer allocateDirectBuffer(int size) {
    try {
      if (CLEANER_CREATE_MH == null) {
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
      // Invoke via MethodHandle; JIT-inlinable, ~3-5x faster than Constructor.newInstance().
      ByteBuffer buffer = (majorVersion < 21)
              ? (ByteBuffer) DBB_CONSTRUCTOR_MH.invoke(memory, size)
              : (ByteBuffer) DBB_CONSTRUCTOR_MH.invoke(memory, (long) size);
      try {
        // Cleaner.create(Object referent, Runnable action):
        //   referent = buffer  (the object whose GC triggers the cleanup action)
        //   action   = lambda that calls freeMemory(memory)
        // MethodHandle.invoke() is a polymorphic-signature method, so each argument's static
        // type is encoded into the call-site descriptor.  Cast buffer to Object explicitly to
        // match the declared (Object, Runnable) signature and avoid WrongMethodTypeException.
        DBB_CLEANER_VH.set(buffer,
                CLEANER_CREATE_MH.invoke((Object) buffer, (Runnable) () -> freeMemory(memory)));
      } catch (IllegalAccessException | java.lang.reflect.InvocationTargetException e) {
        freeMemory(memory);
        throw new IllegalStateException(e);
      }
      return buffer;
    } catch (Throwable e) {
      throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }

  public static void setMemory(Object object, long offset, long size, byte value) {
    _UNSAFE.setMemory(object, offset, size, value);
  }

  public static void setMemory(long address, byte value, long size) {
    _UNSAFE.setMemory(address, size, value);
  }

  public static void copyMemory(
          Object src, long srcOffset, Object dst, long dstOffset, long length) {
    if (src != dst) {
      // Different objects are always disjoint: unconditional forward copy, no direction check.
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
        srcOffset += size;
        dstOffset += size;
      }
    } else {
      // Same object (or both null = raw addresses): only reverse when ranges truly overlap
      // and dst falls inside the source region ahead of srcOffset.
      if (dstOffset <= srcOffset || srcOffset + length <= dstOffset) {
        // No overlap, or dst is entirely past src+length: safe forward copy.
        while (length > 0) {
          long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
          _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
          length -= size;
          srcOffset += size;
          dstOffset += size;
        }
      } else {
        // Overlapping, dst > src: must copy backwards to avoid clobbering unread bytes.
        srcOffset += length;
        dstOffset += length;
        while (length > 0) {
          long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
          srcOffset -= size;
          dstOffset -= size;
          _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
          length -= size;
        }
      }
    }
  }

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   */
  public static void throwException(Throwable t) {
    _UNSAFE.throwException(t);
  }

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
   * allow safepoint polling during a large copy.
   * On Java 17+ the JVM inserts safepoint polls at loop back-edges automatically, so a larger
   * threshold reduces loop overhead for big copies (e.g. Spark shuffle) without sacrificing GC
   * responsiveness.
   */
  private static final long UNSAFE_COPY_THRESHOLD =
          (majorVersion >= 17) ? 4L * 1024L * 1024L : 1024L * 1024L;

  static {
    sun.misc.Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      unsafe = null;
    }
    _UNSAFE = unsafe;

    if (_UNSAFE != null) {
      BOOLEAN_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(boolean[].class);
      BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
      SHORT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(short[].class);
      INT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(int[].class);
      LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);
      FLOAT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(float[].class);
      DOUBLE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(double[].class);
    } else {
      BOOLEAN_ARRAY_OFFSET = 0;
      BYTE_ARRAY_OFFSET = 0;
      SHORT_ARRAY_OFFSET = 0;
      INT_ARRAY_OFFSET = 0;
      LONG_ARRAY_OFFSET = 0;
      FLOAT_ARRAY_OFFSET = 0;
      DOUBLE_ARRAY_OFFSET = 0;
    }
  }

  // This requires `_UNSAFE`.
  static {
    boolean _unaligned;
    String arch = System.getProperty("os.arch", "");
    if (arch.equals("ppc64le") || arch.equals("ppc64") || arch.equals("s390x")) {
      // Since java.nio.Bits.unaligned() doesn't return true on ppc (See JDK-8165231), but
      // ppc64 and ppc64le support it
      _unaligned = true;
    } else {
      try {
        Class<?> bitsClass =
                Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
        if (_UNSAFE != null) {
          Field unalignedField = bitsClass.getDeclaredField("UNALIGNED");
          _unaligned = _UNSAFE.getBoolean(
                  _UNSAFE.staticFieldBase(unalignedField), _UNSAFE.staticFieldOffset(unalignedField));
        } else {
          Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
          unalignedMethod.setAccessible(true);
          _unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
        }
      } catch (Throwable t) {
        // We at least know x86 and x64 support unaligned access.
        //noinspection DynamicRegexReplaceableByCompiledPattern
        _unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64)$");
      }
    }
    unaligned = _unaligned;
  }
}
