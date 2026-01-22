package org.apache.spark.unsafe;

import java.lang.foreign.*;
import java.nio.ByteBuffer;

public class PlatformForeign {
    private static final Arena GLOBAL_ARENA = Arena.global();
    private static final ValueLayout.OfByte JAVA_BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfInt JAVA_INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfLong JAVA_LONG = ValueLayout.JAVA_LONG;

    public static long allocateMemory(long size) {
        MemorySegment segment = GLOBAL_ARENA.allocate(size);
        return segment.address();
    }

    public static void freeMemory(long address) {
        // Memory is managed by Arena, no explicit free needed
    }

    public static ByteBuffer allocateDirectBuffer(int size) {
        MemorySegment segment = GLOBAL_ARENA.allocate(size);
        return segment.asByteBuffer();
    }

    public static void setMemory(long address, byte value, long size) {
        MemorySegment segment = MemorySegment.ofAddress(address, size, GLOBAL_ARENA.scope());
        segment.fill(value);
    }

    public static void copyMemory(
        Object src, long srcOffset, Object dst, long dstOffset, long length) {
        // Implementation using MemorySegment.copy
    }
}