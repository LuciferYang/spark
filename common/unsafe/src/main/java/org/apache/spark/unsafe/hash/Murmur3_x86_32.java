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

package org.apache.spark.unsafe.hash;

import java.nio.ByteOrder;

import org.apache.spark.unsafe.Platform;

/**
 * 32-bit Murmur3 hasher.  This is based on Guava's Murmur3_32HashFunction.
 */
public final class Murmur3_x86_32 {
  private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  private final int seed;

  public Murmur3_x86_32(int seed) {
    this.seed = seed;
  }

  @Override
  public String toString() {
    return "Murmur3_32(seed=" + seed + ")";
  }

  public int hashInt(int input) {
    return hashInt(input, seed);
  }

  public static int hashInt(int input, int seed) {
    int k1 = mixK1(input);
    int h1 = mixH1(seed, k1);

    return fmix(h1, 4);
  }

  public int hashUnsafeWords(Object base, long offset, int lengthInBytes) {
    return hashUnsafeWords(base, offset, lengthInBytes, seed);
  }

  public static int hashUnsafeWords(Object base, long offset, int lengthInBytes, int seed) {
    // This is based on Guava's `Murmur32_Hasher.processRemaining(ByteBuffer)` method.
    assert (lengthInBytes % 8 == 0): "lengthInBytes must be a multiple of 8 (word-aligned)";
    int h1 = hashBytesByInt(base, offset, lengthInBytes, seed);
    return fmix(h1, lengthInBytes);
  }

  public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes, int seed) {
    // This is not compatible with original and another implementations.
    // But remain it for backward compatibility for the components existing before 2.3.
    assert (lengthInBytes >= 0): "lengthInBytes cannot be negative";
    int lengthAligned = lengthInBytes - lengthInBytes % 4;
    int h1 = hashBytesByInt(base, offset, lengthAligned, seed);
    for (int i = lengthAligned; i < lengthInBytes; i++) {
      int halfWord = Platform.getByte(base, offset + i);
      int k1 = mixK1(halfWord);
      h1 = mixH1(h1, k1);
    }
    return fmix(h1, lengthInBytes);
  }

  public static int hashUnsafeBytes2(Object base, long offset, int lengthInBytes, int seed) {
    // This is compatible with original and another implementations.
    // Use this method for new components after Spark 2.3.
    assert (lengthInBytes >= 0): "lengthInBytes cannot be negative";
    int lengthAligned = lengthInBytes - lengthInBytes % 4;
    int h1 = hashBytesByInt(base, offset, lengthAligned, seed);
    int k1 = 0;
    for (int i = lengthAligned, shift = 0; i < lengthInBytes; i++, shift += 8) {
      k1 ^= (Platform.getByte(base, offset + i) & 0xFF) << shift;
    }
    h1 ^= mixK1(k1);
    return fmix(h1, lengthInBytes);
  }

  private static int hashBytesByInt(Object base, long offset, int lengthInBytes, int seed) {
    assert (lengthInBytes % 4 == 0);
    int h1 = seed;
    for (int i = 0; i < lengthInBytes; i += 4) {
      int halfWord = Platform.getInt(base, offset + i);
      if (isBigEndian) {
        halfWord = Integer.reverseBytes(halfWord);
      }
      h1 = mixH1(h1, mixK1(halfWord));
    }
    return h1;
  }

  public int hashLong(long input) {
    return hashLong(input, seed);
  }

  public static int hashLong(long input, int seed) {
    int low = (int) input;
    int high = (int) (input >>> 32);

    int k1 = mixK1(low);
    int h1 = mixH1(seed, k1);

    k1 = mixK1(high);
    h1 = mixH1(h1, k1);

    return fmix(h1, 8);
  }

  private static int mixK1(int k1) {
    k1 *= C1;
    k1 = Integer.rotateLeft(k1, 15);
    k1 *= C2;
    return k1;
  }

  private static int mixH1(int h1, int k1) {
    h1 ^= k1;
    h1 = Integer.rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  // Finalization mix - force all bits of a hash block to avalanche
  private static int fmix(int h1, int length) {
    h1 ^= length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }

  private static final int PRIME32_1 = 0x9E3779B1;
  private static final int PRIME32_2 = 0x85EBCA77;
  private static final int PRIME32_3 = 0xC2B2AE3D;
  private static final int PRIME32_4 = 0x27D4EB2F;
  private static final int PRIME32_5 = 0x165667B1;

  public static int hashUnsafeBytes3(Object base, long offset, int length, int seed) {
    int h32;
    int remaining = length;
    long ptr = offset;

    // 预取内存（提示 CPU 提前加载数据到缓存）
    if (remaining >= 64) {
      long end = ptr + remaining;
      for (long p = ptr; p < end; p += 64) {
        Platform.loadFence();
        Platform.getInt(base, p);
      }
    }

    if (remaining >= 16) {
      int v1 = seed + PRIME32_1 + PRIME32_2;
      int v2 = seed + PRIME32_2;
      int v3 = seed;
      int v4 = seed - PRIME32_1;

      // 每次处理 32 字节（8 个 int），手动展开循环
      while (remaining >= 32) {
        v1 = round(v1, getInt(base, ptr));
        v2 = round(v2, getInt(base, ptr + 4));
        v3 = round(v3, getInt(base, ptr + 8));
        v4 = round(v4, getInt(base, ptr + 12));
        v1 = round(v1, getInt(base, ptr + 16));
        v2 = round(v2, getInt(base, ptr + 20));
        v3 = round(v3, getInt(base, ptr + 24));
        v4 = round(v4, getInt(base, ptr + 28));

        ptr += 32;
        remaining -= 32;
      }

      h32 = round(v1, 1) + round(v2, 7) + round(v3, 12) + round(v4, 18);
    } else {
      h32 = seed + PRIME32_5;
    }

    h32 += length;

    // 合并处理剩余 4-byte 块（消除循环分支）
    int remainingAligned = remaining & ~3;
    if (remainingAligned >= 4) {
      int blocks = remainingAligned / 4;
      long endPtr = ptr + remainingAligned;
      do {
        h32 += getInt(base, ptr) * PRIME32_3;
        h32 = round(h32, 17) * PRIME32_4;
        ptr += 4;
      } while (ptr < endPtr);
      remaining -= remainingAligned;
    }

    // 合并处理尾部字节（避免逐字节循环）
    if (remaining > 0) {
      int k = 0;
      switch (remaining) {
        case 3: k ^= (Platform.getByte(base, ptr + 2) & 0xFF) << 16;
        case 2: k ^= (Platform.getByte(base, ptr + 1) & 0xFF) << 8;
        case 1: k ^= (Platform.getByte(base, ptr) & 0xFF);
      }
      h32 += k * PRIME32_5;
      h32 = round(h32, 11) * PRIME32_1;
    }

    // Final mix（合并计算步骤）
    h32 ^= h32 >>> 15;
    h32 *= PRIME32_2;
    h32 ^= h32 >>> 13;
    h32 *= PRIME32_3;
    h32 ^= h32 >>> 16;

    return h32;
  }

  private static int round(int acc, int input) {
    acc += input * PRIME32_2;
    acc = (acc << 13) | (acc >>> 19);
    return acc * PRIME32_1;
  }

  private static int getInt(Object base, long offset) {
    int value = Platform.getInt(base, offset);
    return isBigEndian ? Integer.reverseBytes(value) : value;
  }
}
