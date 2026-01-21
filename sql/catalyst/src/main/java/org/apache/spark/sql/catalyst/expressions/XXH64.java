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
package org.apache.spark.sql.catalyst.expressions;

import java.nio.ByteOrder;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

// scalastyle: off
/**
 * xxHash64. A high quality and fast 64 bit hash code by Yann Colet and Mathias Westerdahl. The
 * class below is modelled like its Murmur3_x86_32 cousin.
 * <p/>
 * This was largely based on the following (original) C and Java implementations:
 * https://github.com/Cyan4973/xxHash/blob/master/xxhash.c
 * https://github.com/OpenHFT/Zero-Allocation-Hashing/blob/master/src/main/java/net/openhft/hashing/XxHash_r39.java
 * https://github.com/airlift/slice/blob/master/src/main/java/io/airlift/slice/XxHash64.java
 */
// scalastyle: on
public final class XXH64 {
  private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

  private final long seed;

  public XXH64(long seed) {
    super();
    this.seed = seed;
  }

  @Override
  public String toString() {
    return "xxHash64(seed=" + seed + ")";
  }

  public long hashInt(int input) {
    return hashInt(input, seed);
  }

  public static long hashInt(int input, long seed) {
    long hash = seed + PRIME64_5 + 4L;
    hash ^= (input & 0xFFFFFFFFL) * PRIME64_1;
    hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
    return fmix(hash);
  }

  public long hashLong(long input) {
    return hashLong(input, seed);
  }

  public static long hashLong(long input, long seed) {
    long hash = seed + PRIME64_5 + 8L;
    hash ^= Long.rotateLeft(input * PRIME64_2, 31) * PRIME64_1;
    hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
    return fmix(hash);
  }

  public long hashUnsafeWords(Object base, long offset, int length) {
    return hashUnsafeWords(base, offset, length, seed);
  }

  public static long hashUnsafeWords(Object base, long offset, int length, long seed) {
    assert (length % 8 == 0) : "lengthInBytes must be a multiple of 8 (word-aligned)";
    long hash = hashBytesByWords(base, offset, length, seed);
    return fmix(hash);
  }

  public long hashUnsafeBytes(Object base, long offset, int length) {
    return hashUnsafeBytes(base, offset, length, seed);
  }

  public static long hashUnsafeBytes(Object base, long offset, int length, long seed) {
    assert (length >= 0) : "lengthInBytes cannot be negative";
    long hash = hashBytesByWords(base, offset, length, seed);
    long end = offset + length;
    offset += length & -8;

    if (offset + 4L <= end) {
      int k1 = Platform.getInt(base, offset);
      if (isBigEndian) {
        k1 = Integer.reverseBytes(k1);
      }
      hash ^= (k1 & 0xFFFFFFFFL) * PRIME64_1;
      hash = Long.rotateLeft(hash, 23) * PRIME64_2 + PRIME64_3;
      offset += 4L;
    }

    // Process remaining 1-3 bytes - unrolled to reduce branch misprediction
    long remaining = end - offset;
    if (remaining > 0) {
      hash ^= (Platform.getByte(base, offset) & 0xFFL) * PRIME64_5;
      hash = Long.rotateLeft(hash, 11) * PRIME64_1;

      if (remaining > 1) {
        hash ^= (Platform.getByte(base, offset + 1) & 0xFFL) * PRIME64_5;
        hash = Long.rotateLeft(hash, 11) * PRIME64_1;

        if (remaining > 2) {
          hash ^= (Platform.getByte(base, offset + 2) & 0xFFL) * PRIME64_5;
          hash = Long.rotateLeft(hash, 11) * PRIME64_1;
        }
      }
    }
    return fmix(hash);
  }

  public static long hashUTF8String(UTF8String str, long seed) {
    return hashUnsafeBytes(str.getBaseObject(), str.getBaseOffset(), str.numBytes(), seed);
  }

  private static long fmix(long hash) {
    hash ^= hash >>> 33;
    hash *= PRIME64_2;
    hash ^= hash >>> 29;
    hash *= PRIME64_3;
    hash ^= hash >>> 32;
    return hash;
  }

  private static long hashBytesByWords(Object base, long offset, int length, long seed) {
    long end = offset + length;
    long hash;
    if (length >= 32) {
      long limit = end - 32;
      long v1 = seed + PRIME64_1 + PRIME64_2;
      long v2 = seed + PRIME64_2;
      long v3 = seed;
      long v4 = seed - PRIME64_1;

      do {
        // Extracted to inline-friendly method for better JIT optimization
        v1 = round(v1, getLongLE(base, offset));
        v2 = round(v2, getLongLE(base, offset + 8));
        v3 = round(v3, getLongLE(base, offset + 16));
        v4 = round(v4, getLongLE(base, offset + 24));
        offset += 32L;
      } while (offset <= limit);

      // Merge accumulators - extracted for better code organization and JIT inlining
      hash = mergeAccumulators(v1, v2, v3, v4);
    } else {
      hash = seed + PRIME64_5;
    }

    hash += length;

    long limit = end - 8;
    while (offset <= limit) {
      long k1 = getLongLE(base, offset);
      hash ^= Long.rotateLeft(k1 * PRIME64_2, 31) * PRIME64_1;
      hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
      offset += 8L;
    }
    return hash;
  }

  /**
   * Single round of hashing - extracted for JIT inlining
   */
  private static long round(long acc, long input) {
    acc += input * PRIME64_2;
    acc = Long.rotateLeft(acc, 31);
    return acc * PRIME64_1;
  }

  /**
   * Merge four accumulators into final hash value
   */
  private static long mergeAccumulators(long v1, long v2, long v3, long v4) {
    long hash = Long.rotateLeft(v1, 1)
            + Long.rotateLeft(v2, 7)
            + Long.rotateLeft(v3, 12)
            + Long.rotateLeft(v4, 18);

    hash = mergeRound(hash, v1);
    hash = mergeRound(hash, v2);
    hash = mergeRound(hash, v3);
    hash = mergeRound(hash, v4);

    return hash;
  }

  /**
   * Single merge round - extracted for JIT inlining
   */
  private static long mergeRound(long hash, long v) {
    v *= PRIME64_2;
    v = Long.rotateLeft(v, 31);
    v *= PRIME64_1;
    hash ^= v;
    return hash * PRIME64_1 + PRIME64_4;
  }

  /**
   * Read little-endian long value
   */
  private static long getLongLE(Object base, long offset) {
    long value = Platform.getLong(base, offset);
    return isBigEndian ? Long.reverseBytes(value) : value;
  }
}
