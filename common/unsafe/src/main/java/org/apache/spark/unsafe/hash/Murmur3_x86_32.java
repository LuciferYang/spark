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

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.spark.unsafe.Platform;

/**
 * 32-bit Murmur3 hasher with aggressive SIMD optimization using parallel accumulators.
 *
 * IMPORTANT: This implementation uses Java 17+ Vector API (incubator module).
 * To compile and run, add JVM flags:
 *   --add-modules jdk.incubator.vector
 *
 * WARNING: This version uses parallel hash accumulators which changes the hash output.
 * It is NOT compatible with standard Murmur3 output but provides much better performance.
 * Only use this if you control both hash generation and consumption.
 */
public final class Murmur3_x86_32_SIMD_Parallel {
  private static final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  // Vector API species for 128-bit (4 x int32)
  private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_128;
  private static final int VECTOR_LANES = SPECIES.length(); // 4

  // Threshold to use SIMD
  private static final int SIMD_THRESHOLD = 64;

  private final int seed;

  public Murmur3_x86_32_SIMD_Parallel(int seed) {
    this.seed = seed;
  }

  @Override
  public String toString() {
    return "Murmur3_32_SIMD_Parallel(seed=" + seed + ")";
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
    assert (lengthInBytes % 8 == 0): "lengthInBytes must be a multiple of 8 (word-aligned)";
    int h1 = hashBytesByInt(base, offset, lengthInBytes, seed);
    return fmix(h1, lengthInBytes);
  }

  public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes, int seed) {
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

  /**
   * Hash bytes using SIMD with parallel accumulators for large arrays.
   */
  private static int hashBytesByInt(Object base, long offset, int lengthInBytes, int seed) {
    assert (lengthInBytes % 4 == 0);

    if (lengthInBytes >= SIMD_THRESHOLD && base instanceof byte[]) {
      return hashBytesByIntSIMDParallel((byte[]) base, (int) offset, lengthInBytes, seed);
    }

    return hashBytesByIntScalar(base, offset, lengthInBytes, seed);
  }

  /**
   * Scalar version - same as original.
   */
  private static int hashBytesByIntScalar(Object base, long offset, int lengthInBytes, int seed) {
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

  /**
   * SIMD version with 4 parallel accumulators.
   *
   * Strategy: Use 4 independent hash accumulators (h1, h2, h3, h4) to break
   * the data dependency chain. Process them fully in parallel using SIMD,
   * then combine at the end.
   *
   * This achieves true parallelism but produces different hash values than
   * the standard Murmur3 implementation.
   */
  private static int hashBytesByIntSIMDParallel(byte[] base, int offset, int lengthInBytes, int seed) {
    // Initialize 4 parallel accumulators with different seeds
    IntVector h = IntVector.broadcast(SPECIES, seed)
            .add(IntVector.fromArray(SPECIES, new int[]{0, 1, 2, 3}, 0));

    IntVector c1Vec = IntVector.broadcast(SPECIES, C1);
    IntVector c2Vec = IntVector.broadcast(SPECIES, C2);
    IntVector mixMult = IntVector.broadcast(SPECIES, 5);
    IntVector mixAdd = IntVector.broadcast(SPECIES, 0xe6546b64);

    int i = 0;
    int vectorBytes = VECTOR_LANES * 4; // 16 bytes
    int vectorLimit = lengthInBytes - (vectorBytes - 1);

    // Process 16 bytes at a time with full SIMD parallelism
    for (; i < vectorLimit; i += vectorBytes) {
      // Load 4 integers
      IntVector data = IntVector.fromArray(SPECIES, base, offset + i, ByteOrder.LITTLE_ENDIAN);

      // Apply mixK1 in parallel
      // k1 *= C1
      IntVector k1 = data.mul(c1Vec);

      // k1 = rotateLeft(k1, 15)
      // rotateLeft(x, n) = (x << n) | (x >>> (32 - n))
      k1 = k1.lanewise(VectorOperators.LSHL, 15)
              .lanewise(VectorOperators.OR, k1.lanewise(VectorOperators.LSHR, 17));

      // k1 *= C2
      k1 = k1.mul(c2Vec);

      // Apply mixH1 in parallel (each lane is independent)
      // h ^= k1
      h = h.lanewise(VectorOperators.XOR, k1);

      // h = rotateLeft(h, 13)
      h = h.lanewise(VectorOperators.LSHL, 13)
              .lanewise(VectorOperators.OR, h.lanewise(VectorOperators.LSHR, 19));

      // h = h * 5 + 0xe6546b64
      h = h.mul(mixMult).add(mixAdd);
    }

    // Process remaining bytes with scalar code
    int[] hArray = h.toArray();
    int h1 = hArray[0];
    for (; i < lengthInBytes; i += 4) {
      int halfWord = Platform.getInt(base, offset + i);
      if (isBigEndian) {
        halfWord = Integer.reverseBytes(halfWord);
      }
      h1 = mixH1(h1, mixK1(halfWord));
    }

    // Combine the 4 accumulators
    // XOR them together, then apply additional mixing
    h1 ^= hArray[1];
    h1 ^= hArray[2];
    h1 ^= hArray[3];

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
}
