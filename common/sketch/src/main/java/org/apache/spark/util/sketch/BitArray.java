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

package org.apache.spark.util.sketch;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

final class BitArray {
  private final long[] data;
  private final long bitSize;
  /**
   * Mask for fast index mapping when {@link #bitSize} is a power of 2, or {@code -1L} sentinel
   * for legacy deserialized filters whose bit array length is not a power of 2.
   */
  private final long bitMask;
  private long bitCount;

  static int numWords(long numBits) {
    if (numBits <= 0) {
      throw new IllegalArgumentException("numBits must be positive, but got " + numBits);
    }
    long numWords = (long) Math.ceil(numBits / 64.0);
    if (numWords > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Can't allocate enough space for " + numBits + " bits");
    }
    return (int) numWords;
  }

  /**
   * Rounds {@code numWords} up to the next power of 2, enabling bitmask-based index mapping in
   * hot hash paths. Returns {@code numWords} unchanged if it is already a power of 2 or if
   * rounding up would overflow a positive {@code int}.
   */
  static int roundUpToPowerOfTwo(int numWords) {
    int highestBit = Integer.highestOneBit(numWords);
    if (highestBit == numWords) {
      return numWords;
    }
    long doubled = (long) highestBit << 1;
    return doubled > Integer.MAX_VALUE ? numWords : (int) doubled;
  }

  BitArray(long numBits) {
    this(new long[roundUpToPowerOfTwo(numWords(numBits))]);
  }

  private BitArray(long[] data) {
    this.data = data;
    this.bitSize = (long) data.length * Long.SIZE;
    // If data.length is a power of 2 (and non-zero), bitSize is also a power of 2 and we can
    // replace (hash % bitSize) with (hash & bitMask) in hot paths. Otherwise, fall back to
    // modulo — this happens only for legacy filters deserialized via readFrom() where the
    // stored numWords predates the power-of-2 rounding introduced in the public constructor.
    this.bitMask = (data.length > 0 && (data.length & (data.length - 1)) == 0) ? bitSize - 1 : -1L;
    long bitCount = 0;
    for (long word : data) {
      bitCount += Long.bitCount(word);
    }
    this.bitCount = bitCount;
  }

  /**
   * Maps a non-negative {@code long} hash value to a bit index in {@code [0, bitSize)}.
   * Uses a bitmask fast path when {@link #bitSize} is a power of 2; falls back to {@code %}
   * for legacy bit arrays whose length is not a power of 2.
   *
   * <p>Callers must pass a non-negative hash. Keeping the sign-normalization
   * ({@code hash < 0 ? ~hash : hash}) at the call site preserves the historical hash-to-index
   * mapping so that filters written by the updated code remain readable by older Spark
   * versions (which apply modulo on power-of-2 {@code bitSize}, yielding identical indices for
   * non-negative inputs).
   */
  long indexFor(long hash) {
    return bitMask != -1L ? (hash & bitMask) : (hash % bitSize);
  }

  /** Returns true if the bit changed value. */
  boolean set(long index) {
    if (!get(index)) {
      data[(int) (index >>> 6)] |= (1L << index);
      bitCount++;
      return true;
    }
    return false;
  }

  boolean get(long index) {
    return (data[(int) (index >>> 6)] & (1L << index)) != 0;
  }

  /** Number of bits */
  long bitSize() {
    return bitSize;
  }

  /** Number of set bits (1s) */
  long cardinality() {
    return bitCount;
  }

  /** Combines the two BitArrays using bitwise OR. */
  void putAll(BitArray array) {
    assert data.length == array.data.length : "BitArrays must be of equal length when merging";
    long bitCount = 0;
    for (int i = 0; i < data.length; i++) {
      data[i] |= array.data[i];
      bitCount += Long.bitCount(data[i]);
    }
    this.bitCount = bitCount;
  }

  /** Combines the two BitArrays using bitwise AND. */
  void and(BitArray array) {
    assert data.length == array.data.length : "BitArrays must be of equal length when merging";
    long bitCount = 0;
    for (int i = 0; i < data.length; i++) {
      data[i] &= array.data[i];
      bitCount += Long.bitCount(data[i]);
    }
    this.bitCount = bitCount;
  }

  void writeTo(DataOutputStream out) throws IOException {
    out.writeInt(data.length);
    for (long datum : data) {
      out.writeLong(datum);
    }
  }

  static BitArray readFrom(DataInputStream in) throws IOException {
    int numWords = in.readInt();
    long[] data = new long[numWords];
    for (int i = 0; i < numWords; i++) {
      data[i] = in.readLong();
    }
    return new BitArray(data);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof BitArray that)) return false;
    return Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
}
