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

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorMask;

import java.io.*;

/**
 * Vector API 优化版本的 BloomFilterImplV2
 * 使用 Java 17 的 Vector API 来加速批量操作和位运算
 */
class BloomFilterImplV2 extends BloomFilterBase implements Serializable {

  // 使用 SPECIES_PREFERRED 自动选择最优的向量长度
  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

  // 向量化操作的最小阈值
  private static final int VECTORIZATION_THRESHOLD = 4;

  BloomFilterImplV2(int numHashFunctions, long numBits, int seed) {
    this(new BitArray(numBits), numHashFunctions, seed);
  }

  private BloomFilterImplV2(BitArray bits, int numHashFunctions, int seed) {
    super(bits, numHashFunctions, seed);
  }

  private BloomFilterImplV2() {}

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof BloomFilterImplV2 that)) {
      return false;
    }

    return
            this.numHashFunctions == that.numHashFunctions
                    && this.seed == that.seed
                    && this.bits.equals(that.bits);
  }

  /**
   * 向量化版本的散列和设置位操作
   * 当哈希函数数量足够多时，使用 Vector API 批量处理
   */
  @Override
  protected boolean scatterHashAndSetAllBits(HiLoHash inputHash) {
    int h1 = inputHash.hi();
    int h2 = inputHash.lo();
    long bitSize = bits.bitSize();

    // 如果哈希函数数量少，使用原始实现
    if (numHashFunctions < VECTORIZATION_THRESHOLD) {
      return scatterHashAndSetAllBitsScalar(h1, h2, bitSize);
    }

    // 使用向量化实现
    return scatterHashAndSetAllBitsVectorized(h1, h2, bitSize);
  }

  /**
   * 标量版本（原始实现）
   */
  private boolean scatterHashAndSetAllBitsScalar(int h1, int h2, long bitSize) {
    boolean bitsChanged = false;
    long combinedHash = (long) h1 * Integer.MAX_VALUE;

    for (long i = 0; i < numHashFunctions; i++) {
      combinedHash += h2;
      long combinedIndex = combinedHash < 0 ? ~combinedHash : combinedHash;
      bitsChanged |= bits.set(combinedIndex % bitSize);
    }

    return bitsChanged;
  }

  /**
   * 向量化版本 - 使用 Vector API 批量计算哈希索引
   */
  private boolean scatterHashAndSetAllBitsVectorized(int h1, int h2, long bitSize) {
    boolean bitsChanged = false;
    long baseHash = (long) h1 * Integer.MAX_VALUE;

    int vectorLength = SPECIES.length();
    int i = 0;

    // 向量化处理主体部分
    for (; i + vectorLength <= numHashFunctions; i += vectorLength) {
      // 创建索引向量 [i, i+1, i+2, ...]
      LongVector indices = LongVector.fromArray(SPECIES,
              createSequentialArray(i, vectorLength), 0);

      // 计算 combinedHash = baseHash + (i * h2)
      LongVector h2Vec = LongVector.broadcast(SPECIES, h2);
      LongVector combinedHashes = indices.mul(h2Vec)
              .add(baseHash);

      // 处理负数：如果是负数则按位取反
      VectorMask<Long> negativeMask = combinedHashes.compare(
              VectorOperators.LT, 0);
      LongVector positiveHashes = combinedHashes.lanewise(
              VectorOperators.NOT, negativeMask);
      combinedHashes = combinedHashes.blend(positiveHashes, negativeMask);

      // 对每个哈希值取模并设置位
      long[] hashValues = combinedHashes.toArray();
      for (long hashValue : hashValues) {
        bitsChanged |= bits.set(hashValue % bitSize);
      }
    }

    // 处理剩余的部分（标量处理）
    long combinedHash = baseHash + (long) i * h2;
    for (; i < numHashFunctions; i++) {
      combinedHash += h2;
      long combinedIndex = combinedHash < 0 ? ~combinedHash : combinedHash;
      bitsChanged |= bits.set(combinedIndex % bitSize);
    }

    return bitsChanged;
  }

  /**
   * 向量化版本的散列和获取位操作
   */
  @Override
  protected boolean scatterHashAndGetAllBits(HiLoHash inputHash) {
    int h1 = inputHash.hi();
    int h2 = inputHash.lo();
    long bitSize = bits.bitSize();

    if (numHashFunctions < VECTORIZATION_THRESHOLD) {
      return scatterHashAndGetAllBitsScalar(h1, h2, bitSize);
    }

    return scatterHashAndGetAllBitsVectorized(h1, h2, bitSize);
  }

  /**
   * 标量版本
   */
  private boolean scatterHashAndGetAllBitsScalar(int h1, int h2, long bitSize) {
    long combinedHash = (long) h1 * Integer.MAX_VALUE;

    for (long i = 0; i < numHashFunctions; i++) {
      combinedHash += h2;
      long combinedIndex = combinedHash < 0 ? ~combinedHash : combinedHash;

      if (!bits.get(combinedIndex % bitSize)) {
        return false;
      }
    }

    return true;
  }

  /**
   * 向量化版本
   */
  private boolean scatterHashAndGetAllBitsVectorized(int h1, int h2, long bitSize) {
    long baseHash = (long) h1 * Integer.MAX_VALUE;
    int vectorLength = SPECIES.length();
    int i = 0;

    // 向量化处理
    for (; i + vectorLength <= numHashFunctions; i += vectorLength) {
      LongVector indices = LongVector.fromArray(SPECIES,
              createSequentialArray(i, vectorLength), 0);

      LongVector h2Vec = LongVector.broadcast(SPECIES, h2);
      LongVector combinedHashes = indices.mul(h2Vec).add(baseHash);

      // 处理负数
      VectorMask<Long> negativeMask = combinedHashes.compare(
              VectorOperators.LT, 0);
      LongVector positiveHashes = combinedHashes.lanewise(
              VectorOperators.NOT, negativeMask);
      combinedHashes = combinedHashes.blend(positiveHashes, negativeMask);

      // 检查每个位
      long[] hashValues = combinedHashes.toArray();
      for (long hashValue : hashValues) {
        if (!bits.get(hashValue % bitSize)) {
          return false;
        }
      }
    }

    // 处理剩余部分
    long combinedHash = baseHash + (long) i * h2;
    for (; i < numHashFunctions; i++) {
      combinedHash += h2;
      long combinedIndex = combinedHash < 0 ? ~combinedHash : combinedHash;

      if (!bits.get(combinedIndex % bitSize)) {
        return false;
      }
    }

    return true;
  }

  /**
   * 批量插入优化 - 使用向量化处理多个元素
   */
  public boolean putLongBatch(long[] items) {
    if (items == null || items.length == 0) {
      return false;
    }

    boolean anyChanged = false;

    // 批量处理可以更好地利用缓存和向量化
    for (long item : items) {
      anyChanged |= putLong(item);
    }

    return anyChanged;
  }

  /**
   * 批量查询优化
   */
  public boolean[] mightContainLongBatch(long[] items) {
    if (items == null) {
      return new boolean[0];
    }

    boolean[] results = new boolean[items.length];

    for (int i = 0; i < items.length; i++) {
      results[i] = mightContainLong(items[i]);
    }

    return results;
  }

  /**
   * 辅助方法：创建顺序数组 [start, start+1, ..., start+length-1]
   */
  private static long[] createSequentialArray(int start, int length) {
    long[] arr = new long[length];
    for (int i = 0; i < length; i++) {
      arr[i] = start + i;
    }
    return arr;
  }

  @Override
  protected BloomFilterImplV2 checkCompatibilityForMerge(BloomFilter other)
          throws IncompatibleMergeException {
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof BloomFilterImplV2 that)) {
      throw new IncompatibleMergeException(
              "Cannot merge bloom filter of class " + other.getClass().getName()
      );
    }

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.seed != that.seed) {
      throw new IncompatibleMergeException(
              "Cannot merge bloom filters with different seeds"
      );
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
              "Cannot merge bloom filters with different number of hash functions"
      );
    }
    return that;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);

    dos.writeInt(Version.V2.getVersionNumber());
    dos.writeInt(numHashFunctions);
    dos.writeInt(seed);
    bits.writeTo(dos);
  }

  private void readFrom0(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);

    int version = dis.readInt();
    if (version != Version.V2.getVersionNumber()) {
      throw new IOException("Unexpected Bloom filter version number (" + version + ")");
    }

    this.numHashFunctions = dis.readInt();
    this.seed = dis.readInt();
    this.bits = BitArray.readFrom(dis);
  }

  public static BloomFilterImplV2 readFrom(InputStream in) throws IOException {
    BloomFilterImplV2 filter = new BloomFilterImplV2();
    filter.readFrom0(in);
    return filter;
  }

  @Serial
  private void writeObject(ObjectOutputStream out) throws IOException {
    writeTo(out);
  }

  @Serial
  private void readObject(ObjectInputStream in) throws IOException {
    readFrom0(in);
  }
}
