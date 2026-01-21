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

import java.io.*;
import java.util.stream.IntStream;

// Vector API需要在module-info.java中声明 requires jdk.incubator.vector;
// 由于兼容性考虑，这里使用条件编译

class BloomFilterImpl extends BloomFilterBase implements Serializable {

  BloomFilterImpl(int numHashFunctions, long numBits) {
    super(numHashFunctions, numBits);
  }

  private BloomFilterImpl() {}

  protected boolean scatterHashAndSetAllBits(HiLoHash inputHash) {
    int h1 = inputHash.hi();
    int h2 = inputHash.lo();
    long bitSize = bits.bitSize();

    // 对于大量hash函数的情况，使用批量处理优化
    if (numHashFunctions >= 4) {
      return scatterHashBatchOptimized(h1, h2, bitSize, true);
    } else {
      return scatterHashSequential(h1, h2, bitSize, true);
    }
  }

  /**
   * 批量优化的哈希计算实现
   */
  private boolean scatterHashBatchOptimized(int h1, int h2, long bitSize, boolean setBits) {
    boolean result = false;

    // 批量计算hash值，减少循环开销
    int batchSize = Math.min(8, numHashFunctions); // 每次处理8个hash函数
    int processed = 0;

    while (processed < numHashFunctions) {
      int currentBatch = Math.min(batchSize, numHashFunctions - processed);

      // 批量计算当前批次的hash值
      for (int i = 0; i < currentBatch; i++) {
        int idx = processed + i + 1;
        int combinedHash = h1 + (idx * h2);
        if (combinedHash < 0) {
          combinedHash = ~combinedHash;
        }
        int bitIndex = (int)(combinedHash % bitSize);

        if (setBits) {
          result |= bits.set(bitIndex);
        } else if (!bits.get(bitIndex)) {
          return false;
        }
      }

      processed += currentBatch;
    }

    return setBits ? result : true;
  }

  /**
   * 顺序哈希计算（适用于少量hash函数）
   */
  private boolean scatterHashSequential(int h1, int h2, long bitSize, boolean setBits) {
    boolean result = false;
    for (int i = 1; i <= numHashFunctions; i++) {
      int combinedHash = h1 + (i * h2);
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      int bitIndex = (int)(combinedHash % bitSize);

      if (setBits) {
        result |= bits.set(bitIndex);
      } else if (!bits.get(bitIndex)) {
        return false;
      }
    }
    return setBits ? result : true;
  }

  protected boolean scatterHashAndGetAllBits(HiLoHash inputHash) {
    int h1 = inputHash.hi();
    int h2 = inputHash.lo();
    long bitSize = bits.bitSize();

    // 对于大量hash函数的情况，使用批量处理优化
    if (numHashFunctions >= 4) {
      return scatterHashBatchOptimized(h1, h2, bitSize, false);
    } else {
      return scatterHashSequential(h1, h2, bitSize, false);
    }
  }

  protected BloomFilterImpl checkCompatibilityForMerge(BloomFilter other)
          throws IncompatibleMergeException {
    // Duplicates the logic of `isCompatible` here to provide better error message.
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof BloomFilterImpl that)) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filter of class " + other.getClass().getName()
      );
    }

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
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

    dos.writeInt(Version.V1.getVersionNumber());
    dos.writeInt(numHashFunctions);
    // ignore seed
    bits.writeTo(dos);
  }

  private void readFrom0(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);

    int version = dis.readInt();
    if (version != Version.V1.getVersionNumber()) {
      throw new IOException("Unexpected Bloom filter version number (" + version + ")");
    }

    this.numHashFunctions = dis.readInt();
    this.seed = DEFAULT_SEED;
    this.bits = BitArray.readFrom(dis);
  }

  public static BloomFilterImpl readFrom(InputStream in) throws IOException {
    BloomFilterImpl filter = new BloomFilterImpl();
    filter.readFrom0(in);
    return filter;
  }

  // no longer necessary, but can't remove without triggering MIMA violations
  @Deprecated
  public static BloomFilter readFrom(byte[] bytes) throws IOException {
    return BloomFilter.readFrom(bytes);
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
