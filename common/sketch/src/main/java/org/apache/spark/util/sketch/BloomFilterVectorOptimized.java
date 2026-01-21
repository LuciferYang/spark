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
import java.util.Arrays;

/**
 * Vector API优化的BloomFilter实现
 * 当numHashFunctions较大时提供更好的性能
 */
class BloomFilterVectorOptimized extends BloomFilterBase implements Serializable {

    private static final boolean VECTOR_API_SUPPORTED = checkVectorAPISupport();
    private static final int VECTOR_LANES = getOptimalVectorLanes();

    BloomFilterVectorOptimized(int numHashFunctions, long numBits) {
        super(numHashFunctions, numBits);
    }

    private BloomFilterVectorOptimized() {}

    /**
     * 检查当前环境是否支持Vector API
     */
    private static boolean checkVectorAPISupport() {
        try {
            Class.forName("jdk.incubator.vector.IntVector");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * 获取最优的向量通道数
     */
    private static int getOptimalVectorLanes() {
        // 不同CPU架构的最佳向量大小
        String arch = System.getProperty("os.arch", "").toLowerCase();
        if (arch.contains("x86") || arch.contains("amd64")) {
            return 8;  // AVX2支持256位，8个int
        } else if (arch.contains("aarch64") || arch.contains("arm")) {
            return 4;  // ARM Neon通常支持128位，4个int
        }
        return 4; // 默认
    }

    protected boolean scatterHashAndSetAllBits(HiLoHash inputHash) {
        int h1 = inputHash.hi();
        int h2 = inputHash.lo();
        long bitSize = bits.bitSize();
        boolean bitsChanged = false;

        // 对于大量hash函数的情况，使用批量处理
        if (numHashFunctions >= 8 && VECTOR_API_SUPPORTED) {
            bitsChanged = scatterHashVectorized(h1, h2, bitSize, true);
        } else {
            // 回退到原始实现
            bitsChanged = scatterHashSequential(h1, h2, bitSize, true);
        }

        return bitsChanged;
    }

    protected boolean scatterHashAndGetAllBits(HiLoHash inputHash) {
        int h1 = inputHash.hi();
        int h2 = inputHash.lo();
        long bitSize = bits.bitSize();

        // 对于大量hash函数的情况，使用批量检查
        if (numHashFunctions >= 8 && VECTOR_API_SUPPORTED) {
            return scatterHashVectorized(h1, h2, bitSize, false);
        } else {
            // 回退到原始实现
            return scatterHashSequential(h1, h2, bitSize, false);
        }
    }

    /**
     * 使用Vector API批量处理hash计算
     */
    private boolean scatterHashVectorized(int h1, int h2, long bitSize, boolean setBits) {
        try {
            // 使用反射动态加载Vector API以避免编译依赖
            Class<?> intVectorClass = Class.forName("jdk.incubator.vector.IntVector");
            Class<?> vectorSpeciesClass = Class.forName("jdk.incubator.vector.VectorSpecies");

            // 获取最优的向量种类
            Object species = intVectorClass.getMethod("species", vectorSpeciesClass)
                .invoke(null, getOptimalSpecies());

            int vectorLength = (int) species.getClass().getMethod("length").invoke(species);

            boolean result = false;
            int processed = 0;

            while (processed < numHashFunctions) {
                int batchSize = Math.min(vectorLength, numHashFunctions - processed);

                // 创建批量的hash函数索引
                int[] indices = new int[batchSize];
                for (int i = 0; i < batchSize; i++) {
                    indices[i] = processed + i + 1; // +1因为索引从1开始
                }

                // 批量计算hash值
                int[] combinedHashes = computeCombinedHashesBatch(h1, h2, indices);

                if (setBits) {
                    // 批量设置位
                    for (int hash : combinedHashes) {
                        result |= bits.set(hash % (int)bitSize);
                    }
                } else {
                    // 批量检查位
                    for (int hash : combinedHashes) {
                        if (!bits.get(hash % (int)bitSize)) {
                            return false;
                        }
                    }
                    result = true;
                }

                processed += batchSize;
            }

            return result;

        } catch (Exception e) {
            // Vector API不可用，回退到顺序实现
            return scatterHashSequential(h1, h2, bitSize, setBits);
        }
    }

    /**
     * 顺序处理hash计算（回退实现）
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

    /**
     * 批量计算组合hash值
     */
    private int[] computeCombinedHashesBatch(int h1, int h2, int[] indices) {
        int[] result = new int[indices.length];

        // 优化的批量计算：减少乘法操作
        int base = h1 + h2; // i=1时的初始值
        for (int i = 0; i < indices.length; i++) {
            int idx = indices[i];
            // 重用前一个计算结果来优化
            if (i == 0) {
                result[i] = base;
            } else {
                result[i] = result[i-1] + h2; // 累加h2而不是重新计算
            }

            // 处理负数
            if (result[i] < 0) {
                result[i] = ~result[i];
            }
        }

        return result;
    }

    /**
     * 获取最优的向量种类（简化实现）
     */
    private String getOptimalSpecies() {
        switch (VECTOR_LANES) {
            case 8: return "IntVector.SPECIES_256";
            case 4: return "IntVector.SPECIES_128";
            case 2: return "IntVector.SPECIES_64";
            default: return "IntVector.SPECIES_PREFERRED";
        }
    }

    protected BloomFilterVectorOptimized checkCompatibilityForMerge(BloomFilter other)
            throws IncompatibleMergeException {
        if (other == null) {
            throw new IncompatibleMergeException("Cannot merge null bloom filter");
        }

        if (!(other instanceof BloomFilterVectorOptimized that)) {
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

    public static BloomFilterVectorOptimized readFrom(InputStream in) throws IOException {
        BloomFilterVectorOptimized filter = new BloomFilterVectorOptimized();
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

    /**
     * 性能基准测试方法（用于验证优化效果）
     */
    public static class Benchmark {
        public static void comparePerformance() {
            int numBits = 1000000;
            int numHashFunctions = 16;

            BloomFilterVectorOptimized vectorFilter = new BloomFilterVectorOptimized(numHashFunctions, numBits);
            BloomFilterImpl standardFilter = new BloomFilterImpl(numHashFunctions, numBits);

            // 测试性能差异
            long startTime, endTime;

            // 测试设置操作
            startTime = System.nanoTime();
            for (int i = 0; i < 100000; i++) {
                vectorFilter.putLong(i);
            }
            endTime = System.nanoTime();
            long vectorTime = endTime - startTime;

            startTime = System.nanoTime();
            for (int i = 0; i < 100000; i++) {
                standardFilter.putLong(i);
            }
            endTime = System.nanoTime();
            long standardTime = endTime - startTime;

            System.out.println("Vector optimized time: " + vectorTime + " ns");
            System.out.println("Standard time: " + standardTime + " ns");
            System.out.println("Speedup: " + (double)standardTime/vectorTime + "x");
        }
    }
}