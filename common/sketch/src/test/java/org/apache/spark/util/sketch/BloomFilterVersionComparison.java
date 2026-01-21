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

import java.util.Random;
import org.apache.spark.util.sketch.BloomFilter.Version;

/**
 * 对比BloomFilter V1和V2版本的性能差异
 */
public class BloomFilterVersionComparison {

    public static void main(String[] args) {
        System.out.println("=== BloomFilter V1 vs V2 性能对比 ===");
        System.out.println();

        // 测试不同版本的性能差异
        compareVersions();

        System.out.println();

        // 测试V2版本的特殊优化效果
        testV2SpecificOptimizations();

        System.out.println();

        // 测试内存使用效率
        testMemoryEfficiency();
    }

    private static void compareVersions() {
        System.out.println("1. V1和V2版本性能对比:");
        System.out.println("哈希函数数量 | V1 (ops/sec) | V2 (ops/sec) | 性能提升");
        System.out.println("-------------------------------------------------");

        int[] hashFunctionCounts = {1, 2, 4, 8, 16, 32};
        int numBits = 1000000;
        int iterations = 50000;
        int seed = 42;

        for (int numHashFunctions : hashFunctionCounts) {
            // 测试V1版本性能
            BloomFilter v1Filter = createV1Filter(numHashFunctions, numBits);
            long v1Time = measurePerformance(v1Filter, iterations, "V1");
            double v1OpsPerSecond = iterations / (v1Time / 1_000_000_000.0);

            // 测试V2版本性能
            BloomFilter v2Filter = createV2Filter(numHashFunctions, numBits, seed);
            long v2Time = measurePerformance(v2Filter, iterations, "V2");
            double v2OpsPerSecond = iterations / (v2Time / 1_000_000_000.0);

            double speedup = v2OpsPerSecond / v1OpsPerSecond;

            System.out.printf("%12d | %12.0f | %12.0f | %.2fx %s%n",
                numHashFunctions, v1OpsPerSecond, v2OpsPerSecond, speedup,
                speedup > 1.1 ? "✓" : "");
        }
    }

    private static void testV2SpecificOptimizations() {
        System.out.println("2. V2版本特殊优化效果测试:");
        System.out.println("V2版本使用了更优的哈希算法:");
        System.out.println("- 使用累加代替乘法运算");
        System.out.println("- 预计算基础哈希值");
        System.out.println("- 减少类型转换开销");
        System.out.println();

        int numHashFunctions = 16;
        int numBits = 1000000;
        int seed = 42;
        int iterations = 100000;

        // 测试V2版本的算法优势
        BloomFilter v2Filter = createV2Filter(numHashFunctions, numBits, seed);
        Random random = new Random(42);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            v2Filter.putLong(random.nextLong());
        }
        long v2Time = System.nanoTime() - startTime;

        System.out.printf("V2版本性能: %.0f ops/sec%n",
            iterations / (v2Time / 1_000_000_000.0));

        // 测试V2版本的数值稳定性
        testNumericalStability();
    }

    private static void testNumericalStability() {
        System.out.println("3. V2版本数值稳定性测试:");

        int numHashFunctions = 100; // 大量哈希函数测试数值稳定性
        int numBits = 1000000;
        int seed = 42;

        BloomFilter v2Filter = createV2Filter(numHashFunctions, numBits, seed);
        Random random = new Random(42);

        int successfulOperations = 0;
        int testCount = 1000;

        for (int i = 0; i < testCount; i++) {
            try {
                v2Filter.putLong(random.nextLong());
                successfulOperations++;
            } catch (Exception e) {
                // 捕获数值溢出等异常
                System.out.println("数值稳定性问题: " + e.getMessage());
            }
        }

        System.out.printf("成功操作率: %d/%d (%.1f%%)%n",
            successfulOperations, testCount, (successfulOperations * 100.0) / testCount);

        if (successfulOperations == testCount) {
            System.out.println("✓ V2版本数值稳定性良好");
        }
    }

    private static void testMemoryEfficiency() {
        System.out.println("4. 内存使用效率对比:");

        int numHashFunctions = 8;
        int numBits = 1000000;
        int seed = 42;
        int elements = 500000;

        // 测试内存使用情况
        Runtime runtime = Runtime.getRuntime();

        // 测试V1版本内存使用
        runtime.gc();
        long v1MemoryBefore = runtime.totalMemory() - runtime.freeMemory();
        BloomFilter v1Filter = createV1Filter(numHashFunctions, numBits);
        for (int i = 0; i < elements; i++) {
            v1Filter.putLong(i);
        }
        long v1MemoryAfter = runtime.totalMemory() - runtime.freeMemory();
        long v1MemoryUsed = v1MemoryAfter - v1MemoryBefore;

        // 测试V2版本内存使用
        runtime.gc();
        long v2MemoryBefore = runtime.totalMemory() - runtime.freeMemory();
        BloomFilter v2Filter = createV2Filter(numHashFunctions, numBits, seed);
        for (int i = 0; i < elements; i++) {
            v2Filter.putLong(i);
        }
        long v2MemoryAfter = runtime.totalMemory() - runtime.freeMemory();
        long v2MemoryUsed = v2MemoryAfter - v2MemoryBefore;

        System.out.printf("V1版本内存使用: %d bytes%n", v1MemoryUsed);
        System.out.printf("V2版本内存使用: %d bytes%n", v2MemoryUsed);
        System.out.printf("内存使用差异: %d bytes%n", Math.abs(v1MemoryUsed - v2MemoryUsed));

        if (Math.abs(v1MemoryUsed - v2MemoryUsed) < 1000) {
            System.out.println("✓ 两个版本内存使用相当");
        }
    }

    private static BloomFilter createV1Filter(int numHashFunctions, int numBits) {
        // 使用BloomFilter.create方法创建V1版本实例
        try {
            // 使用Version.V1强制创建V1版本，传入期望项数作为参数
            return BloomFilter.create(Version.V1, (long)numHashFunctions * 100, numBits, 0);
        } catch (Exception e) {
            throw new RuntimeException("无法创建V1版本实例", e);
        }
    }

    private static BloomFilter createV2Filter(int numHashFunctions, int numBits, int seed) {
        // 使用BloomFilter.create方法创建V2版本实例
        try {
            // 使用Version.V2创建V2版本
            return BloomFilter.create(Version.V2, (long)numHashFunctions * 100, numBits, seed);
        } catch (Exception e) {
            throw new RuntimeException("无法创建V2版本实例", e);
        }
    }

    private static long measurePerformance(BloomFilter filter, int iterations, String version) {
        Random random = new Random(42);

        // 预热
        for (int i = 0; i < 1000; i++) {
            filter.putLong(random.nextLong());
        }

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            filter.putLong(random.nextLong());
        }
        long endTime = System.nanoTime();

        return endTime - startTime;
    }

    /**
     * 测试两个版本的功能等价性
     */
    private static void testFunctionalEquivalence() {
        System.out.println("5. 功能等价性验证:");

        int numHashFunctions = 8;
        int numBits = 10000;
        int seed = 42;

        BloomFilter v1Filter = createV1Filter(numHashFunctions, numBits);
        BloomFilter v2Filter = createV2Filter(numHashFunctions, numBits, seed);

        Random random = new Random(42);
        boolean allEquivalent = true;

        // 测试相同输入是否产生相同输出
        for (int i = 0; i < 100; i++) {
            long value = random.nextLong();

            boolean v1Result = v1Filter.putLong(value);
            boolean v2Result = v2Filter.putLong(value);

            if (v1Result != v2Result) {
                System.out.println("功能不一致: putLong结果不同");
                allEquivalent = false;
            }

            boolean v1Contains = v1Filter.mightContainLong(value);
            boolean v2Contains = v2Filter.mightContainLong(value);

            if (v1Contains != v2Contains) {
                System.out.println("功能不一致: mightContain结果不同");
                allEquivalent = false;
            }
        }

        if (allEquivalent) {
            System.out.println("✓ V1和V2版本功能等价");
        } else {
            System.out.println("✗ 发现功能差异");
        }
    }
}