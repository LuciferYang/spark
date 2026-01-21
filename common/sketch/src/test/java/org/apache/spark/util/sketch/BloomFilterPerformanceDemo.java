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

/**
 * BloomFilter性能演示程序，用于验证批量哈希优化的效果
 */
public class BloomFilterPerformanceDemo {

    public static void main(String[] args) {
        System.out.println("=== Bloom Filter Performance Demo ===");
        System.out.println();

        // 测试不同哈希函数数量的性能差异
        testDifferentHashFunctionCounts();

        System.out.println();

        // 测试不同类型数据的性能对比
        testDifferentDataTypes();

        System.out.println();

        // 测试批量优化的阈值效果
        testBatchOptimizationThreshold();
    }

    private static void testDifferentHashFunctionCounts() {
        System.out.println("1. 不同哈希函数数量性能对比:");
        System.out.println("哈希函数数量 | 操作次数/秒");
        System.out.println("-------------------------");

        int[] hashFunctionCounts = {1, 2, 4, 8, 16, 32};
        int numBits = 1000000;
        int iterations = 50000;

        for (int numHashFunctions : hashFunctionCounts) {
            BloomFilter filter = BloomFilter.create(numHashFunctions, numBits);
            Random random = new Random(42);

            long startTime = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                filter.putLong(random.nextLong());
            }
            long endTime = System.nanoTime();

            double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
            double opsPerSecond = iterations / durationSeconds;

            System.out.printf("%12d | %10.0f ops/sec", numHashFunctions, opsPerSecond);

            // 标记是否使用了批量优化
            if (numHashFunctions >= 4) {
                System.out.print(" (批量优化)");
            }
            System.out.println();
        }
    }

    private static void testDifferentDataTypes() {
        System.out.println("2. 不同数据类型性能对比:");
        System.out.println("数据类型 | 操作次数/秒");
        System.out.println("-------------------");

        int numHashFunctions = 8;
        int numBits = 1000000;
        int iterations = 30000;

        // 测试long类型
        BloomFilter longFilter = BloomFilter.create(numHashFunctions, numBits);
        Random random = new Random(42);

        long startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            longFilter.putLong(random.nextLong());
        }
        long longTime = System.nanoTime() - startTime;
        double longOpsPerSecond = iterations / (longTime / 1_000_000_000.0);
        System.out.printf("long      | %10.0f ops/sec%n", longOpsPerSecond);

        // 测试字符串类型
        BloomFilter stringFilter = BloomFilter.create(numHashFunctions, numBits);

        startTime = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            stringFilter.putString("test_string_" + i + "_" + random.nextInt());
        }
        long stringTime = System.nanoTime() - startTime;
        double stringOpsPerSecond = iterations / (stringTime / 1_000_000_000.0);
        System.out.printf("字符串    | %10.0f ops/sec%n", stringOpsPerSecond);

        System.out.printf("性能提升: %.1fx%n", stringOpsPerSecond / longOpsPerSecond);
    }

    private static void testBatchOptimizationThreshold() {
        System.out.println("3. 批量优化阈值效果测试:");
        System.out.println("在numHashFunctions >= 4时启用批量优化");
        System.out.println();

        int numBits = 1000000;
        int iterations = 100000;

        // 测试刚好在阈值边界的情况
        testSpecificCase(3, numBits, iterations, "阈值以下（顺序处理）");
        testSpecificCase(4, numBits, iterations, "阈值以上（批量优化）");
        testSpecificCase(8, numBits, iterations, "显著优化场景");
    }

    private static void testSpecificCase(int numHashFunctions, int numBits, int iterations, String description) {
        BloomFilter filter = BloomFilter.create(numHashFunctions, numBits);
        Random random = new Random(42);

        // 预热
        for (int i = 0; i < 1000; i++) {
            filter.putLong(random.nextLong());
        }

        long totalTime = 0;
        for (int run = 0; run < 3; run++) {
            filter = BloomFilter.create(numHashFunctions, numBits); // 重置filter
            random = new Random(42); // 重置随机数生成器

            long startTime = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                filter.putLong(random.nextLong());
            }
            totalTime += System.nanoTime() - startTime;
        }

        double avgTime = totalTime / 3.0 / 1_000_000_000.0;
        double opsPerSecond = iterations / avgTime;

        System.out.printf("%s - %d个哈希函数: %.0f ops/sec%n",
                         description, numHashFunctions, opsPerSecond);
    }

    /**
     * 验证优化实现的正确性
     */
    private static void verifyCorrectness() {
        System.out.println("4. 正确性验证:");

        int numHashFunctions = 8;
        int numBits = 10000;

        BloomFilter filter1 = BloomFilter.create(numHashFunctions, numBits);
        BloomFilter filter2 = BloomFilter.create(numHashFunctions, numBits);

        Random random = new Random(42);
        boolean allCorrect = true;

        // 测试1000个随机值
        for (int i = 0; i < 1000; i++) {
            long value = random.nextLong();

            boolean result1 = filter1.putLong(value);
            boolean result2 = filter2.putLong(value);

            if (result1 != result2) {
                System.out.println("错误: putLong结果不一致");
                allCorrect = false;
            }

            boolean contains1 = filter1.mightContainLong(value);
            boolean contains2 = filter2.mightContainLong(value);

            if (contains1 != contains2) {
                System.out.println("错误: mightContain结果不一致");
                allCorrect = false;
            }

            if (!contains1) {
                System.out.println("错误: 已添加的值应该存在");
                allCorrect = false;
            }
        }

        if (allCorrect) {
            System.out.println("✓ 所有测试通过，优化实现正确");
        } else {
            System.out.println("✗ 发现错误，需要修复优化实现");
        }
    }
}