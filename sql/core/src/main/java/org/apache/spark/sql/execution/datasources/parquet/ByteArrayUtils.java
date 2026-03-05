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
package org.apache.spark.sql.execution.datasources.parquet;

import java.nio.ByteOrder;

import org.apache.spark.unsafe.Platform;

/**
 * Utility class for Parquet data operations including datetime rebase and byte-level access.
 * Provides efficient methods to scan byte arrays and read values with proper endianness handling.
 */
public class ByteArrayUtils {

  private static final boolean BIG_ENDIAN_PLATFORM =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  /**
   * Finds the index of the first 4-byte little endian int in src that is less than threshold,
   * starting from srcIndex with count elements. Returns -1 if none found.
   *
   * This is used to optimize rebase operations by finding the first value that requires
   * rebasing, allowing bulk-write of values before the boundary.
   *
   * <p>Implementation uses a two-phase approach for performance:
   * <ul>
   *   <li>Phase 1: Branch-free scan using {@code |=} accumulation to detect if any value
   *       is below the threshold. This loop has no conditional exits, allowing the JIT
   *       compiler to auto-vectorize it into SIMD instructions.</li>
   *   <li>Phase 2: Only executed when Phase 1 finds a match. Uses an early-exit scan
   *       to locate the exact index of the first value below threshold.</li>
   * </ul>
   *
   * @param src the source byte array containing little-endian encoded integers
   * @param srcIndex the starting index in src
   * @param count the number of 4-byte integers to scan
   * @param threshold the comparison threshold
   * @return the index of the first int less than threshold, or -1 if none found
   */
  public static int findFirstIntLessThan(byte[] src, int srcIndex, int count, int threshold) {
    // Phase 1: branch-free detection — JIT can auto-vectorize this loop
    boolean found = false;
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    if (BIG_ENDIAN_PLATFORM) {
      for (int i = 0; i < count; i++, srcOffset += 4) {
        found |= Integer.reverseBytes(Platform.getInt(src, srcOffset)) < threshold;
      }
    } else {
      for (int i = 0; i < count; i++, srcOffset += 4) {
        found |= Platform.getInt(src, srcOffset) < threshold;
      }
    }
    if (!found) return -1;

    // Phase 2: locate exact index with early exit
    srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    if (BIG_ENDIAN_PLATFORM) {
      for (int i = 0; i < count; i++, srcOffset += 4) {
        if (Integer.reverseBytes(Platform.getInt(src, srcOffset)) < threshold) return i;
      }
    } else {
      for (int i = 0; i < count; i++, srcOffset += 4) {
        if (Platform.getInt(src, srcOffset) < threshold) return i;
      }
    }
    return -1; // unreachable, but needed for compilation
  }

  /**
   * Finds the index of the first 8-byte little endian long in src that is less than threshold,
   * starting from srcIndex with count elements. Returns -1 if none found.
   *
   * This is used to optimize rebase operations by finding the first value that requires
   * rebasing, allowing bulk-write of values before the boundary.
   *
   * <p>Implementation uses a two-phase approach for performance:
   * <ul>
   *   <li>Phase 1: Branch-free scan using {@code |=} accumulation to detect if any value
   *       is below the threshold. This loop has no conditional exits, allowing the JIT
   *       compiler to auto-vectorize it into SIMD instructions.</li>
   *   <li>Phase 2: Only executed when Phase 1 finds a match. Uses an early-exit scan
   *       to locate the exact index of the first value below threshold.</li>
   * </ul>
   *
   * @param src the source byte array containing little-endian encoded longs
   * @param srcIndex the starting index in src
   * @param count the number of 8-byte longs to scan
   * @param threshold the comparison threshold
   * @return the index of the first long less than threshold, or -1 if none found
   */
  public static int findFirstLongLessThan(byte[] src, int srcIndex, int count, long threshold) {
    // Phase 1: branch-free detection — JIT can auto-vectorize this loop
    boolean found = false;
    int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    if (BIG_ENDIAN_PLATFORM) {
      for (int i = 0; i < count; i++, srcOffset += 8) {
        found |= Long.reverseBytes(Platform.getLong(src, srcOffset)) < threshold;
      }
    } else {
      for (int i = 0; i < count; i++, srcOffset += 8) {
        found |= Platform.getLong(src, srcOffset) < threshold;
      }
    }
    if (!found) return -1;

    // Phase 2: locate exact index with early exit
    srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
    if (BIG_ENDIAN_PLATFORM) {
      for (int i = 0; i < count; i++, srcOffset += 8) {
        if (Long.reverseBytes(Platform.getLong(src, srcOffset)) < threshold) return i;
      }
    } else {
      for (int i = 0; i < count; i++, srcOffset += 8) {
        if (Platform.getLong(src, srcOffset) < threshold) return i;
      }
    }
    return -1; // unreachable, but needed for compilation
  }

  /**
   * Reads a 4-byte little-endian int from the byte array at the given index using Unsafe,
   * handling endianness automatically.
   */
  static int readIntLittleEndian(byte[] src, int srcIndex) {
    int raw = Platform.getInt(src, srcIndex + Platform.BYTE_ARRAY_OFFSET);
    return BIG_ENDIAN_PLATFORM ? Integer.reverseBytes(raw) : raw;
  }

  /**
   * Reads an 8-byte little-endian long from the byte array at the given index using Unsafe,
   * handling endianness automatically.
   */
  static long readLongLittleEndian(byte[] src, int srcIndex) {
    long raw = Platform.getLong(src, srcIndex + Platform.BYTE_ARRAY_OFFSET);
    return BIG_ENDIAN_PLATFORM ? Long.reverseBytes(raw) : raw;
  }
}

