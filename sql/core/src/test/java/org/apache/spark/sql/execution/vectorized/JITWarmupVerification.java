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
package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.Platform;

/**
 * Verifies whether data is actually written to off-heap memory during
 * the "fast" early iterations before JIT C2 compilation kicks in.
 *
 * Run with: -XX:+PrintCompilation to observe compilation events.
 *
 * <pre>{@code
 *   java -XX:+PrintCompilation -cp "..." \
 *     org.apache.spark.sql.execution.vectorized.JITWarmupVerification
 * }</pre>
 */
public class JITWarmupVerification {

    private static final int BATCH_SIZE = 4096;
    private static final byte FILL_VALUE = 42;

    public static void main(String[] args) {
        OldOffHeapColumnVector vector =
            new OldOffHeapColumnVector(BATCH_SIZE, DataTypes.ByteType);
        long dataAddr = vector.valuesNativeAddress();

        System.out.println("=== JIT Warmup Verification ===");
        System.out.println("Batch size: " + BATCH_SIZE);
        System.out.println("Fill value: " + FILL_VALUE);
        System.out.println();

        // Clear memory to 0 first
        Platform.setMemory(null, dataAddr, BATCH_SIZE, (byte) 0);

        for (int round = 1; round <= 10; round++) {
            // Clear to 0
            Platform.setMemory(null, dataAddr, BATCH_SIZE, (byte) 0);

            // Time the putBytes call
            long start = System.nanoTime();
            vector.putBytes(0, BATCH_SIZE, FILL_VALUE);
            long elapsed = System.nanoTime() - start;

            // Verify: check first, middle, and last byte
            byte first  = Platform.getByte(null, dataAddr);
            byte mid    = Platform.getByte(null, dataAddr + BATCH_SIZE / 2);
            byte last   = Platform.getByte(null, dataAddr + BATCH_SIZE - 1);
            boolean ok  = (first == FILL_VALUE && mid == FILL_VALUE && last == FILL_VALUE);

            // Count how many bytes are actually written
            int written = 0;
            for (int i = 0; i < BATCH_SIZE; i++) {
                if (Platform.getByte(null, dataAddr + i) == FILL_VALUE) {
                    written++;
                }
            }

            System.out.printf("Round %2d: %8d ns | data_ok=%s | written=%d/%d%s%n",
                round, elapsed, ok, written, BATCH_SIZE,
                (elapsed < 200) ? "  <<<< SUSPICIOUSLY FAST" : "");
        }

        vector.close();
        System.out.println("\nDone.");
    }
}

