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

package org.apache.spark.storage.memory

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel

class EvictionPrioritySuite extends SparkFunSuite {

  test("StorageLevel evictionPriority default is 0") {
    assert(StorageLevel.MEMORY_AND_DISK.evictionPriority == 0)
    assert(StorageLevel.MEMORY_ONLY.evictionPriority == 0)
  }

  test("StorageLevel withEvictionPriority creates new level") {
    val low = StorageLevel.MEMORY_AND_DISK.withEvictionPriority(-1)
    assert(low.evictionPriority == -1)
    assert(low.useDisk == StorageLevel.MEMORY_AND_DISK.useDisk)
    assert(low.useMemory == StorageLevel.MEMORY_AND_DISK.useMemory)
    assert(low != StorageLevel.MEMORY_AND_DISK)
  }

  test("StorageLevel serialization round-trip preserves evictionPriority") {
    val level = StorageLevel.MEMORY_AND_DISK.withEvictionPriority(-5)
    val baos = new java.io.ByteArrayOutputStream()
    val out = new java.io.ObjectOutputStream(baos)
    level.writeExternal(out)
    out.flush()
    val bais = new java.io.ByteArrayInputStream(baos.toByteArray)
    val in = new java.io.ObjectInputStream(bais)
    val restored = new StorageLevel()
    restored.readExternal(in)
    assert(restored.evictionPriority == -5)
  }
}
