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

package org.apache.spark.storage

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.memory.MemoryMode
import org.apache.spark.util.SparkErrorUtils

/**
 * :: DeveloperApi ::
 * Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
 * or ExternalBlockStore, whether to drop the RDD to disk if it falls out of memory or
 * ExternalBlockStore, whether to keep the data in memory in a serialized format, and whether
 * to replicate the RDD partitions on multiple nodes.
 *
 * The [[org.apache.spark.storage.StorageLevel]] singleton object contains some static constants
 * for commonly useful storage levels. To create your own storage level object, use the
 * factory method of the singleton object (`StorageLevel(...)`).
 */
@DeveloperApi
class StorageLevel private(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useOffHeap: Boolean,
    private var _deserialized: Boolean,
    private var _replication: Int = 1,
    private var _evictionPriority: Int = 0)
  extends Externalizable {

  // Needed by Py4J which uses Java reflection to find constructors by parameter types.
  // Scala default parameters don't generate overloaded constructors in bytecode.
  private[spark] def this(
      useDisk: Boolean,
      useMemory: Boolean,
      useOffHeap: Boolean,
      deserialized: Boolean,
      replication: Int) = {
    this(useDisk, useMemory, useOffHeap, deserialized, replication, 0)
  }

  private def this(flags: Int, replication: Int) = {
    this((flags & 8) != 0, (flags & 4) != 0, (flags & 2) != 0, (flags & 1) != 0, replication, 0)
  }

  def this() = this(false, true, false, false)  // For deserialization

  def useDisk: Boolean = _useDisk
  def useMemory: Boolean = _useMemory
  def useOffHeap: Boolean = _useOffHeap
  def deserialized: Boolean = _deserialized
  def replication: Int = _replication
  def evictionPriority: Int = _evictionPriority

  assert(replication < 40, "Replication restricted to be less than 40 for calculating hash codes")

  private[spark] def memoryMode: MemoryMode = {
    if (useOffHeap) MemoryMode.OFF_HEAP
    else MemoryMode.ON_HEAP
  }

  override def clone(): StorageLevel = {
    new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication, evictionPriority)
  }

  override def equals(other: Any): Boolean = other match {
    case s: StorageLevel =>
      s.useDisk == useDisk &&
      s.useMemory == useMemory &&
      s.useOffHeap == useOffHeap &&
      s.deserialized == deserialized &&
      s.replication == replication &&
      s.evictionPriority == evictionPriority
    case _ =>
      false
  }

  def isValid: Boolean = (useMemory || useDisk) && (replication > 0)

  def toInt: Int = {
    var ret = 0
    if (_useDisk) {
      ret |= 8
    }
    if (_useMemory) {
      ret |= 4
    }
    if (_useOffHeap) {
      ret |= 2
    }
    if (_deserialized) {
      ret |= 1
    }
    ret
  }

  // Serialization: [flags:1byte][replication:1byte][priority:4bytes] = 6 bytes.
  // Always writes 6 bytes -- Externalizable has no object boundaries, so the
  // reader must consume exactly what the writer wrote. readExternal handles
  // old 2-byte data (without priority) via try-catch, defaulting to 0.

  override def writeExternal(out: ObjectOutput): Unit = SparkErrorUtils.tryOrIOException {
    out.writeByte(toInt)
    out.writeByte(_replication)
    out.writeInt(_evictionPriority)
  }

  override def readExternal(in: ObjectInput): Unit = SparkErrorUtils.tryOrIOException {
    val flags = in.readByte()
    _useDisk = (flags & 8) != 0
    _useMemory = (flags & 4) != 0
    _useOffHeap = (flags & 2) != 0
    _deserialized = (flags & 1) != 0
    _replication = in.readByte()
    _evictionPriority = try {
      in.readInt()
    } catch {
      case _: Exception => 0
    }
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = StorageLevel.getCachedStorageLevel(this)

  override def toString: String = {
    val disk = if (useDisk) "disk" else ""
    val memory = if (useMemory) "memory" else ""
    val heap = if (useOffHeap) "offheap" else ""
    val deserialize = if (deserialized) "deserialized" else ""
    val priority = if (evictionPriority != 0) s"priority=$evictionPriority" else ""

    val output =
      Seq(disk, memory, heap, deserialize, s"$replication replicas", priority).filter(_.nonEmpty)
    s"StorageLevel(${output.mkString(", ")})"
  }

  override def hashCode(): Int = (toInt * 41 + replication) * 41 + evictionPriority

  /**
   * Return a new StorageLevel with the given eviction priority. Lower priority levels are
   * evicted from memory before higher priority levels (default priority is 0).
   *
   * @throws IllegalArgumentException if this StorageLevel does not use memory,
   *         since eviction priority only affects in-memory blocks.
   */
  def withEvictionPriority(priority: Int): StorageLevel = {
    require(useMemory,
      "evictionPriority has no effect on StorageLevels that do not use memory")
    StorageLevel.getCachedStorageLevel(
      new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication, priority))
  }

  def description: String = {
    var result = ""
    result += (if (useDisk) "Disk " else "")
    if (useMemory) {
      result += (if (useOffHeap) "Memory (off heap) " else "Memory ")
    }
    result += (if (deserialized) "Deserialized " else "Serialized ")
    result += s"${replication}x Replicated"
    result
  }
}


/**
 * Various [[org.apache.spark.storage.StorageLevel]] defined and utility functions for creating
 * new storage levels.
 */
object StorageLevel {
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val DISK_ONLY_3 = new StorageLevel(true, false, false, false, 3)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(true, true, true, false, 1)

  /**
   * :: DeveloperApi ::
   * Return the StorageLevel object with the specified name.
   */
  @DeveloperApi
  def fromString(s: String): StorageLevel = {
    try {
      StorageLevelMapper.fromString(s)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Invalid StorageLevel: $s")
    }
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object.
   */
  @DeveloperApi
  def apply(
      useDisk: Boolean,
      useMemory: Boolean,
      useOffHeap: Boolean,
      deserialized: Boolean,
      replication: Int): StorageLevel = {
    getCachedStorageLevel(
      new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication))
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object without setting useOffHeap.
   */
  @DeveloperApi
  def apply(
      useDisk: Boolean,
      useMemory: Boolean,
      deserialized: Boolean,
      replication: Int = 1): StorageLevel = {
    getCachedStorageLevel(new StorageLevel(useDisk, useMemory, false, deserialized, replication))
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object from its integer representation.
   */
  @DeveloperApi
  def apply(flags: Int, replication: Int): StorageLevel = {
    getCachedStorageLevel(new StorageLevel(flags, replication))
  }

  /**
   * :: DeveloperApi ::
   * Read StorageLevel object from ObjectInput stream.
   */
  @DeveloperApi
  def apply(in: ObjectInput): StorageLevel = {
    val obj = new StorageLevel()
    obj.readExternal(in)
    getCachedStorageLevel(obj)
  }

  private[spark] val storageLevelCache = new ConcurrentHashMap[StorageLevel, StorageLevel]()

  private[spark] def getCachedStorageLevel(level: StorageLevel): StorageLevel = {
    storageLevelCache.putIfAbsent(level, level)
    storageLevelCache.get(level)
  }
}
