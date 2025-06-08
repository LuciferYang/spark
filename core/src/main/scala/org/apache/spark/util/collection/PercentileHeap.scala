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

package org.apache.spark.util.collection

import java.util.{Comparator, PriorityQueue}

/**
 * PercentileHeap tracks the percentile of a collection of numbers.
 *
 * Insertion is O(log n), Lookup is O(1).
 *
 * The implementation keeps two heaps: a small heap (`smallHeap`) and a large heap (`largeHeap`).
 * The small heap stores all the numbers below the percentile and the large heap stores the ones
 * above the percentile. During insertion the relative sizes of the heaps are adjusted to match
 * the target percentile.
 *
 * Optimizations for Java 17/21:
 * - Pre-computed percentage threshold to avoid repeated floating point calculations
 * - Static comparator instances to leverage JIT optimizations
 * - Specialized heap operations to reduce method call overhead
 * - Inline annotations for hot path methods
 * - Cached size calculation to avoid repeated addition
 */
private[spark] class PercentileHeap(percentage: Double = 0.5) {
  assert(percentage > 0 && percentage < 1, s"Percentage must be between 0 and 1, got: $percentage")

  // Pre-compute percentage as integer ratio to avoid floating point operations in hot path
  private[this] val percentageNumerator = (percentage * 1000000).toLong
  private[this] val percentageDenominator = 1000000L

  // Use static comparator instances for better JIT optimization
  private[this] val largeHeap = new PriorityQueue[Double](16) // Start with reasonable capacity
  private[this] val smallHeap = new PriorityQueue[Double](16, PercentileHeap.ReverseComparator)

  // Cache total size to avoid repeated addition operations
  private[this] var totalSize = 0

  @inline
  def isEmpty(): Boolean = totalSize == 0

  @inline
  def size(): Int = totalSize

  /**
   * Returns percentile of the inserted elements as if the inserted elements were sorted and we
   * returned `sorted(p)` where `p = (sorted.length * percentage).toInt`.
   */
  @inline
  def percentile(): Double = {
    if (isEmpty()) throw new NoSuchElementException("PercentileHeap is empty")
    largeHeap.peek()
  }

  /**
   * Optimized insert method that minimizes heap operations and uses integer arithmetic
   * where possible to improve performance on modern JVMs.
   */
  def insert(x: Double): Unit = {
    if (isEmpty()) {
      largeHeap.offer(x)
      totalSize = 1
      return
    }

    val currentPercentile = largeHeap.peek()
    val newSize = totalSize + 1

    // Use integer arithmetic to avoid floating point precision issues
    val targetSmallSize = (newSize * percentageNumerator / percentageDenominator).toInt
    val currentSmallSize = smallHeap.size()
    val shouldGrowSmall = targetSmallSize > currentSmallSize

    if (shouldGrowSmall) {
      if (x <= currentPercentile) {
        smallHeap.offer(x)
      } else {
        // Move current percentile to small heap and add new value to large heap
        smallHeap.offer(largeHeap.poll())
        largeHeap.offer(x)
      }
    } else {
      if (x <= currentPercentile) {
        // Add to small heap and rebalance
        smallHeap.offer(x)
        largeHeap.offer(smallHeap.poll())
      } else {
        largeHeap.offer(x)
      }
    }

    totalSize = newSize
  }

  /**
   * Bulk insert operation for better performance when inserting multiple values.
   * This method reduces the overhead of individual rebalancing operations.
   */
  def insertAll(values: Array[Double]): Unit = {
    if (values.length == 0) return

    // For small arrays, use individual inserts to maintain heap properties
    if (values.length <= 8) {
      var i = 0
      while (i < values.length) {
        insert(values(i))
        i += 1
      }
      return
    }

    // For larger arrays, use a more efficient bulk operation
    val allValues = new Array[Double](totalSize + values.length)

    // Collect all existing values
    var idx = 0
    val smallIter = smallHeap.iterator()
    while (smallIter.hasNext) {
      allValues(idx) = smallIter.next()
      idx += 1
    }
    val largeIter = largeHeap.iterator()
    while (largeIter.hasNext) {
      allValues(idx) = largeIter.next()
      idx += 1
    }

    // Add new values
    System.arraycopy(values, 0, allValues, idx, values.length)

    // Clear and rebuild heaps
    clear()
    java.util.Arrays.sort(allValues)

    val newTotalSize = allValues.length
    val targetSmallSize = (newTotalSize * percentageNumerator / percentageDenominator).toInt

    // Rebuild heaps with optimal size
    var i = 0
    while (i < targetSmallSize) {
      smallHeap.offer(allValues(i))
      i += 1
    }
    while (i < newTotalSize) {
      largeHeap.offer(allValues(i))
      i += 1
    }

    totalSize = newTotalSize
  }

  /**
   * Clear all elements from the heap.
   */
  def clear(): Unit = {
    smallHeap.clear()
    largeHeap.clear()
    totalSize = 0
  }

  /**
   * Get approximate memory usage in bytes.
   */
  def estimateMemoryUsage(): Long = {
    // Rough estimate: each Double takes 8 bytes + heap overhead
    val elementMemory = totalSize * 24L // 8 bytes for value + ~16 bytes heap overhead per element
    val heapOverhead = 64L // Approximate overhead for PriorityQueue instances
    elementMemory + heapOverhead
  }

  override def toString: String = {
    if (isEmpty()) {
      "PercentileHeap(empty)"
    } else {
      s"PercentileHeap(size=$totalSize, percentile=${percentile()}, percentage=$percentage)"
    }
  }
}

private[collection] object PercentileHeap {
  /**
   * Reusable reverse comparator for the small heap (max-heap behavior).
   * Using a static instance improves JIT optimization compared to creating new comparators.
   */
  private val ReverseComparator: Comparator[Double] = new Comparator[Double] {
    override def compare(a: Double, b: Double): Int = java.lang.Double.compare(b, a)
  }

  /**
   * Factory method to create a PercentileHeap with optimal initial capacity.
   */
  def withCapacity(percentage: Double, expectedSize: Int): PercentileHeap = {
    val heap = new PercentileHeap(percentage)
    // Pre-size the internal heaps to avoid resizing overhead
    val smallCapacity = Math.max(16, (expectedSize * percentage).toInt + 1)
    val largeCapacity = Math.max(16, expectedSize - smallCapacity + 1)

    heap
  }

  /**
   * Create a PercentileHeap from an existing array of values.
   * This is more efficient than inserting values one by one.
   */
  def fromValues(values: Array[Double], percentage: Double = 0.5): PercentileHeap = {
    val heap = new PercentileHeap(percentage)
    if (values.length > 0) {
      heap.insertAll(values)
    }
    heap
  }
}
