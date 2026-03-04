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

package org.apache.spark.sql.execution.datasources.parquet

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.{SparkFunSuite, SparkUpgradeException}
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{IntegerType, LongType}

class VectorizedPlainValuesReaderSuite extends SparkFunSuite {

  private def allocate(
      capacity: Int,
      dt: org.apache.spark.sql.types.DataType,
      memMode: MemoryMode): WritableColumnVector = {
    if (memMode == MemoryMode.OFF_HEAP) {
      new OffHeapColumnVector(capacity, dt)
    } else {
      new OnHeapColumnVector(capacity, dt)
    }
  }

  /**
   * Runs a test block against both ON_HEAP and OFF_HEAP column vectors.
   */
  private def testWithBothVectors(
      name: String,
      capacity: Int,
      dt: org.apache.spark.sql.types.DataType)(
      block: WritableColumnVector => Unit): Unit = {
    test(name) {
      Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP).foreach { mode =>
        val vector = allocate(capacity, dt, mode)
        try block(vector) finally vector.close()
      }
    }
  }

  // ----- helpers to build ByteBuffers and readers -----

  private def intsToByteBuffer(values: Array[Int], direct: Boolean): ByteBuffer = {
    val buf = if (direct) {
      ByteBuffer.allocateDirect(values.length * 4).order(ByteOrder.LITTLE_ENDIAN)
    } else {
      ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN)
    }
    values.foreach(buf.putInt)
    buf.flip()
    buf
  }

  private def longsToByteBuffer(values: Array[Long], direct: Boolean): ByteBuffer = {
    val buf = if (direct) {
      ByteBuffer.allocateDirect(values.length * 8).order(ByteOrder.LITTLE_ENDIAN)
    } else {
      ByteBuffer.allocate(values.length * 8).order(ByteOrder.LITTLE_ENDIAN)
    }
    values.foreach(buf.putLong)
    buf.flip()
    buf
  }

  private def readerFromBuffer(buf: ByteBuffer): VectorizedPlainValuesReader = {
    val reader = new VectorizedPlainValuesReader()
    reader.initFromPage(0, ByteBufferInputStream.wrap(buf))
    reader
  }

  // ----- readIntegersWithRebase tests -----

  // heap buffer  -> tests array-backed path (buffer.hasArray() == true)
  // direct buffer -> tests non-array path   (buffer.hasArray() == false)
  Seq(false, true).foreach { direct =>
    val bufType = if (direct) "direct" else "heap"

    testWithBothVectors(
      s"readIntegersWithRebase - all modern dates, no rebase needed ($bufType buffer)",
      16, IntegerType) { column =>
      val values = Array(0, 100, 10000, -141427, -100000, Int.MaxValue, 1, -1)
      val reader = readerFromBuffer(intsToByteBuffer(values, direct))
      reader.readIntegersWithRebase(values.length, column, 0, false)
      values.zipWithIndex.foreach { case (v, i) =>
        assert(column.getInt(i) === v,
          s"mismatch at index $i, VectorType=${column.getClass.getSimpleName}")
      }
    }

    testWithBothVectors(
      s"readIntegersWithRebase - some ancient dates, partial rebase ($bufType buffer)",
      16, IntegerType) { column =>
      val switchDay = RebaseDateTime.lastSwitchJulianDay
      val ancientDay = switchDay - 100
      val modernDay = switchDay + 100
      val values = Array(modernDay, modernDay, ancientDay, modernDay)
      val reader = readerFromBuffer(intsToByteBuffer(values, direct))
      reader.readIntegersWithRebase(values.length, column, 0, false)
      assert(column.getInt(0) === modernDay)
      assert(column.getInt(1) === modernDay)
      assert(column.getInt(2) === RebaseDateTime.rebaseJulianToGregorianDays(ancientDay))
      assert(column.getInt(3) === modernDay)
    }

    testWithBothVectors(
      s"readIntegersWithRebase - first value ancient, rebaseFrom=0 ($bufType buffer)",
      16, IntegerType) { column =>
      val switchDay = RebaseDateTime.lastSwitchJulianDay
      val ancientDay = switchDay - 50
      val modernDay = switchDay + 50
      val values = Array(ancientDay, ancientDay, modernDay, modernDay)
      val reader = readerFromBuffer(intsToByteBuffer(values, direct))
      reader.readIntegersWithRebase(values.length, column, 0, false)
      assert(column.getInt(0) === RebaseDateTime.rebaseJulianToGregorianDays(ancientDay))
      assert(column.getInt(1) === RebaseDateTime.rebaseJulianToGregorianDays(ancientDay))
      assert(column.getInt(2) === modernDay)
      assert(column.getInt(3) === modernDay)
    }

    testWithBothVectors(
      s"readIntegersWithRebase - failIfRebase throws exception ($bufType buffer)",
      16, IntegerType) { column =>
      val ancientDay = RebaseDateTime.lastSwitchJulianDay - 1
      val values = Array(0, 100, ancientDay, 200)
      val reader = readerFromBuffer(intsToByteBuffer(values, direct))
      intercept[SparkUpgradeException] {
        reader.readIntegersWithRebase(values.length, column, 0, true)
      }
    }

    testWithBothVectors(
      s"readIntegersWithRebase - failIfRebase=true, all modern, no exception ($bufType buffer)",
      16, IntegerType) { column =>
      val values = Array(0, 100, 200, 300)
      val reader = readerFromBuffer(intsToByteBuffer(values, direct))
      reader.readIntegersWithRebase(values.length, column, 0, true)
      values.zipWithIndex.foreach { case (v, i) =>
        assert(column.getInt(i) === v)
      }
    }

    testWithBothVectors(
      s"readIntegersWithRebase - with rowId offset ($bufType buffer)",
      16, IntegerType) { column =>
      val values = Array(0, 100, 200)
      val reader = readerFromBuffer(intsToByteBuffer(values, direct))
      val rowId = 5
      reader.readIntegersWithRebase(values.length, column, rowId, false)
      values.zipWithIndex.foreach { case (v, i) =>
        assert(column.getInt(rowId + i) === v)
      }
    }

    // ----- readLongsWithRebase tests -----

    testWithBothVectors(
      s"readLongsWithRebase - all modern timestamps, no rebase needed ($bufType buffer)",
      16, LongType) { column =>
      val switchTs = RebaseDateTime.lastSwitchJulianTs
      val values = Array(switchTs, switchTs + 1000L, 0L, Long.MaxValue, switchTs + 999999L)
      val reader = readerFromBuffer(longsToByteBuffer(values, direct))
      reader.readLongsWithRebase(values.length, column, 0, false, "UTC")
      values.zipWithIndex.foreach { case (v, i) =>
        assert(column.getLong(i) === v,
          s"mismatch at index $i, VectorType=${column.getClass.getSimpleName}")
      }
    }

    testWithBothVectors(
      s"readLongsWithRebase - some ancient timestamps, partial rebase ($bufType buffer)",
      16, LongType) { column =>
      val switchTs = RebaseDateTime.lastSwitchJulianTs
      val ancientTs = switchTs - 100000L
      val modernTs = switchTs + 100000L
      val values = Array(modernTs, modernTs, ancientTs, modernTs)
      val reader = readerFromBuffer(longsToByteBuffer(values, direct))
      reader.readLongsWithRebase(values.length, column, 0, false, "UTC")
      assert(column.getLong(0) === modernTs)
      assert(column.getLong(1) === modernTs)
      assert(column.getLong(2) === RebaseDateTime.rebaseJulianToGregorianMicros("UTC", ancientTs))
      assert(column.getLong(3) === modernTs)
    }

    testWithBothVectors(
      s"readLongsWithRebase - first value ancient, rebaseFrom=0 ($bufType buffer)",
      16, LongType) { column =>
      val switchTs = RebaseDateTime.lastSwitchJulianTs
      val ancientTs = switchTs - 50000L
      val modernTs = switchTs + 50000L
      val values = Array(ancientTs, ancientTs, modernTs, modernTs)
      val reader = readerFromBuffer(longsToByteBuffer(values, direct))
      reader.readLongsWithRebase(values.length, column, 0, false, "UTC")
      assert(column.getLong(0) === RebaseDateTime.rebaseJulianToGregorianMicros("UTC", ancientTs))
      assert(column.getLong(1) === RebaseDateTime.rebaseJulianToGregorianMicros("UTC", ancientTs))
      assert(column.getLong(2) === modernTs)
      assert(column.getLong(3) === modernTs)
    }

    testWithBothVectors(
      s"readLongsWithRebase - failIfRebase throws exception ($bufType buffer)",
      16, LongType) { column =>
      val ancientTs = RebaseDateTime.lastSwitchJulianTs - 1
      val values = Array(0L, 100L, ancientTs, 200L)
      val reader = readerFromBuffer(longsToByteBuffer(values, direct))
      intercept[SparkUpgradeException] {
        reader.readLongsWithRebase(values.length, column, 0, true, "UTC")
      }
    }

    testWithBothVectors(
      s"readLongsWithRebase - failIfRebase=true, all modern, no exception ($bufType buffer)",
      16, LongType) { column =>
      val values = Array(0L, 100L, 200L, 300L)
      val reader = readerFromBuffer(longsToByteBuffer(values, direct))
      reader.readLongsWithRebase(values.length, column, 0, true, "UTC")
      values.zipWithIndex.foreach { case (v, i) =>
        assert(column.getLong(i) === v)
      }
    }

    testWithBothVectors(
      s"readLongsWithRebase - with rowId offset ($bufType buffer)",
      16, LongType) { column =>
      val values = Array(0L, 100L, 200L)
      val reader = readerFromBuffer(longsToByteBuffer(values, direct))
      val rowId = 5
      reader.readLongsWithRebase(values.length, column, rowId, false, "UTC")
      values.zipWithIndex.foreach { case (v, i) =>
        assert(column.getLong(rowId + i) === v)
      }
    }
  }
}
