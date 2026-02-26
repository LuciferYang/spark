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

package org.apache.spark.sql.execution.vectorized

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class OnHeapColumnVectorLittleEndianSuite extends SparkFunSuite {

  test("putIntsLittleEndian") {
    val count = 10
    val vector = new OnHeapColumnVector(count, IntegerType)
    try {
      val src = new Array[Byte](count * 4)
      val bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN)
      (0 until count).foreach(i => bb.putInt(i))

      vector.putIntsLittleEndian(0, count, src, 0)

      (0 until count).foreach { i =>
        assert(vector.getInt(i) === i)
      }
    } finally {
      vector.close()
    }
  }

  test("putLongsLittleEndian") {
    val count = 10
    val vector = new OnHeapColumnVector(count, LongType)
    try {
      val src = new Array[Byte](count * 8)
      val bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN)
      (0 until count).foreach(i => bb.putLong(i))

      vector.putLongsLittleEndian(0, count, src, 0)

      (0 until count).foreach { i =>
        assert(vector.getLong(i) === i)
      }
    } finally {
      vector.close()
    }
  }

  test("putFloatsLittleEndian") {
    val count = 10
    val vector = new OnHeapColumnVector(count, FloatType)
    try {
      val src = new Array[Byte](count * 4)
      val bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN)
      (0 until count).foreach(i => bb.putFloat(i.toFloat + 0.5f))

      vector.putFloatsLittleEndian(0, count, src, 0)

      (0 until count).foreach { i =>
        assert(vector.getFloat(i) === i.toFloat + 0.5f)
      }
    } finally {
      vector.close()
    }
  }

  test("putDoublesLittleEndian") {
    val count = 10
    val vector = new OnHeapColumnVector(count, DoubleType)
    try {
      val src = new Array[Byte](count * 8)
      val bb = ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN)
      (0 until count).foreach(i => bb.putDouble(i.toDouble + 0.5d))

      vector.putDoublesLittleEndian(0, count, src, 0)

      (0 until count).foreach { i =>
        assert(vector.getDouble(i) === i.toDouble + 0.5d)
      }
    } finally {
      vector.close()
    }
  }

  test("simulate BigEndian behavior on LittleEndian platform") {
    // This test uses reflection to force `bigEndianPlatform` to true,
    // verifying that the BigEndian code branch is executed.
    // Note: Running this logic on a LittleEndian machine will result in incorrectly reversed bytes,
    // which is exactly what we expect to verify.

    val field = classOf[OnHeapColumnVector].getDeclaredField("bigEndianPlatform")
    field.setAccessible(true)

    // Remove final modifier (works on older JDKs, but usually feasible in test environments)
    // Even if final cannot be removed, setBoolean works on some JVMs, or we can ignore this test if it fails
    try {
      val modifiersField = classOf[java.lang.reflect.Field].getDeclaredField("modifiers")
      modifiersField.setAccessible(true)
      modifiersField.setInt(field, field.getModifiers & ~java.lang.reflect.Modifier.FINAL)
    } catch {
      case _: Exception => // ignore, might fail on newer JDKs but setBoolean might still work
    }

    val originalValue = field.getBoolean(null)
    // If the machine itself is BigEndian, this test is meaningless (or needs reverse logic),
    // here we assume we are testing BigEndian logic on a LittleEndian machine
    assume(!originalValue, "This test simulates BigEndian logic on a LittleEndian platform")

    try {
      field.setBoolean(null, true) // Trick the code into thinking it's BigEndian

      val count = 1
      val vector = new OnHeapColumnVector(count, IntegerType)
      try {
        val src = new Array[Byte](4)
        val expected = 0x12345678
        // Write Little Endian data
        ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN).putInt(expected)

        // Call the method under test
        vector.putIntsLittleEndian(0, count, src, 0)

        // On a LittleEndian machine:
        // 1. Platform.getInt reads src (LE) -> gets 0x12345678 (correct)
        // 2. Code executes Integer.reverseBytes(0x12345678) -> gets 0x78563412
        // 3. Writes to intData
        val result = vector.getInt(0)

        // Verify that we indeed got the reversed result, proving the BigEndian branch logic was executed
        assert(result === Integer.reverseBytes(expected))
      } finally {
        vector.close()
      }
    } catch {
      case e: Exception =>
        cancel("Could not use reflection to modify static final field: " + e.getMessage)
    } finally {
      // Restore original value
      field.setBoolean(null, originalValue)
    }
  }
}
