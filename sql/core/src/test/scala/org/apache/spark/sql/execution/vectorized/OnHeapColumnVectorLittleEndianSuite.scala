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
}
