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

package org.apache.spark.util.sketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class BitArraySuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("error case when create BitArray") {
    intercept[IllegalArgumentException](new BitArray(0))
    intercept[IllegalArgumentException](new BitArray(64L * Integer.MAX_VALUE + 1))
  }

  test("bitSize") {
    assert(new BitArray(64).bitSize() == 64)
    // BitArray is word-aligned, so 65~128 bits need 2 long to store, which is 128 bits.
    assert(new BitArray(65).bitSize() == 128)
    assert(new BitArray(127).bitSize() == 128)
    assert(new BitArray(128).bitSize() == 128)
  }

  test("bitSize is rounded up to the next power of 2") {
    // 129 bits require 3 longs; rounded up to 4 longs = 256 bits.
    assert(new BitArray(129).bitSize() == 256)
    // 192 bits = 3 longs, rounded up to 4 longs = 256 bits.
    assert(new BitArray(192).bitSize() == 256)
    // 321 bits = 6 longs, rounded up to 8 longs = 512 bits.
    assert(new BitArray(321).bitSize() == 512)
    // Already power-of-2 sizes pass through unchanged.
    assert(new BitArray(1024).bitSize() == 1024)
    assert(new BitArray(4096).bitSize() == 4096)
  }

  test("indexFor uses power-of-2 bitmask fast path for new filters") {
    val bitArray = new BitArray(256) // 4 longs, power of 2
    // indexFor must map any non-negative hash to a valid index in [0, 256).
    val hashes = Seq(0L, 1L, 255L, 256L, 1024L, 1_000_000L, Int.MaxValue.toLong, Long.MaxValue)
    hashes.foreach { h =>
      val idx = bitArray.indexFor(h)
      assert(idx >= 0 && idx < 256, s"indexFor($h) = $idx out of range")
      // Fast path semantics: bitmask equals plain modulo for non-negative hashes and pow2 size.
      assert(idx == h % 256, s"indexFor($h) = $idx, expected ${h % 256}")
    }
  }

  test("indexFor falls back to modulo for non-power-of-2 legacy bit arrays") {
    // Legacy filters can have arbitrary numWords; simulate via serialize-of-a-constructed array
    // and then manual deserialization with a non-power-of-2 word count.
    val legacy = deserializeWithNumWords(3) // 3 words = 192 bits, not a power of 2
    assert(legacy.bitSize() == 192)
    // Slow path: (hash % 192) must be used.
    val hashes = Seq(0L, 1L, 191L, 192L, 1024L, 5_000_000L)
    hashes.foreach { h =>
      assert(legacy.indexFor(h) == h % 192, s"indexFor($h) expected ${h % 192}")
    }
  }

  test("legacy non-power-of-2 filters round-trip through serialization unchanged") {
    // Simulate an old serialized filter with 6 words (not a power of 2). The new deserializer
    // must preserve the original word count so hash-to-bit mappings remain stable.
    val legacy = deserializeWithNumWords(6) // 6 * 64 = 384 bits
    assert(legacy.bitSize() == 384)

    val buf = new ByteArrayOutputStream()
    legacy.writeTo(new DataOutputStream(buf))
    val roundTripped = BitArray.readFrom(
      new DataInputStream(new ByteArrayInputStream(buf.toByteArray)))
    assert(roundTripped.bitSize() == 384)
    assert(roundTripped.indexFor(500L) == 500L % 384)
  }

  private def deserializeWithNumWords(numWords: Int): BitArray = {
    val buf = new ByteArrayOutputStream()
    val dos = new DataOutputStream(buf)
    dos.writeInt(numWords)
    (0 until numWords).foreach(_ => dos.writeLong(0L))
    BitArray.readFrom(new DataInputStream(new ByteArrayInputStream(buf.toByteArray)))
  }

  test("set") {
    val bitArray = new BitArray(64)
    assert(bitArray.set(1))
    // Only returns true if the bit changed.
    assert(!bitArray.set(1))
    assert(bitArray.set(2))
  }

  test("normal operation") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)

    val bitArray = new BitArray(320)
    val indexes = (1 to 100).map(_ => r.nextInt(320).toLong).distinct

    indexes.foreach(bitArray.set)
    indexes.foreach(i => assert(bitArray.get(i)))
    assert(bitArray.cardinality() == indexes.length)
  }

  test("merge") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)

    val bitArray1 = new BitArray(64 * 6)
    val bitArray2 = new BitArray(64 * 6)

    val indexes1 = (1 to 100).map(_ => r.nextInt(64 * 6).toLong).distinct
    val indexes2 = (1 to 100).map(_ => r.nextInt(64 * 6).toLong).distinct

    indexes1.foreach(bitArray1.set)
    indexes2.foreach(bitArray2.set)

    bitArray1.putAll(bitArray2)
    indexes1.foreach(i => assert(bitArray1.get(i)))
    indexes2.foreach(i => assert(bitArray1.get(i)))
    assert(bitArray1.cardinality() == (indexes1 ++ indexes2).distinct.length)
  }
}
