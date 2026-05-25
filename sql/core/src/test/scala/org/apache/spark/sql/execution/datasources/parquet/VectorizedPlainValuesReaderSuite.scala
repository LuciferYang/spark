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
import java.util.Arrays

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.BinaryType

/**
 * Correctness tests for `VectorizedPlainValuesReader.readBinary(int, WritableColumnVector, int)`,
 * focusing on the code paths introduced by the bulk snapshot rewrite:
 *   - single-row shim (`total == 1`): bypasses the bulk machinery
 *   - bulk fast path: payload fits in the current buffer (heap and direct)
 *   - bulk cross-buffer length read: the 4-byte length straddles a buffer boundary
 *   - bulk cross-buffer payload read: payload spans multiple buffers
 *   - defensive checks: `total == 0`, negative length, zero length, empty stream
 */
class VectorizedPlainValuesReaderSuite extends SparkFunSuite {

  private def encodePlainBinary(values: Seq[Array[Byte]]): Array[Byte] = {
    val total = values.map(_.length + 4).sum
    val bb = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN)
    values.foreach { v =>
      bb.putInt(v.length)
      bb.put(v)
    }
    bb.array()
  }

  private def bytesOf(s: String): Array[Byte] = s.getBytes("UTF-8")

  private def newReader(in: ByteBufferInputStream): VectorizedPlainValuesReader = {
    val r = new VectorizedPlainValuesReader
    r.initFromPage(/* valueCount = */ 0, in)
    r
  }

  private def assertVectorMatches(v: OnHeapColumnVector, expected: Seq[Array[Byte]]): Unit = {
    var i = 0
    while (i < expected.length) {
      assert(Arrays.equals(v.getBinary(i), expected(i)),
        s"row $i mismatch: got ${v.getBinary(i).mkString(",")}, " +
          s"expected ${expected(i).mkString(",")}")
      i += 1
    }
  }

  test("readBinary: total == 0 is a no-op and does not advance the stream") {
    val values = Seq(bytesOf("hello"), bytesOf("world"))
    val bytes = encodePlainBinary(values)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(4, BinaryType)
    r.readBinary(0, v, 0)
    assert(in.position() == 0, "no-op call must not advance the stream")
    // Subsequent read of all values still works.
    r.readBinary(2, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: total == 1 (shim path) reads one value and advances the stream") {
    val values = Seq(bytesOf("hello"), bytesOf("world"))
    val bytes = encodePlainBinary(values)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(4, BinaryType)
    r.readBinary(1, v, 0)
    assert(Arrays.equals(v.getBinary(0), values.head))
    assert(in.position() == 4 + values.head.length,
      "shim path must advance the stream by exactly 4 + len bytes")
    // Subsequent shim call reads the next value.
    r.readBinary(1, v, 1)
    assert(Arrays.equals(v.getBinary(1), values(1)))
  }

  test("readBinary: bulk path on a single heap buffer (the fast path)") {
    val values = Seq.tabulate(10)(i => bytesOf(s"v$i-${"x" * (i % 5)}"))
    val bytes = encodePlainBinary(values)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
    assert(in.position() == bytes.length, "stream must be fully consumed")
  }

  test("readBinary: bulk path with row-offset > 0 writes only the targeted rows") {
    val values = Seq(bytesOf("a"), bytesOf("bb"), bytesOf("ccc"))
    val bytes = encodePlainBinary(values)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length + 2, BinaryType)
    r.readBinary(values.length, v, 2)
    // Pre-offset rows must be untouched (no spurious writes from a bad rowId arithmetic).
    assert(v.getBinary(0).length == 0, "row 0 must be untouched by readBinary")
    assert(v.getBinary(1).length == 0, "row 1 must be untouched by readBinary")
    var i = 0
    while (i < values.length) {
      assert(Arrays.equals(v.getBinary(i + 2), values(i)))
      i += 1
    }
    assert(in.position() == bytes.length, "stream must be fully consumed")
  }

  test("readBinary: bulk path on a direct (off-heap) buffer routes through ByteBuffer overload") {
    val values = Seq(bytesOf("alpha"), bytesOf("beta"), bytesOf("gamma"))
    val heap = encodePlainBinary(values)
    val direct = ByteBuffer.allocateDirect(heap.length)
    direct.put(heap)
    direct.flip()
    val in = ByteBufferInputStream.wrap(direct)
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: bulk path across multiple buffers, payload aligned within buffers") {
    val values = Seq(bytesOf("aaa"), bytesOf("bbb"), bytesOf("ccc"))
    val bytes = encodePlainBinary(values)
    // Split exactly between row 1 and row 2: row 1 ends at offset 4+3 + 4+3 = 14.
    val split = 4 + values.head.length + 4 + values(1).length
    val buf1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, split))
    val buf2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, split, bytes.length))
    val in = ByteBufferInputStream.wrap(Arrays.asList(buf1, buf2))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: bulk path with payload straddling a buffer boundary") {
    val values = Seq(bytesOf("hello-world"))
    val bytes = encodePlainBinary(values)
    // Split mid-payload: first buffer holds [4-byte length + first 3 payload bytes],
    // second buffer holds the remaining payload bytes.
    val split = 4 + 3
    val buf1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, split))
    val buf2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, split, bytes.length))
    val in = ByteBufferInputStream.wrap(Arrays.asList(buf1, buf2))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: bulk path with length straddling a buffer boundary") {
    val values = Seq(bytesOf("xx"), bytesOf("yy"))
    val bytes = encodePlainBinary(values)
    // First buffer ends 2 bytes into the second row's length field.
    val split = 4 + values.head.length + 2
    val buf1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, split))
    val buf2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, split, bytes.length))
    val in = ByteBufferInputStream.wrap(Arrays.asList(buf1, buf2))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: bulk path leaves the stream positioned exactly past consumed bytes") {
    val values = Seq.tabulate(5)(i => bytesOf(s"row-$i"))
    val bytes = encodePlainBinary(values)
    // Pad with extra trailing bytes the reader should NOT consume.
    val padded = bytes ++ Array.fill[Byte](16)(0x7F.toByte)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(padded))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assert(in.position() == bytes.length,
      s"stream advanced to ${in.position()}, expected ${bytes.length}")
    // Trailing 16 bytes must still be available.
    assert(in.available() == 16)
  }

  test("readBinary: negative length throws ParquetDecodingException (shim path)") {
    val bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(-1).array()
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(1, BinaryType)
    val ex = intercept[ParquetDecodingException](r.readBinary(1, v, 0))
    assert(ex.getMessage.contains("Negative binary length"))
  }

  test("readBinary: negative length throws ParquetDecodingException (bulk path)") {
    val good = bytesOf("ok")
    val out = ByteBuffer.allocate(4 + good.length + 4).order(ByteOrder.LITTLE_ENDIAN)
    out.putInt(good.length).put(good).putInt(-1)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(out.array()))
    val r = newReader(in)
    val v = new OnHeapColumnVector(2, BinaryType)
    val ex = intercept[ParquetDecodingException](r.readBinary(2, v, 0))
    assert(ex.getMessage.contains("Negative binary length"))
  }

  test("readBinary: bulk path with total > 0 on empty stream throws EOF on length") {
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(Array.emptyByteArray))
    val r = newReader(in)
    val v = new OnHeapColumnVector(1, BinaryType)
    // total == 2 routes to bulk path; pin the specific failure site.
    val ex = intercept[ParquetDecodingException](r.readBinary(2, v, 0))
    assert(ex.getMessage.contains("Unexpected EOF while reading length"),
      s"expected EOF-on-length message, got: ${ex.getMessage}")
  }

  test("readBinary: zero-length payload values are handled correctly (shim and bulk)") {
    val values = Seq(Array.emptyByteArray, bytesOf("x"), Array.emptyByteArray)
    val bytes = encodePlainBinary(values)
    // Bulk path (total == 3) covers the cur.position(cur.position() + 0) + consumed += 0 case.
    val in1 = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r1 = newReader(in1)
    val v1 = new OnHeapColumnVector(values.length, BinaryType)
    r1.readBinary(values.length, v1, 0)
    assertVectorMatches(v1, values)
    // Shim path (total == 1) over three consecutive calls.
    val in2 = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r2 = newReader(in2)
    val v2 = new OnHeapColumnVector(values.length, BinaryType)
    var i = 0
    while (i < values.length) {
      r2.readBinary(1, v2, i)
      i += 1
    }
    assertVectorMatches(v2, values)
  }

  test("readBinary: bulk path with payload straddling three buffers") {
    val payload = bytesOf("abcdefghij") // 10 bytes
    val bytes = encodePlainBinary(Seq(payload))
    // Split into 3 segments: [4-byte length + 2 payload bytes],
    // [4 payload bytes], [4 payload bytes].
    val split1 = 4 + 2
    val split2 = split1 + 4
    val b1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, split1))
    val b2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, split1, split2))
    val b3 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, split2, bytes.length))
    val in = ByteBufferInputStream.wrap(Arrays.asList(b1, b2, b3))
    val r = newReader(in)
    val v = new OnHeapColumnVector(1, BinaryType)
    // Shim path uses getBuffer, which copies across; also valid.
    r.readBinary(1, v, 0)
    assert(Arrays.equals(v.getBinary(0), payload))

    // Re-test through the bulk path (total >= 2).
    val twoValues = Seq(payload, payload)
    val bytes2 = encodePlainBinary(twoValues)
    val s1 = 4 + 2
    val s2 = s1 + 4
    val s3 = s2 + 6 // crosses into the second value
    val c1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes2, 0, s1))
    val c2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes2, s1, s2))
    val c3 = ByteBuffer.wrap(Arrays.copyOfRange(bytes2, s2, s3))
    val c4 = ByteBuffer.wrap(Arrays.copyOfRange(bytes2, s3, bytes2.length))
    val in2 = ByteBufferInputStream.wrap(Arrays.asList(c1, c2, c3, c4))
    val r2 = newReader(in2)
    val v2 = new OnHeapColumnVector(2, BinaryType)
    r2.readBinary(2, v2, 0)
    assertVectorMatches(v2, twoValues)
  }

  test("readBinary: bulk path throws ParquetDecodingException on negative length header") {
    // Craft a 4-byte LE int with the sign bit set so the bulk path's inline negative-length
    // check fires. Bulk path enters when total >= 2; pad with a valid first record so we
    // reach the second row's length read on the bulk fast path (not the shim path).
    val firstPayload = bytesOf("ok")
    val firstHeader = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      .putInt(firstPayload.length).array()
    val negativeHeader = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      .putInt(-1).array()
    val bytes = firstHeader ++ firstPayload ++ negativeHeader
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(2, BinaryType)
    val ex = intercept[ParquetDecodingException] {
      r.readBinary(2, v, 0)
    }
    assert(ex.getMessage.contains("Negative binary length"),
      s"expected negative-length message, got: ${ex.getMessage}")
  }

  test("readBinary: bulk path falls back when 4-byte length header straddles end of buffer") {
    // The fast-path entry-condition is nBuffers == 1, but the bulk loop's early-return
    // `limit - p < 4` (truncated header within the single buffer) is distinct from the
    // payload-truncation case. Construct a stream where the second row's length header is
    // partially missing from the only available buffer; expect the fallback per-row loop
    // to surface a clean EOF.
    val firstPayload = bytesOf("hi")
    val firstHeader = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      .putInt(firstPayload.length).array()
    // Only 2 bytes of the second row's length (need 4 to decode).
    val partialSecondHeader = Array[Byte](0x05, 0x00)
    val bytes = firstHeader ++ firstPayload ++ partialSecondHeader
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(2, BinaryType)
    intercept[ParquetDecodingException] {
      r.readBinary(2, v, 0)
    }
  }

  test("readBinary: bulk path scratch grows via doubling when prev*2 >= n") {
    // First call seeds scratch at 64; second call needs 80 -> grow path picks
    // max(80, 64*2=128) = 128 (the doubling branch), not n. A regression that dropped
    // Math.max would still allocate 80, sized too small only relative to expectations
    // but still functional. The behavioral check is correctness, not allocation size:
    // verify both calls decode correctly through the grow boundary.
    val first = Seq.tabulate(8)(i => bytesOf(s"f-$i"))
    val second = Seq.tabulate(80)(i => bytesOf(s"s-$i"))
    val all = first ++ second
    val bytes = encodePlainBinary(all)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(all.length, BinaryType)
    r.readBinary(first.length, v, 0)
    r.readBinary(second.length, v, first.length)
    assertVectorMatches(v, all)
    assert(in.position() == bytes.length, "stream must be fully consumed")
  }

  test("readBinary: bulk path scratch arrays grow correctly across batches") {
    // Exercises the lazy-grow logic in ensureScratchSrcOffsets / ensureScratchLengths.
    // First call sizes scratch to the initial floor (64); a later call with a much larger
    // batch forces a reallocation. Then a smaller batch verifies reuse without stomping.
    val small = Seq.tabulate(8)(i => bytesOf(s"s-$i"))
    val large = Seq.tabulate(200)(i => bytesOf(s"large-payload-$i"))
    val tail = Seq.tabulate(16)(i => bytesOf(s"t-$i"))
    val all = small ++ large ++ tail
    val bytes = encodePlainBinary(all)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(all.length, BinaryType)
    r.readBinary(small.length, v, 0)
    r.readBinary(large.length, v, small.length)
    r.readBinary(tail.length, v, small.length + large.length)
    assertVectorMatches(v, all)
    assert(in.position() == bytes.length,
      "stream must be fully consumed after small -> large -> small bulk reads")
  }

  test("readBinary: interleaved bulk -> shim -> bulk calls on a single reader") {
    // Verifies the bulk path's mark/reset/skipFully leaves the underlying stream in a state
    // the next shim call can read from cleanly, and vice versa.
    val values = Seq.tabulate(6)(i => bytesOf(s"value-$i"))
    val bytes = encodePlainBinary(values)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(2, v, 0) // bulk path (total >= 2)
    r.readBinary(1, v, 2) // shim path (total == 1)
    r.readBinary(3, v, 3) // bulk path again
    assertVectorMatches(v, values)
    assert(in.position() == bytes.length, "stream must be fully consumed across mixed calls")
  }

  test("readBinary: bulk path tolerates an empty buffer in the middle of the list") {
    val values = Seq(bytesOf("first"), bytesOf("second"))
    val bytes = encodePlainBinary(values)
    val split = 4 + values.head.length
    val b1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, split))
    val bEmpty = ByteBuffer.allocate(0)
    val b2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, split, bytes.length))
    val in = ByteBufferInputStream.wrap(Arrays.asList(b1, bEmpty, b2))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: bulk path skips multiple consecutive empty buffers") {
    // Exercise the nextNonEmpty while-loop running more than one iteration.
    val values = Seq(bytesOf("first"), bytesOf("second"))
    val bytes = encodePlainBinary(values)
    val split = 4 + values.head.length
    val b1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, split))
    val e1 = ByteBuffer.allocate(0)
    val e2 = ByteBuffer.allocate(0)
    val e3 = ByteBuffer.allocate(0)
    val b2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, split, bytes.length))
    val in = ByteBufferInputStream.wrap(Arrays.asList(b1, e1, e2, e3, b2))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: bulk path throws EOF on payload when declared length exceeds remaining bytes") {
    // 4-byte length declares 100, but only 5 payload bytes follow.
    val bb = ByteBuffer.allocate(4 + 5).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(100).put(Array[Byte](1, 2, 3, 4, 5))
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bb.array()))
    val r = newReader(in)
    val v = new OnHeapColumnVector(2, BinaryType)
    val ex = intercept[ParquetDecodingException](r.readBinary(2, v, 0))
    assert(ex.getMessage.contains("Unexpected EOF while reading payload"),
      s"expected EOF-on-payload message, got: ${ex.getMessage}")
  }

  test("readBinary: bulk path detects negative length assembled across buffers") {
    // Encode -1 as 0xFF 0xFF 0xFF 0xFF, split across two buffers so the byte-wise
    // length-assembly path (not the cur.getInt() fast path) reconstructs it.
    val good = bytesOf("ok")
    val full = ByteBuffer.allocate(4 + good.length + 4).order(ByteOrder.LITTLE_ENDIAN)
    full.putInt(good.length).put(good).putInt(-1)
    val raw = full.array()
    // Split so the second length straddles the boundary (2 bytes in each buffer).
    val splitAt = 4 + good.length + 2
    val b1 = ByteBuffer.wrap(Arrays.copyOfRange(raw, 0, splitAt))
    val b2 = ByteBuffer.wrap(Arrays.copyOfRange(raw, splitAt, raw.length))
    val in = ByteBufferInputStream.wrap(Arrays.asList(b1, b2))
    val r = newReader(in)
    val v = new OnHeapColumnVector(2, BinaryType)
    val ex = intercept[ParquetDecodingException](r.readBinary(2, v, 0))
    assert(ex.getMessage.contains("Negative binary length"),
      s"expected 'Negative binary length' from byte-assembly path, got: ${ex.getMessage}")
  }

  test("readBinary: bulk path on direct buffers across multiple segments") {
    val values = Seq(bytesOf("alpha"), bytesOf("beta"))
    val bytes = encodePlainBinary(values)
    val split = 4 + values.head.length
    val d1 = ByteBuffer.allocateDirect(split)
    d1.put(bytes, 0, split).flip()
    val d2 = ByteBuffer.allocateDirect(bytes.length - split)
    d2.put(bytes, split, bytes.length - split).flip()
    val in = ByteBufferInputStream.wrap(Arrays.asList(d1, d2))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: bulk path with direct buffers and payload straddling buffers") {
    // Exercise the cross-buffer payload copy loop (cur.get(tmp, copied, n)) on direct buffers.
    val payload = bytesOf("0123456789")
    val bytes = encodePlainBinary(Seq(payload, payload))
    val split = 4 + 5 // mid-first-payload
    val d1 = ByteBuffer.allocateDirect(split)
    d1.put(bytes, 0, split).flip()
    val d2 = ByteBuffer.allocateDirect(bytes.length - split)
    d2.put(bytes, split, bytes.length - split).flip()
    val in = ByteBufferInputStream.wrap(Arrays.asList(d1, d2))
    val r = newReader(in)
    val v = new OnHeapColumnVector(2, BinaryType)
    r.readBinary(2, v, 0)
    assertVectorMatches(v, Seq(payload, payload))
  }

  test("readBinary: bulk path detects mid-stream EOF during byte-wise length assembly") {
    // Two buffers: first has [valid value 1] + [2 bytes of next length], second is empty / absent.
    // Triggers the `bufIdx >= nBuffers` throw inside the byte-wise length-assembly shift loop,
    // after some bytes have already been read from the first buffer.
    val good = bytesOf("g")
    val full = ByteBuffer.allocate(4 + good.length + 2).order(ByteOrder.LITTLE_ENDIAN)
    full.putInt(good.length).put(good).put(Array[Byte](0x00, 0x00))
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(full.array()))
    val r = newReader(in)
    val v = new OnHeapColumnVector(2, BinaryType)
    val ex = intercept[ParquetDecodingException](r.readBinary(2, v, 0))
    assert(ex.getMessage.contains("Unexpected EOF while reading length"),
      s"expected EOF-on-length message, got: ${ex.getMessage}")
  }

  test("readBinary: bulk path with rowId > 0 and payload straddling buffers") {
    val payload = bytesOf("a-payload-that-spans")
    val bytes = encodePlainBinary(Seq(payload))
    val split = 4 + 7
    val b1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, split))
    val b2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes, split, bytes.length))
    // For total >= 2, route to bulk. Use two copies of the same payload across the same bufs.
    val values2 = Seq(payload, payload)
    val bytes2 = encodePlainBinary(values2)
    val splitA = 4 + 5
    val splitB = splitA + 8
    val c1 = ByteBuffer.wrap(Arrays.copyOfRange(bytes2, 0, splitA))
    val c2 = ByteBuffer.wrap(Arrays.copyOfRange(bytes2, splitA, splitB))
    val c3 = ByteBuffer.wrap(Arrays.copyOfRange(bytes2, splitB, bytes2.length))
    val in = ByteBufferInputStream.wrap(Arrays.asList(c1, c2, c3))
    val r = newReader(in)
    val v = new OnHeapColumnVector(5, BinaryType)
    r.readBinary(2, v, 3)
    assert(v.getBinary(0).length == 0)
    assert(v.getBinary(1).length == 0)
    assert(v.getBinary(2).length == 0)
    assert(Arrays.equals(v.getBinary(3), payload))
    assert(Arrays.equals(v.getBinary(4), payload))
    // Silence the unused warning on the single-payload version.
    assert(b1.capacity() + b2.capacity() == bytes.length)
  }

  test("readBinary: bulk path with rowId > 0 and direct buffers fitting in a single buffer") {
    val values = Seq(bytesOf("x"), bytesOf("yz"))
    val bytes = encodePlainBinary(values)
    val direct = ByteBuffer.allocateDirect(bytes.length)
    direct.put(bytes).flip()
    val in = ByteBufferInputStream.wrap(direct)
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length + 1, BinaryType)
    r.readBinary(values.length, v, 1)
    assert(v.getBinary(0).length == 0, "row 0 must be untouched")
    assert(Arrays.equals(v.getBinary(1), values.head))
    assert(Arrays.equals(v.getBinary(2), values(1)))
  }

  test("readBinary: bulk path tolerates a trailing empty buffer after the last value") {
    val values = Seq(bytesOf("a"), bytesOf("b"))
    val bytes = encodePlainBinary(values)
    val b1 = ByteBuffer.wrap(bytes)
    val bEmpty = ByteBuffer.allocate(0)
    val in = ByteBufferInputStream.wrap(Arrays.asList(b1, bEmpty))
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
  }

  test("readBinary: bulk fast path with non-zero arrayOffset on the heap buffer") {
    // Guard against an off-by-one in `tryBulkReadBinary`'s `base + p` arithmetic: if the
    // wrapped buffer reports `arrayOffset() != 0` (which happens when upstream code slices
    // a wrapped array), the fast path must add `base` when reading length bytes AND when
    // recording per-row src offsets passed to putByteArrays. A regression that drops
    // `base +` from either site reads garbage / writes the wrong slice into the vector.
    val values = Seq(bytesOf("alpha"), bytesOf("beta"), bytesOf("gamma-extra"))
    val payload = encodePlainBinary(values)
    val padPrefix = 11
    val padded = new Array[Byte](padPrefix + payload.length + 7)
    java.util.Arrays.fill(padded, 0, padPrefix, 0x5A.toByte) // sentinel before the data
    System.arraycopy(payload, 0, padded, padPrefix, payload.length)
    java.util.Arrays.fill(padded, padPrefix + payload.length, padded.length, 0x6B.toByte)
    // Slice to produce a buffer whose `arrayOffset() == padPrefix` (non-zero) and
    // `position() == 0`. ByteBuffer.wrap(arr, off, len) alone gives arrayOffset == 0
    // with position == off; slicing forces arrayOffset to advance.
    val full = ByteBuffer.wrap(padded, padPrefix, payload.length).slice()
    assert(full.arrayOffset() == padPrefix,
      s"sliced buffer must carry non-zero arrayOffset, got ${full.arrayOffset()}")
    val in = ByteBufferInputStream.wrap(full)
    val r = newReader(in)
    val v = new OnHeapColumnVector(values.length, BinaryType)
    r.readBinary(values.length, v, 0)
    assertVectorMatches(v, values)
    assert(in.position() == payload.length,
      s"stream must advance exactly past the data bytes, got ${in.position()}")
  }

  test("readBinary: subsequent read after bulk consumes the trailing bytes correctly") {
    // Pin the reset+skipFully arithmetic: after a bulk read, a follow-on read on the same
    // stream should return values from the not-yet-consumed tail of the underlying bytes.
    val firstBatch = Seq.tabulate(3)(i => bytesOf(s"a$i"))
    val secondBatch = Seq.tabulate(2)(i => bytesOf(s"b$i"))
    val bytes = encodePlainBinary(firstBatch ++ secondBatch)
    val in = ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    val r = newReader(in)
    val v = new OnHeapColumnVector(firstBatch.length + secondBatch.length, BinaryType)
    r.readBinary(firstBatch.length, v, 0)
    r.readBinary(secondBatch.length, v, firstBatch.length)
    assertVectorMatches(v, firstBatch ++ secondBatch)
    assert(in.position() == bytes.length, "stream must be fully consumed")
  }
}
