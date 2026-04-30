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

import java.nio.ByteBuffer
import java.util.PrimitiveIterator

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReaderTestUtils._
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.IntegerType

/**
 * Focused correctness tests for `VectorizedRleValuesReader.readBatch` PACKED-mode decoding,
 * covering patterns the P0 optimization cares about: run boundaries, batch boundaries, and
 * nested def-level grouping. Uses the same reflection bridge as the benchmark.
 *
 * Dispatcher coverage notes:
 *   - Tests that pass `null` rowIndexes (the default) route through the NoFilter branch
 *     of `readBatchInternal` (when `withDefLevels=false`) or
 *     `readBatchInternalWithDefLevels` (when `withDefLevels=true`).
 *   - `runAndAssertFiltered` routes through the WithFilter branch of `readBatchInternal`
 *     by default; with `withDefLevels=true`, through `readBatchInternalWithDefLevels`'s
 *     WithFilter branch.
 *   - The repeated/nested path (`readBatchRepeatedInternal*`) is exercised end-to-end by
 *     `ParquetVectorizedSuite`, not directly here, since this suite calls only the 5-arg
 *     `readBatch` overload.
 */
class VectorizedRleValuesReaderSuite extends SparkFunSuite {

  import VectorizedRleValuesReaderSuite._

  test("PACKED: alternating null/non-null (many single-element runs)") {
    val n = 1024
    val defLevels = Array.tabulate(n)(i => i & 1)
    runAndAssert(defLevels, maxDef = 1, batchSize = n, withDefLevels = false)
  }

  test("PACKED: 4-element runs aligned to 8-group boundaries") {
    val n = 1024
    val defLevels = Array.tabulate(n)(i => if ((i / 4) % 2 == 0) 0 else 1)
    runAndAssert(defLevels, maxDef = 1, batchSize = n, withDefLevels = false)
  }

  test("PACKED: long null and non-null runs within PACKED blocks") {
    // 7-long null then 1 non-null then 7-long null ... forces PACKED (each 8-group is mixed),
    // with asymmetric run lengths typical of realistic sparse data.
    val pattern = Array.fill(7)(0) ++ Array(1)
    val defLevels = Array.fill(128)(pattern).flatten.take(1024)
    runAndAssert(defLevels, maxDef = 1, batchSize = 1024, withDefLevels = false)
  }

  test("PACKED: runs span batch boundaries (state carries across readBatch calls)") {
    // 32-long null run starting at position 100 spans multiple 64-row batches.
    val n = 512
    val defLevels = Array.tabulate(n) { i =>
      if (i >= 100 && i < 132) 0 // null run
      else if ((i / 4) % 2 == 0) 0 // PACKED-forcing background pattern
      else 1
    }
    runAndAssert(defLevels, maxDef = 1, batchSize = 64, withDefLevels = false)
  }

  test("PACKED with defLevels: nested column maxDef=3 with mixed def-level values") {
    // Simulates a nested column where def-level values 0, 1, 2 all mean null (at different
    // nesting levels) and 3 means non-null. Tests that readValuesN groups by exact def-level
    // value so per-level null semantics are preserved.
    val pattern = Array(0, 1, 2, 3, 0, 1, 2, 3, 1, 2, 0, 3)
    val defLevels = Array.fill(64)(pattern).flatten.take(768)
    runAndAssert(defLevels, maxDef = 3, batchSize = 256, withDefLevels = true)
  }

  test("PACKED: cross-batch continuity with defLevels") {
    val n = 256
    val defLevels = Array.tabulate(n)(i => if ((i / 3) % 2 == 0) 0 else 1)
    runAndAssert(defLevels, maxDef = 1, batchSize = 64, withDefLevels = true)
  }

  test("RLE fast path: single long run, no nulls") {
    val defLevels = Array.fill(1024)(1)
    runAndAssert(defLevels, maxDef = 1, batchSize = 1024, withDefLevels = false)
  }

  test("RLE fast path: single long run, all nulls") {
    val defLevels = Array.fill(1024)(0)
    runAndAssert(defLevels, maxDef = 1, batchSize = 1024, withDefLevels = false)
  }

  test("PACKED group larger than initial 16-int currentBuffer (triggers buffer grow)") {
    // ~1024 alternating values produce one PACKED block well beyond the initial 16-int buffer,
    // exercising `new int[currentCount]` in readNextGroup.
    val defLevels = Array.tabulate(1024)(i => i & 1)
    runAndAssert(defLevels, maxDef = 1, batchSize = 1024, withDefLevels = false)
  }

  test("RLE + PACKED mixed in a single page") {
    val defLevels =
      Array.fill(200)(1) ++ Array.tabulate(256)(i => i & 1) ++ Array.fill(200)(0)
    runAndAssert(defLevels, maxDef = 1, batchSize = 256, withDefLevels = true)
  }

  test("required column (maxDef=0): single implicit RLE run") {
    val defLevels = Array.fill(64)(0)
    runAndAssert(defLevels, maxDef = 0, batchSize = 64, withDefLevels = false)
  }

  test("PACKED: row-index filtering with contiguous included range") {
    val n = 256
    val defLevels = Array.tabulate(n)(i => i & 1)
    runAndAssertFiltered(defLevels, maxDef = 1, includedPositions = (50 to 200).toArray)
  }

  test("PACKED: row-index filtering with multiple disjoint ranges") {
    val n = 256
    val defLevels = Array.tabulate(n)(i => if ((i / 3) % 2 == 0) 0 else 1)
    val included = ((10 to 30) ++ (80 to 120) ++ (200 to 240)).toArray
    runAndAssertFiltered(defLevels, maxDef = 1, includedPositions = included)
  }

  test("PACKED with defLevels: row-index filtering routes through WithDefLevelsWithFilter") {
    val n = 256
    val defLevels = Array.tabulate(n)(i => if ((i / 3) % 2 == 0) 0 else 1)
    val included = ((20 to 60) ++ (140 to 180)).toArray
    runAndAssertFiltered(
      defLevels, maxDef = 1, includedPositions = included, withDefLevels = true)
  }

  test("PACKED with defLevels: empty rowIndexes iterator skips all rows " +
    "(WithDefLevelsWithFilter skip-only path)") {
    // Same boundary as the 5-arg empty-iterator test, but routed through the WithDefLevels
    // dispatcher branch to exercise readBatchInternalWithDefLevelsWithFilter's
    // END_ROW_RANGE skip-only path (where every iteration calls skipValues, and
    // readValuesN is never invoked).
    val n = 256
    val defLevels = Array.tabulate(n)(i => i & 1)
    val bitWidth = 1
    val encoded = encodeRle(defLevels, bitWidth)
    val nonNullCount = defLevels.count(_ == 1)
    val plainBytes = plainIntBytes(nonNullCount)(valueAt)

    val reader = new VectorizedRleValuesReader(bitWidth, false)
    reader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
    val valueReader = new VectorizedPlainValuesReader
    valueReader.initFromPage(
      nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))

    val state = ParquetTestAccess.newState(
      intColumnDescriptor(maxDef = 1), isRequired = false,
      longIterator(Array.emptyIntArray))
    ParquetTestAccess.resetForNewPage(state, n, 0L)

    val batchSize = 64
    val values = new OnHeapColumnVector(batchSize, IntegerType)
    val defLevelsVec = new OnHeapColumnVector(batchSize, IntegerType)
    // Pre-fill defLevelsVec with a sentinel so a regression that incorrectly writes a
    // valid def-level value (e.g., 0) is still detectable. `0` is a real def-level and
    // matches the zero-init default; without poisoning we cannot distinguish unwritten
    // cells from spurious writes of 0.
    val sentinel = -1
    var k = 0
    while (k < batchSize) { defLevelsVec.putInt(k, sentinel); k += 1 }

    ParquetTestAccess.resetForNewBatch(state, batchSize)
    ParquetTestAccess.readBatch(
      reader, state, values, defLevelsVec, valueReader, integerUpdater)

    assert(values.numNulls() == 0,
      "no row matched the filter; putNulls/putNull should not have been called")
    var j = 0
    while (j < batchSize) {
      assert(!values.isNullAt(j), s"values cell $j unexpectedly marked null")
      assert(values.getInt(j) == 0, s"values cell $j unexpectedly written")
      assert(defLevelsVec.getInt(j) == sentinel,
        s"defLevels cell $j unexpectedly overwritten: ${defLevelsVec.getInt(j)}")
      j += 1
    }
  }

  test("multi-page filtered: row-index ranges spanning pages " +
    "(WithFilter resetForNewPage continuity)") {
    // Two pages, page indices 0-127 and 128-255. Filter selects positions in both pages
    // (range 50-200 spans the page boundary at 128). Verifies that resetForNewPage
    // correctly carries the WithFilter state across pages and that the row-range cursor
    // advances based on the absolute pageFirstRowIndex, not per-page-relative positions.
    val pageSize = 128
    val page1 = Array.tabulate(pageSize)(i => i & 1)
    val page2 = Array.tabulate(pageSize)(i => (i + 1) & 1)
    val combined = page1 ++ page2
    val included = (50 to 200).toArray
    val bitWidth = 1
    val nonNullCount = combined.count(_ == 1)

    val state = ParquetTestAccess.newState(
      intColumnDescriptor(maxDef = 1), isRequired = false, longIterator(included))

    val size = included.length
    val values = new OnHeapColumnVector(size, IntegerType)
    ParquetTestAccess.resetForNewBatch(state, size)

    // Drive both pages through the same state with separate readers/value-readers.
    val reader = new VectorizedRleValuesReader(bitWidth, false)
    var pageFirstRow = 0L
    var globalNonNullSeen = 0
    Seq(page1, page2).foreach { page =>
      val pageEncoded = encodeRle(page, bitWidth)
      val pageNonNullCount = page.count(_ == 1)
      val plainBytesForPage = plainIntBytes(pageNonNullCount)(i => valueAt(globalNonNullSeen + i))
      reader.initFromPage(page.length,
        ByteBufferInputStream.wrap(ByteBuffer.wrap(pageEncoded)))
      val valueReader = new VectorizedPlainValuesReader
      valueReader.initFromPage(pageNonNullCount,
        ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytesForPage)))
      ParquetTestAccess.resetForNewPage(state, page.length, pageFirstRow)
      ParquetTestAccess.readBatch(reader, state, values, null, valueReader, integerUpdater)
      pageFirstRow += page.length
      globalNonNullSeen += pageNonNullCount
    }
    // Self-check on the test fixture: confirms the per-page partitioning of
    // non-null counts equals the count over the concatenated input. Tautological by
    // construction (combined = page1 ++ page2), but guards against a future edit
    // that splits pages incorrectly.
    assert(globalNonNullSeen == nonNullCount, "internal: page partition incorrect")

    val prefixNonNulls = combined.scanLeft(0) { (c, d) =>
      c + (if (d == 1) 1 else 0)
    }
    var j = 0
    while (j < size) {
      val p = included(j)
      if (combined(p) == 1) {
        assert(!values.isNullAt(j), s"included pos $p (output $j) should be non-null")
        val expected = valueAt(prefixNonNulls(p))
        assert(values.getInt(j) == expected,
          s"included pos $p (output $j): got ${values.getInt(j)}, expected $expected")
      } else {
        assert(values.isNullAt(j), s"included pos $p (output $j) should be null")
      }
      j += 1
    }
  }

  test("multi-page: reader reinitialized between pages, state carried via resetForNewPage") {
    val page1 = Array.tabulate(128)(i => i & 1)
    val page2 = Array.fill(64)(1) ++ Array.tabulate(64)(i => i & 1)
    runAndAssertMultiPage(Seq(page1, page2), maxDef = 1, batchSize = 64)
  }

  test("dispatcher: hasNoRowRanges with null vs empty vs non-empty rowIndexes iterator") {
    val descriptor = intColumnDescriptor(maxDef = 1)
    // null rowIndexes: no filter installed, NoFilter dispatcher branch.
    val nullState = ParquetTestAccess.newState(descriptor, isRequired = false, null)
    assert(ParquetTestAccess.hasNoRowRanges(nullState),
      "null rowIndexes must report hasNoRowRanges == true")

    // Empty (but non-null) iterator: filter installed but selects nothing. WithFilter branch.
    val emptyState = ParquetTestAccess.newState(
      descriptor, isRequired = false, longIterator(Array.emptyIntArray))
    assert(!ParquetTestAccess.hasNoRowRanges(emptyState),
      "empty rowIndexes iterator must report hasNoRowRanges == false")

    // Non-empty iterator: filter installed, selects listed rows. WithFilter branch.
    val nonEmptyState = ParquetTestAccess.newState(
      descriptor, isRequired = false, longIterator(Array(0, 5, 10)))
    assert(!ParquetTestAccess.hasNoRowRanges(nonEmptyState),
      "non-empty rowIndexes iterator must report hasNoRowRanges == false")
  }

  test("PACKED: empty rowIndexes iterator routes to WithFilter and skips all rows") {
    // Boundary case for the dispatcher: an empty (but non-null) iterator routes to the
    // WithFilter branch with currentRange = END_ROW_RANGE = (Long.MAX_VALUE, Long.MIN_VALUE).
    // The first range check (rowId + n < rangeStart) is true for every iteration, so
    // skipValues is called repeatedly until the page is drained. Verifies the END_ROW_RANGE
    // skip path completes without exception when batchSize > 0.
    val n = 256
    val defLevels = Array.tabulate(n)(i => i & 1)
    val bitWidth = 1
    val encoded = encodeRle(defLevels, bitWidth)
    val nonNullCount = defLevels.count(_ == 1)
    val plainBytes = plainIntBytes(nonNullCount)(valueAt)

    val reader = new VectorizedRleValuesReader(bitWidth, false)
    reader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
    val valueReader = new VectorizedPlainValuesReader
    valueReader.initFromPage(
      nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))

    val state = ParquetTestAccess.newState(
      intColumnDescriptor(maxDef = 1), isRequired = false,
      longIterator(Array.emptyIntArray))
    assert(!ParquetTestAccess.hasNoRowRanges(state),
      "empty iterator should route to WithFilter")
    ParquetTestAccess.resetForNewPage(state, n, 0L)

    val batchSize = 64
    val values = new OnHeapColumnVector(batchSize, IntegerType)
    ParquetTestAccess.resetForNewBatch(state, batchSize)
    ParquetTestAccess.readBatch(reader, state, values, null, valueReader, integerUpdater)
    // Reader must not write anything when no row matches. `OnHeapColumnVector` zeroes its
    // backing arrays at construction, so a freshly-allocated vector reports `numNulls == 0`,
    // `isNullAt(j) == false`, and `getInt(j) == 0` for every cell. A regression that
    // erroneously calls `putInt` with a real `valueAt` (which is non-zero, see `valueAt`)
    // or `putNull(s)` while iterating the page would change one of these.
    assert(values.numNulls() == 0,
      "no row matched the filter; putNulls/putNull should not have been called")
    var j = 0
    while (j < batchSize) {
      assert(!values.isNullAt(j), s"cell $j unexpectedly marked null")
      assert(values.getInt(j) == 0, s"cell $j unexpectedly written: ${values.getInt(j)}")
      j += 1
    }
  }

  test("non-repeated dispatcher: empty batch (leftInBatch == 0) is a no-op for both branches") {
    // Pins the contract that the while-loop guard (`leftInBatch > 0 && leftInPage > 0`)
    // in readBatchInternal* short-circuits on entry when the caller passes batchSize == 0.
    // Exercises both branches of the non-repeated dispatcher: null rowIndexes -> NoFilter,
    // empty iterator -> WithFilter. The repeated dispatcher (readBatchRepeatedInternal*)
    // has a different guard `(leftInBatch > 0 || !state.lastListCompleted) && leftInPage > 0`
    // and is not exercised by this test; that path is covered end-to-end by
    // `ParquetVectorizedSuite`.
    val n = 16
    val defLevels = Array.tabulate(n)(i => i & 1)
    val bitWidth = 1
    val encoded = encodeRle(defLevels, bitWidth)
    val nonNullCount = defLevels.count(_ == 1)
    val plainBytes = plainIntBytes(nonNullCount)(valueAt)

    Seq(
      "noFilter" -> null,
      "withFilter" -> longIterator(Array.emptyIntArray)
    ).foreach { case (label, iter) =>
      val reader = new VectorizedRleValuesReader(bitWidth, false)
      reader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
      val valueReader = new VectorizedPlainValuesReader
      valueReader.initFromPage(
        nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
      val state = ParquetTestAccess.newState(
        intColumnDescriptor(maxDef = 1), isRequired = false, iter)
      ParquetTestAccess.resetForNewPage(state, n, 0L)
      // Zero-sized vector: any write would throw, so the assertion is implicit on success.
      val values = new OnHeapColumnVector(0, IntegerType)
      ParquetTestAccess.resetForNewBatch(state, 0)
      ParquetTestAccess.readBatch(
        reader, state, values, null, valueReader, integerUpdater)
      assert(values.numNulls() == 0, s"$label: empty-batch readBatch must not write nulls")
    }
  }
}

private object VectorizedRleValuesReaderSuite {

  /**
   * Runs readBatch end-to-end and asserts null-bits, non-null values, and def levels.
   * Each batch uses a fresh output vector since `state.valueOffset` resets to 0 per batch,
   * mirroring production where `VectorizedColumnReader` hands in a batch-sized vector.
   */
  // Non-trivial value formula: off-by-one mismatches won't coincidentally align.
  private def valueAt(idx: Int): Int = idx * 100 + 7

  private def runAndAssert(
      defLevels: Array[Int],
      maxDef: Int,
      batchSize: Int,
      withDefLevels: Boolean): Unit = {
    val n = defLevels.length
    val bitWidth = if (maxDef == 0) 0 else 32 - Integer.numberOfLeadingZeros(maxDef)
    // When bitWidth == 0 (required column), the reader treats the page as an implicit RLE run
    // of zeros and never consumes bytes; the encoded array is a placeholder.
    val encoded = if (bitWidth == 0) Array.emptyByteArray else encodeRle(defLevels, bitWidth)
    val nonNullCount = defLevels.count(_ == maxDef)
    val plainBytes = plainIntBytes(nonNullCount)(valueAt)

    val reader = new VectorizedRleValuesReader(bitWidth, false)
    reader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
    val valueReader = new VectorizedPlainValuesReader
    valueReader.initFromPage(
      nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
    val state = ParquetTestAccess.newState(intColumnDescriptor(maxDef), maxDef == 0)
    ParquetTestAccess.resetForNewPage(state, n, 0L)

    var produced = 0
    var expectedValueIdx = 0
    while (produced < n) {
      val toRead = math.min(batchSize, n - produced)
      val values = new OnHeapColumnVector(toRead, IntegerType)
      val defLevelsVec = new OnHeapColumnVector(toRead, IntegerType)
      ParquetTestAccess.resetForNewBatch(state, toRead)
      val defLevelsArg: WritableColumnVector = if (withDefLevels) defLevelsVec else null
      ParquetTestAccess.readBatch(
        reader, state, values, defLevelsArg, valueReader, integerUpdater)

      var expectedNullsInBatch = 0
      var i = 0
      while (i < toRead) {
        val absPos = produced + i
        if (defLevels(absPos) == maxDef) {
          assert(!values.isNullAt(i), s"pos $absPos should be non-null")
          val expected = valueAt(expectedValueIdx)
          assert(
            values.getInt(i) == expected,
            s"pos $absPos value mismatch: got ${values.getInt(i)}, expected $expected")
          expectedValueIdx += 1
        } else {
          assert(values.isNullAt(i), s"pos $absPos should be null")
          expectedNullsInBatch += 1
        }
        if (withDefLevels) {
          assert(
            defLevelsVec.getInt(i) == defLevels(absPos),
            s"defLevel at pos $absPos: got ${defLevelsVec.getInt(i)}, " +
              s"expected ${defLevels(absPos)}")
        }
        i += 1
      }
      assert(
        values.numNulls() == expectedNullsInBatch,
        s"batch starting at $produced: numNulls ${values.numNulls()}, " +
          s"expected $expectedNullsInBatch")
      produced += toRead
    }
  }

  /**
   * Variant of `runAndAssert` that passes a `rowIndexes` iterator so the reader only emits
   * rows at the listed positions. Verifies that skipped value positions advance the value
   * reader correctly and that included rows map to the expected values in order. When
   * `withDefLevels` is true, also asserts the def-level vector matches the source.
   */
  private def runAndAssertFiltered(
      defLevels: Array[Int],
      maxDef: Int,
      includedPositions: Array[Int],
      withDefLevels: Boolean = false): Unit = {
    val n = defLevels.length
    val bitWidth = if (maxDef == 0) 0 else 32 - Integer.numberOfLeadingZeros(maxDef)
    val encoded = if (bitWidth == 0) Array.emptyByteArray else encodeRle(defLevels, bitWidth)
    val nonNullCount = defLevels.count(_ == maxDef)
    val plainBytes = plainIntBytes(nonNullCount)(valueAt)

    val reader = new VectorizedRleValuesReader(bitWidth, false)
    reader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
    val valueReader = new VectorizedPlainValuesReader
    valueReader.initFromPage(
      nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
    val state = ParquetTestAccess.newState(
      intColumnDescriptor(maxDef), maxDef == 0, longIterator(includedPositions))
    ParquetTestAccess.resetForNewPage(state, n, 0L)

    val size = includedPositions.length
    val values = new OnHeapColumnVector(size, IntegerType)
    val defLevelsVec = new OnHeapColumnVector(size, IntegerType)
    ParquetTestAccess.resetForNewBatch(state, size)
    val defLevelsArg: WritableColumnVector = if (withDefLevels) defLevelsVec else null
    ParquetTestAccess.readBatch(
      reader, state, values, defLevelsArg, valueReader, integerUpdater)

    val prefixNonNulls = defLevels.scanLeft(0) { (c, d) =>
      c + (if (d == maxDef) 1 else 0)
    }
    var j = 0
    while (j < size) {
      val p = includedPositions(j)
      if (defLevels(p) == maxDef) {
        assert(!values.isNullAt(j), s"included pos $p (output $j) should be non-null")
        val expected = valueAt(prefixNonNulls(p))
        assert(
          values.getInt(j) == expected,
          s"included pos $p (output $j): got ${values.getInt(j)}, expected $expected")
      } else {
        assert(values.isNullAt(j), s"included pos $p (output $j) should be null")
      }
      if (withDefLevels) {
        assert(
          defLevelsVec.getInt(j) == defLevels(p),
          s"defLevel at included pos $p (output $j): got ${defLevelsVec.getInt(j)}, " +
            s"expected ${defLevels(p)}")
      }
      j += 1
    }
  }

  /**
   * Simulates a column chunk with multiple pages: the same reader instance is reused, pointing
   * to fresh encoded bytes per page and with `resetForNewPage` called between pages.
   */
  private def runAndAssertMultiPage(
      pages: Seq[Array[Int]],
      maxDef: Int,
      batchSize: Int): Unit = {
    val bitWidth = if (maxDef == 0) 0 else 32 - Integer.numberOfLeadingZeros(maxDef)
    val reader = new VectorizedRleValuesReader(bitWidth, false)
    val state =
      ParquetTestAccess.newState(intColumnDescriptor(maxDef), maxDef == 0)

    var pageFirstRow = 0L
    pages.foreach { pageDefLevels =>
      val pageN = pageDefLevels.length
      val encoded = if (bitWidth == 0) Array.emptyByteArray else encodeRle(pageDefLevels, bitWidth)
      val nonNullCount = pageDefLevels.count(_ == maxDef)
      val plainBytes = plainIntBytes(nonNullCount)(valueAt)

      reader.initFromPage(pageN, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
      val valueReader = new VectorizedPlainValuesReader
      valueReader.initFromPage(
        nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
      ParquetTestAccess.resetForNewPage(state, pageN, pageFirstRow)

      var produced = 0
      var expectedValueIdx = 0
      while (produced < pageN) {
        val toRead = math.min(batchSize, pageN - produced)
        val values = new OnHeapColumnVector(toRead, IntegerType)
        ParquetTestAccess.resetForNewBatch(state, toRead)
        ParquetTestAccess.readBatch(
          reader, state, values, null, valueReader, integerUpdater)

        var i = 0
        while (i < toRead) {
          val absPos = produced + i
          if (pageDefLevels(absPos) == maxDef) {
            assert(!values.isNullAt(i), s"page@$pageFirstRow pos $absPos should be non-null")
            val expected = valueAt(expectedValueIdx)
            assert(values.getInt(i) == expected)
            expectedValueIdx += 1
          } else {
            assert(values.isNullAt(i), s"page@$pageFirstRow pos $absPos should be null")
          }
          i += 1
        }
        produced += toRead
      }
      pageFirstRow += pageN
    }
  }

  private def longIterator(values: Array[Int]): PrimitiveIterator.OfLong =
    new PrimitiveIterator.OfLong {
      private var idx = 0
      override def hasNext: Boolean = idx < values.length
      override def nextLong(): Long = { val v = values(idx).toLong; idx += 1; v }
    }
}
