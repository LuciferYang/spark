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
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.ParquetDecodingException;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.GeographyType;
import org.apache.spark.sql.types.GeometryType;
import org.apache.spark.util.ByteBufferOutputStream;

/**
 * An implementation of the Parquet PLAIN decoder that supports the vectorized interface.
 */
public class VectorizedPlainValuesReader extends ValuesReader implements VectorizedValuesReader {
  private ByteBufferInputStream in = null;

  // Only used for booleans.
  private int bitOffset;
  private byte currentByte = 0;

  // Scratch arrays reused across {@link #readBinaryBulk} invocations to avoid per-batch
  // allocation when the bulk-write fast path is taken (single heap-backed source buffer
  // covering all rows). Sized lazily and grown as needed.
  private int[] scratchSrcOffsets;
  private int[] scratchLengths;

  public VectorizedPlainValuesReader() {
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.in = in;
  }

  @Override
  public void skip() {
    throw SparkUnsupportedOperationException.apply();
  }

  private void updateCurrentByte() {
    try {
      currentByte = (byte) in.read();
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read a byte", e);
    }
  }

  @Override
  public final void readBooleans(int total, WritableColumnVector c, int rowId) {
    int i = 0;
    if (bitOffset > 0) {
      i = Math.min(8 - bitOffset, total);
      c.putBooleans(rowId, i, currentByte, bitOffset);
      bitOffset = (bitOffset + i) & 7;
    }
    // Batch-read all full bytes in a single getBuffer call instead of per-byte in.read().
    // getBuffer returns a slice with position=0 and remaining=fullBytes.
    int fullBytes = (total - i) / 8;
    if (fullBytes > 0) {
      ByteBuffer buffer = getBuffer(fullBytes);
      if (buffer.hasArray()) {
        byte[] array = buffer.array();
        int offset = buffer.arrayOffset() + buffer.position();
        for (int j = 0; j < fullBytes; j++) {
          c.putBooleans(rowId + i + j * 8, array[offset + j]);
        }
      } else {
        for (int j = 0; j < fullBytes; j++) {
          c.putBooleans(rowId + i + j * 8, buffer.get());
        }
      }
      i += fullBytes * 8;
    }
    if (i < total) {
      updateCurrentByte();
      bitOffset = total - i;
      c.putBooleans(rowId + i, bitOffset, currentByte, 0);
    }
  }

  @Override
  public final void skipBooleans(int total) {
    int i = 0;
    if (bitOffset > 0) {
      i = Math.min(8 - bitOffset, total);
      bitOffset = (bitOffset + i) & 7;
    }
    if (i + 7 < total) {
      int numBytesToSkip = (total - i) / 8;
      try {
        in.skipFully(numBytesToSkip);
      } catch (IOException e) {
        throw new ParquetDecodingException("Failed to skip bytes", e);
      }
      i += numBytesToSkip * 8;
    }
    if (i < total) {
      updateCurrentByte();
      bitOffset = total - i;
    }
  }

  private ByteBuffer getBuffer(int length) {
    try {
      return in.slice(length).order(ByteOrder.LITTLE_ENDIAN);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
    }
  }

  @Override
  public final void readIntegers(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putIntsLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putInt(rowId + i, buffer.getInt());
      }
    }
  }

  @Override
  public void skipIntegers(int total) {
    in.skip(total * 4L);
  }

  @Override
  public final void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    for (int i = 0; i < total; i += 1) {
      c.putLong(rowId + i, Integer.toUnsignedLong(buffer.getInt()));
    }
  }

  @Override
  public final void readIntegersAsLongs(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    // No `hasArray` bulk-copy path: source (int32) and target (int64) have different byte
    // widths so a contiguous byte copy is impossible. Matches the pattern in peer
    // type-converting bulk methods such as `readUnsignedIntegers`.
    for (int i = 0; i < total; i += 1) {
      c.putLong(rowId + i, buffer.getInt());
    }
  }

  @Override
  public final void readIntegersAsDoubles(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    // No `hasArray` bulk-copy path: source (int32) and target (double, 8 bytes) have
    // different widths so a contiguous byte copy is impossible. Matches the pattern in
    // `readIntegersAsLongs` and `readUnsignedIntegers`.
    for (int i = 0; i < total; i += 1) {
      c.putDouble(rowId + i, buffer.getInt());
    }
  }

  @Override
  public final void readFloatsAsDoubles(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    // No `hasArray` bulk-copy path: source (float, 4 bytes) and target (double, 8 bytes)
    // have different widths so a contiguous byte copy is impossible. Matches the pattern
    // in `readIntegersAsLongs`.
    for (int i = 0; i < total; i += 1) {
      c.putDouble(rowId + i, buffer.getFloat());
    }
  }

  @Override
  public final void readLongsAsInts(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);
    // No `hasArray` bulk-copy path: source (int64, 8 bytes) and target (int32, 4 bytes)
    // have different widths so a contiguous byte copy is impossible. Matches the pattern
    // in `readIntegersAsLongs`.
    for (int i = 0; i < total; i += 1) {
      c.putInt(rowId + i, (int) buffer.getLong());
    }
  }

  @Override
  public final void readIntegersAsTimestampMicros(
      int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    // Per-element conversion calls into `DateTimeUtils.daysToMicros`, which is `days *
    // MICROS_PER_DAY` for UTC plus an overflow check via `Math.multiplyExact`. No
    // `hasArray` bulk-copy path because source and target have different widths.
    for (int i = 0; i < total; i += 1) {
      c.putLong(rowId + i, DateTimeUtils.daysToMicros(buffer.getInt(), ZoneOffset.UTC));
    }
  }

  // A fork of `readIntegers` to rebase the date values. For performance reasons, this method
  // iterates the values twice: check if we need to rebase first, then go to the optimized branch
  // if rebase is not needed.
  @Override
  public final void readIntegersWithRebase(
      int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);
    boolean rebase = false;
    for (int i = 0; i < total; i += 1) {
      rebase |= buffer.getInt(buffer.position() + i * 4) < RebaseDateTime.lastSwitchJulianDay();
    }
    if (rebase) {
      if (failIfRebase) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
      } else {
        for (int i = 0; i < total; i += 1) {
          c.putInt(rowId + i, RebaseDateTime.rebaseJulianToGregorianDays(buffer.getInt()));
        }
      }
    } else {
      if (buffer.hasArray()) {
        int offset = buffer.arrayOffset() + buffer.position();
        c.putIntsLittleEndian(rowId, total, buffer.array(), offset);
      } else {
        for (int i = 0; i < total; i += 1) {
          c.putInt(rowId + i, buffer.getInt());
        }
      }
    }
  }

  @Override
  public final void readLongs(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putLongsLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putLong(rowId + i, buffer.getLong());
      }
    }
  }

  @Override
  public void skipLongs(int total) {
    in.skip(total * 8L);
  }

  @Override
  public final void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);
    // scratch buffer: max 9 bytes (0x00 sign byte + 8 value bytes), reused per batch
    byte[] scratch = new byte[9];
    if (buffer.hasArray()) {
      byte[] src = buffer.array();
      int offset = buffer.arrayOffset() + buffer.position();
      for (int i = 0; i < total; i++, rowId++, offset += 8) {
        putLittleEndianBytesAsBigInteger(c, rowId, src, offset, scratch);
      }
    } else {
      // direct buffer fallback: copy 8 bytes per value
      byte[] data = new byte[8];
      for (int i = 0; i < total; i++, rowId++) {
        buffer.get(data);
        putLittleEndianBytesAsBigInteger(c, rowId, data, 0, scratch);
      }
    }
  }

  /**
   * Writes 8 little-endian bytes from {@code src[offset..offset+7]} into {@code c} at
   * {@code rowId} as a big-endian byte array compatible with {@link java.math.BigInteger}
   * two's-complement encoding.
   *
   * <p>The output matches the semantics of {@link java.math.BigInteger#toByteArray()}:
   * <ul>
   *   <li>Big-endian byte order</li>
   *   <li>Minimal encoding: no unnecessary leading zero bytes</li>
   *   <li>A {@code 0x00} sign byte is prepended if the most significant byte has bit 7
   *       set, ensuring the value is interpreted as positive by {@code BigInteger}</li>
   *   <li>Zero is encoded as {@code [0x00]} (1 byte), not an empty array, because
   *       {@code new BigInteger(new byte[0])} throws {@link NumberFormatException}</li>
   * </ul>
   *
   * <p>This is used by {@link #readUnsignedLongs} to store Parquet {@code UINT_64} values
   * into a {@code DecimalType(20, 0)} column vector, where each value is stored as a
   * byte array in {@code arrayData()} and later reconstructed via
   * {@code new BigInteger(bytes)}.
   *
   * @param c      the target column vector; must be of {@code DecimalType(20, 0)}
   * @param rowId  the row index to write into
   * @param src    the source byte array containing little-endian encoded data
   * @param offset the starting position in {@code src}; reads bytes
   *               {@code src[offset..offset+7]}
   * @param scratch a caller-provided reusable buffer of at least 9 bytes; its contents
   *                after this call are undefined
   */
  private static void putLittleEndianBytesAsBigInteger(
     WritableColumnVector c, int rowId, byte[] src, int offset, byte[] scratch) {
    // src is little-endian; the most significant byte is at src[offset + 7].
    // Scan from the most significant end to find the first non-zero byte,
    // which determines the minimal number of bytes needed for encoding.
    int msbIndex = offset + 7;
    while (msbIndex > offset && src[msbIndex] == 0) {
      msbIndex--;
    }

    // Zero value: must write [0x00] rather than an empty array.
    // BigInteger.ZERO.toByteArray() returns [0x00], and new BigInteger(new byte[0])
    // throws NumberFormatException("Zero length BigInteger").
    if (msbIndex == offset && src[offset] == 0) {
      scratch[0] = 0x00;
      // putByteArray copies the bytes into arrayData(), so scratch can be safely reused
      c.putByteArray(rowId, scratch, 0, 1);
      return;
    }

    // Prepend a 0x00 sign byte if the most significant byte has bit 7 set.
    // This matches BigInteger.toByteArray() behavior: positive values whose highest
    // magnitude byte has the MSB set are prefixed with 0x00 to distinguish them
    // from negative values in two's-complement encoding.
    boolean needSignByte = (src[msbIndex] & 0x80) != 0;
    int valueLen = msbIndex - offset + 1;
    int totalLen = needSignByte ? valueLen + 1 : valueLen;

    int scratchOffset = 0;
    if (needSignByte) {
      scratch[scratchOffset++] = 0x00;
    }
    // Reverse byte order: little-endian src → big-endian dest
    for (int i = msbIndex; i >= offset; i--) {
      scratch[scratchOffset++] = src[i];
    }
    // putByteArray copies the bytes into arrayData(), so scratch can be safely reused
    c.putByteArray(rowId, scratch, 0, totalLen);
  }

  // A fork of `readLongs` to rebase the timestamp values. For performance reasons, this method
  // iterates the values twice: check if we need to rebase first, then go to the optimized branch
  // if rebase is not needed.
  @Override
  public final void readLongsWithRebase(
      int total,
      WritableColumnVector c,
      int rowId,
      boolean failIfRebase,
      String timeZone) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);
    boolean rebase = false;
    for (int i = 0; i < total; i += 1) {
      rebase |= buffer.getLong(buffer.position() + i * 8) < RebaseDateTime.lastSwitchJulianTs();
    }
    if (rebase) {
      if (failIfRebase) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
      } else {
        for (int i = 0; i < total; i += 1) {
          c.putLong(
            rowId + i,
            RebaseDateTime.rebaseJulianToGregorianMicros(timeZone, buffer.getLong()));
        }
      }
    } else {
      if (buffer.hasArray()) {
        int offset = buffer.arrayOffset() + buffer.position();
        c.putLongsLittleEndian(rowId, total, buffer.array(), offset);
      } else {
        for (int i = 0; i < total; i += 1) {
          c.putLong(rowId + i, buffer.getLong());
        }
      }
    }
  }

  @Override
  public final void readFloats(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putFloatsLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putFloat(rowId + i, buffer.getFloat());
      }
    }
  }

  @Override
  public void skipFloats(int total) {
    in.skip(total * 4L);
  }

  @Override
  public final void readDoubles(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 8;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putDoublesLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putDouble(rowId + i, buffer.getDouble());
      }
    }
  }

  @Override
  public void skipDoubles(int total) {
    in.skip(total * 8L);
  }

  @Override
  public final void readBytes(int total, WritableColumnVector c, int rowId) {
    // Bytes are stored as a 4-byte little endian int. Just read the first byte.
    // TODO: consider pushing this in ColumnVector by adding a readBytes with a stride.
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      byte[] array = buffer.array();
      int offset = buffer.arrayOffset() + buffer.position();
      for (int i = 0; i < total; i++) {
        c.putByte(rowId + i, array[offset + i * 4]);
      }
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putByte(rowId + i, buffer.get());
        // skip the next 3 bytes
        buffer.position(buffer.position() + 3);
      }
    }
  }

  @Override
  public final void skipBytes(int total) {
    in.skip(total * 4L);
  }

  @Override
  public final void readShorts(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes);

    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putShortsFromIntsLittleEndian(rowId, total, buffer.array(), offset);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putShort(rowId + i, (short) buffer.getInt());
      }
    }
  }

  @Override
  public void skipShorts(int total) {
    in.skip(total * 4L);
  }

  @Override
  public final boolean readBoolean() {
    if (bitOffset == 0) {
      updateCurrentByte();
    }

    boolean v = (currentByte & (1 << bitOffset)) != 0;
    bitOffset += 1;
    if (bitOffset == 8) {
      bitOffset = 0;
    }
    return v;
  }

  @Override
  public final int readInteger() {
    return getBuffer(4).getInt();
  }

  @Override
  public final long readLong() {
    return getBuffer(8).getLong();
  }

  @Override
  public final byte readByte() {
    return (byte) readInteger();
  }

  @Override
  public short readShort() {
    return (short) readInteger();
  }

  @Override
  public final float readFloat() {
    return getBuffer(4).getFloat();
  }

  @Override
  public final double readDouble() {
    return getBuffer(8).getDouble();
  }

  @Override
  public final void readBinary(int total, WritableColumnVector v, int rowId) {
    if (total <= 0) {
      return;
    }
    // Single-row reads (called per-row from BinaryUpdater.readValue in nullable/PACKED paths)
    // bypass the bulk snapshot machinery; the mark/sliceBuffers/reset/skipFully overhead would
    // not amortize over a single row.
    if (total == 1) {
      int len = readInteger();
      if (len < 0) {
        throw new ParquetDecodingException("Negative binary length: " + len);
      }
      ByteBuffer buffer = getBuffer(len);
      if (buffer.hasArray()) {
        v.putByteArray(rowId, buffer.array(), buffer.arrayOffset() + buffer.position(), len);
      } else {
        v.putByteArray(rowId, buffer, buffer.position(), len);
      }
      return;
    }
    readBinaryBulk(total, v, rowId);
  }

  /**
   * Bulk path for {@link #readBinary(int, WritableColumnVector, int)} when {@code total >= 2}.
   * Snapshots the remaining stream once via {@link ByteBufferInputStream#sliceBuffers}, then walks
   * the resulting {@link ByteBuffer}s locally to avoid the per-row {@code ByteBuffer} wrapper
   * allocation the previous {@code getBuffer(4).getInt()} / {@code getBuffer(len)} pattern
   * incurred (two wrappers per row). {@code sliceBuffers} drains the underlying stream, so we
   * {@code mark} beforehand and {@code reset} + {@code skipFully(consumed)} after the walk to
   * leave the stream positioned exactly past the bytes actually consumed.
   */
  private void readBinaryBulk(int total, WritableColumnVector v, int rowId) {
    int available = in.available();
    in.mark(available);
    List<ByteBuffer> remaining;
    try {
      remaining = in.sliceBuffers(available);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to snapshot remaining bytes for readBinary", e);
    }
    for (ByteBuffer b : remaining) {
      b.order(ByteOrder.LITTLE_ENDIAN);
    }
    int nBuffers = remaining.size();

    // Fast path: a single heap-backed source buffer covers the entire batch. Pre-scan
    // lengths into scratch arrays, then make ONE bulk call to {@link
    // WritableColumnVector#putByteArrays} that batches the per-row offset/length metadata
    // writes (and capacity reserves) instead of paying that overhead N times.
    if (nBuffers == 1) {
      ByteBuffer only = remaining.get(0);
      if (only.hasArray()) {
        long consumedBulk = tryBulkReadBinary(total, v, rowId, only);
        if (consumedBulk >= 0) {
          try {
            in.reset();
            in.skipFully(consumedBulk);
          } catch (IOException e) {
            throw new ParquetDecodingException("Failed to advance stream after readBinary", e);
          }
          return;
        }
      }
    }

    int bufIdx = 0;
    ByteBuffer cur = nBuffers == 0 ? null : remaining.get(0);
    long consumed = 0;

    for (int i = 0; i < total; i++) {
      if (cur != null && cur.remaining() == 0) {
        bufIdx = nextNonEmpty(remaining, bufIdx + 1);
        cur = bufIdx < nBuffers ? remaining.get(bufIdx) : null;
      }

      int len;
      if (cur != null && cur.remaining() >= 4) {
        len = cur.getInt();
      } else {
        int acc = 0;
        int shift = 0;
        while (shift < 32) {
          if (cur == null || cur.remaining() == 0) {
            bufIdx = nextNonEmpty(remaining, bufIdx + 1);
            if (bufIdx >= nBuffers) {
              throw new ParquetDecodingException("Unexpected EOF while reading length");
            }
            cur = remaining.get(bufIdx);
          }
          acc |= (cur.get() & 0xFF) << shift;
          shift += 8;
        }
        len = acc;
      }
      if (len < 0) {
        throw new ParquetDecodingException("Negative binary length: " + len);
      }
      consumed += 4;

      if (cur != null && cur.remaining() == 0) {
        bufIdx = nextNonEmpty(remaining, bufIdx + 1);
        cur = bufIdx < nBuffers ? remaining.get(bufIdx) : null;
      }

      if (cur != null && cur.remaining() >= len) {
        if (cur.hasArray()) {
          v.putByteArray(rowId + i, cur.array(), cur.arrayOffset() + cur.position(), len);
        } else {
          v.putByteArray(rowId + i, cur, cur.position(), len);
        }
        cur.position(cur.position() + len);
      } else {
        byte[] tmp = new byte[len];
        int copied = 0;
        while (copied < len) {
          if (cur == null || cur.remaining() == 0) {
            bufIdx = nextNonEmpty(remaining, bufIdx + 1);
            if (bufIdx >= nBuffers) {
              throw new ParquetDecodingException("Unexpected EOF while reading payload");
            }
            cur = remaining.get(bufIdx);
          }
          int n = Math.min(len - copied, cur.remaining());
          cur.get(tmp, copied, n);
          copied += n;
        }
        v.putByteArray(rowId + i, tmp, 0, len);
      }
      consumed += len;
    }

    try {
      in.reset();
      in.skipFully(consumed);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to advance stream after readBinary", e);
    }
  }

  /**
   * Bulk fast path for {@link #readBinaryBulk}: when the entire batch lives in a single
   * heap-backed source buffer, pre-scan all {@code total} length integers (4 bytes each,
   * little-endian) into {@link #scratchLengths} while recording each payload's absolute
   * offset within the source array into {@link #scratchSrcOffsets}, then dispatch one
   * {@link WritableColumnVector#putByteArrays} call.
   *
   * @return total bytes consumed (length headers + payloads) on success, or {@code -1}
   *         if the bulk attempt is not viable (e.g. truncated stream) so the caller falls
   *         back to the per-row loop.
   */
  private long tryBulkReadBinary(int total, WritableColumnVector v, int rowId, ByteBuffer buf) {
    int[] offs = ensureScratchSrcOffsets(total);
    int[] lens = ensureScratchLengths(total);
    byte[] arr = buf.array();
    int base = buf.arrayOffset();
    int p = buf.position();
    int limit = buf.limit();
    for (int i = 0; i < total; i++) {
      if (limit - p < 4) {
        return -1L;
      }
      int len = (arr[base + p] & 0xFF)
              | ((arr[base + p + 1] & 0xFF) << 8)
              | ((arr[base + p + 2] & 0xFF) << 16)
              | ((arr[base + p + 3] & 0xFF) << 24);
      if (len < 0) {
        throw new ParquetDecodingException("Negative binary length: " + len);
      }
      p += 4;
      if (limit - p < len) {
        return -1L;
      }
      offs[i] = base + p;
      lens[i] = len;
      p += len;
    }
    v.putByteArrays(rowId, total, arr, offs, lens);
    return (long) (p - buf.position());
  }

  private int[] ensureScratchSrcOffsets(int n) {
    if (scratchSrcOffsets == null || scratchSrcOffsets.length < n) {
      int prev = scratchSrcOffsets == null ? 64 : scratchSrcOffsets.length * 2;
      scratchSrcOffsets = new int[Math.max(n, prev)];
    }
    return scratchSrcOffsets;
  }

  private int[] ensureScratchLengths(int n) {
    if (scratchLengths == null || scratchLengths.length < n) {
      int prev = scratchLengths == null ? 64 : scratchLengths.length * 2;
      scratchLengths = new int[Math.max(n, prev)];
    }
    return scratchLengths;
  }

  /**
   * Returns the index of the first non-empty buffer at or after {@code from},
   * or {@code bufs.size()} if none remain.
   */
  private static int nextNonEmpty(List<ByteBuffer> bufs, int from) {
    int n = bufs.size();
    int i = from;
    while (i < n && bufs.get(i).remaining() == 0) {
      i++;
    }
    return i;
  }

  @Override
  public void skipBinary(int total) {
    for (int i = 0; i < total; i++) {
      int len = readInteger();
      in.skip(len);
    }
  }

  @Override
  public final Binary readBinary(int len) {
    ByteBuffer buffer = getBuffer(len);
    if (buffer.hasArray()) {
      return Binary.fromConstantByteArray(
          buffer.array(), buffer.arrayOffset() + buffer.position(), len);
    } else {
      byte[] bytes = new byte[len];
      buffer.get(bytes);
      return Binary.fromConstantByteArray(bytes);
    }
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    in.skip(total * (long) len);
  }

  @Override
  public void readGeometry(int total, WritableColumnVector v, int rowId) {
    assert(v.dataType() instanceof GeometryType);
    int srid = ((GeometryType) v.dataType()).srid();
    try {
      readGeoData(total, v, rowId, srid, WKBToGeometryConverter.INSTANCE);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read geometry", e);
    }
  }

  @Override
  public void readGeography(int total, WritableColumnVector v, int rowId) {
    assert(v.dataType() instanceof GeographyType);
    int srid = ((GeographyType) v.dataType()).srid();
    try {
      readGeoData(total, v, rowId, srid, WKBToGeographyConverter.INSTANCE);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read geography", e);
    }
  }

  private void readGeoData(int total, WritableColumnVector v, int rowId, int srid,
     WKBConverterStrategy converter) throws IOException {
    // Go through the input stream and convert the WKB to the internal representation
    // writing it to the output buffer and putting the (offset, length) in the vector.
    // Finally, append all data from the output buffer in a single operation.
    int base = v.arrayData().getElementsAppended();
    int dataLen = 0;
    final int intSize = 4;
    ByteBuffer lenBuffer = ByteBuffer.allocate(intSize);
    ByteBufferOutputStream out = new ByteBufferOutputStream();

    for (int i = 0; i < total; i++) {
      int len = readInteger();

      // Converts WKB into a physical representation of geometry/geography.
      byte[] physicalValue = converter.convert(in.readNBytes(len), srid);
      v.putArray(rowId + i, base + dataLen + intSize, physicalValue.length);

      lenBuffer.putInt(0, physicalValue.length);
      out.write(lenBuffer.array());
      out.write(physicalValue);

      dataLen += intSize + physicalValue.length;
    }
    out.close();
    v.arrayData().appendBytes(dataLen, out.toByteArray(), 0);
  }
}
