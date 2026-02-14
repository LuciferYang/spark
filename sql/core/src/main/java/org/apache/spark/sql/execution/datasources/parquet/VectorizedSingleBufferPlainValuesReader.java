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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * An implementation of the Parquet PLAIN decoder that supports the vectorized interface.
 */
public class VectorizedSingleBufferPlainValuesReader extends ValuesReader implements VectorizedValuesReader {
  private ByteBuffer buffer = null;

  // Only used for booleans.
  private int bitOffset;
  private byte currentByte = 0;

  public VectorizedSingleBufferPlainValuesReader() {
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    assert "org.apache.parquet.bytes.SingleBufferInputStream".equals(in.getClass().getName()) :
      "VectorizedSingleBufferPlainValuesReader only supports SingleBufferInputStream, but got: "
        + in.getClass().getName();
    try {
      this.buffer = in.slice((int) in.available()).order(ByteOrder.LITTLE_ENDIAN);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read a page", e);
    }
  }

  @Override
  public void skip() {
    throw SparkUnsupportedOperationException.apply();
  }

  private void checkBufferSize(int requiredBytes) {
    if (buffer.remaining() < requiredBytes) {
      throw new ParquetDecodingException("Failed to read " + requiredBytes + " bytes");
    }
  }

  private void updateCurrentByte() {
    checkBufferSize(1);
    currentByte = buffer.get();
  }

  @Override
  public final void readBooleans(int total, WritableColumnVector c, int rowId) {
    int i = 0;
    if (bitOffset > 0) {
      i = Math.min(8 - bitOffset, total);
      c.putBooleans(rowId, i, currentByte, bitOffset);
      bitOffset = (bitOffset + i) & 7;
    }
    for (; i + 7 < total; i += 8) {
      updateCurrentByte();
      c.putBooleans(rowId + i, currentByte);
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
      buffer.position(buffer.position() + numBytesToSkip);
      i += numBytesToSkip * 8;
    }
    if (i < total) {
      updateCurrentByte();
      bitOffset = total - i;
    }
  }

  @Override
  public final void readIntegers(int total, WritableColumnVector c, int rowId) {
    checkBufferSize(total * 4);
    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putIntsLittleEndian(rowId, total, buffer.array(), offset);
      buffer.position(buffer.position() + total * 4);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putInt(rowId + i, buffer.getInt());
      }
    }
  }

  @Override
  public void skipIntegers(int total) {
    checkBufferSize(total * 4);
    buffer.position(buffer.position() + total * 4);
  }

  @Override
  public final void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    checkBufferSize(total * 4);
    for (int i = 0; i < total; i += 1) {
      c.putLong(rowId + i, Integer.toUnsignedLong(buffer.getInt()));
    }
  }

  // A fork of `readIntegers` to rebase the date values. For performance reasons, this method
  // iterates the values twice: check if we need to rebase first, then go to the optimized branch
  // if rebase is not needed.
  @Override
  public final void readIntegersWithRebase(
      int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
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
      readIntegers(total, c, rowId);
    }
  }

  @Override
  public final void readLongs(int total, WritableColumnVector c, int rowId) {
    checkBufferSize(total * 8);
    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putLongsLittleEndian(rowId, total, buffer.array(), offset);
      buffer.position(buffer.position() + total * 8);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putLong(rowId + i, buffer.getLong());
      }
    }
  }

  @Override
  public void skipLongs(int total) {
    checkBufferSize(total * 8);
    buffer.position(buffer.position() + total * 8);
  }

  @Override
  public final void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    checkBufferSize(total * 8);
    for (int i = 0; i < total; i += 1) {
      c.putByteArray(
        rowId + i, new BigInteger(Long.toUnsignedString(buffer.getLong())).toByteArray());
    }
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
      readLongs(total, c, rowId);
    }
  }

  @Override
  public final void readFloats(int total, WritableColumnVector c, int rowId) {
    checkBufferSize(total * 4);
    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putFloatsLittleEndian(rowId, total, buffer.array(), offset);
      buffer.position(buffer.position() + total * 4);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putFloat(rowId + i, buffer.getFloat());
      }
    }
  }

  @Override
  public void skipFloats(int total) {
    checkBufferSize(total * 4);
    buffer.position(buffer.position() + total * 4);
  }

  @Override
  public final void readDoubles(int total, WritableColumnVector c, int rowId) {
    checkBufferSize(total * 8);
    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      c.putDoublesLittleEndian(rowId, total, buffer.array(), offset);
      buffer.position(buffer.position() + total * 8);
    } else {
      for (int i = 0; i < total; i += 1) {
        c.putDouble(rowId + i, buffer.getDouble());
      }
    }
  }

  @Override
  public void skipDoubles(int total) {
    checkBufferSize(total * 8);
    buffer.position(buffer.position() + total * 8);
  }

  @Override
  public final void readBytes(int total, WritableColumnVector c, int rowId) {
    // Bytes are stored as a 4-byte little endian int. Just read the first byte.
    // TODO: consider pushing this in ColumnVector by adding a readBytes with a stride.
    checkBufferSize(total * 4);
    if (buffer.hasArray()) {
      // Fast path: direct array access
      byte[] array = buffer.array();
      int offset = buffer.arrayOffset() + buffer.position();
      for (int i = 0; i < total; i++) {
        c.putByte(rowId + i, array[offset + i * 4]);
      }
      buffer.position(buffer.position() + total * 4);
    } else {
      // Slow path: use ByteBuffer methods
      for (int i = 0; i < total; i += 1) {
        c.putByte(rowId + i, buffer.get());
        // skip the next 3 bytes
        buffer.position(buffer.position() + 3);
      }
    }
  }

  @Override
  public final void skipBytes(int total) {
    checkBufferSize(total * 4);
    buffer.position(buffer.position() + total * 4);
  }

  @Override
  public final void readShorts(int total, WritableColumnVector c, int rowId) {
    checkBufferSize(total * 4);
    for (int i = 0; i < total; i += 1) {
      c.putShort(rowId + i, (short) buffer.getInt());
    }
  }

  @Override
  public void skipShorts(int total) {
    checkBufferSize(total * 4);
    buffer.position(buffer.position() + total * 4);
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
    checkBufferSize(4);
    return buffer.getInt();
  }

  @Override
  public final long readLong() {
    checkBufferSize(8);
    return buffer.getLong();
  }

  @Override
  public final byte readByte() {
    return (byte) readInteger();
  }

  @Override
  public final short readShort() {
    return (short) readInteger();
  }

  @Override
  public final float readFloat() {
    checkBufferSize(4);
    return buffer.getFloat();
  }

  @Override
  public final double readDouble() {
    checkBufferSize(8);
    return buffer.getDouble();
  }

  @Override
  public final void readBinary(int total, WritableColumnVector v, int rowId) {
    for (int i = 0; i < total; i++) {
      int len = readInteger();
      checkBufferSize(len);
      if (buffer.hasArray()) {
        v.putByteArray(rowId + i, buffer.array(), buffer.arrayOffset() + buffer.position(), len);
        buffer.position(buffer.position() + len);
      } else {
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        v.putByteArray(rowId + i, bytes);
      }
    }
  }

  @Override
  public void skipBinary(int total) {
    for (int i = 0; i < total; i++) {
      int len = readInteger();
      buffer.position(buffer.position() + len);
    }
  }

  @Override
  public final Binary readBinary(int len) {
    checkBufferSize(len);
    if (buffer.hasArray()) {
      Binary result = Binary.fromConstantByteArray(
          buffer.array(), buffer.arrayOffset() + buffer.position(), len);
      buffer.position(buffer.position() + len);
      return result;
    } else {
      byte[] bytes = new byte[len];
      buffer.get(bytes);
      return Binary.fromConstantByteArray(bytes);
    }
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    checkBufferSize((int) (total * (long) len));
    buffer.position(buffer.position() + (int) (total * (long) len));
  }
}
