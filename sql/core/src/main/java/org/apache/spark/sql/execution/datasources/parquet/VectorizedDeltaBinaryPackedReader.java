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
import java.util.Arrays;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.spark.sql.execution.datasources.DataSourceUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;

/**
 * An implementation of the Parquet DELTA_BINARY_PACKED decoder that supports the vectorized
 * interface. DELTA_BINARY_PACKED is a delta encoding for integer and long types that stores values
 * as a delta between consecutive values. Delta values are themselves bit packed. Similar to RLE but
 * is more effective in the case of large variation of values in the encoded column.
 * <p>
 * DELTA_BINARY_PACKED is the default encoding for integer and long columns in Parquet V2.
 * <p>
 * Supported Types: INT32, INT64
 * <p>
 *
 * @see <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5">
 * Parquet format encodings: DELTA_BINARY_PACKED</a>
 */
public class VectorizedDeltaBinaryPackedReader extends VectorizedReaderBase {

  // header data
  private int blockSizeInValues;
  private int miniBlockNumInABlock;
  private int totalValueCount;
  private long firstValue;

  private int miniBlockSizeInValues;

  // values read by the caller
  private int valuesRead = 0;

  // variables to keep state of the current block and miniblock
  private long lastValueRead;  // needed to compute the next value
  private long minDeltaInCurrentBlock; // needed to compute the next value
  // currentMiniBlock keeps track of the mini block within the current block that
  // we read and decoded most recently. Only used as an index into
  // bitWidths array
  private int currentMiniBlock = 0;
  private int[] bitWidths; // bit widths for each miniBlock in the current block
  private int remainingInBlock = 0; // values in current block still to be read
  private int remainingInMiniBlock = 0; // values in current mini block still to be read
  private long[] unpackedValuesBuffer;

  private ByteBufferInputStream in;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    // Check argument using standard Parquet/Spark utility
    if (valueCount < 1) {
      throw new IOException("Page must have at least one value, but it has " + valueCount);
    }
    this.in = in;
    // Read the header
    this.blockSizeInValues = BytesUtils.readUnsignedVarInt(in);
    this.miniBlockNumInABlock = BytesUtils.readUnsignedVarInt(in);
    double miniSize = (double) blockSizeInValues / miniBlockNumInABlock;
    if (miniSize % 8 != 0) {
      throw new IOException("miniBlockSize must be multiple of 8, but it's " + miniSize);
    }
    this.miniBlockSizeInValues = (int) miniSize;
    // True value count. May be less than valueCount because of nulls
    this.totalValueCount = BytesUtils.readUnsignedVarInt(in);
    this.bitWidths = new int[miniBlockNumInABlock];
    this.unpackedValuesBuffer = new long[miniBlockSizeInValues];
    // read the first value
    firstValue = BytesUtils.readZigZagVarLong(in);

    // Reset state
    this.valuesRead = 0;
    this.remainingInBlock = 0;
    this.remainingInMiniBlock = 0;
  }

  // True value count. May be less than valueCount because of nulls
  int getTotalValueCount() {
    return totalValueCount;
  }

  // --- Optimized Vectorized Read Methods ---

  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    checkRemaining(total);
    int remaining = total;
    int currentRow = rowId;

    if (valuesRead == 0 && remaining > 0) {
      c.putInt(currentRow++, (int) firstValue);
      lastValueRead = firstValue;
      remaining--;
      valuesRead++;
    }

    while (remaining > 0) {
      prepareState();
      int n = Math.min(remaining, remainingInMiniBlock);
      int startIdx = miniBlockSizeInValues - remainingInMiniBlock;

      // Hoist members to local variables for register allocation
      long localLastValue = this.lastValueRead;
      long localMinDelta = this.minDeltaInCurrentBlock;

      for (int i = 0; i < n; i++) {
        localLastValue += localMinDelta + unpackedValuesBuffer[startIdx + i];
        c.putInt(currentRow + i, (int) localLastValue);
      }

      lastValueRead = localLastValue;
      updateStateAfterRead(n);
      currentRow += n;
      remaining -= n;
    }
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    checkRemaining(total);
    int remaining = total;
    int currentRow = rowId;

    if (valuesRead == 0 && remaining > 0) {
      c.putLong(currentRow++, firstValue);
      lastValueRead = firstValue;
      remaining--;
      valuesRead++;
    }

    while (remaining > 0) {
      prepareState();
      int n = Math.min(remaining, remainingInMiniBlock);
      int startIdx = miniBlockSizeInValues - remainingInMiniBlock;

      long localLastValue = this.lastValueRead;
      long localMinDelta = this.minDeltaInCurrentBlock;

      for (int i = 0; i < n; i++) {
        localLastValue += localMinDelta + unpackedValuesBuffer[startIdx + i];
        c.putLong(currentRow + i, localLastValue);
      }

      lastValueRead = localLastValue;
      updateStateAfterRead(n);
      currentRow += n;
      remaining -= n;
    }
  }

  // --- Specialized methods with Rebase logic ---

  @Override
  public final void readIntegersWithRebase(
          int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    checkRemaining(total);
    int remaining = total;
    int currentRow = rowId;

    while (remaining > 0) {
      // First value logic simplified within the loop context
      long v;
      if (valuesRead == 0) {
        v = firstValue;
        lastValueRead = firstValue;
        valuesRead++;
      } else {
        prepareState();
        v = lastValueRead + minDeltaInCurrentBlock +
                unpackedValuesBuffer[miniBlockSizeInValues - remainingInMiniBlock];
        lastValueRead = v;
        updateStateAfterRead(1);
      }

      if (v < RebaseDateTime.lastSwitchJulianDay()) {
        if (failIfRebase) throw DataSourceUtils.newRebaseExceptionInRead("Parquet");
        c.putInt(currentRow, RebaseDateTime.rebaseJulianToGregorianDays((int) v));
      } else {
        c.putInt(currentRow, (int) v);
      }
      currentRow++;
      remaining--;
    }
  }

  // --- Internal Decoding Logic ---

  /**
   * Ensure block and miniblock are loaded.
   */
  private void prepareState() {
    if (remainingInBlock == 0) {
      readBlockHeader();
    }
    if (remainingInMiniBlock == 0) {
      try {
        unpackMiniBlock();
      } catch (IOException e) {
        throw new ParquetDecodingException("Error unpacking mini block", e);
      }
    }
  }

  private void updateStateAfterRead(int n) {
    remainingInBlock -= n;
    remainingInMiniBlock -= n;
    valuesRead += n;
  }

  private void readBlockHeader() {
    try {
      minDeltaInCurrentBlock = BytesUtils.readZigZagVarLong(in);
    } catch (IOException e) {
      throw new ParquetDecodingException("Can not read min delta in current block", e);
    }
    readBitWidthsForMiniBlocks();
    remainingInBlock = blockSizeInValues;
    currentMiniBlock = 0;
    remainingInMiniBlock = 0;
  }

  private void unpackMiniBlock() throws IOException {
    int bitWidth = bitWidths[currentMiniBlock];
    if (bitWidth == 0) {
      Arrays.fill(unpackedValuesBuffer, 0);
      // Skip the bytes that would have been read (0 bytes for 0 bitwidth)
    } else {
      BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
      for (int j = 0; j < miniBlockSizeInValues; j += 8) {
        // Optimization: Use in.slice sparingly or use specialized unpacker if available
        ByteBuffer buffer = in.slice(bitWidth);
        if (buffer.hasArray()) {
          packer.unpack8Values(buffer.array(), buffer.arrayOffset() + buffer.position(), unpackedValuesBuffer, j);
        } else {
          packer.unpack8Values(buffer, buffer.position(), unpackedValuesBuffer, j);
        }
      }
    }
    remainingInMiniBlock = miniBlockSizeInValues;
    currentMiniBlock++;
  }

  private void readBitWidthsForMiniBlocks() {
    for (int i = 0; i < miniBlockNumInABlock; i++) {
      try {
        bitWidths[i] = BytesUtils.readIntLittleEndianOnOneByte(in);
      } catch (IOException e) {
        throw new ParquetDecodingException("Can not decode bitwidth in block header", e);
      }
    }
  }

  private void checkRemaining(int total) {
    if (valuesRead + total > totalValueCount) {
      throw new ParquetDecodingException("No more values to read.");
    }
  }

  // --- Fallback or Scalar Methods ---

  @Override
  public long readLong() {
    // For scalar reads, we keep it simple but correct
    if (valuesRead == 0) {
      lastValueRead = firstValue;
      valuesRead++;
      return firstValue;
    }
    prepareState();
    long delta = unpackedValuesBuffer[miniBlockSizeInValues - remainingInMiniBlock];
    long value = lastValueRead + minDeltaInCurrentBlock + delta;
    lastValueRead = value;
    updateStateAfterRead(1);
    return value;
  }

  @Override
  public int readInteger() { return (int) readLong(); }
  @Override
  public byte readByte() { return (byte) readLong(); }
  @Override
  public short readShort() { return (short) readLong(); }

  // Skip logic
  private void skipValues(int total) {
    // Skipping is still heavy because deltas are relative;
    // we must decode to maintain lastValueRead state.
    int remaining = total;
    while (remaining > 0) {
      prepareState();
      int n = Math.min(remaining, remainingInMiniBlock);
      int startIdx = miniBlockSizeInValues - remainingInMiniBlock;
      for (int i = 0; i < n; i++) {
        lastValueRead += minDeltaInCurrentBlock + unpackedValuesBuffer[startIdx + i];
      }
      updateStateAfterRead(n);
      remaining -= n;
    }
  }

  @Override public void skipBytes(int total) { skipValues(total); }
  @Override public void skipShorts(int total) { skipValues(total); }
  @Override public void skipIntegers(int total) { skipValues(total); }
  @Override public void skipLongs(int total) { skipValues(total); }
}
