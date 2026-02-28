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

package org.apache.spark.sql.execution.vectorized;

import java.nio.ByteBuffer;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A wrapper around a WritableColumnVector that supports lazy materialization.
 * The underlying vector is not populated until the first time a read method is called.
 */
public class LazyColumnVector extends WritableColumnVector {

  private final WritableColumnVector vector;
  private Runnable loadTask;
  private boolean isLoaded;

  public LazyColumnVector(WritableColumnVector vector) {
    // We pass 0 as capacity because we don't want the super class to allocate any memory.
    // We delegate everything to the wrapped vector.
    super(0, vector.dataType());
    this.vector = vector;
    this.isLoaded = false;
  }

  public void setLoadTask(Runnable loadTask) {
    this.loadTask = loadTask;
    this.isLoaded = false;
  }

  private void ensureLoaded() {
    if (!isLoaded) {
      if (loadTask != null) {
        loadTask.run();
      }
      isLoaded = true;
    }
  }

  @Override
  public void reset() {
    vector.reset();
    isLoaded = false;
    loadTask = null;
  }

  @Override
  public void close() {
    vector.close();
    super.close();
  }

  @Override
  public boolean hasNull() {
    ensureLoaded();
    return vector.hasNull();
  }

  @Override
  public int numNulls() {
    ensureLoaded();
    return vector.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    ensureLoaded();
    return vector.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    ensureLoaded();
    return vector.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    ensureLoaded();
    return vector.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    ensureLoaded();
    return vector.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    ensureLoaded();
    return vector.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    ensureLoaded();
    return vector.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    ensureLoaded();
    return vector.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    ensureLoaded();
    return vector.getDouble(rowId);
  }

  // getArray and getMap are final in WritableColumnVector, so we cannot override them.
  // Instead, we override the methods they rely on: arrayData(), getArrayOffset, getArrayLength, getChild.

  @Override
  public WritableColumnVector arrayData() {
    ensureLoaded();
    return vector.arrayData();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    ensureLoaded();
    return vector.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    ensureLoaded();
    return vector.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    ensureLoaded();
    return vector.getBinary(rowId);
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    ensureLoaded();
    // This is protected in WritableColumnVector.
    // We cannot access protected method of 'vector' if it's not in the same package.
    // LazyColumnVector is in org.apache.spark.sql.execution.vectorized, same as WritableColumnVector.
    // So we can access it.
    return vector.getBytesAsUTF8String(rowId, count);
  }

  @Override
  public ByteBuffer getByteBuffer(int rowId, int count) {
    ensureLoaded();
    return vector.getByteBuffer(rowId, count);
  }

  @Override
  public WritableColumnVector getChild(int ordinal) {
    ensureLoaded();
    return vector.getChild(ordinal);
  }
  
  // Writable methods - delegate directly without ensuring loaded

  @Override
  public void putNotNull(int rowId) {
    vector.putNotNull(rowId);
  }

  @Override
  public void putNull(int rowId) {
    vector.putNull(rowId);
  }

  @Override
  public void putNulls(int rowId, int count) {
    vector.putNulls(rowId, count);
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    vector.putNotNulls(rowId, count);
  }

  @Override
  public void putBoolean(int rowId, boolean value) {
    vector.putBoolean(rowId, value);
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    vector.putBooleans(rowId, count, value);
  }

  @Override
  public void putBooleans(int rowId, byte src) {
    vector.putBooleans(rowId, src);
  }

  @Override
  public void putByte(int rowId, byte value) {
    vector.putByte(rowId, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    vector.putBytes(rowId, count, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    vector.putBytes(rowId, count, src, srcIndex);
  }

  @Override
  public void putShort(int rowId, short value) {
    vector.putShort(rowId, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    vector.putShorts(rowId, count, value);
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    vector.putShorts(rowId, count, src, srcIndex);
  }

  @Override
  public void putShortsFromIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    vector.putShortsFromIntsLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    vector.putShorts(rowId, count, src, srcIndex);
  }

  @Override
  public void putInt(int rowId, int value) {
    vector.putInt(rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    vector.putInts(rowId, count, value);
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    vector.putInts(rowId, count, src, srcIndex);
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    vector.putInts(rowId, count, src, srcIndex);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    vector.putIntsLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public void putLong(int rowId, long value) {
    vector.putLong(rowId, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    vector.putLongs(rowId, count, value);
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    vector.putLongs(rowId, count, src, srcIndex);
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    vector.putLongs(rowId, count, src, srcIndex);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    vector.putLongsLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public void putFloat(int rowId, float value) {
    vector.putFloat(rowId, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    vector.putFloats(rowId, count, value);
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    vector.putFloats(rowId, count, src, srcIndex);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    vector.putFloats(rowId, count, src, srcIndex);
  }

  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    vector.putFloatsLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public void putDouble(int rowId, double value) {
    vector.putDouble(rowId, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    vector.putDoubles(rowId, count, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    vector.putDoubles(rowId, count, src, srcIndex);
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    vector.putDoubles(rowId, count, src, srcIndex);
  }

  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    vector.putDoublesLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    vector.putArray(rowId, offset, length);
  }

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int count) {
    return vector.putByteArray(rowId, value, offset, count);
  }

  @Override
  public int getArrayLength(int rowId) {
    ensureLoaded();
    return vector.getArrayLength(rowId);
  }

  @Override
  public int getArrayOffset(int rowId) {
    ensureLoaded();
    return vector.getArrayOffset(rowId);
  }

  @Override
  public WritableColumnVector reserveNewColumn(int capacity, DataType type) {
    return vector.reserveNewColumn(capacity, type);
  }

  @Override
  protected void reserveInternal(int capacity) {
    // This is called by reserve().
    // We should delegate to vector.reserve() but reserveInternal is protected.
    // However, WritableColumnVector.reserve calls reserveInternal.
    // If we call vector.reserve(), it will call vector.reserveInternal().
    // But we override reserve().
  }

  @Override
  public void reserve(int requiredCapacity) {
    vector.reserve(requiredCapacity);
  }
  
  @Override
  protected void releaseMemory() {
    // handled by vector.close() or explicit release if we exposed it.
    // WritableColumnVector.close() calls releaseMemory().
    // We delegated close(), so vector will release memory.
    // This method is called by WritableColumnVector logic which we largely bypass/delegate.
  }

  @Override
  public int getDictId(int rowId) {
    ensureLoaded();
    return vector.getDictId(rowId);
  }
}
