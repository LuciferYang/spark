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
package org.apache.spark.sql.execution.datasources.v2;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A struct-typed {@link ColumnVector} backed by a fixed array of arbitrary child vectors.
 * Composes the V2 {@code _metadata} struct column from per-file
 * {@link org.apache.spark.sql.execution.vectorized.ConstantColumnVector}s (for constant
 * fields like {@code file_path}) and per-row child vectors supplied by the format reader
 * (e.g., Parquet's {@code _tmp_metadata_row_index}).
 *
 * <p>Intentionally minimal: only {@link #getChild(int)}, {@link #isNullAt(int)}, and
 * {@link #close()} carry behavior. The parent {@link ColumnVector#getStruct(int)} routes
 * struct field access through {@code getChild}, so scalar getters are never called and
 * throw if invoked.
 *
 * <p>Children are owned by their producers (the input batch or the metadata wrapper); this
 * class does not close them.
 */
final class CompositeStructColumnVector extends ColumnVector {

  private final ColumnVector[] children;

  CompositeStructColumnVector(StructType type, ColumnVector[] children) {
    super(type);
    if (children.length != type.fields().length) {
      throw new IllegalArgumentException(
          "Children count " + children.length + " does not match struct field count "
              + type.fields().length);
    }
    for (int i = 0; i < children.length; i++) {
      if (children[i] == null) {
        throw new IllegalArgumentException("Child column vector at index " + i + " is null");
      }
    }
    this.children = children;
  }

  @Override
  public void close() {
    // Children are owned by the underlying batch / extractor; do not close them here.
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public int numNulls() {
    return 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return false;
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return children[ordinal];
  }

  // Scalar accessors are unreachable for a struct vector and exist only to satisfy the
  // abstract base class contract.
  @Override
  public boolean getBoolean(int rowId) { throw unsupported("getBoolean"); }
  @Override
  public byte getByte(int rowId) { throw unsupported("getByte"); }
  @Override
  public short getShort(int rowId) { throw unsupported("getShort"); }
  @Override
  public int getInt(int rowId) { throw unsupported("getInt"); }
  @Override
  public long getLong(int rowId) { throw unsupported("getLong"); }
  @Override
  public float getFloat(int rowId) { throw unsupported("getFloat"); }
  @Override
  public double getDouble(int rowId) { throw unsupported("getDouble"); }
  @Override
  public ColumnarArray getArray(int rowId) { throw unsupported("getArray"); }
  @Override
  public ColumnarMap getMap(int ordinal) { throw unsupported("getMap"); }
  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw unsupported("getDecimal");
  }
  @Override
  public UTF8String getUTF8String(int rowId) { throw unsupported("getUTF8String"); }
  @Override
  public byte[] getBinary(int rowId) { throw unsupported("getBinary"); }

  private UnsupportedOperationException unsupported(String method) {
    return new UnsupportedOperationException(
        method + " is not supported on " + getClass().getSimpleName()
            + "; access struct fields via getChild()");
  }
}
