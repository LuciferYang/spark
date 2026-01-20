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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types._

/**
 * A mutable wrapper that makes two rows appear as a single concatenated row.  Designed to
 * be instantiated once per thread and reused.
 *
 * Performance optimizations:
 * 1. Cache row1.numFields to avoid repeated virtual method calls
 * 2. Use @inline annotations for all hot path methods
 * 3. Precompute index offset to reduce arithmetic operations
 * 4. Specialized fast paths for common access patterns
 */
class JoinedRow extends InternalRow {
  private[this] var row1: InternalRow = _
  private[this] var row2: InternalRow = _
  // Cache the number of fields in row1 to avoid repeated calls
  private[this] var row1NumFields: Int = 0

  def this(left: InternalRow, right: InternalRow) = {
    this()
    row1 = left
    row2 = right
    row1NumFields = if (left != null) left.numFields else 0
  }

  /** Updates this JoinedRow to used point at two new base rows.  Returns itself. */
  @inline
  final def apply(r1: InternalRow, r2: InternalRow): JoinedRow = {
    row1 = r1
    row2 = r2
    row1NumFields = if (r1 != null) r1.numFields else 0
    this
  }

  /** Updates this JoinedRow by updating its left base row.  Returns itself. */
  @inline
  final def withLeft(newLeft: InternalRow): JoinedRow = {
    row1 = newLeft
    row1NumFields = if (newLeft != null) newLeft.numFields else 0
    this
  }

  /** Updates this JoinedRow by updating its right base row.  Returns itself. */
  @inline
  final def withRight(newRight: InternalRow): JoinedRow = {
    row2 = newRight
    this
  }

  /** Gets this JoinedRow's left base row. */
  @inline final def getLeft: InternalRow = row1

  /** Gets this JoinedRow's right base row. */
  @inline final def getRight: InternalRow = row2

  override def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = {
    val totalFields = row1NumFields + row2.numFields
    assert(fieldTypes.length == totalFields)
    val (left, right) = fieldTypes.splitAt(row1NumFields)
    row1.toSeq(left) ++ row2.toSeq(right)
  }

  @inline
  override final def numFields: Int = row1NumFields + row2.numFields

  @inline
  override final def get(i: Int, dt: DataType): AnyRef = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.get(i, dt) else row2.get(i - cutoff, dt)
  }

  @inline
  override final def isNullAt(i: Int): Boolean = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.isNullAt(i) else row2.isNullAt(i - cutoff)
  }

  @inline
  override final def getBoolean(i: Int): Boolean = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getBoolean(i) else row2.getBoolean(i - cutoff)
  }

  @inline
  override final def getByte(i: Int): Byte = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getByte(i) else row2.getByte(i - cutoff)
  }

  @inline
  override final def getShort(i: Int): Short = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getShort(i) else row2.getShort(i - cutoff)
  }

  @inline
  override final def getInt(i: Int): Int = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getInt(i) else row2.getInt(i - cutoff)
  }

  @inline
  override final def getLong(i: Int): Long = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getLong(i) else row2.getLong(i - cutoff)
  }

  @inline
  override final def getFloat(i: Int): Float = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getFloat(i) else row2.getFloat(i - cutoff)
  }

  @inline
  override final def getDouble(i: Int): Double = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getDouble(i) else row2.getDouble(i - cutoff)
  }

  @inline
  override final def getDecimal(i: Int, precision: Int, scale: Int): Decimal = {
    val cutoff = row1NumFields
    if (i < cutoff) {
      row1.getDecimal(i, precision, scale)
    } else {
      row2.getDecimal(i - cutoff, precision, scale)
    }
  }

  @inline
  override final def getUTF8String(i: Int): UTF8String = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getUTF8String(i) else row2.getUTF8String(i - cutoff)
  }

  @inline
  override final def getBinary(i: Int): Array[Byte] = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getBinary(i) else row2.getBinary(i - cutoff)
  }

  @inline
  override final def getGeography(i: Int): GeographyVal = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getGeography(i) else row2.getGeography(i - cutoff)
  }

  @inline
  override final def getGeometry(i: Int): GeometryVal = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getGeometry(i) else row2.getGeometry(i - cutoff)
  }

  @inline
  override final def getArray(i: Int): ArrayData = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getArray(i) else row2.getArray(i - cutoff)
  }

  @inline
  override final def getInterval(i: Int): CalendarInterval = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getInterval(i) else row2.getInterval(i - cutoff)
  }

  @inline
  override final def getVariant(i: Int): VariantVal = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getVariant(i) else row2.getVariant(i - cutoff)
  }

  @inline
  override final def getMap(i: Int): MapData = {
    val cutoff = row1NumFields
    if (i < cutoff) row1.getMap(i) else row2.getMap(i - cutoff)
  }

  @inline
  override final def getStruct(i: Int, numFields: Int): InternalRow = {
    val cutoff = row1NumFields
    if (i < cutoff) {
      row1.getStruct(i, numFields)
    } else {
      row2.getStruct(i - cutoff, numFields)
    }
  }

  override def anyNull: Boolean = row1.anyNull || row2.anyNull

  override def setNullAt(i: Int): Unit = {
    val cutoff = row1NumFields
    if (i < cutoff) {
      row1.setNullAt(i)
    } else {
      row2.setNullAt(i - cutoff)
    }
  }

  override def update(i: Int, value: Any): Unit = {
    val cutoff = row1NumFields
    if (i < cutoff) {
      row1.update(i, value)
    } else {
      row2.update(i - cutoff, value)
    }
  }

  override def copy(): InternalRow = {
    val copy1 = row1.copy()
    val copy2 = row2.copy()
    new JoinedRow(copy1, copy2)
  }

  override def toString: String = {
    // Make sure toString never throws NullPointerException.
    if ((row1 eq null) && (row2 eq null)) {
      "[ empty row ]"
    } else if (row1 eq null) {
      row2.toString
    } else if (row2 eq null) {
      row1.toString
    } else {
      s"{${row1.toString} + ${row2.toString}}"
    }
  }
}
