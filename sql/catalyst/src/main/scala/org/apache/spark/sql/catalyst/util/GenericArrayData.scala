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

package org.apache.spark.sql.catalyst.util

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types._

class GenericArrayData (private val underlying: Any) extends ArrayData {

  // Backing array storage
  lazy val array: Array[Any] = underlying match {

    case arr: Array[Any] => arr  // array of objects, so no need to convert
    case arr: Array[_] => arr.toArray[Any] // array of primitives, so box them
    // Specified this as`scala.collection.Seq` because seqOrArray can be
    // `mutable.ArraySeq` in Scala 2.13
    case seq: scala.collection.Seq[Any] => seq.toArray
    case list: java.util.List[_] => list.asScala.toArray // array of primitives, so box them
    case _ => throw new IllegalArgumentException(s"Unexpected data type: ${underlying.getClass}")
  }
  
  override def copy(): ArrayData = {
    val newValues = new Array[Any](numElements())
    var i = 0
    while (i < newValues.length) {
      newValues(i) = InternalRow.copyValue(array(i))
      i += 1
    }
    new GenericArrayData(newValues)
  }

  override def numElements(): Int = underlying match {
    case seq: scala.collection.Seq[_] => seq.length
    case arr: Array[_] => arr.length
    case _ => array.length
  }

  private def getAs[T](ordinal: Int): T = array(ordinal).asInstanceOf[T]
  override def isNullAt(ordinal: Int): Boolean = array(ordinal) == null
  override def get(ordinal: Int, elementType: DataType): AnyRef = getAs(ordinal)
  override def getBoolean(ordinal: Int): Boolean = getAs(ordinal)
  override def getByte(ordinal: Int): Byte = getAs(ordinal)
  override def getShort(ordinal: Int): Short = getAs(ordinal)
  override def getInt(ordinal: Int): Int = getAs(ordinal)
  override def getLong(ordinal: Int): Long = getAs(ordinal)
  override def getFloat(ordinal: Int): Float = getAs(ordinal)
  override def getDouble(ordinal: Int): Double = getAs(ordinal)
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = getAs(ordinal)
  override def getUTF8String(ordinal: Int): UTF8String = getAs(ordinal)
  override def getBinary(ordinal: Int): Array[Byte] = getAs(ordinal)
  override def getGeography(ordinal: Int): GeographyVal = getAs(ordinal)
  override def getGeometry(ordinal: Int): GeometryVal = getAs(ordinal)
  override def getInterval(ordinal: Int): CalendarInterval = getAs(ordinal)
  override def getVariant(ordinal: Int): VariantVal = getAs(ordinal)
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = getAs(ordinal)
  override def getArray(ordinal: Int): ArrayData = getAs(ordinal)
  override def getMap(ordinal: Int): MapData = getAs(ordinal)

  override def setNullAt(ordinal: Int): Unit = array(ordinal) = null

  override def update(ordinal: Int, value: Any): Unit = array(ordinal) = value

  override def toString(): String = array.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericArrayData]
    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      val isNull1 = isNullAt(i)
      val isNull2 = other.isNullAt(i)
      if (isNull1 != isNull2) {
        return false
      }
      if (!isNull1) {
        val o1 = array(i)
        val o2 = other.array(i)
        if (!compareValues(o1, o2)) {
          return false
        }
      }
      i += 1
    }
    true
  }

  private def compareValues(v1: Any, v2: Any): Boolean = {
    v1 match {
      case b1: Array[Byte] =>
        v2.isInstanceOf[Array[Byte]] && java.util.Arrays.equals(b1, v2.asInstanceOf[Array[Byte]])
      case f1: Float =>
        if (java.lang.Float.isNaN(f1)) {
          v2.isInstanceOf[Float] && java.lang.Float.isNaN(v2.asInstanceOf[Float])
        } else {
          v1 == v2
        }
      case d1: Double =>
        if (java.lang.Double.isNaN(d1)) {
          v2.isInstanceOf[Double] && java.lang.Double.isNaN(v2.asInstanceOf[Double])
        } else {
          v1 == v2
        }
      case _ => v1 == v2
    }
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          array(i) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte => b.toInt
            case s: Short => s.toInt
            case i: Int => i
            case l: Long => (l ^ (l >>> 32)).toInt
            case f: Float => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case a: Array[Byte] => java.util.Arrays.hashCode(a)
            case other => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }
}
