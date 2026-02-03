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

class GenericArrayData(private var data: Any) extends ArrayData {

  // Specified this as`scala.collection.Seq` because seqOrArray can be
  // `mutable.ArraySeq` in Scala 2.13
  def this(seq: scala.collection.Seq[Any]) = this(seq.toArray)
  def this(list: java.util.List[Any]) = this(list.asScala.toArray)

  // Get the internal data as Array[Any], converting primitive arrays if necessary
  private def ensureAnyArray(): Array[Any] = {
    data = data match {
      case arr: Array[Any] => arr
      case arr: Array[Int] => arr.toArray[Any]
      case arr: Array[Long] => arr.toArray[Any]
      case arr: Array[Float] => arr.toArray[Any]
      case arr: Array[Double] => arr.toArray[Any]
      case arr: Array[Short] => arr.toArray[Any]
      case arr: Array[Byte] => arr.toArray[Any]
      case arr: Array[Boolean] => arr.toArray[Any]
      case _ => throw new IllegalStateException(s"Unexpected data type: ${data.getClass}")
    }
    data.asInstanceOf[Array[Any]]
  }

  // Get the internal data as Array[Any] (for backward compatibility)
  override def array: Array[Any] = ensureAnyArray()

  override def copy(): ArrayData = {
    data match {
      case arr: Array[Any] =>
        val newValues = new Array[Any](arr.length)
        var i = 0
        while (i < arr.length) {
          newValues(i) = InternalRow.copyValue(arr(i))
          i += 1
        }
        new GenericArrayData(newValues)
      case arr: Array[Int] => new GenericArrayData(arr.clone())
      case arr: Array[Long] => new GenericArrayData(arr.clone())
      case arr: Array[Float] => new GenericArrayData(arr.clone())
      case arr: Array[Double] => new GenericArrayData(arr.clone())
      case arr: Array[Short] => new GenericArrayData(arr.clone())
      case arr: Array[Byte] => new GenericArrayData(arr.clone())
      case arr: Array[Boolean] => new GenericArrayData(arr.clone())
      case _ => throw new IllegalStateException(s"Unexpected data type: ${data.getClass}")
    }
  }

  override def numElements(): Int = {
    data match {
      case arr: Array[_] => arr.length
      case _ => throw new IllegalStateException(s"Unexpected data type: ${data.getClass}")
    }
  }

  private def getAs[T](ordinal: Int): T = {
    data match {
      case arr: Array[Any] => arr(ordinal).asInstanceOf[T]
      case arr: Array[Int] => arr(ordinal).asInstanceOf[T]
      case arr: Array[Long] => arr(ordinal).asInstanceOf[T]
      case arr: Array[Float] => arr(ordinal).asInstanceOf[T]
      case arr: Array[Double] => arr(ordinal).asInstanceOf[T]
      case arr: Array[Short] => arr(ordinal).asInstanceOf[T]
      case arr: Array[Byte] => arr(ordinal).asInstanceOf[T]
      case arr: Array[Boolean] => arr(ordinal).asInstanceOf[T]
      case _ => throw new IllegalStateException(s"Unexpected data type: ${data.getClass}")
    }
  }

  override def isNullAt(ordinal: Int): Boolean = {
    data match {
      case arr: Array[Any] => arr(ordinal).asInstanceOf[AnyRef] eq null
      case _: Array[_] => false  // primitive arrays can't have nulls
    }
  }

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

  override def setNullAt(ordinal: Int): Unit = {
    val anyArr = ensureAnyArray()
    anyArr(ordinal) = null
  }

  override def update(ordinal: Int, value: Any): Unit = {
    data match {
      case arr: Array[Any] =>
        arr(ordinal) = value
      case arr: Array[Int] if value.isInstanceOf[Int] =>
        arr(ordinal) = value.asInstanceOf[Int]
      case arr: Array[Long] if value.isInstanceOf[Long] =>
        arr(ordinal) = value.asInstanceOf[Long]
      case arr: Array[Float] if value.isInstanceOf[Float] =>
        arr(ordinal) = value.asInstanceOf[Float]
      case arr: Array[Double] if value.isInstanceOf[Double] =>
        arr(ordinal) = value.asInstanceOf[Double]
      case arr: Array[Short] if value.isInstanceOf[Short] =>
        arr(ordinal) = value.asInstanceOf[Short]
      case arr: Array[Byte] if value.isInstanceOf[Byte] =>
        arr(ordinal) = value.asInstanceOf[Byte]
      case arr: Array[Boolean] if value.isInstanceOf[Boolean] =>
        arr(ordinal) = value.asInstanceOf[Boolean]
      case _ =>
        // Non-primitive type or null value, convert to Array[Any]
        val anyArr = ensureAnyArray()
        anyArr(ordinal) = value
    }
  }

  override def toString(): String = {
    data match {
      case arr: Array[Any] => arr.mkString("[", ",", "]")
      case arr: Array[Int] => arr.mkString("[", ",", "]")
      case arr: Array[Long] => arr.mkString("[", ",", "]")
      case arr: Array[Float] => arr.mkString("[", ",", "]")
      case arr: Array[Double] => arr.mkString("[", ",", "]")
      case arr: Array[Short] => arr.mkString("[", ",", "]")
      case arr: Array[Byte] => arr.mkString("[", ",", "]")
      case arr: Array[Boolean] => arr.mkString("[", ",", "]")
      case _ => throw new IllegalStateException(s"Unexpected data type: ${data.getClass}")
    }
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    // Try to directly compare primitive arrays of the same type
    (data, other.data) match {
      case (arr1: Array[Int], arr2: Array[Int]) =>
        java.util.Arrays.equals(arr1, arr2)
      case (arr1: Array[Long], arr2: Array[Long]) =>
        java.util.Arrays.equals(arr1, arr2)
      case (arr1: Array[Float], arr2: Array[Float]) =>
        java.util.Arrays.equals(arr1, arr2)
      case (arr1: Array[Double], arr2: Array[Double]) =>
        java.util.Arrays.equals(arr1, arr2)
      case (arr1: Array[Short], arr2: Array[Short]) =>
        java.util.Arrays.equals(arr1, arr2)
      case (arr1: Array[Byte], arr2: Array[Byte]) =>
        java.util.Arrays.equals(arr1, arr2)
      case (arr1: Array[Boolean], arr2: Array[Boolean]) =>
        java.util.Arrays.equals(arr1, arr2)
      case _ =>
        // Fall back to general comparison logic
        var i = 0
        while (i < len) {
          val isNull1 = isNullAt(i)
          val isNull2 = other.isNullAt(i)
          if (isNull1 != isNull2) {
            return false
          }
          if (!isNull1) {
            (data, other.data) match {
              case (arr1: Array[Any], arr2: Array[Any]) =>
                val o1 = arr1(i)
                val o2 = arr2(i)
                compareValues(o1, o2) match {
                  case false => return false
                  case _ =>
                }
              case _ =>
                val o1 = get(i, null)
                val o2 = other.get(i, null)
                compareValues(o1, o2) match {
                  case false => return false
                  case _ =>
                }
            }
          }
          i += 1
        }
        true
    }
  }

  // Compare two values for equality, handling special cases like NaN
  private def compareValues(o1: Any, o2: Any): Boolean = {
    (o1, o2) match {
      case (b1: Array[Byte], b2: Array[Byte]) =>
        java.util.Arrays.equals(b1, b2)
      case (f1: java.lang.Float, f2: java.lang.Float) =>
        (f1 == f2) || (java.lang.Float.isNaN(f1) && java.lang.Float.isNaN(f2))
      case (d1: java.lang.Double, d2: java.lang.Double) =>
        (d1 == d2) || (java.lang.Double.isNaN(d1) && java.lang.Double.isNaN(d2))
      case _ =>
        o1.getClass == o2.getClass && o1 == o2
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
          data match {
            case arr: Array[Any] => arr(i) match {
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
            case arr: Array[Int] => arr(i)
            case arr: Array[Long] => (arr(i) ^ (arr(i) >>> 32)).toInt
            case arr: Array[Float] => java.lang.Float.floatToIntBits(arr(i))
            case arr: Array[Double] =>
              val b = java.lang.Double.doubleToLongBits(arr(i))
              (b ^ (b >>> 32)).toInt
            case arr: Array[Short] => arr(i).toInt
            case arr: Array[Byte] => arr(i).toInt
            case arr: Array[Boolean] => if (arr(i)) 0 else 1
            case _ => throw new IllegalStateException(s"Unexpected data type: ${data.getClass}")
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }
}
