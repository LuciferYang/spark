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

package org.apache.spark.sql.connect.planner

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.{expressions, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object LiteralExpressionProtoConverter {

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   *
   * @return
   *   Expression
   */
  def toCatalystExpression(lit: proto.Expression.Literal): expressions.Literal = {
    lit.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.NULL =>
        expressions.Literal(null, DataTypeProtoConverter.toCatalystType(lit.getNull))

      case proto.Expression.Literal.LiteralTypeCase.BINARY =>
        expressions.Literal(lit.getBinary.toByteArray, BinaryType)

      case proto.Expression.Literal.LiteralTypeCase.BOOLEAN =>
        expressions.Literal(lit.getBoolean, BooleanType)

      case proto.Expression.Literal.LiteralTypeCase.BYTE =>
        expressions.Literal(lit.getByte.toByte, ByteType)

      case proto.Expression.Literal.LiteralTypeCase.SHORT =>
        expressions.Literal(lit.getShort.toShort, ShortType)

      case proto.Expression.Literal.LiteralTypeCase.INTEGER =>
        expressions.Literal(lit.getInteger, IntegerType)

      case proto.Expression.Literal.LiteralTypeCase.LONG =>
        expressions.Literal(lit.getLong, LongType)

      case proto.Expression.Literal.LiteralTypeCase.FLOAT =>
        expressions.Literal(lit.getFloat, FloatType)

      case proto.Expression.Literal.LiteralTypeCase.DOUBLE =>
        expressions.Literal(lit.getDouble, DoubleType)

      case proto.Expression.Literal.LiteralTypeCase.DECIMAL =>
        val decimal = Decimal.apply(lit.getDecimal.getValue)
        var precision = decimal.precision
        if (lit.getDecimal.hasPrecision) {
          precision = math.max(precision, lit.getDecimal.getPrecision)
        }
        var scale = decimal.scale
        if (lit.getDecimal.hasScale) {
          scale = math.max(scale, lit.getDecimal.getScale)
        }
        expressions.Literal(decimal, DecimalType(math.max(precision, scale), scale))

      case proto.Expression.Literal.LiteralTypeCase.STRING =>
        expressions.Literal(UTF8String.fromString(lit.getString), StringType)

      case proto.Expression.Literal.LiteralTypeCase.DATE =>
        expressions.Literal(lit.getDate, DateType)

      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP =>
        expressions.Literal(lit.getTimestamp, TimestampType)

      case proto.Expression.Literal.LiteralTypeCase.TIMESTAMP_NTZ =>
        expressions.Literal(lit.getTimestampNtz, TimestampNTZType)

      case proto.Expression.Literal.LiteralTypeCase.CALENDAR_INTERVAL =>
        val interval = new CalendarInterval(
          lit.getCalendarInterval.getMonths,
          lit.getCalendarInterval.getDays,
          lit.getCalendarInterval.getMicroseconds)
        expressions.Literal(interval, CalendarIntervalType)

      case proto.Expression.Literal.LiteralTypeCase.YEAR_MONTH_INTERVAL =>
        expressions.Literal(lit.getYearMonthInterval, YearMonthIntervalType())

      case proto.Expression.Literal.LiteralTypeCase.DAY_TIME_INTERVAL =>
        expressions.Literal(lit.getDayTimeInterval, DayTimeIntervalType())

      case proto.Expression.Literal.LiteralTypeCase.ARRAY =>
        expressions.Literal.create(
          toArrayData(lit.getArray),
          ArrayType(DataTypeProtoConverter.toCatalystType(lit.getArray.getElementType)))

      case proto.Expression.Literal.LiteralTypeCase.MAP =>
        expressions.Literal.create(
          toMapData(lit.getMap),
          MapType(
            DataTypeProtoConverter.toCatalystType(lit.getMap.getKeyType),
            DataTypeProtoConverter.toCatalystType(lit.getMap.getValueType)))

      case proto.Expression.Literal.LiteralTypeCase.STRUCT =>
        val dataType = DataTypeProtoConverter.toCatalystType(lit.getStruct.getStructType)
        val structData = toStructData(lit.getStruct)
        val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
        expressions.Literal.create(convert(structData), dataType)

      case _ =>
        throw InvalidPlanInput(
          s"Unsupported Literal Type: ${lit.getLiteralTypeCase.getNumber}" +
            s"(${lit.getLiteralTypeCase.name})")
    }
  }

  def toCatalystValue(lit: proto.Expression.Literal): Any = {
    lit.getLiteralTypeCase match {
      case proto.Expression.Literal.LiteralTypeCase.STRING => lit.getString

      case _ => toCatalystExpression(lit).value
    }
  }

  private def getConverter(dataType: proto.DataType): proto.Expression.Literal => Any = {
    if (dataType.hasShort) { v =>
      v.getShort.toShort
    } else if (dataType.hasInteger) { v =>
      v.getInteger
    } else if (dataType.hasLong) { v =>
      v.getLong
    } else if (dataType.hasDouble) { v =>
      v.getDouble
    } else if (dataType.hasByte) { v =>
      v.getByte.toByte
    } else if (dataType.hasFloat) { v =>
      v.getFloat
    } else if (dataType.hasBoolean) { v =>
      v.getBoolean
    } else if (dataType.hasString) { v =>
      v.getString
    } else if (dataType.hasBinary) { v =>
      v.getBinary.toByteArray
    } else if (dataType.hasDate) { v =>
      DateTimeUtils.toJavaDate(v.getDate)
    } else if (dataType.hasTimestamp) { v =>
      DateTimeUtils.toJavaTimestamp(v.getTimestamp)
    } else if (dataType.hasTimestampNtz) { v =>
      DateTimeUtils.microsToLocalDateTime(v.getTimestampNtz)
    } else if (dataType.hasDayTimeInterval) { v =>
      IntervalUtils.microsToDuration(v.getDayTimeInterval)
    } else if (dataType.hasYearMonthInterval) { v =>
      IntervalUtils.monthsToPeriod(v.getYearMonthInterval)
    } else if (dataType.hasDecimal) { v =>
      Decimal(v.getDecimal.getValue)
    } else if (dataType.hasCalendarInterval) { v =>
    {
      val interval = v.getCalendarInterval
      new CalendarInterval(interval.getMonths, interval.getDays, interval.getMicroseconds)
    }
    } else if (dataType.hasArray) { v =>
      toArrayData(v.getArray)
    } else if (dataType.hasMap) { v =>
      toMapData(v.getMap)
    } else if (dataType.hasStruct) { v =>
      toStructData(v.getStruct)
    } else {
      throw InvalidPlanInput(s"Unsupported Literal Type: $dataType)")
    }
  }

  private def toArrayData(array: proto.Expression.Literal.Array): Any = {
    def makeArrayData[T](converter: proto.Expression.Literal => T)(implicit
        tag: ClassTag[T]): Array[T] = {
      val builder = mutable.ArrayBuilder.make[T]
      val elementList = array.getElementsList
      builder.sizeHint(elementList.size())
      val iter = elementList.iterator()
      while (iter.hasNext) {
        builder += converter(iter.next())
      }
      builder.result()
    }

    val elementType = array.getElementType
    makeArrayData(getConverter(elementType))
  }

  private def toMapData(map: proto.Expression.Literal.Map): Any = {
    def makeMapData[K, V](
        keyConverter: proto.Expression.Literal => K,
        valueConverter: proto.Expression.Literal => V)(implicit
        tagK: ClassTag[K],
        tagV: ClassTag[V]): mutable.Map[K, V] = {
      val builder = mutable.HashMap.empty[K, V]
      val keys = map.getKeyList.asScala
      val values = map.getValueList.asScala
      builder.sizeHint(keys.size)
      keys.zip(values).foreach { case (key, value) =>
        builder += ((keyConverter(key), valueConverter(value)))
      }
      builder
    }

    makeMapData(getConverter(map.getKeyType), getConverter(map.getValueType))
  }

  private def toStructData(struct: proto.Expression.Literal.Struct): Any = {
    val elements = struct.getElementList.asScala
    val dataTypes = struct.getStructType.getStruct.getFieldsList.asScala.map(_.getDataType)
    val structData = elements
      .zip(dataTypes)
      .map { case (element, dataType) =>
        getConverter(dataType)(element)
      }
      .toList

    structData match {
      case List(a) => (a)
      case List(a, b) => (a, b)
      case List(a, b, c) => (a, b, c)
      case List(a, b, c, d) => (a, b, c, d)
      case List(a, b, c, d, e) => (a, b, c, d, e)
      case _ => throw InvalidPlanInput(s"Unsupported Literal: $structData)")
    }
  }
}
