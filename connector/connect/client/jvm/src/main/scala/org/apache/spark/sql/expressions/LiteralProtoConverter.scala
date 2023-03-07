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
package org.apache.spark.sql.expressions

import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Double => JDouble, Float => JFloat, Integer => JInteger, Iterable => JIterable, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInteger}
import java.sql.{Date, Timestamp}
import java.time._

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.connect.client.unsupported
import org.apache.spark.sql.connect.common.DataTypeProtoConverter._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object LiteralProtoConverter {

  private lazy val nullType =
    proto.DataType.newBuilder().setNull(proto.DataType.NULL.getDefaultInstance).build()

  /**
   * Transforms literal value to the `proto.Expression.Literal.Builder`.
   *
   * @return
   *   proto.Expression.Literal.Builder
   */
  @scala.annotation.tailrec
  def toLiteralProtoBuilder(literal: Any): proto.Expression.Literal.Builder = {
    val builder = proto.Expression.Literal.newBuilder()

    def decimalBuilder(precision: Int, scale: Int, value: String) = {
      builder.getDecimalBuilder.setPrecision(precision).setScale(scale).setValue(value)
    }

    def calendarIntervalBuilder(months: Int, days: Int, microseconds: Long) = {
      builder.getCalendarIntervalBuilder
        .setMonths(months)
        .setDays(days)
        .setMicroseconds(microseconds)
    }

    def arrayBuilder(array: Array[_]) = {
      val ab = builder.getArrayBuilder
        .setElementType(toConnectProtoType(toDataType(array.getClass.getComponentType)))
      array.foreach(x => ab.addElement(toLiteralProto(x)))
      ab
    }

    literal match {
      case v: Boolean => builder.setBoolean(v)
      case v: Byte => builder.setByte(v)
      case v: Short => builder.setShort(v)
      case v: Int => builder.setInteger(v)
      case v: Long => builder.setLong(v)
      case v: Float => builder.setFloat(v)
      case v: Double => builder.setDouble(v)
      case v: BigDecimal =>
        builder.setDecimal(decimalBuilder(v.precision, v.scale, v.toString))
      case v: JBigDecimal =>
        builder.setDecimal(decimalBuilder(v.precision, v.scale, v.toString))
      case v: String => builder.setString(v)
      case v: Char => builder.setString(v.toString)
      case v: Array[Char] => builder.setString(String.valueOf(v))
      case v: Array[Byte] => builder.setBinary(ByteString.copyFrom(v))
      case v: collection.mutable.WrappedArray[_] => toLiteralProtoBuilder(v.array)
      case v: LocalDate => builder.setDate(v.toEpochDay.toInt)
      case v: Decimal =>
        builder.setDecimal(decimalBuilder(Math.max(v.precision, v.scale), v.scale, v.toString))
      case v: Instant => builder.setTimestamp(DateTimeUtils.instantToMicros(v))
      case v: Timestamp => builder.setTimestamp(DateTimeUtils.fromJavaTimestamp(v))
      case v: LocalDateTime => builder.setTimestampNtz(DateTimeUtils.localDateTimeToMicros(v))
      case v: Date => builder.setDate(DateTimeUtils.fromJavaDate(v))
      case v: Duration => builder.setDayTimeInterval(IntervalUtils.durationToMicros(v))
      case v: Period => builder.setYearMonthInterval(IntervalUtils.periodToMonths(v))
      case v: Array[_] => builder.setArray(arrayBuilder(v))
      case v: CalendarInterval =>
        builder.setCalendarInterval(calendarIntervalBuilder(v.months, v.days, v.microseconds))
      case null => builder.setNull(nullType)
      case _ => unsupported(s"literal $literal not supported (yet).")
    }
  }

  def createLiteralProtoBuilder[T: TypeTag](literal: T): proto.Expression.Literal.Builder = Try {
    val builder = proto.Expression.Literal.newBuilder()

    def arrayBuilder(at: ArrayType, array: Array[Any]) = {
      val ab = builder.getArrayBuilder
        .setElementType(toConnectProtoType(at.elementType))
      array.foreach(x => ab.addElement(createLiteralProtoBuilder(x)))
      ab
    }

    val ScalaReflection.Schema(dataType, _) = ScalaReflection.schemaFor[T]
    (dataType, getConverterForType(dataType).convert(literal)) match {
      case (StringType, v: String) => builder.setString(v)
      case (DateType, v: Int) => builder.setDate(v)
      case (TimestampType, v: Long) => builder.setTimestamp(v)
      case (TimestampNTZType, v: Long) => builder.setTimestampNtz(v)
      case (_: DecimalType, v: Decimal) =>
        val db = builder.getDecimalBuilder
          .setPrecision(v.precision)
          .setScale(v.scale)
          .setValue(v.toString)
        builder.setDecimal(db)
      case (BooleanType, v: Boolean) => builder.setBoolean(v)
      case (ByteType, v: Byte) => builder.setByte(v)
      case (ShortType, v: Short) => builder.setShort(v)
      case (IntegerType, v: Int) => builder.setInteger(v)
      case (LongType, v: Long) => builder.setLong(v)
      case (FloatType, v: Float) => builder.setFloat(v)
      case (DoubleType, v: Double) => builder.setDouble(v)
      case (DayTimeIntervalType(_, _), v: Long) => builder.setDayTimeInterval(v)
      case (YearMonthIntervalType(_, _), v: Int) => builder.setYearMonthInterval(v)
      case (at: ArrayType, array: Array[Any]) =>
        builder.setArray(arrayBuilder(at, array))
//      case udt: UserDefinedType[_] => UDTConverter(udt)
//      case mapType: MapType => MapConverter(mapType.keyType, mapType.valueType)
//      case structType: StructType => StructConverter(structType)
    }
  }.getOrElse {
    toLiteralProtoBuilder(literal)
  }

  private def getConverterForType(dataType: DataType): ValueConverter[Any, Any] = {
    val converter = dataType match {
      case arrayType: ArrayType => ArrayConverter(arrayType.elementType)
      case StringType => StringConverter
      case DateType => DateConverter
      case TimestampType => TimestampConverter
      case TimestampNTZType => TimestampNTZConverter
      case dt: DecimalType => new DecimalConverter(dt)
      case BooleanType => BooleanValueConverter
      case ByteType => ByteValueConverter
      case ShortType => ShortValueConverter
      case IntegerType => IntValueConverter
      case LongType => LongValueConverter
      case FloatType => FloatValueConverter
      case DoubleType => DoubleValueConverter
      case DayTimeIntervalType(_, endField) => DurationConverter(endField)
      case YearMonthIntervalType(_, endField) => PeriodConverter(endField)
      case _ => throw new UnsupportedOperationException()
    }
    converter.asInstanceOf[ValueConverter[Any, Any]]
  }

  /**
   * Transforms literal value to the `proto.Expression.Literal`.
   *
   * @return
   *   proto.Expression.Literal
   */
  private def toLiteralProto(literal: Any): proto.Expression.Literal =
    toLiteralProtoBuilder(literal).build()

  private def toDataType(clz: Class[_]): DataType = clz match {
    // primitive types
    case JShort.TYPE => ShortType
    case JInteger.TYPE => IntegerType
    case JLong.TYPE => LongType
    case JDouble.TYPE => DoubleType
    case JByte.TYPE => ByteType
    case JFloat.TYPE => FloatType
    case JBoolean.TYPE => BooleanType
    case JChar.TYPE => StringType

    // java classes
    case _ if clz == classOf[LocalDate] || clz == classOf[Date] => DateType
    case _ if clz == classOf[Instant] || clz == classOf[Timestamp] => TimestampType
    case _ if clz == classOf[LocalDateTime] => TimestampNTZType
    case _ if clz == classOf[Duration] => DayTimeIntervalType.DEFAULT
    case _ if clz == classOf[Period] => YearMonthIntervalType.DEFAULT
    case _ if clz == classOf[JBigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[Array[Byte]] => BinaryType
    case _ if clz == classOf[Array[Char]] => StringType
    case _ if clz == classOf[JShort] => ShortType
    case _ if clz == classOf[JInteger] => IntegerType
    case _ if clz == classOf[JLong] => LongType
    case _ if clz == classOf[JDouble] => DoubleType
    case _ if clz == classOf[JByte] => ByteType
    case _ if clz == classOf[JFloat] => FloatType
    case _ if clz == classOf[JBoolean] => BooleanType

    // other scala classes
    case _ if clz == classOf[String] => StringType
    case _ if clz == classOf[BigInt] || clz == classOf[BigDecimal] => DecimalType.SYSTEM_DEFAULT
    case _ if clz == classOf[CalendarInterval] => CalendarIntervalType
    case _ if clz.isArray => ArrayType(toDataType(clz.getComponentType))
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported component type $clz in arrays.")
  }

  private abstract class ValueConverter[IN, OUT] extends Serializable {
    final def convert(input: Any): OUT = {
      input match {
        case null | None => null.asInstanceOf[OUT]
        case opt: Some[IN] => convertInternal(opt.get)
        case other => convertInternal(other.asInstanceOf[IN])
      }
    }
    protected def convertInternal(input: IN): OUT
  }

  private class DecimalConverter(dataType: DecimalType) extends ValueConverter[Any, Decimal] {

    private val nullOnOverflow = !SQLConf.get.ansiEnabled

    override def convertInternal(input: Any): Decimal = {
      val decimal = input match {
        case d: BigDecimal => Decimal(d)
        case d: JBigDecimal => Decimal(d)
        case d: JBigInteger => Decimal(d)
        case d: Decimal => d
        case other =>
          throw new IllegalArgumentException(
            s"The value (${other.toString}) of the type (${other.getClass.getCanonicalName}) "
              + s"cannot be converted to ${dataType.catalogString}")
      }
      decimal.toPrecision(
        dataType.precision,
        dataType.scale,
        Decimal.ROUND_HALF_UP,
        nullOnOverflow)
    }
  }

  private object BooleanValueConverter extends ValueConverter[Boolean, Boolean] {
    final override def convertInternal(input: Boolean): Boolean = input
  }

  private object ByteValueConverter extends ValueConverter[Byte, Byte] {
    override protected def convertInternal(input: Byte): Byte = input
  }

  private object ShortValueConverter extends ValueConverter[Short, Short] {
    override protected def convertInternal(input: Short): Short = input
  }

  private object IntValueConverter extends ValueConverter[Int, Int] {
    override protected def convertInternal(input: Int): Int = input
  }

  private object LongValueConverter extends ValueConverter[Long, Long] {
    override protected def convertInternal(input: Long): Long = input
  }

  private object FloatValueConverter extends ValueConverter[Float, Float] {
    override protected def convertInternal(input: Float): Float = input
  }

  private object DoubleValueConverter extends ValueConverter[Double, Double] {
    override protected def convertInternal(input: Double): Double = input
  }

  private object StringConverter extends ValueConverter[Any, String] {
    override def convertInternal(input: Any): String = input match {
      case str: String => str
      case utf8: UTF8String => utf8.toString
      case chr: Char => chr.toString
      case ac: Array[Char] => String.valueOf(ac)
      case other =>
        throw new IllegalArgumentException(
          s"The value (${other.toString}) of the type (${other.getClass.getCanonicalName}) "
            + s"cannot be converted to the string type")
    }
  }

  private object DateConverter extends ValueConverter[Any, Int] {
    override def convertInternal(input: Any): Int = input match {
      case d: Date => DateTimeUtils.fromJavaDate(d)
      case l: LocalDate => DateTimeUtils.localDateToDays(l)
      case other =>
        throw new IllegalArgumentException(
          s"The value (${other.toString}) of the type (${other.getClass.getCanonicalName}) "
            + s"cannot be converted to the ${DateType.sql} type")
    }
  }

  private object TimestampConverter extends ValueConverter[Any, Long] {
    override def convertInternal(input: Any): Long = input match {
      case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t)
      case i: Instant => DateTimeUtils.instantToMicros(i)
      case other =>
        throw new IllegalArgumentException(
          s"The value (${other.toString}) of the type (${other.getClass.getCanonicalName}) "
            + s"cannot be converted to the ${TimestampType.sql} type")
    }
  }

  private object TimestampNTZConverter extends ValueConverter[Any, Long] {
    override def convertInternal(input: Any): Long = input match {
      case l: LocalDateTime => DateTimeUtils.localDateTimeToMicros(l)
      case other =>
        throw new IllegalArgumentException(
          s"The value (${other.toString}) of the type (${other.getClass.getCanonicalName}) "
            + s"cannot be converted to the ${TimestampNTZType.sql} type")
    }
  }

  private case class DurationConverter(endField: Byte) extends ValueConverter[Duration, Long] {
    override def convertInternal(input: Duration): Long = {
      IntervalUtils.durationToMicros(input, endField)
    }
  }

  private case class PeriodConverter(endField: Byte) extends ValueConverter[Period, Int] {
    override def convertInternal(input: Period): Int = {
      IntervalUtils.periodToMonths(input, endField)
    }
  }

  private case class ArrayConverter(elementType: DataType)
      extends ValueConverter[Any, Array[Any]] {

    private[this] val elementConverter = getConverterForType(elementType)

    override def convertInternal(input: Any): Array[Any] = {
      input match {
        case a: Array[_] => a.map(elementConverter.convert)
        case s: scala.collection.Seq[_] => s.map(elementConverter.convert).toArray
        case i: JIterable[_] =>
          val iter = i.iterator
          val convertedIterable = scala.collection.mutable.ArrayBuffer.empty[Any]
          while (iter.hasNext) {
            val item = iter.next()
            convertedIterable += elementConverter.convert(item)
          }
          convertedIterable.toArray
        case other =>
          throw new IllegalArgumentException(
            s"The value (${other.toString}) of the type (${other.getClass.getCanonicalName}) "
              + s"cannot be converted to an array of ${elementType.catalogString}")
      }
    }
  }
}
