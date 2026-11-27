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

package org.apache.spark.sql.catalyst

import java.lang.reflect.{Constructor, Modifier}
import javax.lang.model.SourceVersion

import scala.reflect.ClassTag

import org.apache.commons.lang3.ClassUtils
import org.apache.commons.lang3.reflect.ConstructorUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TypeTags.TypeTag
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, OuterScopes}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, VariantVal}

/**
 * Builds encoders and schemas from [[TypeTag]]s, following the same rules as the Scala 2.13
 * build. The Scala 2.13 build reads the types through Scala reflection; this one reads the
 * [[TypeDesc]] the compiler generates for the tag.
 */
object ScalaReflection extends Logging {

  case class Schema(dataType: DataType, nullable: Boolean)

  /** Returns a catalyst DataType and its nullability for the given Scala type. */
  def schemaFor[T: TypeTag]: Schema = schemaFor(implicitly[TypeTag[T]].desc)

  /** Returns a catalyst DataType and its nullability for the given type description. */
  def schemaFor(desc: TypeDesc): Schema = {
    val enc = encoderFor(desc)
    Schema(enc.dataType, enc.nullable)
  }

  /**
   * Create an [[AgnosticEncoder]] from a [[TypeTag]].
   *
   * If the given type is not supported, i.e. there is no encoder can be built for this type, an
   * [[SparkUnsupportedOperationException]] will be thrown with detailed error message to explain
   * the type path walked so far and which class we are not supporting.
   */
  def encoderFor[E: TypeTag]: AgnosticEncoder[E] =
    encoderFor(implicitly[TypeTag[E]].desc).asInstanceOf[AgnosticEncoder[E]]

  /**
   * Same as [[encoderFor]] but with extended support to return [[UnboundRowEncoder]] for [[Row]]
   * type.
   */
  def encoderForWithRowEncoderSupport[E: TypeTag]: AgnosticEncoder[E] =
    encoderFor(implicitly[TypeTag[E]].desc, isRowEncoderSupported = true)
      .asInstanceOf[AgnosticEncoder[E]]

  /** Same as [[encoderFor]], for a [[TypeTag]] whose type is not known statically. */
  private[sql] def encoderForTypeTag(tag: TypeTag[_]): AgnosticEncoder[_] = encoderFor(tag.desc)

  /** Whether the type of `tag` is a [[Seq]] of [[Row]]. */
  private[sql] def isSeqOfRow(tag: TypeTag[_]): Boolean = {
    val desc = tag.desc
    isSubclass(desc, classOf[Seq[_]]) &&
      desc.typeArgs.headOption.exists(_.runtimeClass == classOf[Row])
  }

  /** Create an [[AgnosticEncoder]] for a type description. */
  def encoderFor(desc: TypeDesc, isRowEncoderSupported: Boolean = false): AgnosticEncoder[_] = {
    val walkedTypePath = WalkedTypePath().recordRoot(desc.className)
    encoderFor(desc, Set.empty, walkedTypePath, isRowEncoderSupported)
  }

  /** The runtime class of a type description; the Scala 2.13 build takes a Scala type. */
  def getClassFromType(desc: TypeDesc): Class[_] = desc.runtimeClass

  /**
   * Returns the parameter names for the primary constructor of this class.
   *
   * Java reflection cannot tell the primary constructor apart, so this takes the constructor
   * with the most parameters among those whose parameters are all fields of the class, and the
   * constructor with the most parameters if there is none.
   */
  def getConstructorParameterNames(cls: Class[_]): Seq[String] = {
    val fieldNames = Iterator.iterate[Class[_]](cls)(_.getSuperclass)
      .takeWhile(_ != null)
      .flatMap(_.getDeclaredFields)
      .filterNot(f => Modifier.isStatic(f.getModifiers))
      .map(_.getName)
      .toSet
    def params(c: Constructor[_]): Seq[String] =
      c.getParameters.toSeq.map(_.getName).filterNot(_ == "$outer")
    val constructors = cls.getDeclaredConstructors.toSeq.filterNot(_.isSynthetic)
    val matching = constructors.filter(c => params(c).forall(fieldNames.contains))
    val candidates = if (matching.nonEmpty) matching else constructors
    if (candidates.isEmpty) Nil else params(candidates.maxBy(_.getParameterCount))
  }

  /**
   * Finds an accessible constructor with compatible parameters. This is a more flexible search
   * than the exact matching algorithm in `Class.getConstructor`. The first assignment-compatible
   * matching constructor is returned if it exists. Otherwise, we check for additional compatible
   * constructors defined in the companion object as `apply` methods. Otherwise, it returns
   * `None`.
   */
  def findConstructor[T](cls: Class[T], paramTypes: Seq[Class[_]]): Option[Seq[AnyRef] => T] = {
    Option(ConstructorUtils.getMatchingAccessibleConstructor(cls, paramTypes: _*)) match {
      case Some(c) => Some(x => c.newInstance(x: _*))
      case None =>
        // The companion is looked up in the loader of `cls`, which is not always the one that
        // `Utils.classForName` uses.
        // scalastyle:off classforname
        val companion = scala.util.Try {
          val companionCls = Class.forName(cls.getName + "$", true, cls.getClassLoader)
          companionCls.getField("MODULE$").get(null)
        }.toOption
        // scalastyle:on classforname
        companion.flatMap { module =>
          val applyMethods = module.getClass.getMethods.toSeq.filter(_.getName == "apply")
          applyMethods.find { m =>
            m.getParameterCount == paramTypes.size &&
              m.getParameterTypes.zip(paramTypes).forall { case (expected, actual) =>
                ClassUtils.isAssignable(actual, expected, true)
              }
          }.map { method =>
            val expectedArgsCount = method.getParameterCount
            (_args: Seq[AnyRef]) => {
              // Drop the "outer" argument if it is provided
              val args = if (_args.size == expectedArgsCount) _args else _args.tail
              method.invoke(module, args: _*).asInstanceOf[T]
            }
          }
        }
    }
  }

  def encodeFieldNameToIdentifier(fieldName: String): String = {
    scala.reflect.NameTransformer.encode(fieldName)
  }

  private def isSubclass(desc: TypeDesc, cls: Class[_]): Boolean =
    cls.isAssignableFrom(desc.runtimeClass)

  private def definedByConstructorParams(desc: TypeDesc): Boolean = {
    isSubclass(desc, classOf[Product]) || isSubclass(desc, classOf[DefinedByConstructorParams])
  }

  private def encoderFor(
      desc: TypeDesc,
      seenTypeSet: Set[TypeDesc],
      path: WalkedTypePath,
      isRowEncoderSupported: Boolean): AgnosticEncoder[_] = {
    def elementEncoder(elementType: TypeDesc, elementPath: WalkedTypePath): AgnosticEncoder[_] =
      encoderFor(elementType, seenTypeSet, elementPath, isRowEncoderSupported)

    def createIterableEncoder(fallbackClass: Class[_]): AgnosticEncoder[_] = {
      val elementType = desc.typeArgs.head
      val encoder = elementEncoder(elementType, path.recordArray(elementType.className))
      val targetClass = if (desc.hasNewBuilder) desc.runtimeClass else fallbackClass
      IterableEncoder(
        ClassTag(targetClass),
        encoder,
        encoder.nullable,
        lenientSerialization = false)
    }

    val cls = desc.runtimeClass
    // Null comes first, as with the Scala 2.13 build, where it is a subtype of every class.
    if (desc.isNull || desc.isNothing) {
      NullEncoder
    } else {
      leafEncoder(cls).getOrElse {
        if (classOf[java.lang.Enum[_]].isAssignableFrom(cls)) {
          JavaEnumEncoder(ClassTag(cls))
        } else if (classOf[Enumeration#Value].isAssignableFrom(cls) && desc.enumParent != null) {
          ScalaEnumEncoder(desc.enumParent, ClassTag(cls))
        } else if (cls.getAnnotation(classOf[SQLUserDefinedType]) != null) {
          val udt = cls
            .getAnnotation(classOf[SQLUserDefinedType])
            .udt()
            .getConstructor()
            .newInstance()
            .asInstanceOf[UserDefinedType[Any]]
          val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
          UDTEncoder(udt, udtClass)
        } else if (UDTRegistration.exists(desc.className)) {
          val udt = UDTRegistration
            .getUDTFor(desc.className)
            .get
            .getConstructor()
            .newInstance()
            .asInstanceOf[UserDefinedType[Any]]
          UDTEncoder(udt, udt.getClass)
        } else if (classOf[Option[_]].isAssignableFrom(cls)) {
          val optType = desc.typeArgs.head
          OptionEncoder(elementEncoder(optType, path.recordOption(optType.className)))
        } else if (cls.isArray) {
          val elementType = desc.typeArgs.head
          val encoder = elementEncoder(elementType, path.recordArray(elementType.className))
          ArrayEncoder(encoder, encoder.nullable)
        } else if (classOf[scala.collection.Seq[_]].isAssignableFrom(cls)) {
          createIterableEncoder(classOf[scala.collection.Seq[_]])
        } else if (classOf[scala.collection.Set[_]].isAssignableFrom(cls)) {
          createIterableEncoder(classOf[scala.collection.Set[_]])
        } else if (classOf[scala.collection.Map[_, _]].isAssignableFrom(cls)) {
          val Seq(keyType, valueType) = desc.typeArgs
          val keyEncoder = elementEncoder(keyType, path.recordKeyForMap(keyType.className))
          val valueEncoder =
            elementEncoder(valueType, path.recordValueForMap(valueType.className))
          MapEncoder(ClassTag(cls), keyEncoder, valueEncoder, valueEncoder.nullable)
        } else if (definedByConstructorParams(desc)) {
          productEncoder(desc, seenTypeSet, path, isRowEncoderSupported)
        } else {
          throw ExecutionErrors.cannotFindEncoderForTypeError(desc.typeName)
        }
      }
    }
  }

  private def productEncoder(
      desc: TypeDesc,
      seenTypeSet: Set[TypeDesc],
      path: WalkedTypePath,
      isRowEncoderSupported: Boolean): AgnosticEncoder[_] = {
    if (seenTypeSet.contains(desc)) {
      throw ExecutionErrors.cannotHaveCircularReferencesInClassError(desc.typeName)
    }
    desc.constructorError match {
      case TypeSpec.NoConstructor =>
        throw ExecutionErrors.cannotFindConstructorForTypeError(desc.typeName)
      case TypeSpec.NoPrimaryConstructor =>
        throw ExecutionErrors.primaryConstructorNotFoundError(desc.runtimeClass)
      case _ =>
    }
    val params = desc.fields.map { case (fieldName, fieldType) =>
      if (SourceVersion.isKeyword(fieldName) ||
        !SourceVersion.isIdentifier(encodeFieldNameToIdentifier(fieldName))) {
        throw ExecutionErrors.cannotUseInvalidJavaIdentifierAsFieldNameError(fieldName, path)
      }
      val encoder = encoderFor(
        fieldType,
        seenTypeSet + desc,
        path.recordField(fieldType.className, fieldName),
        isRowEncoderSupported)
      EncoderField(fieldName, encoder, encoder.nullable, Metadata.empty)
    }
    val cls = desc.runtimeClass
    ProductEncoder(ClassTag(cls), params, Option(OuterScopes.getOuterScope(cls)))
  }

  // The encoders that the Scala 2.13 build picks by subtype before the composite types.
  private def leafEncoder(cls: Class[_]): Option[AgnosticEncoder[_]] = {
    def is(c: Class[_]): Boolean = c.isAssignableFrom(cls)
    val encoder: AgnosticEncoder[_] =
      if (cls == java.lang.Boolean.TYPE) PrimitiveBooleanEncoder
      else if (cls == java.lang.Byte.TYPE) PrimitiveByteEncoder
      else if (cls == java.lang.Short.TYPE) PrimitiveShortEncoder
      else if (cls == java.lang.Integer.TYPE) PrimitiveIntEncoder
      else if (cls == java.lang.Long.TYPE) PrimitiveLongEncoder
      else if (cls == java.lang.Float.TYPE) PrimitiveFloatEncoder
      else if (cls == java.lang.Double.TYPE) PrimitiveDoubleEncoder
      else if (is(classOf[java.lang.Boolean])) BoxedBooleanEncoder
      else if (is(classOf[java.lang.Byte])) BoxedByteEncoder
      else if (is(classOf[java.lang.Short])) BoxedShortEncoder
      else if (is(classOf[java.lang.Integer])) BoxedIntEncoder
      else if (is(classOf[java.lang.Long])) BoxedLongEncoder
      else if (is(classOf[java.lang.Float])) BoxedFloatEncoder
      else if (is(classOf[java.lang.Double])) BoxedDoubleEncoder
      else if (cls == classOf[Array[Byte]]) BinaryEncoder
      else if (is(classOf[String])) StringEncoder
      else if (is(classOf[Decimal])) DEFAULT_SPARK_DECIMAL_ENCODER
      else if (is(classOf[BigDecimal])) DEFAULT_SCALA_DECIMAL_ENCODER
      else if (is(classOf[java.math.BigDecimal])) DEFAULT_JAVA_DECIMAL_ENCODER
      else if (is(classOf[BigInt])) ScalaBigIntEncoder
      else if (is(classOf[java.math.BigInteger])) JavaBigIntEncoder
      else if (is(classOf[CalendarInterval])) CalendarIntervalEncoder
      else if (is(classOf[java.time.Duration])) DayTimeIntervalEncoder
      else if (is(classOf[java.time.Period])) YearMonthIntervalEncoder
      else if (is(classOf[java.sql.Date])) STRICT_DATE_ENCODER
      else if (is(classOf[java.time.LocalDate])) STRICT_LOCAL_DATE_ENCODER
      else if (is(classOf[java.sql.Timestamp])) STRICT_TIMESTAMP_ENCODER
      else if (is(classOf[java.time.Instant])) STRICT_INSTANT_ENCODER
      else if (is(classOf[java.time.LocalDateTime])) LocalDateTimeEncoder
      else if (is(classOf[java.time.LocalTime])) DEFAULT_LOCAL_TIME_ENCODER
      else if (is(classOf[VariantVal])) VariantEncoder
      else if (is(classOf[Geography])) DEFAULT_GEOGRAPHY_ENCODER
      else if (is(classOf[Geometry])) DEFAULT_GEOMETRY_ENCODER
      else if (is(classOf[Row])) UnboundRowEncoder
      else null
    Option(encoder)
  }
}
