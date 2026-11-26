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

package org.apache.spark.serializer

import java.math.{BigDecimal => JBigDecimal}

import scala.collection.immutable.{HashMap, HashSet, ListMap, ListSet, Queue, TreeMap, TreeSet,
  WrappedString}
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.matching.Regex

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.{ClassTagSerializer, JavaWrapperCollectionRegistrar, ManifestSerializer,
  RegexSerializer, RichKryo, ScalaCollectionsRegistrar, ScalaTupleSerialization,
  SingletonSerializer, StreamSerializer, VolatileByteRefSerializer}
import com.twitter.chill.java.{Java8ClosureRegistrar, PackageRegistrar}

/**
 * Registers Kryo serializers for classes of the Scala standard library.
 *
 * chill is published for Scala 2.13 only. Its `AllScalaRegistrar` creates an
 * `EnumerationSerializer`, which looks up the method `scala$Enumeration$$outerEnum`; the
 * standard library of Scala 3.8 and later names it `outerEnum`, so the lookup throws. This
 * object makes the registrations of chill 0.10.0's `AllScalaRegistrar` in the same order, with
 * [[ScalaKryoRegistrar.EnumerationSerializer]] in place of chill's. `BigDecimalSerializer` is
 * also redefined here because chill's is private.
 */
private[serializer] object ScalaKryoRegistrar {

  def register(kryo: Kryo): Unit = {
    val k = new RichKryo(kryo)

    // AllScalaRegistrar_0_9_2
    new ScalaCollectionsRegistrar().apply(kryo)
    new JavaWrapperCollectionRegistrar().apply(kryo)
    ScalaTupleSerialization.register.apply(kryo)
    k.forClass[Symbol](new SymbolSerializer)
    k.forSubclass[Regex](new RegexSerializer)
    k.forClass[ClassTag[Any]](new ClassTagSerializer[Any])
    k.forSubclass[Manifest[Any]](new ManifestSerializer[Any])
    k.forSubclass[Enumeration#Value](new EnumerationSerializer)
    val boxedUnit = scala.runtime.BoxedUnit.UNIT
    kryo.register(boxedUnit.getClass, new SingletonSerializer(boxedUnit))
    PackageRegistrar.all().apply(kryo)
    new Java8ClosureRegistrar().apply(kryo)

    // AllScalaRegistrar_0_9_5
    kryo.register(classOf[Range.Exclusive])
    k.registerClasses(Seq(
      classOf[Array[Byte]],
      classOf[Array[Short]],
      classOf[Array[Int]],
      classOf[Array[Long]],
      classOf[Array[Float]],
      classOf[Array[Double]],
      classOf[Array[Boolean]],
      classOf[Array[Char]],
      classOf[Array[String]],
      classOf[Array[Any]],
      classOf[Class[_]],
      classOf[Any],
      mutable.ArraySeq.make(Array[Byte]()).getClass,
      mutable.ArraySeq.make(Array[Short]()).getClass,
      mutable.ArraySeq.make(Array[Int]()).getClass,
      mutable.ArraySeq.make(Array[Long]()).getClass,
      mutable.ArraySeq.make(Array[Float]()).getClass,
      mutable.ArraySeq.make(Array[Double]()).getClass,
      mutable.ArraySeq.make(Array[Boolean]()).getClass,
      mutable.ArraySeq.make(Array[Char]()).getClass,
      mutable.ArraySeq.make(Array[String]()).getClass,
      None.getClass,
      classOf[Queue[_]],
      Nil.getClass,
      classOf[::[_]],
      classOf[Range],
      classOf[WrappedString],
      classOf[TreeSet[_]],
      classOf[TreeMap[_, _]],
      Ordering.Byte.getClass,
      Ordering.Short.getClass,
      Ordering.Int.getClass,
      Ordering.Long.getClass,
      Ordering.Float.getClass,
      Ordering.Double.getClass,
      Ordering.Boolean.getClass,
      Ordering.Char.getClass,
      Ordering.String.getClass))
    val a = Symbol("a")
    k.forConcreteTraversableClass(Set[Any]())
    k.forConcreteTraversableClass(ListSet[Any]())
    k.forConcreteTraversableClass(ListSet[Any](a))
    k.forConcreteTraversableClass(HashSet[Any]())
    k.forConcreteTraversableClass(HashSet[Any](a))
    k.forConcreteTraversableClass(Map[Any, Any]())
    k.forConcreteTraversableClass(HashMap[Any, Any]())
    k.forConcreteTraversableClass(HashMap(a -> a))
    k.forConcreteTraversableClass(ListMap[Any, Any]())
    k.forConcreteTraversableClass(ListMap(a -> a))
    kryo.register(classOf[Stream.Cons[_]], new StreamSerializer[Any])
    kryo.register(Stream.empty[Any].getClass)
    k.forClass[scala.runtime.VolatileByteRef](new VolatileByteRefSerializer)
    k.forClass[BigDecimal](new BigDecimalSerializer)
    kryo.register(Queue.empty[Any].getClass)
    k.forConcreteTraversableClass(Map(1 -> 2).view.filterKeys(_ != 2).toMap)
    k.forConcreteTraversableClass(Map(1 -> 2).view.mapValues(_ + 1).toMap)
    k.forConcreteTraversableClass(Map(1 -> 2).keySet)

    // AllScalaRegistrarCompat
    k.forConcreteTraversableClass(Vector[Any]())
    k.forConcreteTraversableClass(Vector(a))
  }

  private class SymbolSerializer extends Serializer[Symbol] {
    setImmutable(true)
    override def write(kryo: Kryo, out: Output, obj: Symbol): Unit = out.writeString(obj.name)
    override def read(kryo: Kryo, in: Input, cls: Class[Symbol]): Symbol = Symbol(in.readString)
  }

  /** chill's `BigDecimalSerializer`, which is private to chill. */
  private class BigDecimalSerializer extends Serializer[BigDecimal] {
    override def write(kryo: Kryo, out: Output, obj: BigDecimal): Unit =
      kryo.writeClassAndObject(out, obj.bigDecimal)
    override def read(kryo: Kryo, in: Input, cls: Class[BigDecimal]): BigDecimal =
      BigDecimal(kryo.readClassAndObject(in).asInstanceOf[JBigDecimal])
  }

  /**
   * chill's `EnumerationSerializer`, reading the enumeration that a value belongs to through
   * `Enumeration.Value#outerEnum` as the Scala 3 standard library names it.
   */
  private[serializer] class EnumerationSerializer extends Serializer[Enumeration#Value] {
    private val outerMethod = classOf[Enumeration#Value].getMethod("outerEnum")
    private val enumMap = mutable.Map[Enumeration#Value, Enumeration]()

    private def enumOf(v: Enumeration#Value): Enumeration = enumMap.synchronized {
      enumMap.getOrElseUpdate(v, outerMethod.invoke(v).asInstanceOf[Enumeration])
    }

    override def write(kryo: Kryo, out: Output, obj: Enumeration#Value): Unit = {
      // The enumeration is a Scala object, so this writes its class only.
      kryo.writeClassAndObject(out, enumOf(obj))
      out.writeInt(obj.id)
    }

    override def read(kryo: Kryo, in: Input, cls: Class[Enumeration#Value]): Enumeration#Value = {
      val enumeration = kryo.readClassAndObject(in).asInstanceOf[Enumeration]
      enumeration(in.readInt)
    }
  }
}
