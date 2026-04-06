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

package org.apache.spark.serializer.kryo

import java.lang.reflect.Field
import java.math.{BigDecimal => JBigDecimal}
import java.net.InetSocketAddress
import java.util.Collections

import scala.collection.Factory
import scala.collection.immutable.{BitSet, SortedMap, SortedSet}
import scala.collection.mutable.{ArrayBuilder, ArraySeq, Map => MMap}
import scala.reflect.ClassTag
import scala.runtime.VolatileByteRef
import scala.util.matching.Regex

import com.esotericsoftware.kryo.kryo5.{Kryo, Serializer}
import com.esotericsoftware.kryo.kryo5.io.{Input, Output}
import com.esotericsoftware.kryo.kryo5.serializers.{JavaSerializer => KryoJavaSerializer}

// --- Scala collection serializers (from chill-scala module) ---

/**
 * Generic serializer for Scala Iterable types using Factory.
 * Originally from com.twitter.chill.TraversableSerializer
 * (chill-scala module, 2.13+ version).
 */
private[spark] class TraversableSerializer[T, C <: Iterable[T]](
    override val isImmutable: Boolean = true)(implicit cbf: Factory[T, C])
  extends Serializer[C] {

  override def write(kser: Kryo, out: Output, obj: C): Unit = {
    out.writeInt(obj.size, true)
    obj.foreach { t =>
      kser.writeClassAndObject(out, t.asInstanceOf[AnyRef])
      out.flush()
    }
  }

  override def read(kser: Kryo, in: Input, cls: Class[_ <: C]): C = {
    val size = in.readInt(true)
    var idx = 0
    val builder = cbf.newBuilder
    builder.sizeHint(size)
    while (idx < size) {
      builder += kser.readClassAndObject(in).asInstanceOf[T]
      idx += 1
    }
    builder.result()
  }
}

/**
 * Serializer for Scala ArraySeq (WrappedArray in 2.12).
 * Originally from com.twitter.chill.WrappedArraySerializer
 * (chill-scala module, 2.13+ version).
 */
private[spark] class WrappedArraySerializer[T] extends Serializer[ArraySeq[T]] {
  override def write(kser: Kryo, out: Output, obj: ArraySeq[T]): Unit = {
    kser.writeObject(out, obj.elemTag.runtimeClass)
    kser.writeClassAndObject(out, obj.array)
  }

  override def read(kser: Kryo, in: Input, cls: Class[_ <: ArraySeq[T]]): ArraySeq[T] = {
    val clazz = kser.readObject(in, classOf[Class[T]])
    val array = kser.readClassAndObject(in).asInstanceOf[Array[T]]
    val bldr = ArrayBuilder.make[T](ClassTag[T](clazz))
    bldr.sizeHint(array.length)
    bldr ++= array
    bldr.result()
  }
}

/** Serializer for immutable.BitSet. */
private[spark] class ScalaBitSetSerializer extends Serializer[BitSet] {
  override def write(k: Kryo, o: Output, v: BitSet): Unit = {
    val size = v.size
    o.writeInt(size, true)
    if (size > 0) {
      o.writeInt(v.max, true)
    }
    var previous: Int = -1
    v.foreach { vi =>
      if (previous >= 0) {
        o.writeInt(vi - previous, true)
      } else {
        o.writeInt(vi, true)
      }
      previous = vi
    }
  }
  override def read(k: Kryo, i: Input, c: Class[_ <: BitSet]): BitSet = {
    val size = i.readInt(true)
    if (size == 0) {
      BitSet.empty
    } else {
      var sum = 0
      val bits = new Array[Long](i.readInt(true) / 64 + 1)
      (0 until size).foreach { _ =>
        sum += i.readInt(true)
        bits(sum / 64) |= 1L << (sum % 64)
      }
      BitSet.fromBitMask(bits)
    }
  }
}

/** Serializer for immutable.SortedSet. */
private[spark] class SortedSetSerializer[T] extends Serializer[SortedSet[T]] {
  override def write(kser: Kryo, out: Output, set: SortedSet[T]): Unit = {
    out.writeInt(set.size, true)
    kser.writeClassAndObject(out, set.ordering.asInstanceOf[AnyRef])
    set.foreach { t =>
      kser.writeClassAndObject(out, t.asInstanceOf[AnyRef])
      out.flush()
    }
  }

  override def read(kser: Kryo, in: Input, cls: Class[_ <: SortedSet[T]]): SortedSet[T] = {
    val size = in.readInt(true)
    val ordering = kser.readClassAndObject(in).asInstanceOf[Ordering[T]]
    var idx = 0
    val builder = SortedSet.newBuilder[T](ordering)
    builder.sizeHint(size)
    while (idx < size) {
      builder += kser.readClassAndObject(in).asInstanceOf[T]
      idx += 1
    }
    builder.result()
  }
}

/** Serializer for immutable.SortedMap. */
private[spark] class SortedMapSerializer[A, B] extends Serializer[SortedMap[A, B]] {
  override def write(kser: Kryo, out: Output, map: SortedMap[A, B]): Unit = {
    out.writeInt(map.size, true)
    kser.writeClassAndObject(out, map.ordering.asInstanceOf[AnyRef])
    map.foreach { t =>
      kser.writeClassAndObject(out, t.asInstanceOf[AnyRef])
      out.flush()
    }
  }

  override def read(kser: Kryo, in: Input, cls: Class[_ <: SortedMap[A, B]]): SortedMap[A, B] = {
    val size = in.readInt(true)
    val ordering = kser.readClassAndObject(in).asInstanceOf[Ordering[A]]
    var idx = 0
    val builder = SortedMap.newBuilder[A, B](ordering)
    builder.sizeHint(size)
    while (idx < size) {
      builder += kser.readClassAndObject(in).asInstanceOf[(A, B)]
      idx += 1
    }
    builder.result()
  }
}

/** Serializer for Some[T]. */
private[spark] class SomeSerializer[T] extends Serializer[Some[T]] {
  override def write(kser: Kryo, out: Output, item: Some[T]): Unit =
    kser.writeClassAndObject(out, item.get)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Some[T]]): Some[T] =
    Some(kser.readClassAndObject(in).asInstanceOf[T])
}

/** Serializer for Left[A, B]. */
private[spark] class LeftSerializer[A, B] extends Serializer[Left[A, B]] {
  override def write(kser: Kryo, out: Output, left: Left[A, B]): Unit =
    kser.writeClassAndObject(out, left.value)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Left[A, B]]): Left[A, B] =
    Left(kser.readClassAndObject(in).asInstanceOf[A])
}

/** Serializer for Right[A, B]. */
private[spark] class RightSerializer[A, B] extends Serializer[Right[A, B]] {
  override def write(kser: Kryo, out: Output, right: Right[A, B]): Unit =
    kser.writeClassAndObject(out, right.value)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Right[A, B]]): Right[A, B] =
    Right(kser.readClassAndObject(in).asInstanceOf[B])
}

/** Serializer for Stream[T] (deprecated in 2.13, but still used). */
@annotation.nowarn("cat=deprecation")
private[spark] class StreamSerializer[T] extends Serializer[Stream[T]] {
  override def write(kser: Kryo, out: Output, stream: Stream[T]): Unit =
    kser.writeClassAndObject(out, stream.toList)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Stream[T]]): Stream[T] =
    kser.readClassAndObject(in).asInstanceOf[List[T]].toStream
}

/** Serializer for known singleton values. */
private[spark] class SingletonSerializer[T](obj: T) extends Serializer[T] {
  override def write(kser: Kryo, out: Output, o: T): Unit = {}
  override def read(kser: Kryo, in: Input, cls: Class[_ <: T]): T = obj
}

/** Serializer for Enumeration#Value. */
private[spark] class EnumerationSerializer extends Serializer[Enumeration#Value] {
  private val outerMethod = classOf[Enumeration#Value].getMethod("scala$Enumeration$$outerEnum")
  private val enumMap = MMap[Enumeration#Value, Enumeration]()

  private def enumOf(v: Enumeration#Value): Enumeration =
    enumMap.synchronized {
      enumMap.getOrElseUpdate(v, outerMethod.invoke(v).asInstanceOf[Enumeration])
    }

  override def write(kser: Kryo, out: Output, obj: Enumeration#Value): Unit = {
    kser.writeClassAndObject(out, enumOf(obj))
    out.writeInt(obj.id)
  }

  override def read(
      kser: Kryo, in: Input,
      cls: Class[_ <: Enumeration#Value]): Enumeration#Value = {
    val enumeration = kser.readClassAndObject(in).asInstanceOf[Enumeration]
    enumeration(in.readInt).asInstanceOf[Enumeration#Value]
  }
}

/** Serializer for ClassTag[T]. */
private[spark] class ClassTagSerializer[T] extends Serializer[ClassTag[T]] {
  override def write(kser: Kryo, out: Output, obj: ClassTag[T]): Unit =
    kser.writeObject(out, obj.runtimeClass)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: ClassTag[T]]): ClassTag[T] =
    ClassTag(kser.readObject(in, classOf[Class[T]]))
}

/** Serializer for Manifest[T]. */
private[spark] class ManifestSerializer[T] extends Serializer[Manifest[T]] {
  private val singletons: IndexedSeq[Manifest[_]] = IndexedSeq(
    Manifest.Any, Manifest.AnyVal, Manifest.Boolean, Manifest.Byte, Manifest.Char,
    Manifest.Double, Manifest.Float, Manifest.Int, Manifest.Long, Manifest.Nothing,
    Manifest.Null, Manifest.Object, Manifest.Short, Manifest.Unit
  )
  private val singletonToIdx: Map[Manifest[_], Int] = singletons.zipWithIndex.toMap

  private def writeInternal(kser: Kryo, out: Output, obj: Manifest[_]): Unit = {
    val idxOpt = singletonToIdx.get(obj)
    if (idxOpt.isDefined) {
      out.writeInt(idxOpt.get + 1, true)
    } else {
      out.writeInt(0, true)
      kser.writeObject(out, obj.runtimeClass)
      val targs = obj.typeArguments
      out.writeInt(targs.size, true)
      out.flush()
      targs.foreach(writeInternal(kser, out, _))
    }
  }

  override def write(kser: Kryo, out: Output, obj: Manifest[T]): Unit =
    writeInternal(kser, out, obj)

  override def read(kser: Kryo, in: Input, cls: Class[_ <: Manifest[T]]): Manifest[T] = {
    val sidx = in.readInt(true)
    if (sidx == 0) {
      val clazz = kser.readObject(in, classOf[Class[T]])
      val targsCnt = in.readInt(true)
      if (targsCnt == 0) {
        Manifest.classType(clazz)
      } else {
        val typeArgs = (0 until targsCnt).map(_ => read(kser, in, null))
        Manifest.classType(clazz, typeArgs.head, typeArgs.tail: _*)
      }
    } else {
      singletons(sidx - 1).asInstanceOf[Manifest[T]]
    }
  }
}

/** Serializer for scala.util.matching.Regex. */
private[spark] class ScalaRegexSerializer extends Serializer[Regex] {
  override def write(kser: Kryo, out: Output, obj: Regex): Unit =
    out.writeString(obj.pattern.pattern)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Regex]): Regex =
    new Regex(in.readString)
}

/** Serializer for scala.math.BigDecimal. */
private[spark] class ScalaBigDecimalSerializer extends Serializer[BigDecimal] {
  override def write(kryo: Kryo, output: Output, obj: BigDecimal): Unit =
    kryo.writeClassAndObject(output, obj.bigDecimal)
  override def read(kryo: Kryo, input: Input, cls: Class[_ <: BigDecimal]): BigDecimal =
    BigDecimal(kryo.readClassAndObject(input).asInstanceOf[JBigDecimal])
}

/** Serializer for VolatileByteRef. */
private[spark] class VolatileByteRefSerializer extends Serializer[VolatileByteRef] {
  override def write(kser: Kryo, out: Output, item: VolatileByteRef): Unit =
    out.writeByte(item.elem)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: VolatileByteRef]): VolatileByteRef =
    new VolatileByteRef(in.readByte)
}

// --- Java serializers not built into Kryo 5 (from chill-java module) ---

/** Serializer for InetSocketAddress.
 * Originally from com.twitter.chill.java.InetSocketAddressSerializer
 * (chill-java module, Apache 2.0 licensed).
 */
private[spark] class InetSocketAddressSerializer extends Serializer[InetSocketAddress] {
  override def write(kryo: Kryo, output: Output, obj: InetSocketAddress): Unit = {
    output.writeString(obj.getHostName)
    output.writeInt(obj.getPort, true)
  }
  override def read(kryo: Kryo, input: Input, cls: Class[_ <: InetSocketAddress])
    : InetSocketAddress = {
    new InetSocketAddress(input.readString, input.readInt(true))
  }
}

/** Serializer for SimpleDateFormat. Falls back to Java serialization.
 * Originally from com.twitter.chill.java.SimpleDateFormatSerializer
 * (chill-java module, Apache 2.0 licensed).
 */
private[spark] class SimpleDateFormatSerializer extends KryoJavaSerializer

/**
 * Base serializer for Collections.unmodifiable* wrappers.
 * Originally from com.twitter.chill.java.UnmodifiableJavaCollectionSerializer
 * (chill-java module, Apache 2.0 licensed).
 */
private[spark] abstract class UnmodifiableJavaCollectionSerializer[T]
  extends Serializer[T] {

  protected def getInnerField(): Field
  protected def newInstance(inner: Any): T

  @transient private lazy val innerField: Field = {
    val f = getInnerField()
    f.setAccessible(true)
    f
  }

  override def write(kryo: Kryo, output: Output, obj: T): Unit = {
    val inner = innerField.get(obj)
    kryo.writeClassAndObject(output, inner)
  }

  override def read(kryo: Kryo, input: Input, cls: Class[_ <: T]): T = {
    val inner = kryo.readClassAndObject(input)
    newInstance(inner)
  }
}

// scalastyle:off classforname
private[spark] class UnmodifiableCollectionSerializer
  extends UnmodifiableJavaCollectionSerializer[java.util.Collection[_]] {
  override protected def getInnerField(): Field =
    Class.forName("java.util.Collections$UnmodifiableCollection").getDeclaredField("c")
  override protected def newInstance(c: Any): java.util.Collection[_] =
    Collections.unmodifiableCollection(c.asInstanceOf[java.util.Collection[_]])
}

private[spark] class UnmodifiableListSerializer
  extends UnmodifiableJavaCollectionSerializer[java.util.List[_]] {
  override protected def getInnerField(): Field =
    Class.forName("java.util.Collections$UnmodifiableCollection").getDeclaredField("c")
  override protected def newInstance(c: Any): java.util.List[_] =
    Collections.unmodifiableList(c.asInstanceOf[java.util.List[_]])
}

private[spark] class UnmodifiableSetSerializer
  extends UnmodifiableJavaCollectionSerializer[java.util.Set[_]] {
  override protected def getInnerField(): Field =
    Class.forName("java.util.Collections$UnmodifiableCollection").getDeclaredField("c")
  override protected def newInstance(c: Any): java.util.Set[_] =
    Collections.unmodifiableSet(c.asInstanceOf[java.util.Set[_]])
}

private[spark] class UnmodifiableMapSerializer
  extends UnmodifiableJavaCollectionSerializer[java.util.Map[_, _]] {
  override protected def getInnerField(): Field =
    Class.forName("java.util.Collections$UnmodifiableMap").getDeclaredField("m")
  override protected def newInstance(c: Any): java.util.Map[_, _] =
    Collections.unmodifiableMap(c.asInstanceOf[java.util.Map[_, _]])
}

private[spark] class UnmodifiableSortedMapSerializer
  extends UnmodifiableJavaCollectionSerializer[java.util.SortedMap[_, _]] {
  override protected def getInnerField(): Field =
    Class.forName("java.util.Collections$UnmodifiableSortedMap").getDeclaredField("sm")
  override protected def newInstance(c: Any): java.util.SortedMap[_, _] =
    Collections.unmodifiableSortedMap(c.asInstanceOf[java.util.SortedMap[_, _]])
}

private[spark] class UnmodifiableSortedSetSerializer
  extends UnmodifiableJavaCollectionSerializer[java.util.SortedSet[_]] {
  override protected def getInnerField(): Field =
    Class.forName("java.util.Collections$UnmodifiableSortedSet").getDeclaredField("ss")
  override protected def newInstance(c: Any): java.util.SortedSet[_] =
    Collections.unmodifiableSortedSet(c.asInstanceOf[java.util.SortedSet[_]])
}
// scalastyle:on classforname
