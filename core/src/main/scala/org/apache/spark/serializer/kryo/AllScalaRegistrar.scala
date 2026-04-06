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

import scala.collection.immutable.{ArraySeq, BitSet, HashMap, HashSet, ListMap, ListSet,
  NumericRange, Queue, Range, SortedMap, SortedSet, TreeMap, TreeSet, WrappedString}
import scala.collection.mutable.{ArraySeq => MArraySeq, BitSet => MBitSet, Buffer,
  HashMap => MHashMap, HashSet => MHashSet, ListBuffer, Map => MMap, Queue => MQueue,
  Set => MSet}
import scala.jdk.CollectionConverters._

import com.esotericsoftware.kryo.kryo5.{Kryo, Serializer}
import com.esotericsoftware.kryo.kryo5.io.{Input, Output}
import com.esotericsoftware.kryo.kryo5.serializers.FieldSerializer

/**
 * Registrar chain for Scala and Java types, adapted for Kryo 5 and Scala 2.13+.
 *
 * Source attribution:
 *  - KryoRegistrar, ScalaCollectionsRegistrar, AllScalaRegistrar_0_9_2,
 *    AllScalaRegistrar_0_9_5, AllScalaRegistrarCompat, AllScalaRegistrar
 *    are originally from com.twitter.chill (chill-scala module, Apache 2.0 licensed).
 *  - JavaRegistrar is originally from com.twitter.chill.java.PackageRegistrar
 *    and JavaWrapperCollectionRegistrar (chill-java module, Apache 2.0 licensed).
 *    Many chill-java serializers (ArraysAsList, java.util.BitSet, PriorityQueue,
 *    java.util.regex.Pattern, SqlDate, SqlTime, Timestamp, URI, UUID, Locale) have
 *    equivalents built into Kryo 5's default serializers and are NOT registered here.
 */

// --- Registrar trait (replaces chill's IKryoRegistrar) ---

/** Trait for classes that register serializers with a Kryo instance. */
private[spark] trait KryoRegistrar extends java.io.Serializable {
  def apply(k: Kryo): Unit
}

// --- ScalaCollectionsRegistrar ---

/**
 * Registers serializers for core Scala collection types.
 * Originally from com.twitter.chill.ScalaCollectionsRegistrar.
 */
private[spark] class ScalaCollectionsRegistrar extends KryoRegistrar {
  def apply(newK: Kryo): Unit = {
    // Register Java/Scala collection wrappers using FieldSerializer with synthetic fields
    def useField[T](cls: Class[T]): Unit = {
      val fs = new FieldSerializer(newK, cls)
      fs.getFieldSerializerConfig.setIgnoreSyntheticFields(false)
      fs.updateFields()
      newK.register(cls, fs)
    }
    useField(List(1, 2, 3).asJava.getClass)
    useField(List(1, 2, 3).iterator.asJava.getClass)
    useField(Map(1 -> 2, 4 -> 3).asJava.getClass)
    useField(new _root_.java.util.ArrayList().asScala.getClass)
    useField(new _root_.java.util.HashMap().asScala.getClass)

    // Use implicit RichKryo conversions from package object
    newK
      .forSubclass[MArraySeq[Any]](new WrappedArraySerializer[Any])
      .forSubclass[BitSet](new ScalaBitSetSerializer)
      .forSubclass[SortedSet[Any]](new SortedSetSerializer)
      .forClass[Some[Any]](new SomeSerializer[Any])
      .forClass[Left[Any, Any]](new LeftSerializer[Any, Any])
      .forClass[Right[Any, Any]](new RightSerializer[Any, Any])
      .forTraversableSubclass(Queue.empty[Any])
      .forTraversableSubclass(List.empty[Any])
      .forTraversableSubclass(ListBuffer.empty[Any], isImmutable = false)
      .forTraversableSubclass(Buffer.empty[Any], isImmutable = false)
      .forTraversableClass(Vector.empty[Any])
      .forTraversableSubclass(ListSet.empty[Any])
      // specifically register small sets since Scala represents them differently
      .forConcreteTraversableClass(Set[Any](Symbol("a")))
      .forConcreteTraversableClass(Set[Any](Symbol("a"), Symbol("b")))
      .forConcreteTraversableClass(Set[Any](Symbol("a"), Symbol("b"), Symbol("c")))
      .forConcreteTraversableClass(
        Set[Any](Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d")))
      .forConcreteTraversableClass(
        HashSet[Any](Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"), Symbol("e")))
      // specifically register small maps since Scala represents them differently
      .forConcreteTraversableClass(Map[Any, Any](Symbol("a") -> Symbol("a")))
      .forConcreteTraversableClass(
        Map[Any, Any](Symbol("a") -> Symbol("a"), Symbol("b") -> Symbol("b")))
      .forConcreteTraversableClass(
        Map[Any, Any](Symbol("a") -> Symbol("a"), Symbol("b") -> Symbol("b"),
          Symbol("c") -> Symbol("c")))
      .forConcreteTraversableClass(
        Map[Any, Any](Symbol("a") -> Symbol("a"), Symbol("b") -> Symbol("b"),
          Symbol("c") -> Symbol("c"), Symbol("d") -> Symbol("d")))
      .forConcreteTraversableClass(
        HashMap[Any, Any](Symbol("a") -> Symbol("a"), Symbol("b") -> Symbol("b"),
          Symbol("c") -> Symbol("c"), Symbol("d") -> Symbol("d"), Symbol("e") -> Symbol("e")))
      .registerClasses(
        Seq(classOf[Range.Inclusive],
          classOf[NumericRange.Inclusive[_]],
          classOf[NumericRange.Exclusive[_]]))
      .forSubclass[SortedMap[Any, Any]](new SortedMapSerializer)
      .forTraversableSubclass(ListMap.empty[Any, Any])
      .forTraversableSubclass(HashMap.empty[Any, Any])
      .forTraversableSubclass(Map.empty[Any, Any])
      // mutable collections
      .forTraversableClass(MBitSet.empty, isImmutable = false)
      .forTraversableClass(MHashMap.empty[Any, Any], isImmutable = false)
      .forTraversableClass(MHashSet.empty[Any], isImmutable = false)
      .forTraversableSubclass(MQueue.empty[Any], isImmutable = false)
      .forTraversableSubclass(MMap.empty[Any, Any], isImmutable = false)
      .forTraversableSubclass(MSet.empty[Any], isImmutable = false)
  }
}

// --- JavaWrapperCollectionRegistrar ---

/**
 * Registers the unmodifiable Java collection wrapper serializers and
 * InetSocketAddress/SimpleDateFormat serializers that are not built into Kryo 5.
 * Originally from com.twitter.chill.java.PackageRegistrar and
 * JavaWrapperCollectionRegistrar (chill-java module, Apache 2.0 licensed).
 *
 * Note: Many chill-java serializers (ArraysAsList, java.util.BitSet, PriorityQueue,
 * java.util.regex.Pattern, SqlDate, SqlTime, Timestamp, URI, UUID, Locale) have
 * equivalents built into Kryo 5's default serializers and are NOT registered here.
 */
private[spark] class JavaRegistrar extends KryoRegistrar {
  def apply(k: Kryo): Unit = {
    // Unmodifiable collection wrappers - still needed in Kryo 5
    // scalastyle:off classforname
    k.addDefaultSerializer(
      Class.forName("java.util.Collections$UnmodifiableCollection"),
      new UnmodifiableCollectionSerializer)
    // scalastyle:on classforname
    k.register(
      java.util.Collections.unmodifiableList(
        new java.util.ArrayList[AnyRef]()).getClass,
      new UnmodifiableListSerializer)
    k.register(
      java.util.Collections.unmodifiableSet(
        new java.util.HashSet[AnyRef]()).getClass,
      new UnmodifiableSetSerializer)
    k.register(
      java.util.Collections.unmodifiableMap(
        new java.util.HashMap[AnyRef, AnyRef]()).getClass,
      new UnmodifiableMapSerializer)
    k.register(
      java.util.Collections.unmodifiableSortedMap(
        new java.util.TreeMap[AnyRef, AnyRef]()).getClass,
      new UnmodifiableSortedMapSerializer)
    k.register(
      java.util.Collections.unmodifiableSortedSet(
        new java.util.TreeSet[AnyRef]()).getClass,
      new UnmodifiableSortedSetSerializer)
    // InetSocketAddress and SimpleDateFormat - not built into Kryo 5
    k.register(classOf[java.net.InetSocketAddress], new InetSocketAddressSerializer)
    k.register(classOf[java.text.SimpleDateFormat], new SimpleDateFormatSerializer)
  }
}

// --- AllScalaRegistrar_0_9_2 ---

/**
 * Registrar for everything that was registered in chill 0.9.2.
 * Originally from com.twitter.chill.AllScalaRegistrar_0_9_2.
 */
private[spark] class AllScalaRegistrar_0_9_2 extends KryoRegistrar {
  def apply(k: Kryo): Unit = {
    new ScalaCollectionsRegistrar()(k)

    // Register all 22 tuple serializers and specialized serializers
    ScalaTupleSerialization.register(k)

    k.forClass[Symbol](new Serializer[Symbol] {
      override def isImmutable: Boolean = true
      override def write(kser: Kryo, out: Output, obj: Symbol): Unit =
        out.writeString(obj.name)
      override def read(kser: Kryo, in: Input, cls: Class[_ <: Symbol]): Symbol =
        Symbol(in.readString)
    }).forSubclass[scala.util.matching.Regex](new ScalaRegexSerializer)
      .forClass[scala.reflect.ClassTag[Any]](new ClassTagSerializer[Any])
      .forSubclass[Manifest[Any]](new ManifestSerializer[Any])
      .forSubclass[scala.Enumeration#Value](new EnumerationSerializer)

    // use the singleton serializer for boxed Unit
    val boxedUnit = scala.runtime.BoxedUnit.UNIT
    k.register(boxedUnit.getClass, new SingletonSerializer(boxedUnit))

    // Java serializers (subset not built into Kryo 5)
    new JavaRegistrar()(k)
  }
}

// --- AllScalaRegistrar_0_9_5 ---

/**
 * Registrar for everything that was registered in chill 0.9.5.
 * Originally from com.twitter.chill.AllScalaRegistrar_0_9_5.
 */
// scalastyle:off line.size.limit
private[spark] class AllScalaRegistrar_0_9_5 extends KryoRegistrar {
  @annotation.nowarn("cat=deprecation")
  def apply(k: Kryo): Unit = {
    new AllScalaRegistrar_0_9_2()(k)

    // Scala 2.13+ compat: Range.Exclusive
    k.register(classOf[Range.Exclusive])

    k.registerClasses(
      Seq(
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
        ArraySeq.unsafeWrapArray(Array[Byte]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Short]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Int]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Long]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Float]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Double]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Boolean]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Char]()).getClass,
        ArraySeq.unsafeWrapArray(Array[String]()).getClass,
        None.getClass,
        classOf[Queue[_]],
        Nil.getClass,
        classOf[::[_]],
        classOf[Range],
        classOf[WrappedString],
        classOf[TreeSet[_]],
        classOf[TreeMap[_, _]],
        // The most common orderings for TreeSet and TreeMap
        Ordering.Byte.getClass,
        Ordering.Short.getClass,
        Ordering.Int.getClass,
        Ordering.Long.getClass,
        Ordering.Float.getClass,
        Ordering.Double.getClass,
        Ordering.Boolean.getClass,
        Ordering.Char.getClass,
        Ordering.String.getClass
      )
    ).forConcreteTraversableClass(Set[Any]())
      .forConcreteTraversableClass(ListSet[Any]())
      .forConcreteTraversableClass(ListSet[Any](Symbol("a")))
      .forConcreteTraversableClass(HashSet[Any]())
      .forConcreteTraversableClass(HashSet[Any](Symbol("a")))
      .forConcreteTraversableClass(Map[Any, Any]())
      .forConcreteTraversableClass(HashMap[Any, Any]())
      .forConcreteTraversableClass(HashMap(Symbol("a") -> Symbol("a")))
      .forConcreteTraversableClass(ListMap[Any, Any]())
      .forConcreteTraversableClass(ListMap(Symbol("a") -> Symbol("a")))

    k.register(classOf[Stream.Cons[_]], new StreamSerializer[Any])
    k.register(Stream.empty[Any].getClass)
    k.forClass[scala.runtime.VolatileByteRef](new VolatileByteRefSerializer)
    k.forClass[BigDecimal](new ScalaBigDecimalSerializer)
    k.register(Queue.empty[Any].getClass)
    k.forConcreteTraversableClass(Map(1 -> 2).filter(_._1 != 2))
      .forConcreteTraversableClass(Map(1 -> 2).map { case (k, v) => (k, v + 1) })
      .forConcreteTraversableClass(Map(1 -> 2).keySet)
  }
}
// scalastyle:on line.size.limit

// --- AllScalaRegistrarCompat (Scala 2.13+) ---

/**
 * Registers Vector0-6 internal classes and ArraySeq variants for Scala 2.13+.
 * Originally from com.twitter.chill.AllScalaRegistrarCompat.
 */
private[spark] class AllScalaRegistrarCompat extends KryoRegistrar {
  def apply(newK: Kryo): Unit = {
    val t: TraversableSerializer[Any, Vector[_]] = new TraversableSerializer(true)
    // scalastyle:off classforname
    newK.register(Class.forName("scala.collection.immutable.Vector0$"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector1"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector2"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector3"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector4"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector5"), t)
    newK.register(Class.forName("scala.collection.immutable.Vector6"), t)
    // scalastyle:on classforname
    newK.registerClasses(
      Seq(
        ArraySeq.unsafeWrapArray(Array[Byte]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Short]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Int]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Long]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Float]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Double]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Boolean]()).getClass,
        ArraySeq.unsafeWrapArray(Array[Char]()).getClass,
        ArraySeq.unsafeWrapArray(Array[String]()).getClass
      )
    )
  }
}

// --- AllScalaRegistrar (top-level entry point) ---

/**
 * Registers all Scala (and Java) serializers.
 * This is the main entry point used by KryoSerializer.
 * Originally from com.twitter.chill.AllScalaRegistrar.
 */
private[spark] class AllScalaRegistrar extends KryoRegistrar {
  def apply(k: Kryo): Unit = {
    new AllScalaRegistrar_0_9_5()(k)
    new AllScalaRegistrarCompat()(k)
  }
}
