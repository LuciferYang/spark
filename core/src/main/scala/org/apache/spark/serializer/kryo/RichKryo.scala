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

import scala.collection.Factory
import scala.reflect._

import com.esotericsoftware.kryo.kryo5.Kryo
import com.esotericsoftware.kryo.kryo5.Serializer
import com.esotericsoftware.kryo.kryo5.serializers.{JavaSerializer => KryoJavaSerializer}

/**
 * Enrichment class for Kryo that provides convenient registration methods.
 * Originally from com.twitter.chill.RichKryo
 * (chill-scala module, Apache 2.0 licensed).
 */
class RichKryo(val k: Kryo) {
  def alreadyRegistered(klass: Class[_]): Boolean =
    k.getClassResolver.getRegistration(klass) != null

  def forSubclass[T](kser: Serializer[T])(implicit cmf: ClassTag[T]): Kryo = {
    k.addDefaultSerializer(cmf.runtimeClass, kser)
    k
  }

  def forClass[T](kser: Serializer[T])(implicit cmf: ClassTag[T]): Kryo = {
    k.register(cmf.runtimeClass, kser)
    k
  }

  def javaForClass[T <: java.io.Serializable](implicit cmf: ClassTag[T]): Kryo = {
    k.register(cmf.runtimeClass, new KryoJavaSerializer)
    k
  }

  def registerClasses(klasses: IterableOnce[Class[_]]): Kryo = {
    klasses.iterator.foreach { klass: Class[_] =>
      if (!alreadyRegistered(klass)) {
        k.register(klass)
      }
    }
    k
  }

  def forTraversableSubclass[T, C <: Iterable[T]](
      c: C with Iterable[T],
      isImmutable: Boolean = true
  )(implicit mf: ClassTag[C], f: Factory[T, C]): Kryo = {
    k.addDefaultSerializer(mf.runtimeClass, new TraversableSerializer(isImmutable)(f))
    k
  }

  def forTraversableClass[T, C <: Iterable[T]](
      c: C with Iterable[T],
      isImmutable: Boolean = true
  )(implicit mf: ClassTag[C], f: Factory[T, C]): Kryo =
    forClass(new TraversableSerializer(isImmutable)(f))

  def forConcreteTraversableClass[T, C <: Iterable[T]](
      c: C with Iterable[T],
      isImmutable: Boolean = true
  )(implicit f: Factory[T, C]): Kryo = {
    k.register(c.getClass, new TraversableSerializer(isImmutable)(f))
    k
  }
}
