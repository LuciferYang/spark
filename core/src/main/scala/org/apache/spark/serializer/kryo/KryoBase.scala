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

import java.lang.reflect.Modifier

import scala.collection.mutable.{Map => MMap}
import scala.util.{Failure, Success, Try}
import scala.util.control.Exception.allCatch

import com.esotericsoftware.kryo.kryo5.{Kryo, KryoException, Serializer}
import com.esotericsoftware.kryo.kryo5.objenesis.instantiator.ObjectInstantiator
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.InstantiatorStrategy
import com.esotericsoftware.kryo.kryo5.reflectasm.ConstructorAccess
import com.esotericsoftware.kryo.kryo5.serializers.FieldSerializer
import com.esotericsoftware.kryo.kryo5.util.{DefaultClassResolver, MapReferenceResolver}

/**
 * A Scala-aware Kryo subclass that handles Scala singletons, synthetic fields,
 * and constructor access. Originally from com.twitter.chill.KryoBase
 * (chill-scala module, Apache 2.0 licensed).
 */
private[spark] class KryoBase extends Kryo(new DefaultClassResolver, new MapReferenceResolver) {

  private lazy val objSer = new ObjectSerializer[AnyRef]

  private var strategy: Option[InstantiatorStrategy] = None

  private val functions: Iterable[Class[_]] = List(
    classOf[Function0[_]],
    classOf[Function1[_, _]],
    classOf[Function2[_, _, _]],
    classOf[Function3[_, _, _, _]],
    classOf[Function4[_, _, _, _, _]],
    classOf[Function5[_, _, _, _, _, _]],
    classOf[Function6[_, _, _, _, _, _, _]],
    classOf[Function7[_, _, _, _, _, _, _, _]],
    classOf[Function8[_, _, _, _, _, _, _, _, _]],
    classOf[Function9[_, _, _, _, _, _, _, _, _, _]],
    classOf[Function10[_, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function11[_, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function12[_, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function13[_, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]]
  )

  private def isFn(klass: Class[_]): Boolean =
    functions.exists(_.isAssignableFrom(klass))

  override def newDefaultSerializer(klass: Class[_]): Serializer[_] =
    if (isSingleton(klass)) {
      objSer
    } else {
      super.newDefaultSerializer(klass) match {
        case fs: FieldSerializer[_] =>
          // Scala has a lot of synthetic fields that must be serialized
          fs.getFieldSerializerConfig.setIgnoreSyntheticFields(false)
          fs.updateFields()
          fs
        case x: Serializer[_] => x
      }
    }

  /** Returns true if this class is a Scala `object` singleton. */
  private def isSingleton(klass: Class[_]): Boolean =
    klass.getName.last == '$' && objSer.accepts(klass)

  private def tryStrategy(cls: Class[_]): InstantiatorStrategy =
    strategy.getOrElse {
      val name = cls.getName
      if (cls.isMemberClass && !Modifier.isStatic(cls.getModifiers)) {
        throw new KryoException("Class cannot be created (non-static member class): " + name)
      } else {
        throw new KryoException("Class cannot be created (missing no-arg constructor): " + name)
      }
    }

  override def setInstantiatorStrategy(st: InstantiatorStrategy): Unit = {
    super.setInstantiatorStrategy(st)
    strategy = Some(st)
  }

  override def newInstantiator(cls: Class[_]): ObjectInstantiator[AnyRef] =
    newTypedInstantiator(cls.asInstanceOf[Class[AnyRef]])

  private def newTypedInstantiator[T](cls: Class[T]): ObjectInstantiator[T] = {
    val fns: List[Class[T] => Try[ObjectInstantiator[T]]] =
      List(reflectAsm[T](_), normalJava[T](_))
    fns.flatMap(fn => fn(cls).toOption)
      .headOption
      .getOrElse(tryStrategy(cls).newInstantiatorOf(cls))
  }

  private def reflectAsm[T](t: Class[T]): Try[ObjectInstantiator[T]] =
    try {
      val access = ConstructorAccess.get(t)
      access.newInstance()
      Success(new ObjectInstantiator[T] {
        override def newInstance(): T =
          try access.newInstance()
          catch {
            case x: Exception =>
              throw new KryoException("Error constructing instance of class: " + t.getName, x)
          }
      })
    } catch {
      case x: Throwable => Failure(x)
    }

  private def normalJava[T](t: Class[T]): Try[ObjectInstantiator[T]] =
    try {
      val cons = try t.getConstructor()
      catch {
        case _: Throwable =>
          val c = t.getDeclaredConstructor()
          c.setAccessible(true)
          c
      }
      Success(new ObjectInstantiator[T] {
        override def newInstance(): T =
          try cons.newInstance()
          catch {
            case x: Exception =>
              throw new KryoException("Error constructing instance of class: " + t.getName, x)
          }
      })
    } catch {
      case x: Throwable => Failure(x)
    }
}

/**
 * Serializer for Scala `object` singletons. Uses reflection to read the MODULE$ field.
 * Originally from com.twitter.chill.ObjectSerializer
 * (chill-scala module, Apache 2.0 licensed).
 */
private[kryo] class ObjectSerializer[T] extends Serializer[T] {
  setImmutable(true)
  private val cachedObj = MMap[Class[_], Option[T]]()

  override def write(kser: Kryo, out: com.esotericsoftware.kryo.kryo5.io.Output, obj: T): Unit = {}

  override def read(
      kser: Kryo,
      in: com.esotericsoftware.kryo.kryo5.io.Input,
      cls: Class[_ <: T]): T =
    cachedRead(cls).get

  def accepts(cls: Class[_]): Boolean = cachedRead(cls).isDefined

  private def createSingleton(cls: Class[_]): Option[T] =
    moduleField(cls).map(_.get(null).asInstanceOf[T])

  private def cachedRead(cls: Class[_]): Option[T] =
    cachedObj.synchronized(cachedObj.getOrElseUpdate(cls, createSingleton(cls)))

  private def moduleField(klass: Class[_]): Option[java.lang.reflect.Field] =
    Some(klass)
      .filter(_.getName.last == '$')
      .flatMap(k => allCatch.opt(k.getDeclaredField("MODULE$")))
}
