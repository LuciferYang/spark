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

import java.io.{ByteArrayOutputStream, Serializable}

import com.esotericsoftware.kryo.kryo5.Kryo
import com.esotericsoftware.kryo.kryo5.io.{Input, Output}
import com.esotericsoftware.kryo.kryo5.util.Pool

/**
 * Use Kryo to provide a "box" which is efficiently Java-serializable even if
 * the underlying value is not, as long as it is serializable with Kryo.
 *
 * Originally from com.twitter.chill.MeatLocker
 * (chill-scala module, Apache 2.0 licensed).
 * Adapted to use Kryo 5's Pool API instead of chill's KryoPool.
 */
private[spark] object MeatLocker {
  def apply[T](t: T): MeatLocker[T] = new MeatLocker(t)

  /** Shared Kryo pool for MeatLocker serialization. */
  private[kryo] val kryoPool: Pool[Kryo] = {
    val cores = Runtime.getRuntime.availableProcessors
    new Pool[Kryo](true, false, cores * 4) {
      override def create(): Kryo = {
        val inst = new EmptyScalaKryoInstantiator
        val k = inst.newKryo()
        new AllScalaRegistrar()(k)
        k
      }
    }
  }

  private[kryo] def toBytesWithClass(obj: Any): Array[Byte] = {
    val kryo = kryoPool.obtain()
    try {
      val baos = new ByteArrayOutputStream()
      val output = new Output(baos)
      kryo.writeClassAndObject(output, obj)
      output.flush()
      baos.toByteArray
    } finally {
      kryoPool.free(kryo)
    }
  }

  private[kryo] def fromBytes(bytes: Array[Byte]): Any = {
    val kryo = kryoPool.obtain()
    try {
      val input = new Input(bytes)
      kryo.readClassAndObject(input)
    } finally {
      kryoPool.free(kryo)
    }
  }
}

private[spark] class MeatLocker[T](@transient protected var t: T) extends Serializable {
  protected val tBytes: Array[Byte] = MeatLocker.toBytesWithClass(t)

  def get: T = {
    if (null.asInstanceOf[T] == t) {
      // we were deserialized via Java, so t is null — restore from bytes
      t = copy
    }
    t
  }

  def copy: T = MeatLocker.fromBytes(tBytes).asInstanceOf[T]
}
