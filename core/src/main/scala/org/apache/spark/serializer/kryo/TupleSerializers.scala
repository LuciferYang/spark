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

import java.io.Serializable

import com.esotericsoftware.kryo.kryo5.{Kryo, Serializer}
import com.esotericsoftware.kryo.kryo5.io.{Input, Output}

// Originally auto-generated in com.twitter.chill.TupleSerializers
// (chill-scala module, Apache 2.0 licensed).
// Adapted for Kryo 5 (read() signature: Class[_ <: T]).

// --- Generic Tuple Serializers (Tuple1 through Tuple22) ---

private[spark] class Tuple1Serializer[A] extends Serializer[Tuple1[A]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple1[A]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple1[A]]): Tuple1[A] =
    new Tuple1[A](kser.readClassAndObject(in).asInstanceOf[A])
}

private[spark] class Tuple2Serializer[A, B] extends Serializer[Tuple2[A, B]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple2[A, B]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple2[A, B]]): Tuple2[A, B] =
    new Tuple2[A, B](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B])
}

private[spark] class Tuple3Serializer[A, B, C]
  extends Serializer[Tuple3[A, B, C]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple3[A, B, C]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple3[A, B, C]]): Tuple3[A, B, C] =
    new Tuple3[A, B, C](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C])
}

private[spark] class Tuple4Serializer[A, B, C, D]
  extends Serializer[Tuple4[A, B, C, D]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple4[A, B, C, D]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple4[A, B, C, D]]): Tuple4[A, B, C, D] =
    new Tuple4[A, B, C, D](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D])
}

private[spark] class Tuple5Serializer[A, B, C, D, E]
  extends Serializer[Tuple5[A, B, C, D, E]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple5[A, B, C, D, E]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple5[A, B, C, D, E]]): Tuple5[A, B, C, D, E] =
    new Tuple5[A, B, C, D, E](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E])
}

private[spark] class Tuple6Serializer[A, B, C, D, E, F]
  extends Serializer[Tuple6[A, B, C, D, E, F]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple6[A, B, C, D, E, F]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple6[A, B, C, D, E, F]]): Tuple6[A, B, C, D, E, F] =
    new Tuple6[A, B, C, D, E, F](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F])
}

private[spark] class Tuple7Serializer[A, B, C, D, E, F, G]
  extends Serializer[Tuple7[A, B, C, D, E, F, G]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple7[A, B, C, D, E, F, G]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple7[A, B, C, D, E, F, G]]): Tuple7[A, B, C, D, E, F, G] =
    new Tuple7[A, B, C, D, E, F, G](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G])
}

private[spark] class Tuple8Serializer[A, B, C, D, E, F, G, H]
  extends Serializer[Tuple8[A, B, C, D, E, F, G, H]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple8[A, B, C, D, E, F, G, H]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple8[A, B, C, D, E, F, G, H]]): Tuple8[A, B, C, D, E, F, G, H] =
    new Tuple8[A, B, C, D, E, F, G, H](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H])
}

private[spark] class Tuple9Serializer[A, B, C, D, E, F, G, H, I]
  extends Serializer[Tuple9[A, B, C, D, E, F, G, H, I]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple9[A, B, C, D, E, F, G, H, I]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple9[A, B, C, D, E, F, G, H, I]]): Tuple9[A, B, C, D, E, F, G, H, I] =
    new Tuple9[A, B, C, D, E, F, G, H, I](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I])
}

// scalastyle:off line.size.limit

private[spark] class Tuple10Serializer[A, B, C, D, E, F, G, H, I, J]
  extends Serializer[Tuple10[A, B, C, D, E, F, G, H, I, J]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple10[A, B, C, D, E, F, G, H, I, J]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple10[A, B, C, D, E, F, G, H, I, J]]): Tuple10[A, B, C, D, E, F, G, H, I, J] =
    new Tuple10[A, B, C, D, E, F, G, H, I, J](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J])
}

private[spark] class Tuple11Serializer[A, B, C, D, E, F, G, H, I, J, K]
  extends Serializer[Tuple11[A, B, C, D, E, F, G, H, I, J, K]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple11[A, B, C, D, E, F, G, H, I, J, K]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple11[A, B, C, D, E, F, G, H, I, J, K]]): Tuple11[A, B, C, D, E, F, G, H, I, J, K] =
    new Tuple11[A, B, C, D, E, F, G, H, I, J, K](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K])
}

private[spark] class Tuple12Serializer[A, B, C, D, E, F, G, H, I, J, K, L]
  extends Serializer[Tuple12[A, B, C, D, E, F, G, H, I, J, K, L]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple12[A, B, C, D, E, F, G, H, I, J, K, L]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple12[A, B, C, D, E, F, G, H, I, J, K, L]]): Tuple12[A, B, C, D, E, F, G, H, I, J, K, L] =
    new Tuple12[A, B, C, D, E, F, G, H, I, J, K, L](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L])
}

private[spark] class Tuple13Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M]
  extends Serializer[Tuple13[A, B, C, D, E, F, G, H, I, J, K, L, M]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple13[A, B, C, D, E, F, G, H, I, J, K, L, M]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple13[A, B, C, D, E, F, G, H, I, J, K, L, M]]): Tuple13[A, B, C, D, E, F, G, H, I, J, K, L, M] =
    new Tuple13[A, B, C, D, E, F, G, H, I, J, K, L, M](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M])
}

private[spark] class Tuple14Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N]
  extends Serializer[Tuple14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple14[A, B, C, D, E, F, G, H, I, J, K, L, M, N]]): Tuple14[A, B, C, D, E, F, G, H, I, J, K, L, M, N] =
    new Tuple14[A, B, C, D, E, F, G, H, I, J, K, L, M, N](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N])
}

private[spark] class Tuple15Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]
  extends Serializer[Tuple15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
    kser.writeClassAndObject(out, obj._15); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]]): Tuple15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O] =
    new Tuple15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N],
      kser.readClassAndObject(in).asInstanceOf[O])
}

private[spark] class Tuple16Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]
  extends Serializer[Tuple16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
    kser.writeClassAndObject(out, obj._15); out.flush()
    kser.writeClassAndObject(out, obj._16); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]]): Tuple16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P] =
    new Tuple16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N],
      kser.readClassAndObject(in).asInstanceOf[O],
      kser.readClassAndObject(in).asInstanceOf[P])
}

private[spark] class Tuple17Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]
  extends Serializer[Tuple17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
    kser.writeClassAndObject(out, obj._15); out.flush()
    kser.writeClassAndObject(out, obj._16); out.flush()
    kser.writeClassAndObject(out, obj._17); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]]): Tuple17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q] =
    new Tuple17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N],
      kser.readClassAndObject(in).asInstanceOf[O],
      kser.readClassAndObject(in).asInstanceOf[P],
      kser.readClassAndObject(in).asInstanceOf[Q])
}

private[spark] class Tuple18Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]
  extends Serializer[Tuple18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
    kser.writeClassAndObject(out, obj._15); out.flush()
    kser.writeClassAndObject(out, obj._16); out.flush()
    kser.writeClassAndObject(out, obj._17); out.flush()
    kser.writeClassAndObject(out, obj._18); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]]): Tuple18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R] =
    new Tuple18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N],
      kser.readClassAndObject(in).asInstanceOf[O],
      kser.readClassAndObject(in).asInstanceOf[P],
      kser.readClassAndObject(in).asInstanceOf[Q],
      kser.readClassAndObject(in).asInstanceOf[R])
}

private[spark] class Tuple19Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]
  extends Serializer[Tuple19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
    kser.writeClassAndObject(out, obj._15); out.flush()
    kser.writeClassAndObject(out, obj._16); out.flush()
    kser.writeClassAndObject(out, obj._17); out.flush()
    kser.writeClassAndObject(out, obj._18); out.flush()
    kser.writeClassAndObject(out, obj._19); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]]): Tuple19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S] =
    new Tuple19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N],
      kser.readClassAndObject(in).asInstanceOf[O],
      kser.readClassAndObject(in).asInstanceOf[P],
      kser.readClassAndObject(in).asInstanceOf[Q],
      kser.readClassAndObject(in).asInstanceOf[R],
      kser.readClassAndObject(in).asInstanceOf[S])
}

private[spark] class Tuple20Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]
  extends Serializer[Tuple20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
    kser.writeClassAndObject(out, obj._15); out.flush()
    kser.writeClassAndObject(out, obj._16); out.flush()
    kser.writeClassAndObject(out, obj._17); out.flush()
    kser.writeClassAndObject(out, obj._18); out.flush()
    kser.writeClassAndObject(out, obj._19); out.flush()
    kser.writeClassAndObject(out, obj._20); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]]): Tuple20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T] =
    new Tuple20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N],
      kser.readClassAndObject(in).asInstanceOf[O],
      kser.readClassAndObject(in).asInstanceOf[P],
      kser.readClassAndObject(in).asInstanceOf[Q],
      kser.readClassAndObject(in).asInstanceOf[R],
      kser.readClassAndObject(in).asInstanceOf[S],
      kser.readClassAndObject(in).asInstanceOf[T])
}

private[spark] class Tuple21Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]
  extends Serializer[Tuple21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
    kser.writeClassAndObject(out, obj._15); out.flush()
    kser.writeClassAndObject(out, obj._16); out.flush()
    kser.writeClassAndObject(out, obj._17); out.flush()
    kser.writeClassAndObject(out, obj._18); out.flush()
    kser.writeClassAndObject(out, obj._19); out.flush()
    kser.writeClassAndObject(out, obj._20); out.flush()
    kser.writeClassAndObject(out, obj._21); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]]): Tuple21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U] =
    new Tuple21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N],
      kser.readClassAndObject(in).asInstanceOf[O],
      kser.readClassAndObject(in).asInstanceOf[P],
      kser.readClassAndObject(in).asInstanceOf[Q],
      kser.readClassAndObject(in).asInstanceOf[R],
      kser.readClassAndObject(in).asInstanceOf[S],
      kser.readClassAndObject(in).asInstanceOf[T],
      kser.readClassAndObject(in).asInstanceOf[U])
}

private[spark] class Tuple22Serializer[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]
  extends Serializer[Tuple22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, obj: Tuple22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]): Unit = {
    kser.writeClassAndObject(out, obj._1); out.flush()
    kser.writeClassAndObject(out, obj._2); out.flush()
    kser.writeClassAndObject(out, obj._3); out.flush()
    kser.writeClassAndObject(out, obj._4); out.flush()
    kser.writeClassAndObject(out, obj._5); out.flush()
    kser.writeClassAndObject(out, obj._6); out.flush()
    kser.writeClassAndObject(out, obj._7); out.flush()
    kser.writeClassAndObject(out, obj._8); out.flush()
    kser.writeClassAndObject(out, obj._9); out.flush()
    kser.writeClassAndObject(out, obj._10); out.flush()
    kser.writeClassAndObject(out, obj._11); out.flush()
    kser.writeClassAndObject(out, obj._12); out.flush()
    kser.writeClassAndObject(out, obj._13); out.flush()
    kser.writeClassAndObject(out, obj._14); out.flush()
    kser.writeClassAndObject(out, obj._15); out.flush()
    kser.writeClassAndObject(out, obj._16); out.flush()
    kser.writeClassAndObject(out, obj._17); out.flush()
    kser.writeClassAndObject(out, obj._18); out.flush()
    kser.writeClassAndObject(out, obj._19); out.flush()
    kser.writeClassAndObject(out, obj._20); out.flush()
    kser.writeClassAndObject(out, obj._21); out.flush()
    kser.writeClassAndObject(out, obj._22); out.flush()
  }
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]]): Tuple22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V] =
    new Tuple22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](
      kser.readClassAndObject(in).asInstanceOf[A],
      kser.readClassAndObject(in).asInstanceOf[B],
      kser.readClassAndObject(in).asInstanceOf[C],
      kser.readClassAndObject(in).asInstanceOf[D],
      kser.readClassAndObject(in).asInstanceOf[E],
      kser.readClassAndObject(in).asInstanceOf[F],
      kser.readClassAndObject(in).asInstanceOf[G],
      kser.readClassAndObject(in).asInstanceOf[H],
      kser.readClassAndObject(in).asInstanceOf[I],
      kser.readClassAndObject(in).asInstanceOf[J],
      kser.readClassAndObject(in).asInstanceOf[K],
      kser.readClassAndObject(in).asInstanceOf[L],
      kser.readClassAndObject(in).asInstanceOf[M],
      kser.readClassAndObject(in).asInstanceOf[N],
      kser.readClassAndObject(in).asInstanceOf[O],
      kser.readClassAndObject(in).asInstanceOf[P],
      kser.readClassAndObject(in).asInstanceOf[Q],
      kser.readClassAndObject(in).asInstanceOf[R],
      kser.readClassAndObject(in).asInstanceOf[S],
      kser.readClassAndObject(in).asInstanceOf[T],
      kser.readClassAndObject(in).asInstanceOf[U],
      kser.readClassAndObject(in).asInstanceOf[V])
}

// scalastyle:on line.size.limit

// --- Specialized Primitive Tuple Serializers ---

private[spark] class Tuple1LongSerializer extends Serializer[Tuple1[Long]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple1[Long]): Unit =
    out.writeLong(tup._1)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple1[Long]]): Tuple1[Long] =
    new Tuple1[Long](in.readLong)
}

private[spark] class Tuple1IntSerializer extends Serializer[Tuple1[Int]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple1[Int]): Unit =
    out.writeInt(tup._1)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple1[Int]]): Tuple1[Int] =
    new Tuple1[Int](in.readInt)
}

private[spark] class Tuple1DoubleSerializer extends Serializer[Tuple1[Double]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple1[Double]): Unit =
    out.writeDouble(tup._1)
  override def read(kser: Kryo, in: Input, cls: Class[_ <: Tuple1[Double]]): Tuple1[Double] =
    new Tuple1[Double](in.readDouble)
}

private[spark] class Tuple2LongLongSerializer
  extends Serializer[Tuple2[Long, Long]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Long, Long]): Unit = {
    out.writeLong(tup._1); out.writeLong(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Long, Long]]): Tuple2[Long, Long] =
    new Tuple2[Long, Long](in.readLong, in.readLong)
}

private[spark] class Tuple2LongIntSerializer
  extends Serializer[Tuple2[Long, Int]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Long, Int]): Unit = {
    out.writeLong(tup._1); out.writeInt(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Long, Int]]): Tuple2[Long, Int] =
    new Tuple2[Long, Int](in.readLong, in.readInt)
}

private[spark] class Tuple2LongDoubleSerializer
  extends Serializer[Tuple2[Long, Double]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Long, Double]): Unit = {
    out.writeLong(tup._1); out.writeDouble(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Long, Double]]): Tuple2[Long, Double] =
    new Tuple2[Long, Double](in.readLong, in.readDouble)
}

private[spark] class Tuple2IntLongSerializer
  extends Serializer[Tuple2[Int, Long]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Int, Long]): Unit = {
    out.writeInt(tup._1); out.writeLong(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Int, Long]]): Tuple2[Int, Long] =
    new Tuple2[Int, Long](in.readInt, in.readLong)
}

private[spark] class Tuple2IntIntSerializer
  extends Serializer[Tuple2[Int, Int]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Int, Int]): Unit = {
    out.writeInt(tup._1); out.writeInt(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Int, Int]]): Tuple2[Int, Int] =
    new Tuple2[Int, Int](in.readInt, in.readInt)
}

private[spark] class Tuple2IntDoubleSerializer
  extends Serializer[Tuple2[Int, Double]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Int, Double]): Unit = {
    out.writeInt(tup._1); out.writeDouble(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Int, Double]]): Tuple2[Int, Double] =
    new Tuple2[Int, Double](in.readInt, in.readDouble)
}

private[spark] class Tuple2DoubleLongSerializer
  extends Serializer[Tuple2[Double, Long]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Double, Long]): Unit = {
    out.writeDouble(tup._1); out.writeLong(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Double, Long]]): Tuple2[Double, Long] =
    new Tuple2[Double, Long](in.readDouble, in.readLong)
}

private[spark] class Tuple2DoubleIntSerializer
  extends Serializer[Tuple2[Double, Int]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Double, Int]): Unit = {
    out.writeDouble(tup._1); out.writeInt(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Double, Int]]): Tuple2[Double, Int] =
    new Tuple2[Double, Int](in.readDouble, in.readInt)
}

private[spark] class Tuple2DoubleDoubleSerializer
  extends Serializer[Tuple2[Double, Double]] with Serializable {
  setImmutable(true)
  override def write(kser: Kryo, out: Output, tup: Tuple2[Double, Double]): Unit = {
    out.writeDouble(tup._1); out.writeDouble(tup._2)
  }
  override def read(kser: Kryo, in: Input,
      cls: Class[_ <: Tuple2[Double, Double]]): Tuple2[Double, Double] =
    new Tuple2[Double, Double](in.readDouble, in.readDouble)
}

// --- Registration helper ---

/** Registers all tuple serializers with a Kryo instance. */
private[spark] object ScalaTupleSerialization {
  // scalastyle:off line.size.limit
  def register(newK: Kryo): Unit = {
    newK.register(classOf[Tuple1[Any]], new Tuple1Serializer[Any])
    newK.register(classOf[Tuple2[Any, Any]], new Tuple2Serializer[Any, Any])
    newK.register(classOf[Tuple3[Any, Any, Any]], new Tuple3Serializer[Any, Any, Any])
    newK.register(classOf[Tuple4[Any, Any, Any, Any]], new Tuple4Serializer[Any, Any, Any, Any])
    newK.register(classOf[Tuple5[Any, Any, Any, Any, Any]], new Tuple5Serializer[Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple6[Any, Any, Any, Any, Any, Any]], new Tuple6Serializer[Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple7[Any, Any, Any, Any, Any, Any, Any]], new Tuple7Serializer[Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple8[Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple8Serializer[Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple9[Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple9Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple10Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple11Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple12Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple13Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple14Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple15Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple16Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple17Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple18Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple19Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple20Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple21Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    newK.register(classOf[Tuple22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], new Tuple22Serializer[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any])
    // Specialized primitive tuples
    // scalastyle:off classforname
    newK.register(Class.forName("scala.Tuple1$mcJ$sp"), new Tuple1LongSerializer)
    newK.register(Class.forName("scala.Tuple1$mcI$sp"), new Tuple1IntSerializer)
    newK.register(Class.forName("scala.Tuple1$mcD$sp"), new Tuple1DoubleSerializer)
    newK.register(Class.forName("scala.Tuple2$mcJJ$sp"), new Tuple2LongLongSerializer)
    newK.register(Class.forName("scala.Tuple2$mcJI$sp"), new Tuple2LongIntSerializer)
    newK.register(Class.forName("scala.Tuple2$mcJD$sp"), new Tuple2LongDoubleSerializer)
    newK.register(Class.forName("scala.Tuple2$mcIJ$sp"), new Tuple2IntLongSerializer)
    newK.register(Class.forName("scala.Tuple2$mcII$sp"), new Tuple2IntIntSerializer)
    newK.register(Class.forName("scala.Tuple2$mcID$sp"), new Tuple2IntDoubleSerializer)
    newK.register(Class.forName("scala.Tuple2$mcDJ$sp"), new Tuple2DoubleLongSerializer)
    newK.register(Class.forName("scala.Tuple2$mcDI$sp"), new Tuple2DoubleIntSerializer)
    newK.register(Class.forName("scala.Tuple2$mcDD$sp"), new Tuple2DoubleDoubleSerializer)
    // scalastyle:on classforname
  }
  // scalastyle:on line.size.limit
}
