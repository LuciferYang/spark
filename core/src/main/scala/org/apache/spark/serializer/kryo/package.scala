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

import scala.language.implicitConversions

/**
 * Scala extensions for Kryo serialization, inlined from Twitter's chill library.
 * Provides type aliases, implicit enrichment for Kryo, and a registrar interface.
 *
 * Originally from com.twitter.chill (chill-scala module, Apache 2.0 licensed).
 */
package object kryo {
  type KryoType = com.esotericsoftware.kryo.kryo5.Kryo
  type KSerializer[T] = com.esotericsoftware.kryo.kryo5.Serializer[T]
  type KryoInput = com.esotericsoftware.kryo.kryo5.io.Input
  type KryoOutput = com.esotericsoftware.kryo.kryo5.io.Output

  implicit def toRichKryo(k: KryoType): RichKryo = new RichKryo(k)
}
