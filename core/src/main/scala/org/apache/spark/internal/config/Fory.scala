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

package org.apache.spark.internal.config

import org.apache.spark.network.util.ByteUnit

private[spark] object Fory {

  val FORY_REGISTRATION_REQUIRED = ConfigBuilder("spark.fory.registrationRequired")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(false)

  val FORY_USER_REGISTRATORS = ConfigBuilder("spark.fory.registrator")
    .version("4.2.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val FORY_CLASSES_TO_REGISTER = ConfigBuilder("spark.fory.classesToRegister")
    .version("4.2.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val FORY_USE_UNSAFE = ConfigBuilder("spark.fory.unsafe")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(true)

  val FORY_USE_POOL = ConfigBuilder("spark.fory.pool")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(true)

  val FORY_REFERENCE_TRACKING = ConfigBuilder("spark.fory.referenceTracking")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(true)

  val FORY_SERIALIZER_BUFFER_SIZE = ConfigBuilder("spark.foryserializer.buffer")
    .version("4.2.0")
    .bytesConf(ByteUnit.KiB)
    .createWithDefaultString("64k")

  val FORY_SERIALIZER_MAX_BUFFER_SIZE = ConfigBuilder("spark.foryserializer.buffer.max")
    .version("4.2.0")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("64m")

  val FORY_COMPATIBLE = ConfigBuilder("spark.fory.compatible")
    .version("4.2.0")
    .stringConf
    .createWithDefault("COMPATIBLE")
}