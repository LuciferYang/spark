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

package org.apache.spark.util

import java.net.URL

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule, JavaTypeable}

import org.apache.spark.network.util.JacksonMapper

object ScalaJacksonMapper extends JacksonMapper {

  def withDefaultScalaModule(): ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)

  def withClassTagExtensions(): ObjectMapper =
    (new ObjectMapper() with ClassTagExtensions).registerModule(DefaultScalaModule)

  object Default {
    private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

    def toJson(o: Any): String = JacksonMapper.toJson(mapper, o)

    def toJsonByteArray(o: Any): Array[Byte] =
      JacksonMapper.toJsonByteArray(mapper, o)

    def fromJson[T](jsonString: String, clazz: Class[T]): T =
      JacksonMapper.fromJson(mapper, jsonString, clazz)

    def fromJson[T](byteArray: Array[Byte], clazz: Class[T]): T =
      JacksonMapper.fromJson(mapper, byteArray, clazz)

    def fromJson[T](content: String, valueTypeRef: TypeReference[T]): T =
      JacksonMapper.fromJson(mapper, content, valueTypeRef)
  }

  object WithClassTagExtensions {

    private val mapper = new ObjectMapper() with ClassTagExtensions
    mapper.registerModule(DefaultScalaModule)

    def toJson(o: Any): String = JacksonMapper.toJson(mapper, o)

    def toJsonByteArray(o: Any): Array[Byte] =
      JacksonMapper.toJsonByteArray(mapper, o)

    def fromJson[T](jsonString: String, clazz: Class[T]): T =
      JacksonMapper.fromJson(mapper, jsonString, clazz)

    def fromJson[T](byteArray: Array[Byte], clazz: Class[T]): T =
      JacksonMapper.fromJson(mapper, byteArray, clazz)

    def fromJson[T](content: String, valueTypeRef: TypeReference[T]): T =
      JacksonMapper.fromJson(mapper, content, valueTypeRef)

    def fromURL[T: JavaTypeable](src: URL): T = mapper.readValue[T](src)
  }
}
