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

package org.apache.spark.sql.catalyst

/**
 * The type tags that Spark SQL APIs such as `Encoders.product` and `functions.udf` take.
 *
 * In Scala 2.13 this is `scala.reflect.runtime.universe.TypeTag`, which the compiler
 * materializes. The Scala 3 build defines its own `TypeTag` under the same name.
 */
object TypeTags {
  type TypeTag[T] = scala.reflect.runtime.universe.TypeTag[T]

  /** Returns the type tag of `T`, as `scala.reflect.runtime.universe.typeTag` does. */
  def typeTag[T](implicit ttag: TypeTag[T]): TypeTag[T] = ttag
}
