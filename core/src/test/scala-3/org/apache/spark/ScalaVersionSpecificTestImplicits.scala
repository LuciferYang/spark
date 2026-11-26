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

package org.apache.spark

import scala.reflect.ClassTag

/**
 * Implicits that the Scala 2 compiler provides on its own and the Scala 3 compiler does not, so
 * that test code shared by both versions compiles the same way. Mixed into [[SparkFunSuite]].
 *
 * Scala 2 materializes a ClassTag for `Nothing` and `Null`. Tests rely on that when a type
 * parameter with a ClassTag context bound is not inferred, such as the combiner type in
 * `new ShuffleDependency(rdd, partitioner)`, or when they build `(key, null)` pairs.
 */
private[spark] trait ScalaVersionSpecificTestImplicits extends LowPriorityTestImplicits {
  // Preferred over `nullClassTag` when a type parameter is left uninstantiated, as Scala 2 does.
  implicit def nothingClassTag: ClassTag[Nothing] = ClassTag.Nothing
}

private[spark] trait LowPriorityTestImplicits {
  implicit def nullClassTag: ClassTag[Null] = ClassTag.Null
}
