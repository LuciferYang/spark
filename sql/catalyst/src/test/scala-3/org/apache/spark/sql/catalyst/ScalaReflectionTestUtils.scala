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

import org.apache.spark.SparkFunSuite

/** Helpers for tests of `ScalaReflection`. */
object ScalaReflectionTestUtils {

  /** Always 0: the Scala 3 build does not use Scala 2 reflection, which keeps an undo log. */
  def undoLogSize: Int = 0
}

/** Tests of `ScalaReflection` that use Scala 2 reflection types directly; none in Scala 3. */
trait ScalaReflectionVersionSpecificTests { self: SparkFunSuite => }
