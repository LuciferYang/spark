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

/** Helpers for tests of the Scala 2 reflection that `ScalaReflection` uses. */
object ScalaReflectionTestUtils {

  /** The size of the undo log that `ScalaReflection.cleanUpReflectionObjects` cleans up. */
  def undoLogSize: Int = {
    scala.reflect.runtime.universe
      .asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.log.size
  }
}

/** Tests of `ScalaReflection` that use Scala 2 reflection types directly. */
trait ScalaReflectionVersionSpecificTests { self: SparkFunSuite =>
  import org.apache.spark.sql.catalyst.ScalaReflection._

  test("isSubtype") {
    assert(isSubtype(localTypeOf[Option[Int]], localTypeOf[Option[_]]))
    assert(isSubtype(localTypeOf[Option[Int]], localTypeOf[Option[Int]]))
    assert(!isSubtype(localTypeOf[Option[_]], localTypeOf[Option[Int]]))
  }
}
