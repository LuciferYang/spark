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

import org.apache.spark.internal.Logging
import org.apache.spark.util.kvstore.GlobalKVStoreIteratorTracker

/**
 * `KVStoreIterator` audit for test suites.
 */
trait KVStoreIteratorAudit extends Logging {

  protected def doKVStoreIteratorPreAudit(): Unit = {
    GlobalKVStoreIteratorTracker.clear()
  }

  protected def doKVStoreIteratorPostAudit(): Unit = {
    val shortSuiteName = this.getClass.getName.replaceAll("org.apache.spark", "o.a.s")
    if (GlobalKVStoreIteratorTracker.nonEmpty()) {
      logWarning(s"\n\n===== ${GlobalKVStoreIteratorTracker.size()} " +
        s"KVStoreIterator LEAK IN SUITE $shortSuiteName =====\n")
      // scalastyle:off throwerror
      throw new AssertionError(s"${GlobalKVStoreIteratorTracker.size()} " +
        s"KVStoreIterator LEAK IN SUITE $shortSuiteName")
      // scalastyle:on throwerror
    }
  }
}
