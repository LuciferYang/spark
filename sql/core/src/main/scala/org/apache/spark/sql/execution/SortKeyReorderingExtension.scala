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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}

/**
 * Registers [[SortKeyReordering]] as a `queryStagePrepRule`. Loaded via
 * `ServiceLoader` from `META-INF/services/org.apache.spark.sql.SparkSessionExtensionsProvider`.
 *
 * The rule itself is gated by `SORT_KEY_REORDERING_ENABLED` (default false),
 * so registering the provider does not change behaviour on its own.
 */
class SortKeyReorderingExtension extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectQueryStagePrepRule(session => SortKeyReordering(session))
  }
}
