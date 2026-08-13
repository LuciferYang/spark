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

package org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;

/**
 * A table-valued function that is bound to argument types.
 * <p>
 * A bound function declares its parameters (for argument validation and by-name rearrangement),
 * its determinism, and its (possibly polymorphic) output schema. It carries NO execution method
 * itself; a concrete function additionally implements exactly one of the invocation mixins:
 * <ul>
 *   <li>{@link SupportsScalarInvocation} -- a scalar-argument function that produces a relation
 *       through a {@code ScanBuilder} (a source; reuses the DSv2 read path, column pruning,
 *       distributed execution);</li>
 *   <li>{@link SupportsTableArgument} -- a function that consumes a TABLE argument and transforms
 *       its rows.</li>
 * </ul>
 * Splitting execution into mixins keeps this interface stable as capabilities are added, and lets
 * a connector implement only the invocation styles it supports.
 *
 * @since 4.3.0
 */
@Evolving
public interface BoundTableFunction extends TableFunction {

  /**
   * Returns the parameters of this table-valued function, used by Spark to validate and rearrange
   * call-site arguments (including by-name arguments). An empty array means the function takes no
   * arguments.
   */
  TableFunctionParameter[] parameters();

  /**
   * Indicates whether this table-valued function is deterministic.
   * <p>
   * A function is deterministic if, given the same arguments (and, for a TABLE-argument function,
   * the same input rows in the same order), it always produces the same rows. Note that a
   * scalar-argument function reached through {@link SupportsScalarInvocation} produces its rows via
   * a {@code Scan}, whose own contract governs re-execution; determinism is consumed by the
   * TABLE-argument path (added in a later phase), where Spark uses it to decide whether the
   * transform may be reordered or reused.
   */
  boolean isDeterministic();

  /**
   * Returns the output schema (the relation's columns) produced by this bound function. May depend
   * on the bound arguments (polymorphic output).
   */
  StructType resultSchema();
}
