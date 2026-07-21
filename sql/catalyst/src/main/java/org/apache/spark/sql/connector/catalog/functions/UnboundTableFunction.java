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
 * A table-valued function that is not bound to argument types.
 * <p>
 * Binding is where the function decides its output schema and (for a TABLE-argument function) any
 * required partitioning/ordering of its input. Because the schema may depend on the actual
 * arguments (polymorphic output), it is exposed on the returned {@link BoundTableFunction}, not
 * here. This mirrors {@code UnboundProcedure#bind} and the Python UDTF {@code analyze} step.
 *
 * @since 4.3.0
 */
@Evolving
public interface UnboundTableFunction extends TableFunction {

  /**
   * Binds this table-valued function to argument types.
   * <p>
   * The provided {@code inputType} is derived from the call-site arguments, one struct field per
   * argument, in the caller's order. A by-name argument carries the
   * {@link TableFunctionParameter#BY_NAME_METADATA_KEY} field metadata; Spark performs the final
   * validation and rearrangement against the returned function's
   * {@link BoundTableFunction#parameters() parameters}.
   * <p>
   * If the catalog supports overloading, the implementation is expected to pick the best matching
   * version; otherwise it may validate the input types while binding or delegate final validation
   * to Spark.
   *
   * @param inputType the argument types to bind to
   * @return the bound table-valued function most suitable for the given argument types
   */
  BoundTableFunction bind(StructType inputType);
}
