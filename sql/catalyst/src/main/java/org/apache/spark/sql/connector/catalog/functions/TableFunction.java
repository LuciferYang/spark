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

/**
 * A base interface for all table-valued functions.
 * <p>
 * A table-valued function is loaded from a
 * {@link org.apache.spark.sql.connector.catalog.TableFunctionCatalog} and, once bound to its
 * argument types, produces a relation. This base interface carries only identity metadata; the
 * {@link UnboundTableFunction} / {@link BoundTableFunction} pair carries binding, and the
 * {@link SupportsScalarInvocation} mixin (and a future TABLE-argument mixin) on
 * {@link BoundTableFunction} carries the execution contract. This mirrors the scalar
 * function ({@code UnboundFunction} / {@code BoundFunction}) and procedure
 * ({@code UnboundProcedure} / {@code BoundProcedure}) families.
 *
 * @since 4.3.0
 */
@Evolving
public interface TableFunction {

  /**
   * Returns the name of this table-valued function.
   */
  String name();

  /**
   * Returns the description of this table-valued function.
   */
  default String description() {
    return getClass().toString();
  }
}
