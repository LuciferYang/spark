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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.functions.UnboundTableFunction;

/**
 * A catalog API for working with table-valued functions (functions that appear in the FROM clause
 * and produce a relation).
 * <p>
 * This is a marker capability, parallel to {@link FunctionCatalog} (scalar/aggregate functions)
 * and {@link ProcedureCatalog} (stored procedures). A catalog that does not implement this
 * interface reports the {@code MISSING_CATALOG_ABILITY.TABLE_VALUED_FUNCTIONS} error when a
 * table-valued function is referenced against it.
 *
 * @since 4.3.0
 */
@Evolving
public interface TableFunctionCatalog extends CatalogPlugin {

  /**
   * List the table-valued functions in a namespace from the catalog.
   *
   * @param namespace a multi-part namespace
   * @return an array of table-valued function identifiers in the namespace
   * @throws NoSuchNamespaceException if the namespace does not exist (optional)
   */
  Identifier[] listTableFunctions(String[] namespace) throws NoSuchNamespaceException;

  /**
   * Load a table-valued function by identifier from the catalog.
   *
   * @param ident a table-valued function identifier
   * @return an unbound table-valued function
   * @throws NoSuchFunctionException if there is no such function
   */
  UnboundTableFunction loadTableFunction(Identifier ident) throws NoSuchFunctionException;

  /**
   * Returns true if the table-valued function exists, false otherwise.
   */
  default boolean tableFunctionExists(Identifier ident) {
    try {
      loadTableFunction(ident);
      return true;
    } catch (NoSuchFunctionException e) {
      return false;
    }
  }
}
