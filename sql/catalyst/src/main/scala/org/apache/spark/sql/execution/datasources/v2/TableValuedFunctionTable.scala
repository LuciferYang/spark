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

package org.apache.spark.sql.execution.datasources.v2

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.connector.catalog.functions.SupportsScalarInvocation
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Adapts a scalar-argument table-valued function (a
 * [[org.apache.spark.sql.connector.catalog.functions.BoundTableFunction]] that implements
 * [[SupportsScalarInvocation]], bound to its evaluated scalar arguments) into a read-only V2
 * [[Table]]. This lets a table-valued function call flow through the ordinary DSv2 read path
 * ([[DataSourceV2Relation]] -> `V2ScanRelationPushDown` -> `BatchScanExec`), giving it distributed
 * execution and (when the returned scan opts in) column pruning / pushdown for free -- exactly
 * like reading a table -- rather than eagerly materializing rows on the driver.
 *
 * The scalar arguments are captured as an already-evaluated [[InternalRow]]; the connector's scan
 * is produced lazily by the scan builder at planning time.
 *
 * @param tableName the qualified function name, used only for display (EXPLAIN, error messages).
 *                  The wrapping [[DataSourceV2Relation]] carries no catalog/identifier, because a
 *                  table-valued function invocation is a synthetic relation with no catalog-managed
 *                  table identity or version -- it must not participate in table refresh or
 *                  cache-by-identity, and its owning catalog need not be a
 *                  [[org.apache.spark.sql.connector.catalog.TableCatalog]].
 */
class TableValuedFunctionTable(
    function: SupportsScalarInvocation,
    input: InternalRow,
    tableName: String) extends Table with SupportsRead {

  override def name(): String = tableName

  override def columns(): Array[Column] =
    CatalogV2Util.structTypeToV2Columns(function.resultSchema())

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    function.newScanBuilder(input)
}
