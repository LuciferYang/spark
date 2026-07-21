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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableValuedFunctionRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.functions.SupportsScalarInvocation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Lowers a resolved scalar-argument V2 table-valued function
 * ([[TableValuedFunctionRelation]]) into an ordinary DSv2 read relation
 * ([[DataSourceV2Relation]]) by evaluating its scalar arguments to a constant row and wrapping
 * the bound function as a read-only V2 table ([[TableValuedFunctionTable]]). The function's rows
 * are then produced through the normal scan path (`V2ScanRelationPushDown` -> `BatchScanExec`)
 * with distributed execution and column pruning / pushdown, rather than being materialized on the
 * driver.
 *
 * This is an OPTIMIZER rule -- the counterpart of the analyzer's
 * [[org.apache.spark.sql.catalyst.analysis.InvokeTableFunctions]] (which only validates). It is
 * scheduled AFTER the optimizer's `FinishAnalysis` batch so that clock-/calendar-dependent
 * foldable arguments (`current_date()`, `CAST(<time> AS TIMESTAMP)`, `CAST('today' AS DATE)`,
 * ...) have already been pinned to query-stable literals by `ComputeCurrentTime` /
 * `SpecialDatetimeValues`, and folded by constant folding. Evaluating them here therefore yields
 * exactly the same values the rest of the query sees. This mirrors the `ResolveInlineTables`
 * (analyze) / `EvalInlineTables` (optimize) split.
 *
 * The produced relation deliberately carries NO catalog/identifier: a table-valued function
 * invocation is a synthetic relation, not a catalog-managed table, so it must not participate in
 * table-metadata refresh or cache-by-identity (`ExtractV2CatalogAndIdentifier` ->
 * `asTableCatalog`), and a `TableFunctionCatalog` is not required to also be a `TableCatalog`. The
 * qualified name is carried on the table itself for display.
 */
object EvalTableValuedFunctions extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case r: TableValuedFunctionRelation if r.arguments.forall(_.resolved) =>
      r.function match {
        case scalarFn: SupportsScalarInvocation =>
          val input = toInternalRow(r.arguments)
          val fnName = (r.catalog.name +: r.ident.namespace :+ r.ident.name).mkString(".")
          val table = new TableValuedFunctionTable(scalarFn, input, fnName)
          DataSourceV2Relation(
            table,
            r.output,
            None,
            None,
            CaseInsensitiveStringMap.empty)
        // A bound function with no supported invocation mixin is already rejected at analysis time
        // by InvokeTableFunctions, so any TableValuedFunctionRelation reaching the optimizer is a
        // SupportsScalarInvocation; leave anything else untouched rather than failing here.
        case _ => r
      }
  }

  private def toInternalRow(args: Seq[Expression]): InternalRow = {
    // InvokeTableFunctions has already validated foldability at analysis time; re-assert here to
    // guard against a future refactor, mirroring InvokeProcedures.toInternalRow. By this point the
    // FinishAnalysis batch has pinned any clock-/calendar-dependent arguments, so eval() is stable.
    require(args.forall(_.foldable),
      "Table-valued function arguments must be foldable before evaluation")
    val values = args.map(_.eval()).toArray
    new GenericInternalRow(values)
  }
}
