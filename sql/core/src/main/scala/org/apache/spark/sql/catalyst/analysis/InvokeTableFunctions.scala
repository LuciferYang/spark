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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.functions.SupportsScalarInvocation
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Validates a resolved scalar-argument V2 table-valued function
 * ([[TableValuedFunctionRelation]]) at analysis time, without lowering it. The lowering to a
 * DSv2 read relation -- evaluating the scalar arguments and wrapping the bound function as a
 * read-only V2 table -- is deferred to the optimizer rule
 * [[org.apache.spark.sql.execution.datasources.v2.EvalTableValuedFunctions]].
 *
 * Why defer the evaluation: the arguments are evaluated with `eval()` to a constant row, but
 * some foldable expressions are not truly constant until the optimizer's `FinishAnalysis` batch
 * pins them -- `current_date()`/`current_timestamp()` ([[ComputeCurrentTime]]), a
 * `CAST(<time> AS TIMESTAMP)` whose date fields come from `CURRENT_DATE`, and a
 * `CAST(<string> AS DATE/TIMESTAMP)` with a special value like `'today'`
 * ([[org.apache.spark.sql.catalyst.optimizer.SpecialDatetimeValues]]). Evaluating those here,
 * before `FinishAnalysis`, would bake a value inconsistent with the rest of the query (or, for a
 * special-date string, silently yield NULL). Keeping the node in the plan until after
 * `FinishAnalysis` lets those arguments be pinned exactly as everywhere else, then evaluated
 * consistently. This mirrors the [[ResolveInlineTables]] (analyze) / `EvalInlineTables`
 * (optimize) split.
 *
 * This rule keeps only the analysis-time checks that cannot be deferred: that the arguments are
 * foldable (a non-foldable argument, e.g. a correlated column, can never be pinned to a constant,
 * so it is a not-yet-supported shape and must fail at analysis), and that the bound function
 * implements a supported invocation mixin.
 */
class InvokeTableFunctions(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case r: TableValuedFunctionRelation if r.arguments.forall(_.resolved) =>
      r.function match {
        case _: SupportsScalarInvocation =>
          if (!r.arguments.forall(_.foldable)) {
            // PR1 accepts only constant/foldable scalar arguments. A non-foldable argument
            // (e.g. a correlated column) can never be pinned to a constant, so it is a
            // not-yet-supported shape. Clock-/calendar-dependent foldable arguments
            // (current_date(), CAST(<time> AS TIMESTAMP), CAST('today' AS DATE), ...) are NOT
            // rejected here -- they are deferred to EvalTableValuedFunctions, which runs after
            // the optimizer's FinishAnalysis batch has pinned them consistently with the rest of
            // the query.
            throw QueryCompilationErrors.tableValuedFunctionRequiresFoldableArgsError(
              (r.catalog.name +: r.ident.namespace :+ r.ident.name).mkString("."))
          }
          // Leave the node in place; EvalTableValuedFunctions lowers it in the optimizer.
          r
        case _ =>
          // The bound function implements no supported invocation mixin. A TABLE-argument call is
          // already rejected earlier in FunctionResolution.resolveV2TableFunction, so reaching
          // here means the connector returned a BoundTableFunction that declares no invocation
          // contract at all -- a connector implementation bug, not a user error. Use the resolved
          // identifier (not other.name(), which a misbehaving connector could return null) for the
          // message.
          val fnName = (r.catalog.name +: r.ident.namespace :+ r.ident.name).mkString(".")
          throw SparkException.internalError(
            s"Table-valued function $fnName is bound but implements no supported " +
              s"invocation interface (expected ${classOf[SupportsScalarInvocation].getName}).")
      }
  }
}
