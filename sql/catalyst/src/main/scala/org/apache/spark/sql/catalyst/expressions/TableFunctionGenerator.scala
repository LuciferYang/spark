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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.connector.catalog.functions.SupportsTableArgument
import org.apache.spark.sql.types.StructType

/**
 * A marker for [[Generator]]s that consume a TABLE argument and natively support that argument's
 * PARTITION BY / WITH SINGLE PARTITION / ORDER BY clauses. The analyzer's TABLE-argument expansion
 * (`ResolveFunctions`) accepts a generator carrying these clauses only if it is one of the Python
 * UDTF generators or is marked with this trait; otherwise it asserts that the repartitioning
 * clauses are unsupported. Keeping this a trait (rather than matching a concrete class) lets later
 * table-argument generators -- e.g. a columnar variant -- slot into the same allowlist.
 */
trait TableArgumentGenerator extends Generator

/**
 * A resolved call to a V2 catalog table-valued function that consumes a TABLE argument, in the
 * logical form Spark uses for TABLE-argument functions: an [[UnevaluableGenerator]] whose child is
 * the TABLE argument. It mirrors [[PythonUDTF]] -- both are matched by the planner and rewritten
 * into a dedicated physical operator (see `TableFunctionExec`); this one carries a
 * [[SupportsTableArgument]] instead of pickled Python state.
 *
 * The generator holds the connector's bound function (used on the driver to obtain the serializable
 * evaluator factory and the required distribution/ordering), the fixed output schema, and -- once
 * the analyzer has expanded the TABLE argument -- the ordinals, within the single struct input
 * column, of the PARTITION BY expressions. The physical operator segments adjacent input rows on
 * those ordinals to reconstruct each PARTITION BY group.
 *
 * @param function the bound V2 table-valued function
 * @param children the function's input; a single [[FunctionTableSubqueryArgumentExpression]] before
 *                 the analyzer expands the TABLE argument, then the struct input column afterwards
 * @param elementSchema the function's output schema, fixed at construction so that rules copying
 *                      the generator do not invalidate references to its output attributes
 * @param udfDeterministic whether the connector's bound function is deterministic; propagated from
 *                         `BoundTableFunction.isDeterministic()` so the optimizer does not reorder,
 *                         de-duplicate, or cache a non-deterministic transform
 * @param inputColumnCount the number of leading fields of the struct input column that are the
 *                         TABLE argument's own columns. The analyzer's TABLE-argument expansion may
 *                         append internal `partition_by_N` marker columns after them (to carry the
 *                         PARTITION BY values for group segmentation); the executor slices each row
 *                         to these leading columns before passing it to the connector's evaluator,
 *                         so the evaluator sees exactly the TABLE argument's schema.
 * @param partitionColumnIndexes the zero-based ordinals, within the struct input column, of the
 *                               PARTITION BY expressions, populated during TABLE-argument
 *                               expansion; empty when the call site specifies no PARTITION BY.
 *                               These may point at the appended marker columns.
 */
case class TableFunctionGenerator(
    function: SupportsTableArgument,
    children: Seq[Expression],
    elementSchema: StructType,
    udfDeterministic: Boolean,
    inputColumnCount: Int,
    partitionColumnIndexes: Seq[Int] = Nil)
  extends UnevaluableGenerator with TableArgumentGenerator {

  override def prettyName: String = function.name()

  // Mirror PythonFuncExpression: the transform is deterministic only if the connector declares it
  // AND its inputs are deterministic, so the optimizer will not reorder/reuse a non-deterministic
  // table function.
  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): TableFunctionGenerator =
    copy(children = newChildren)
}
