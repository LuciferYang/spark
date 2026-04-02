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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.internal.SQLConf

/**
 * Reduces data amplification from high-ratio Expand operators
 * produced by [[RewriteDistinctAggregates]].
 *
 * [[RewriteDistinctAggregates]] rewrites queries with multiple
 * distinct aggregates into:
 * {{{
 *   Aggregate(outer) -> Aggregate(inner) -> Expand(Nx) -> child
 * }}}
 * where the Expand duplicates each row N times. This rule inserts
 * a de-duplication step (grouping-only Aggregate) before the Expand:
 * {{{
 *   ... -> Expand(Nx) -> Aggregate(groupBy: keys + all distinct cols) -> child
 * }}}
 *
 * Applied only when:
 *  - Expand projection count >= configured threshold
 *  - No non-distinct aggregates or FILTER clauses on distinct
 *    aggregates (checked via `expand.producedAttributes` being
 *    a subset of the inner aggregate's GROUP BY)
 *  - Distinct expressions do not inflate the pre-aggregate's
 *    group-by beyond the inner aggregate's. When a composite
 *    expression like `col1 + col2` introduces new leaf attributes
 *    not already referenced by other distinct columns, the
 *    pre-aggregate's Cartesian product grows and dedup becomes
 *    ineffective. However, composites that share leaf attributes
 *    with other distinct columns (e.g. `col1 + col2` alongside
 *    `col1 - col2`) are allowed.
 *  - The Expand child is not already an Aggregate (idempotency)
 *
 * Controlled by `spark.sql.optimizer.optimizeExpandRatio`
 * (default -1 = disabled). Excludable via
 * `spark.sql.optimizer.excludedRules`.
 */
object OptimizeExpand extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    val threshold = conf.getConf(SQLConf.OPTIMIZE_EXPAND_RATIO)
    if (threshold == -1) return plan

    plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case outerAgg @ Aggregate(_, _, innerAgg @ Aggregate(_, _, expand @ Expand(
        projections, _, expandChild), _), _)
        if projections.size >= threshold &&
           !expandChild.isInstanceOf[Aggregate] && // idempotency guard
           canOptimize(innerAgg, expand) =>

        val preAggGroupBy = collectPreAggGroupBy(expand)

        val preAggregate = Aggregate(
          preAggGroupBy,
          preAggGroupBy.map(_.toAttribute),
          expandChild)

        val newExpand = expand.copy(child = preAggregate)
        val newInnerAgg = innerAgg.copy(child = newExpand)
        outerAgg.copy(child = newInnerAgg)
    }
  }

  /**
   * Returns true if the Expand is safe and beneficial to optimize.
   *
   * Safety: the Expand must be the output of
   * [[RewriteDistinctAggregates]] (has gid in GROUP BY), and all
   * Expand-produced attributes must be in the inner GROUP BY.
   * The subset check rejects non-distinct aggregates and FILTER
   * clauses, which produce extra Expand output columns not
   * consumed by the GROUP BY.
   *
   * Performance: the pre-aggregate's group-by column count must
   * not exceed the inner aggregate's (minus gid). When distinct
   * expressions are composite (e.g. `col1 + col2`), the
   * pre-aggregate groups by leaf attributes instead of the
   * expression, inflating the Cartesian product and making dedup
   * ineffective or harmful.
   */
  private def canOptimize(
      innerAgg: Aggregate,
      expand: Expand): Boolean = {
    val hasGid = innerAgg.groupingExpressions.exists {
      case a: Attribute =>
        a.name == "gid" && expand.producedAttributes.contains(a)
      case _ => false
    }
    if (!hasGid) return false

    val innerGroupByAttrs = AttributeSet(
      innerAgg.groupingExpressions.flatMap(_.references))
    if (!expand.producedAttributes.subsetOf(innerGroupByAttrs)) return false

    // Reject composite distinct expressions (e.g., col1 + col2).
    // The pre-aggregate groups by leaf attributes from the child that
    // the Expand references. For simple column refs, this matches the
    // inner aggregate's group-by count (minus gid). For composite
    // expressions, the leaf attribute count exceeds it because each
    // expression fans out into multiple attributes, inflating the
    // Cartesian product and making dedup ineffective.
    val preAggSize = expand.child.output.count(expand.references.contains)
    val innerGroupBySize = innerAgg.groupingExpressions.size - 1 // minus gid
    preAggSize <= innerGroupBySize
  }

  /**
   * Collect the pre-aggregation GROUP BY: all attributes from
   * the Expand's child that the Expand references, preserving
   * the child's output order for plan determinism.
   */
  private def collectPreAggGroupBy(
      expand: Expand): Seq[Attribute] = {
    expand.child.output.filter(expand.references.contains)
  }
}
