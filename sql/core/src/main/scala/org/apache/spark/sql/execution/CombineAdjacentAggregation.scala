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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule combines adjacent aggregation with `Partial` and `Final` to `Complete` mode.
 * Example for hash aggregate:
 *    HashAggregate (Final)         HashAggregate (Complete)
 *          |                             |
 *    HashAggregate (Partial)    =>    Exchange
 *          |
 *       Exchange
 *
 * Example for sort aggregate:
 *    SortAggregateExec (Final)       SortAggregateExec (Complete)
 *          |                               |
 *    SortAggregateExec (Partial)    =>    Sort
 *          |                               |
 *         Sort                          Exchange
 *          |
 *       Exchange
 *
 * It supports [[HashAggregateExec]], [[SortAggregateExec]] and [[ObjectHashAggregateExec]].
 */
object CombineAdjacentAggregation extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED)) {
      return plan
    }

    plan.transformDown {
      case finalAgg @ HashAggregateExec(_, _, _, _, _, _, _, _, partialAgg: HashAggregateExec)
        if ReplaceHashWithSortAgg.isPartialAgg(partialAgg, finalAgg) =>
        finalAgg.copy(
          aggregateExpressions = combineAggExpressions(
            finalAgg.aggregateExpressions, partialAgg.aggregateExpressions),
          child = partialAgg.child)

      case finalAgg @ SortAggregateExec(_, _, _, _, _, _, _, _, partialAgg: SortAggregateExec)
        if ReplaceHashWithSortAgg.isPartialAgg(partialAgg, finalAgg) =>
        finalAgg.copy(
          aggregateExpressions = combineAggExpressions(
            finalAgg.aggregateExpressions, partialAgg.aggregateExpressions),
          child = partialAgg.child)

      case finalAgg @ ObjectHashAggregateExec(_, _, _, _, _, _, _, _,
      partialAgg: ObjectHashAggregateExec)
        if ReplaceHashWithSortAgg.isPartialAgg(partialAgg, finalAgg) =>
        finalAgg.copy(
          aggregateExpressions = combineAggExpressions(
            finalAgg.aggregateExpressions, partialAgg.aggregateExpressions),
          child = partialAgg.child)
    }
  }

  /**
   * Combine aggregate expressions from the final and partial aggregates into Complete mode.
   * Restores any filter conditions from the partial aggregate that were stripped by
   * `mayRemoveAggFilters` when setting the Final mode.
   */
  private def combineAggExpressions(
      finalAggExprs: Seq[AggregateExpression],
      partialAggExprs: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    finalAggExprs.zip(partialAggExprs).map { case (finalExpr, partialExpr) =>
      finalExpr.copy(mode = Complete, filter = partialExpr.filter)
    }
  }
}
