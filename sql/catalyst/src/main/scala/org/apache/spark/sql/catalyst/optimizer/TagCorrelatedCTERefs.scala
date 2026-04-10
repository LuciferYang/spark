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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationRef, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE

/**
 * Tags `CTERelationDef` nodes whose references appear inside a correlated
 * subquery expression (a `SubqueryExpression` whose `outerAttrs` is non-empty).
 *
 * This rule must run BEFORE `RewriteCorrelatedScalarSubquery` (which is in the
 * "Operator Optimization" fixed-point batch). After that batch, correlated
 * scalar subqueries have been decorrelated into joins, and the
 * `SubqueryExpression` nodes that carry the `outerAttrs` signal are gone -
 * the structural evidence that "this CTE was originally referenced from a
 * correlated subquery" is no longer recoverable from the plan.
 *
 * The motivation is the q1/q31/q39a regression risk documented in the
 * cte-spill-priority design doc: caching a CTE that is referenced from a
 * correlated subquery blocks the per-key specialisation that the
 * decorrelated join would otherwise enjoy via runtime filters and predicate
 * pushdown into the inner aggregate. The structural pattern at
 * `ReplaceCTERefWithCache` time is indistinguishable from a user-written
 * top-level join (e.g. q24a's `cs1 join cs2` self-join), so the signal must
 * be captured upstream and stored on the CTE definition.
 *
 * Predicate subqueries (`EXISTS` / `IN` from q23a-style queries) are
 * handled by `RewritePredicateSubquery` which converts them into
 * `LeftSemi`/`LeftAnti` joins. They are NOT correlated in the sense this
 * rule cares about - their inner CTERelationRef is not blocked by caching.
 * The rule therefore filters on `outerAttrs.nonEmpty` to avoid tagging
 * those (which would falsely skip caching for q23a-shape queries).
 */
object TagCorrelatedCTERefs extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(CTE)) {
      return plan
    }

    // First pass: collect the set of CTE ids that have any reference inside
    // a correlated SubqueryExpression. We walk the entire plan including all
    // nested subquery expressions because a CTE can be defined at the top
    // level and referenced from a deeply nested subquery.
    val correlatedCteIds = mutable.HashSet.empty[Long]
    collectCorrelatedRefs(plan, correlatedCteIds)
    if (correlatedCteIds.isEmpty) {
      return plan
    }

    // Second pass: rewrite each WithCTE node, copying any matching
    // CTERelationDef with `correlatedSubqueryRef = true`. Idempotent:
    // re-running on an already-tagged plan is a no-op.
    plan.transformWithSubqueries {
      case w @ WithCTE(_, cteDefs) =>
        val newDefs = cteDefs.map { cteDef =>
          if (correlatedCteIds.contains(cteDef.id) && !cteDef.correlatedSubqueryRef) {
            cteDef.copy(correlatedSubqueryRef = true)
          } else {
            cteDef
          }
        }
        if (newDefs.zip(cteDefs).forall { case (a, b) => a eq b }) w
        else w.copy(cteDefs = newDefs)
    }
  }

  /**
   * Walks `plan` plus everything reachable through `SubqueryExpression`s.
   * For each `SubqueryExpression` with non-empty `outerAttrs`, records the
   * ids of all `CTERelationRef`s found anywhere inside its plan.
   */
  private def collectCorrelatedRefs(
      plan: LogicalPlan,
      sink: mutable.HashSet[Long]): Unit = {
    plan.foreach { node =>
      node.expressions.foreach { expr =>
        expr.foreach {
          case s: SubqueryExpression =>
            if (s.isCorrelated) {
              s.plan.foreach {
                case ref: CTERelationRef => sink += ref.cteId
                case _ =>
              }
            }
            // Recurse: a non-correlated subquery may itself contain a
            // correlated sub-subquery referencing the CTE.
            collectCorrelatedRefs(s.plan, sink)
          case _ =>
        }
      }
    }
  }
}
