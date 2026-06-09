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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, DynamicPruningSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, DYNAMIC_PRUNING_SUBQUERY, PLAN_EXPRESSION}

/**
 * Replaces CTE references that have not been previously inlined with [[Repartition]] operations
 * which will then be planned as shuffles and reused across different reference points.
 *
 * Note that this rule should be called at the very end of the optimization phase to best guarantee
 * that CTE repartition shuffles are reused.
 */
object ReplaceCTERefWithRepartition extends Rule[LogicalPlan] {

  /**
   * Set by `ReplaceCTERefWithCache` on a def it declined to cache only because the body is
   * larger than `spark.sql.auto.cte.cache.maxBodySizeBytes`. For such a body the extra
   * round-robin shuffle this rule adds is not a saving: writing and re-reading it costs more
   * than recomputing the body at each reference, which is what the size cap decided the body
   * is too big to be worth paying once.
   */
  val SKIP_EXTRA_REPARTITION = TreeNodeTag[Unit]("skipExtraRepartitionForOversizedCTE")

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _: Subquery => plan
    case _ =>
      replaceWithRepartition(plan, mutable.HashMap.empty[Long, LogicalPlan])
  }

  private def canSkipExtraRepartition(p: LogicalPlan): Boolean = p match {
    case _: RepartitionOperation => true
    case _: RebalancePartitions => true
    case _ => false
  }

  /**
   * Drop `DynamicPruningSubquery` from a CTE body that is about to be copied or re-analyzed.
   *
   * Two callers, two independent reasons, same answer.
   *
   * `ReplaceCTERefWithCache` calls it because `CacheManager.cacheQuery` re-runs
   * `sessionState.executePlan` on the body alone, and a `DynamicPruningSubquery` whose
   * `buildQuery` lives in the OUTER query cannot satisfy `resolved` out of that context, so
   * `CheckAnalysis` fails the whole query with `INTERNAL_ERROR: Found the unresolved operator`.
   * Refusing to cache is not an alternative there: `InlineCTE` has already declined to inline the
   * def, and it could not have decided otherwise because `PartitionPruning` runs after
   * `Batch("Inline CTE")`.
   *
   * This rule calls it because `DeduplicateRelations` copies the body:
   *
   * `DeduplicateRelations` renumbers the attributes of the copy it produces, including the ones
   * `DynamicPruningSubquery.buildQuery` outputs, but it does not rewrite `buildKeys` -- and
   * `DynamicPruningSubquery.resolved` requires
   * `buildKeys.forall(_.references.subsetOf(buildQuery.outputSet))`. The copy is therefore
   * unresolved and the batch fails with `PLAN_VALIDATION_FAILED_RULE_IN_BATCH`. Measured on
   * TPC-DS q14a: `buildKeys refs = d_date_sk#74` against `buildQuery out = d_date_sk#3871`.
   *
   * The crash needs a def to reach this rule with at least one reference whose `outputSet` does
   * not match the def's. With stock `InlineCTE` that takes a non-deterministic multi-reference
   * body, so it was rare; auto-CTE caching reaches it whenever `ReplaceCTERefWithCache` declines
   * a def that `InlineCTE` kept -- for example `spark.sql.auto.cte.cache.storageLevel=NONE`, or
   * a body over `spark.sql.auto.cte.cache.maxBodySizeBytes`.
   *
   * Replacing only `DynamicPruningSubquery` is safe: it is a pure partition-pruning hint whose
   * `buildQuery` lives in the outer query, so dropping it widens the scan the copy performs
   * without changing which rows the body produces. A bare `DynamicPruningExpression` must NOT be
   * touched -- it can wrap a real user conjunct, and dropping that would change results.
   */
  private[sql] def stripDynamicPruning(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) {
      plan
    } else {
      // `transformAllExpressionsWithSubqueries`, not the plain variant: the guard above sees
      // into subquery plans (`PlanExpression.treePatternBits` unions its inner plan's bits) and
      // a DPP nested inside a scalar subquery must be stripped too, or it survives and keeps
      // the copy unresolved.
      plan.transformAllExpressionsWithSubqueries {
        case _: DynamicPruningSubquery => TrueLiteral
      }
    }
  }

  private def replaceWithRepartition(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, LogicalPlan]): LogicalPlan = plan match {
    case WithCTE(child, cteDefs) =>
      cteDefs.foreach { cteDef =>
        val inlined = replaceWithRepartition(cteDef.child, cteMap)
        val withRepartition =
          if (canSkipExtraRepartition(inlined) ||
              cteDef.underSubquery ||
              cteDef.getTagValue(SKIP_EXTRA_REPARTITION).isDefined) {
            // If the CTE definition plan itself is a repartition operation, if it hosts a merged
            // scalar subquery, or if it was tagged as too large to be worth an extra shuffle, we
            // do not need to add an extra repartition shuffle.
            inlined
          } else {
            RepartitionByExpression(Seq.empty, inlined, None)
          }
        cteMap.put(cteDef.id, withRepartition)
      }
      replaceWithRepartition(child, cteMap)

    case ref: CTERelationRef =>
      val cteDefPlan = cteMap.getOrElse(ref.cteId,
        throw SparkException.internalError(
          s"No CTERelationDef found for CTERelationRef(cteId=${ref.cteId})."))

      if (ref.outputSet == cteDefPlan.outputSet) {
        cteDefPlan
      } else {
        // Strip before deduplicating, and from BOTH sides, so the copy taken from `children(1)`
        // -- the side `DeduplicateRelations` renumbers -- carries no `DynamicPruningSubquery`
        // whose `buildKeys` the renumbering would strand.
        val stripped = stripDynamicPruning(cteDefPlan)
        val ctePlan = DeduplicateRelations(
          Join(stripped, stripped, Inner, None, JoinHint(None, None))).children(1)
        val projectList = ref.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
          Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
        }
        Project(projectList, ctePlan)
      }

    case _ if plan.containsPattern(CTE) =>
      plan
        .withNewChildren(plan.children.map(c => replaceWithRepartition(c, cteMap)))
        .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression =>
            e.withNewPlan(replaceWithRepartition(e.plan, cteMap))
        }

    case _ => plan
  }
}
