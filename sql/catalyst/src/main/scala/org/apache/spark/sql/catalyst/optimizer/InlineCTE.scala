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

import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef, Join, JoinHint, LogicalPlan, Project, Sort, Subquery, UnionLoop, Window, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.internal.SQLConf

/**
 * Inlines CTE definitions into corresponding references if either of the conditions satisfies:
 * 1. The CTE definition does not contain any non-deterministic expressions or contains attribute
 *    references to an outer query. If this CTE definition references another CTE definition that
 *    has non-deterministic expressions, it is still OK to inline the current CTE definition.
 * 2. The CTE definition is only referenced once throughout the main query and all the subqueries.
 *
 * CTE definitions that appear in subqueries and are not inlined will be pulled up to the main
 * query level.
 *
 * @param alwaysInline if true, inline all CTEs in the query plan.
 * @param keepDanglingRelations if true, dangling CTE relations will be kept in the original
 *                              `WithCTE` node.
 */
case class InlineCTE(
    alwaysInline: Boolean = false,
    keepDanglingRelations: Boolean = false) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
      val cteMap = mutable.SortedMap.empty[Long, CTEReferenceInfo]
      buildCTEMap(plan, cteMap)
      cleanCTEMap(cteMap)
      inlineCTE(plan, cteMap)
    } else {
      plan
    }
  }

  private def shouldInline(cteDef: CTERelationDef, refCount: Int): Boolean = {
    if (alwaysInline) return true

    // When auto-CTE caching is enabled, do not inline a multi-reference CTE that
    // has an expensive body (Join / Aggregate / Sort / Window). Such CTEs are
    // candidates for `ReplaceCTERefWithCache` to materialize as `InMemoryRelation`.
    // Without this carve-out, every deterministic CTE is inlined here and the
    // auto-cache rule has nothing left to materialize on real workloads (TPC-DS
    // CTEs are virtually all deterministic).
    //
    // EXCEPTION: if the CTE has a reference inside a correlated subquery
    // (`correlatedSubqueryRef = true`), `ReplaceCTERefWithCache` will refuse
    // to cache it. We must NOT skip inlining in that case, otherwise the
    // CTE survives into `ReplaceCTERefWithRepartition`, which produces an
    // unresolved plan because the multi-reference is not deduped here.
    // Inlining correlated-ref CTEs is the safe baseline behavior.
    if (refCount > 1 &&
        SQLConf.get.getConf(SQLConf.AUTO_REUSED_CTE_ENABLED) &&
        !cteDef.correlatedSubqueryRef &&
        isAutoCacheEligible(cteDef.child)) {
      return false
    }

    // We do not need to check enclosed `CTERelationRef`s for `deterministic` or `OuterReference`,
    // because:
    // 1) It is fine to inline a CTE if it references another CTE that is non-deterministic;
    // 2) Any `CTERelationRef` that contains `OuterReference` would have been inlined first.
    refCount == 1 ||
      cteDef.deterministic ||
      cteDef.child.exists(_.expressions.exists(_.isInstanceOf[OuterReference]))
  }

  /**
   * Returns true if the CTE body is expensive enough that
   * `ReplaceCTERefWithCache` would consider it for caching. Two gates:
   *
   *   1. Structural: contains a Join / Aggregate / Sort / Window.
   *   2. Stats: estimated `sizeInBytes` >= `AUTO_CTE_CACHE_MIN_SIZE_BYTES`.
   *      When stats are unavailable the structural gate alone applies.
   *
   * IMPORTANT: This predicate, together with the `!correlatedSubqueryRef`
   * check in the carve-out above, MUST stay in lock-step with
   * `org.apache.spark.sql.execution.ReplaceCTERefWithCache.shouldAutoCache`
   * (sql/core module). If `shouldAutoCache` adds a new gate, update the
   * carve-out here to match - otherwise this rule may keep a CTE that
   * `ReplaceCTERefWithCache` then refuses to cache, leaving it for
   * `ReplaceCTERefWithRepartition` to handle. That fallback path produces an
   * unresolved plan when the multi-reference CTE was not first deduplicated
   * by InlineCTE itself, because `ReplaceCTERefWithRepartition` does not
   * give cs1/cs2 references separate exprIds.
   *
   * The two call sites can see slightly different stats (InlineCTE runs
   * earlier, before predicate pushdown shrinks the body). For clearly
   * tiny vs clearly large CTEs the divergence is irrelevant; for
   * borderline cases the structural gate is the primary signal.
   *
   * The predicate is duplicated rather than shared because catalyst cannot
   * depend on sql/core. There is no test that catches a divergence between
   * the two definitions; reviewers should manually verify when modifying
   * either.
   */
  private def isAutoCacheEligible(plan: LogicalPlan): Boolean = {
    val structurallyExpensive = plan.exists {
      case _: Join => true
      case _: Aggregate => true
      case _: Sort => true
      case _: Window => true
      case _ => false
    }
    if (!structurallyExpensive) return false

    val threshold = SQLConf.get.getConf(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES)
    try {
      plan.stats.sizeInBytes.toLong >= threshold
    } catch {
      case _: Throwable => true
    }
  }

  /**
   * Accumulates all the CTEs from a plan into a special map.
   *
   * @param plan The plan to collect the CTEs from
   * @param cteMap A mutable map that accumulates the CTEs and their reference information by CTE
   *               ids.
   * @param outerCTEId While collecting the map we use this optional CTE id to identify the
   *                   current outer CTE.
   */
  private def buildCTEMap(
      plan: LogicalPlan,
      cteMap: mutable.Map[Long, CTEReferenceInfo],
      outerCTEId: Option[Long] = None): Unit = {
    plan match {
      case WithCTE(child, cteDefs) =>
        val isDuplicated = cteDefs.forall(cteDef => cteMap.contains(cteDef.id))
        if (isDuplicated) {
          // If we have seen this `WithCTE` node then it must be self-contained so we can clear
          // the references from containers to the definitions, and we don't need to process it
          // again

          cteDefs.foreach { cteDef =>
            cteMap(cteDef.id).container.foreach(c => cteMap(c).outgoingRefs -= cteDef.id)
          }
        } else {
          cteDefs.foreach { cteDef =>
            cteMap(cteDef.id) = CTEReferenceInfo(
              cteDef = cteDef,
              refCount = 0,
              outgoingRefs = mutable.Map.empty.withDefaultValue(0),
              shouldInline = true,
              container = outerCTEId
            )
          }

          cteDefs.foreach { cteDef =>
            buildCTEMap(cteDef, cteMap, Some(cteDef.id))
          }
          buildCTEMap(child, cteMap, outerCTEId)
        }

      case ref: CTERelationRef =>
        cteMap(ref.cteId) = cteMap(ref.cteId).withRefCountIncreased(1)

        // The `outerCTEId` CTE definition can either reference `cteId` definition if `cteId` is in
        // the same or in an outer `WithCTE` node, or `outerCTEId` can contain `cteId` definition if
        // `cteId` is an inner `WithCTE` node inside `outerCTEId`.
        // In both cases we can track the relations in `outgoingRefs` when we see a definition the
        // first time. But if we encounter a conflicting duplicated contains relation later, then we
        // will remove the references of the first contains relation.
        outerCTEId.foreach { cteId =>
          cteMap(cteId).increaseOutgoingRefCount(ref.cteId, 1)
        }

      case _ =>
        if (plan.containsPattern(CTE)) {
          plan.children.foreach { child =>
            buildCTEMap(child, cteMap, outerCTEId)
          }

          plan.expressions.foreach { expr =>
            if (expr.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
              expr.foreach {
                case e: SubqueryExpression => buildCTEMap(e.plan, cteMap, outerCTEId)
                case _ =>
              }
            }
          }
        }
    }
  }

  /**
   * Cleans the CTE map by removing those CTEs that are not referenced at all and corrects those
   * CTE's reference counts where the removed CTE referred to.
   *
   * @param cteMap A mutable map that accumulates the CTEs and their reference information by CTE
   *               ids. Needs to be sorted to speed up cleaning.
   */
  private def cleanCTEMap(cteMap: mutable.SortedMap[Long, CTEReferenceInfo]): Unit = {
    cteMap.keys.toSeq.reverse.foreach { currentCTEId =>
      val refInfo = cteMap(currentCTEId)
      if (refInfo.refCount == 0) {
        refInfo.outgoingRefs.foreach { case (referencedCTEId, uselessRefCount) =>
          cteMap(referencedCTEId) = cteMap(referencedCTEId).withRefCountDecreased(uselessRefCount)
        }
      }
    }
  }

  private def inlineCTE(
      plan: LogicalPlan,
      cteMap: mutable.Map[Long, CTEReferenceInfo]): LogicalPlan = {
    plan match {
      case WithCTE(child, cteDefs) =>
        val notInlined = cteDefs.flatMap {cteDef =>
          val refInfo = cteMap(cteDef.id)
          if (refInfo.refCount > 0) {
            val newDef = refInfo.cteDef.copy(child = inlineCTE(refInfo.cteDef.child, cteMap))
            val inlineDecision = shouldInline(newDef, refInfo.refCount)
            cteMap(cteDef.id) = cteMap(cteDef.id).copy(
              cteDef = newDef, shouldInline = inlineDecision
            )
            if (!inlineDecision) {
              Seq(newDef)
            } else {
              Nil
            }
          } else if (keepDanglingRelations) {
            Seq(refInfo.cteDef)
          } else {
            Nil
          }
        }
        val inlined = inlineCTE(child, cteMap)
        if (notInlined.isEmpty) {
          inlined
        } else {
          // Retain the not-inlined CTE relations in place.
          WithCTE(inlined, notInlined)
        }

      case ref: CTERelationRef =>
        val refInfo = cteMap(ref.cteId)

        val cteBody = if (ref.isUnlimitedRecursion) {
          setUnlimitedRecursion(refInfo.cteDef.child, ref.cteId)
        } else {
          refInfo.cteDef.child
        }
        if (refInfo.shouldInline) {
          if (ref.outputSet == refInfo.cteDef.outputSet) {
            cteBody
          } else {
            val ctePlan = DeduplicateRelations(
              Join(
                cteBody,
                cteBody,
                Inner,
                None,
                JoinHint(None, None)
              )
            ).children(1)
            val projectList = ref.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
              if (srcAttr.semanticEquals(tgtAttr)) {
                tgtAttr
              } else {
                Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
              }
            }
            Project(projectList, ctePlan)
          }
        } else {
          ref
        }

      case _ if plan.containsPattern(CTE) =>
        plan
          .withNewChildren(plan.children.map(child => inlineCTE(child, cteMap)))
          .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
            case e: SubqueryExpression =>
              e.withNewPlan(inlineCTE(e.plan, cteMap))
          }

      case _ => plan
    }
  }

  // Helper function to set unlimited recursion.
  private def setUnlimitedRecursion(plan: LogicalPlan, id: Long): LogicalPlan = {
    plan match {
      case ul: UnionLoop if ul.id == id =>
        // Since there is exactly one UnionLoop node with this id in the CTE body, we can stop the
        // recursion here.
        ul.copy(limit = Some(-1))
      case other =>
        other.withNewChildren(plan.children.map(child => setUnlimitedRecursion(child, id)))
    }
  }
}

/**
 * The bookkeeping information for tracking CTE relation references.
 *
 * @param cteDef The CTE relation definition
 * @param refCount The number of incoming references to this CTE relation. This includes references
 *                 from other CTE relations and regular places.
 * @param outgoingRefs A mutable map that tracks outgoing reference counts to other CTE relations.
 * @param shouldInline If true, this CTE relation should be inlined in the places that reference it.
 * @param container The container of a CTE definition is another CTE definition in which the
 *                  `WithCTE` node of the definition resides.
 */
case class CTEReferenceInfo(
    cteDef: CTERelationDef,
    refCount: Int,
    outgoingRefs: mutable.Map[Long, Int],
    shouldInline: Boolean,
    container: Option[Long]) {

  def withRefCountIncreased(count: Int): CTEReferenceInfo = {
    copy(refCount = refCount + count)
  }

  def withRefCountDecreased(count: Int): CTEReferenceInfo = {
    copy(refCount = refCount - count)
  }

  def increaseOutgoingRefCount(cteDefId: Long, count: Int): Unit = {
    outgoingRefs(cteDefId) += count
  }
}
