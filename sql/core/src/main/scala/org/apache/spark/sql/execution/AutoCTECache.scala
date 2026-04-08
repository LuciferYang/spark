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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

/**
 * Replaces non-inlined CTE references with [[InMemoryRelation]]
 * when `spark.sql.auto.reused.cte.enabled` is true. Each CTE
 * definition is executed once and cached; all references read
 * from cache.
 *
 * CTEs that are too cheap to benefit from caching (no Join, Aggregate,
 * Sort, or Window) are left for
 * [[org.apache.spark.sql.catalyst.optimizer.ReplaceCTERefWithRepartition]]
 * to handle with repartition-based shuffle reuse.
 *
 * Cache lifecycle is managed by [[AutoCTECacheManager]].
 */
object ReplaceCTERefWithCache extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.AUTO_REUSED_CTE_ENABLED)) {
      return plan
    }

    plan match {
      case _: Subquery => plan
      case _ =>
        val spark = SparkSession.active
        spark.sharedState.autoCTECacheManager.evictStaleEntries(spark)
        replaceWithCache(spark, plan, mutable.HashMap.empty)
    }
  }

  /**
   * Checks whether a CTE definition should be auto-cached based on heuristics.
   *
   * Three gates, all of which must pass:
   *
   *   1. `isExpensiveEnough` - the CTE body must contain a Join, Aggregate,
   *      Sort, or Window. Cheap scan-only CTEs are not worth materialising.
   *
   *   2. `!cteDef.correlatedSubqueryRef` - the CTE must NOT have any reference
   *      that originally appeared inside a correlated subquery expression.
   *      `TagCorrelatedCTERefs` populates this flag in an early optimizer
   *      batch BEFORE `RewriteCorrelatedScalarSubquery` decorrelates the
   *      subquery into a join. Without the tag, q1/q31/q39a-style queries
   *      cannot be distinguished structurally from q24a's user-written
   *      `cs1 join cs2` self-join at this point in the pipeline.
   *
   *   3. `!hasDivergentPredicates` - placeholder, see method doc.
   */
  private def shouldAutoCache(cteDef: CTERelationDef): Boolean = {
    !cteDef.correlatedSubqueryRef &&
      !hasDivergentPredicates(cteDef) &&
      isExpensiveEnough(cteDef.child)
  }

  /**
   * The previous heuristic skipped caching whenever multiple references had
   * syntactically distinct predicates. That is too conservative:
   * `PushdownPredicatesAndPruneColumnsForCTEDef` already combines per-reference
   * predicates with `OR` and pushes the combined predicate into the CTE body
   * BEFORE this rule runs (see `pushdownPredicatesAndAttributes`, which calls
   * `newPreds.reduce(Or)` and wraps the body in a `Filter`). The per-reference
   * filters then sit ABOVE the cache, so caching does not block pushdown.
   *
   * Skipping in that case actively hurts queries like TPC-DS q24a/q24b/q64
   * where multiple references use different non-correlated filter constants:
   * the EMR reference cluster caches them, we should too.
   *
   * The original concern (q1/q31/q39a regression risk noted in the design doc)
   * is about CORRELATED outer references inside scalar subqueries. That
   * concern is now handled by `cteDef.correlatedSubqueryRef`, populated by
   * `TagCorrelatedCTERefs` in an earlier optimizer batch. See the
   * `shouldAutoCache` doc for the gating order.
   *
   * This method is now a placeholder. It is kept (rather than inlined as
   * `false`) so that the doc comment above documenting WHY divergent
   * non-correlated predicates are safe to cache stays attached to its
   * subject - and so that any future hand-rolled divergence check has an
   * obvious slot.
   */
  private def hasDivergentPredicates(cteDef: CTERelationDef): Boolean = false

  /**
   * Returns true if the CTE plan contains at least one expensive operator
   * (Join, Aggregate, Sort, Window). Simple scan-only CTEs are cheap to
   * recompute and caching them wastes memory.
   *
   * IMPORTANT: This predicate MUST stay in lock-step with
   * `org.apache.spark.sql.catalyst.optimizer.InlineCTE.isAutoCacheEligible`
   * (sql/catalyst module). InlineCTE uses the same predicate to decide
   * whether to skip inlining a deterministic multi-reference CTE so that
   * this rule can materialise it. If the two diverge, InlineCTE may either
   * inline a CTE this rule would have cached (lost optimisation) or keep a
   * CTE this rule then refuses (no-op InlineCTE skip + ReplaceCTERefWithRepartition
   * fallback).
   *
   * The predicate is duplicated rather than shared because sql/catalyst
   * cannot depend on sql/core. Reviewers must manually keep the two copies
   * in sync; there is no test that catches a divergence directly.
   */
  private def isExpensiveEnough(plan: LogicalPlan): Boolean = {
    plan.exists {
      case _: Join => true
      case _: Aggregate => true
      case _: Sort => true
      case _: Window => true
      case _ => false
    }
  }

  private def replaceWithCache(
      spark: SparkSession,
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, LogicalPlan]): LogicalPlan = plan match {

    case WithCTE(child, cteDefs) =>
      val skippedDefs = mutable.ArrayBuffer.empty[CTERelationDef]
      cteDefs.foreach { cteDef =>
        val resolvedChild = replaceWithCache(spark, cteDef.child, cteMap)

        if (!shouldAutoCache(cteDef)) {
          // Leave for ReplaceCTERefWithRepartition (preserves shuffle reuse)
          skippedDefs += cteDef.copy(child = resolvedChild)
        } else {
          val cacheManager = spark.sharedState.cacheManager
          val autoCTEManager = spark.sharedState.autoCTECacheManager
          val cachedPlan = cacheManager
            .lookupCachedData(spark, resolvedChild)
            .map { cached =>
              // Cache hit -- refresh TTL for the matching entry
              autoCTEManager.recordAccessByPlan(resolvedChild)
              cached.cachedRepresentation.withOutput(resolvedChild.output)
            }
            .getOrElse {
              cacheManager.cacheQuery(
                spark,
                resolvedChild,
                tableName = Some(s"auto_cte_${cteDef.id}"),
                StorageLevel.MEMORY_AND_DISK.withEvictionPriority(-1))
              autoCTEManager.trackEntry(cteDef.id, resolvedChild)
              cacheManager.lookupCachedData(spark, resolvedChild)
                .map(_.cachedRepresentation.withOutput(resolvedChild.output))
                .getOrElse(resolvedChild)
            }

          cteMap.put(cteDef.id, cachedPlan)
        }
      }
      val newChild = replaceWithCache(spark, child, cteMap)
      if (skippedDefs.nonEmpty) {
        WithCTE(newChild, skippedDefs.toSeq)
      } else {
        newChild
      }

    case ref: CTERelationRef if cteMap.contains(ref.cteId) =>
      val ctePlan = cteMap(ref.cteId)
      if (ref.outputSet == ctePlan.outputSet) {
        ctePlan
      } else {
        val deduped = DeduplicateRelations(
          Join(ctePlan, ctePlan, Inner, None, JoinHint(None, None))
        ).children(1)
        val projectList = ref.output.zip(deduped.output).map {
          case (tgtAttr, srcAttr) =>
            if (srcAttr.semanticEquals(tgtAttr)) tgtAttr
            else Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
        }
        Project(projectList, deduped)
      }

    case _ if plan.containsPattern(CTE) =>
      plan
        .withNewChildren(plan.children.map(c =>
          replaceWithCache(spark, c, cteMap)))
        .transformExpressionsWithPruning(
          _.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression =>
            e.withNewPlan(replaceWithCache(spark, e.plan, cteMap))
        }

    case _ => plan
  }
}

/**
 * Tracks auto-cached CTE entries for TTL-based eviction using Guava Cache.
 *
 * This is a lightweight companion to [[CacheManager]]. CacheManager stores
 * the actual cached data; this class only tracks which entries were created
 * by auto-CTE caching so they can be evicted by TTL without affecting
 * entries created by explicit `CACHE TABLE`.
 *
 * Guava's `expireAfterAccess` provides idle-timeout semantics: each
 * `get`/`put` resets the TTL clock automatically.
 *
 * @param ttlMs  idle timeout in milliseconds (0 = no TTL)
 * @param maxSizeBytes  maximum total weight in bytes (-1 = unlimited)
 */
class AutoCTECacheManager(ttlMs: Long, maxSizeBytes: Long) extends Logging {

  import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification, Weigher}

  // Pending uncache plans from eviction -- processed by evictStaleEntries
  private val pendingUncache = new java.util.concurrent.ConcurrentLinkedQueue[LogicalPlan]()

  private val cache: Cache[java.lang.Long, AutoCTEEntry] = {
    val builder = CacheBuilder.newBuilder()
      .removalListener((notification: RemovalNotification[java.lang.Long, AutoCTEEntry]) => {
        if (notification.wasEvicted()) {
          val entry = notification.getValue
          pendingUncache.add(entry.plan)
          logInfo(s"Evicted auto-cached CTE ${entry.tableName}")
        }
      })
    if (ttlMs > 0) {
      builder.expireAfterAccess(ttlMs, java.util.concurrent.TimeUnit.MILLISECONDS)
    }
    if (maxSizeBytes >= 0) {
      builder
        .maximumWeight(maxSizeBytes)
        .weigher(new Weigher[java.lang.Long, AutoCTEEntry] {
          override def weigh(key: java.lang.Long, value: AutoCTEEntry): Int = {
            // Use stats estimate; actual materialized size may differ.
            // Fallback to 1 if stats computation fails.
            try {
              math.min(value.plan.stats.sizeInBytes.toLong, Int.MaxValue).toInt
            } catch {
              case _: Exception => 1
            }
          }
        })
    }
    builder.build()
  }

  def trackEntry(cteId: Long, plan: LogicalPlan): Unit = {
    cache.put(cteId, AutoCTEEntry(plan = plan, tableName = s"auto_cte_$cteId"))
  }

  /**
   * Refreshes the TTL for the entry whose plan matches the given plan.
   * Uses reference equality first (fast path for within-query hits),
   * then falls back to sameResult (for cross-query hits).
   */
  def recordAccessByPlan(plan: LogicalPlan): Unit = {
    val it = cache.asMap().entrySet().iterator()
    while (it.hasNext) {
      val e = it.next()
      if ((e.getValue.plan eq plan) || e.getValue.plan.sameResult(plan)) {
        // getIfPresent refreshes the access time for expireAfterAccess
        cache.getIfPresent(e.getKey)
        return
      }
    }
  }

  /** Triggers Guava's lazy eviction and uncaches expired entries from CacheManager. */
  def evictStaleEntries(spark: SparkSession): Unit = {
    cache.cleanUp()

    var plan = pendingUncache.poll()
    while (plan != null) {
      spark.sharedState.cacheManager.uncacheQuery(spark, plan, cascade = false)
      plan = pendingUncache.poll()
    }
  }

  def clearAll(spark: SparkSession): Unit = {
    val plans = new java.util.ArrayList[LogicalPlan]()
    cache.asMap().values().forEach(e => plans.add(e.plan))
    cache.invalidateAll()
    var plan = pendingUncache.poll()
    while (plan != null) {
      plans.add(plan)
      plan = pendingUncache.poll()
    }
    plans.forEach { p =>
      spark.sharedState.cacheManager.uncacheQuery(spark, p, cascade = false)
    }
  }

  def numEntries: Int = cache.asMap().size()
}

private[sql] case class AutoCTEEntry(
    plan: LogicalPlan,
    tableName: String)
