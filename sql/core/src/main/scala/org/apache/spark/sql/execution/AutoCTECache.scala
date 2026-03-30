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
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Or, SubqueryExpression}
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
   * Returns false if caching would likely cause regressions (e.g., blocking
   * predicate pushdown) or if the CTE is too cheap to benefit from caching.
   */
  private def shouldAutoCache(cteDef: CTERelationDef): Boolean = {
    !hasDivergentPredicates(cteDef) && isExpensiveEnough(cteDef.child)
  }

  /**
   * Checks if different CTE references apply substantially different filter
   * predicates. If yes, caching blocks predicate pushdown and causes regressions.
   *
   * Note: `originalPlanWithPredicates` is cleared by `CleanUpTempCTEInfo` before
   * this rule runs, so we detect divergent predicates by examining the CTE child
   * plan directly. After `PushdownPredicatesAndPruneColumnsForCTEDef`, divergent
   * predicates are combined as `Filter(Or(pred1, pred2), ...)` in the CTE child.
   * If the Or children are semantically different, the predicates are divergent.
   *
   * Known limitation: if the CTE body itself starts with a Filter(Or(...), ...)
   * (not from pushdown), this may produce a false positive. In practice this is
   * rare and the fallback to repartition-based reuse is safe.
   */
  private def hasDivergentPredicates(cteDef: CTERelationDef): Boolean = {
    cteDef.child match {
      case Filter(or: Or, _) =>
        val branches = collectOrBranches(or)
        branches.size > 1 && {
          val hashes = branches.map(_.semanticHash()).distinct
          hashes.size > 1
        }
      case _ => false
    }
  }

  /** Flattens a nested Or tree into its leaf predicates. */
  private def collectOrBranches(expr: Expression): Seq[Expression] = expr match {
    case Or(left, right) => collectOrBranches(left) ++ collectOrBranches(right)
    case other => Seq(other)
  }

  /**
   * Returns true if the CTE plan contains at least one expensive operator
   * (Join, Aggregate, Sort, Window). Simple scan-only CTEs are cheap to
   * recompute and caching them wastes memory.
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
                StorageLevel.MEMORY_AND_DISK)
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
 */
class AutoCTECacheManager extends Logging {

  import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification, Weigher}

  // Pending uncache plans from eviction -- processed by evictStaleEntries
  private val pendingUncache = new java.util.concurrent.ConcurrentLinkedQueue[LogicalPlan]()

  @volatile private var cache: Cache[java.lang.Long, AutoCTEEntry] = buildCache(
    ttlNanos = java.util.concurrent.TimeUnit.HOURS.toNanos(1),
    maxSizeBytes = -1L)

  private def buildCache(ttlNanos: Long, maxSizeBytes: Long)
      : Cache[java.lang.Long, AutoCTEEntry] = {
    val builder = CacheBuilder.newBuilder()
      .removalListener((notification: RemovalNotification[java.lang.Long, AutoCTEEntry]) => {
        if (notification.wasEvicted()) {
          val entry = notification.getValue
          pendingUncache.add(entry.plan)
          logInfo(s"Evicted auto-cached CTE ${entry.tableName}")
        }
      })
    if (ttlNanos > 0) {
      builder.expireAfterAccess(ttlNanos, java.util.concurrent.TimeUnit.NANOSECONDS)
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
    if (!spark.sessionState.conf.getConf(SQLConf.AUTO_CLEAR_CTE_CACHE_ENABLED)) return

    // Rebuild cache if config changed
    val ttl = spark.sessionState.conf.getConf(SQLConf.AUTO_CTE_CACHE_TTL)
    val maxSize = spark.sessionState.conf.getConf(SQLConf.AUTO_CTE_CACHE_MAX_SIZE)
    rebuildCacheIfNeeded(ttl, maxSize)

    // Trigger Guava's lazy expiration
    cache.cleanUp()

    // Drain pending uncache queue
    var plan = pendingUncache.poll()
    while (plan != null) {
      spark.sharedState.cacheManager.uncacheQuery(spark, plan, cascade = false)
      plan = pendingUncache.poll()
    }
  }

  private var currentTtlMs: Long = java.util.concurrent.TimeUnit.HOURS.toMillis(1)
  private var currentMaxSizeBytes: Long = -1L

  private def rebuildCacheIfNeeded(newTtlMs: Long, newMaxSizeBytes: Long): Unit = {
    if (newTtlMs != currentTtlMs || newMaxSizeBytes != currentMaxSizeBytes) {
      val oldEntries = cache.asMap()
      cache = buildCache(
        java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(newTtlMs),
        newMaxSizeBytes)
      cache.putAll(oldEntries)
      currentTtlMs = newTtlMs
      currentMaxSizeBytes = newMaxSizeBytes
    }
  }

  def clearAll(spark: SparkSession): Unit = {
    val plans = new java.util.ArrayList[LogicalPlan]()
    cache.asMap().values().forEach(e => plans.add(e.plan))
    cache.invalidateAll()
    // Also drain anything from pending queue
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
