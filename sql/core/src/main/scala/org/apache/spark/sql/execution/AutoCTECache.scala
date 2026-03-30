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
 * When the feature is disabled, falls back to
 * [[org.apache.spark.sql.catalyst.optimizer.ReplaceCTERefWithRepartition]]
 * behavior (inline with repartition for ReusedExchange).
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
        // Evict stale/oversized auto-CTE cache entries before processing new CTEs
        spark.sharedState.autoCTECacheManager.evictIfNeeded(spark)
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
   */
  private def hasDivergentPredicates(cteDef: CTERelationDef): Boolean = {
    cteDef.originalPlanWithPredicates match {
      case Some((_, predicates)) if predicates.nonEmpty =>
        // If predicates were pushed down, the original plan had filters that
        // differ across references — caching would prevent this optimization
        val distinctPredicates = predicates.map(_.semanticHash()).distinct
        distinctPredicates.size > 1
      case _ => false
    }
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
      cteDefs.foreach { cteDef =>
        val resolvedChild = replaceWithCache(spark, cteDef.child, cteMap)

        if (!shouldAutoCache(cteDef)) {
          // Skip caching — use the resolved plan directly
          cteMap.put(cteDef.id, resolvedChild)
        } else {
          // Check if this CTE plan is already cached (cross-query reuse)
          val cacheManager = spark.sharedState.cacheManager
          val autoCTEManager = spark.sharedState.autoCTECacheManager
          val cachedPlan = cacheManager
            .lookupCachedData(spark, resolvedChild)
            .map { cached =>
              autoCTEManager.recordAccess(cteDef.id)
              cached.cachedRepresentation.withOutput(resolvedChild.output)
            }
            .getOrElse {
              // Cache the CTE definition
              cacheManager.cacheQuery(
                spark,
                resolvedChild,
                tableName = Some(s"auto_cte_${cteDef.id}"),
                StorageLevel.MEMORY_AND_DISK)
              // Track for lifecycle management
              autoCTEManager.trackEntry(cteDef.id, resolvedChild)
              // Retrieve the cached representation
              cacheManager.lookupCachedData(spark, resolvedChild)
                .map(_.cachedRepresentation.withOutput(resolvedChild.output))
                .getOrElse(resolvedChild)
            }

          cteMap.put(cteDef.id, cachedPlan)
        }
      }
      replaceWithCache(spark, child, cteMap)

    case ref: CTERelationRef =>
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
 * Manages the lifecycle of auto-cached CTE entries.
 * Tracks entries for TTL-based and LRU eviction.
 */
class AutoCTECacheManager extends Logging {

  private val entries = mutable.LinkedHashMap.empty[Long, AutoCTEEntry]

  def trackEntry(cteId: Long, plan: LogicalPlan): Unit = {
    entries(cteId) = AutoCTEEntry(
      cteId = cteId,
      plan = plan,
      tableName = s"auto_cte_$cteId",
      createdAt = System.currentTimeMillis(),
      lastAccessedAt = System.currentTimeMillis())
  }

  def recordAccess(cteId: Long): Unit = {
    entries.get(cteId).foreach { entry =>
      entries(cteId) = entry.copy(lastAccessedAt = System.currentTimeMillis())
    }
  }

  def evictIfNeeded(spark: SparkSession): Unit = {
    val conf = spark.sessionState.conf
    if (!conf.getConf(SQLConf.AUTO_CLEAR_CTE_CACHE_ENABLED)) return

    val now = System.currentTimeMillis()
    val ttl = conf.getConf(SQLConf.AUTO_CTE_CACHE_TTL)

    // TTL-based eviction
    if (ttl > 0) {
      val expired = entries.filter { case (_, e) =>
        now - e.lastAccessedAt > ttl
      }.keys.toSeq
      expired.foreach { id =>
        evictEntry(spark, id)
      }
    }

    // Size-based LRU eviction
    val maxSize = conf.getConf(SQLConf.AUTO_CTE_CACHE_MAX_SIZE)
    if (maxSize >= 0) {
      val cacheManager = spark.sharedState.cacheManager
      var totalSize = entries.values.map { entry =>
        cacheManager.lookupCachedData(spark, entry.plan)
          .map(_.cachedRepresentation.cacheBuilder.sizeInBytesStats.value.longValue())
          .getOrElse(0L)
      }.sum

      if (totalSize > maxSize) {
        // Evict LRU entries (LinkedHashMap iterates in insertion order;
        // sort by lastAccessedAt to get true LRU)
        val sortedByAccess = entries.toSeq.sortBy(_._2.lastAccessedAt)
        for ((id, entry) <- sortedByAccess if totalSize > maxSize) {
          val entrySize = cacheManager.lookupCachedData(spark, entry.plan)
            .map(_.cachedRepresentation.cacheBuilder.sizeInBytesStats.value.longValue())
            .getOrElse(0L)
          evictEntry(spark, id)
          totalSize -= entrySize
        }
      }
    }
  }

  private def evictEntry(spark: SparkSession, cteId: Long): Unit = {
    entries.get(cteId).foreach { entry =>
      spark.sharedState.cacheManager.uncacheTableOrView(
        spark, Seq(entry.tableName), cascade = false)
      entries.remove(cteId)
      logInfo(s"Evicted auto-cached CTE ${entry.tableName}")
    }
  }

  def clearAll(spark: SparkSession): Unit = {
    entries.keys.toSeq.foreach(id => evictEntry(spark, id))
  }

  def numEntries: Int = entries.size
}

case class AutoCTEEntry(
    cteId: Long,
    plan: LogicalPlan,
    tableName: String,
    createdAt: Long,
    lastAccessedAt: Long)
