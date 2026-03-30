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
        // Collect all Or-branch leaves
        val branches = collectOrBranches(or)
        // If any branches are semantically different, predicates are divergent
        branches.size > 1 && {
          val hashes = branches.map(_.semanticHash()).distinct
          hashes.size > 1
        }
      case _ => false
    }
  }

  /**
   * Flattens a nested Or tree into its leaf predicates.
   * e.g., Or(Or(a, b), c) => Seq(a, b, c)
   */
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
          // Leave for ReplaceCTERefWithRepartition to handle (preserves shuffle reuse)
          skippedDefs += cteDef.copy(child = resolvedChild)
        } else {
          // Check if this CTE plan is already cached (cross-query reuse)
          val cacheManager = spark.sharedState.cacheManager
          val autoCTEManager = spark.sharedState.autoCTECacheManager
          val cachedPlan = cacheManager
            .lookupCachedData(spark, resolvedChild)
            .map { cached =>
              // Use plan-based access recording for cross-query reuse,
              // since the new query has different CTE IDs
              autoCTEManager.recordAccessByPlan(spark, resolvedChild)
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
      val newChild = replaceWithCache(spark, child, cteMap)
      // Rebuild WithCTE if any CTEs were skipped, so ReplaceCTERefWithRepartition
      // can handle them with proper shuffle reuse
      if (skippedDefs.nonEmpty) {
        WithCTE(newChild, skippedDefs.toSeq)
      } else {
        newChild
      }

    case ref: CTERelationRef if cteMap.contains(ref.cteId) =>
      // Only replace refs for CTEs that were cached; leave others for
      // ReplaceCTERefWithRepartition
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

  def trackEntry(cteId: Long, plan: LogicalPlan): Unit = synchronized {
    entries(cteId) = AutoCTEEntry(
      cteId = cteId,
      plan = plan,
      tableName = s"auto_cte_$cteId",
      createdAt = System.currentTimeMillis(),
      lastAccessedAt = System.currentTimeMillis())
  }

  def recordAccess(cteId: Long): Unit = synchronized {
    entries.get(cteId).foreach { entry =>
      entries(cteId) = entry.copy(lastAccessedAt = System.currentTimeMillis())
    }
  }

  /**
   * Records access by plan matching (for cross-query reuse where the new query
   * has different CTE IDs than the original cached entry).
   */
  def recordAccessByPlan(spark: SparkSession, plan: LogicalPlan): Unit = synchronized {
    val normalized = QueryExecution.normalize(spark, plan)
    entries.find { case (_, entry) =>
      QueryExecution.normalize(spark, entry.plan).sameResult(normalized)
    }.foreach { case (id, entry) =>
      entries(id) = entry.copy(lastAccessedAt = System.currentTimeMillis())
    }
  }

  def evictIfNeeded(spark: SparkSession): Unit = {
    val conf = spark.sessionState.conf
    if (!conf.getConf(SQLConf.AUTO_CLEAR_CTE_CACHE_ENABLED)) return

    // Collect entries to evict under the lock, then uncache outside the lock
    // to avoid holding AutoCTECacheManager.synchronized while calling into
    // CacheManager (which has its own synchronization).
    val toEvict = synchronized {
      val evictIds = mutable.ArrayBuffer.empty[Long]
      val now = System.currentTimeMillis()
      val ttl = conf.getConf(SQLConf.AUTO_CTE_CACHE_TTL)

      // TTL-based eviction
      if (ttl > 0) {
        evictIds ++= entries.filter { case (_, e) =>
          now - e.lastAccessedAt > ttl
        }.keys
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
          val sortedByAccess = entries.toSeq
            .filterNot(e => evictIds.contains(e._1)) // skip already-marked
            .sortBy(_._2.lastAccessedAt)
          for ((id, entry) <- sortedByAccess if totalSize > maxSize) {
            val entrySize = cacheManager.lookupCachedData(spark, entry.plan)
              .map(_.cachedRepresentation.cacheBuilder.sizeInBytesStats.value.longValue())
              .getOrElse(0L)
            evictIds += id
            totalSize -= entrySize
          }
        }
      }

      // Remove from entries map under the lock, collect plans to uncache
      evictIds.flatMap { id =>
        entries.remove(id).map { entry =>
          logInfo(s"Evicted auto-cached CTE ${entry.tableName}")
          entry.plan
        }
      }.toSeq
    }

    // Uncache from CacheManager outside the lock
    toEvict.foreach { plan =>
      spark.sharedState.cacheManager.uncacheQuery(spark, plan, cascade = false)
    }
  }

  def clearAll(spark: SparkSession): Unit = {
    val plans = synchronized {
      val result = entries.values.map(_.plan).toSeq
      entries.clear()
      result
    }
    plans.foreach { plan =>
      spark.sharedState.cacheManager.uncacheQuery(spark, plan, cascade = false)
    }
  }

  def numEntries: Int = synchronized { entries.size }

  /** Visible for testing. */
  private[sql] def entryIds: Seq[Long] = synchronized { entries.keys.toSeq }
}

case class AutoCTEEntry(
    cteId: Long,
    plan: LogicalPlan,
    tableName: String,
    createdAt: Long,
    lastAccessedAt: Long)
