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
   * Returns true if the CTE plan is expensive enough to be worth materialising
   * as an `InMemoryRelation`. Two gates, both must pass:
   *
   *   1. Structural: contains a Join / Aggregate / Sort / Window. Pure
   *      scan-only CTEs are cheap to recompute.
   *   2. Stats: estimated `sizeInBytes` is at least
   *      `AUTO_CTE_CACHE_MIN_SIZE_BYTES`. Guards against caching CTEs that
   *      look complex but operate on tiny inputs (e.g.
   *      `SELECT * FROM small_dim ORDER BY x`). When stats are unavailable
   *      the structural gate alone applies (`Throwable` fallback returns
   *      `true`).
   *
   * IMPORTANT: This predicate MUST stay in lock-step with
   * `org.apache.spark.sql.catalyst.optimizer.InlineCTE.isAutoCacheEligible`
   * (sql/catalyst module). InlineCTE uses the same predicate to decide
   * whether to skip inlining a deterministic multi-reference CTE so that
   * this rule can materialise it. If the two diverge, InlineCTE may either
   * inline a CTE this rule would have cached (lost optimisation) or keep a
   * CTE this rule then refuses (no-op InlineCTE skip + ReplaceCTERefWithRepartition
   * fallback - which produces an unresolved plan because the multi-ref CTE
   * has not been deduplicated).
   *
   * The predicate is duplicated rather than shared because sql/catalyst
   * cannot depend on sql/core. Reviewers must manually keep the two copies
   * in sync; there is no test that catches a divergence directly. The two
   * call sites can see slightly different stats (InlineCTE runs earlier,
   * before predicate pushdown shrinks the body) - for clearly tiny vs
   * clearly large CTEs the divergence is irrelevant; for borderline cases
   * the structural gate is the primary signal.
   */
  private def isExpensiveEnough(plan: LogicalPlan): Boolean = {
    val structurallyExpensive = plan.exists {
      case _: Join => true
      case _: Aggregate => true
      case _: Sort => true
      case _: Window => true
      case _ => false
    }
    if (!structurallyExpensive) return false

    val threshold = conf.getConf(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES)
    try {
      plan.stats.sizeInBytes.toLong >= threshold
    } catch {
      // Permissive fallback: if stats computation throws (a stats provider
      // bug, an unbound subquery, etc.) we err on the side of caching. The
      // structural gate has already passed, so we know the plan is at least
      // job-shaped. Returning false here would silently disable caching for
      // any plan whose stats are unreliable, which would be a regression
      // for queries that previously cached on default config. NonFatal
      // swallows ordinary exceptions but lets fatal errors propagate.
      case scala.util.control.NonFatal(_) => true
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

  /**
   * Secondary index from canonicalized plan to the cteIds that materialised it.
   * Used by `recordAccessByPlan` for O(1) lookup instead of an O(n) scan over
   * the entire Guava cache. The key is `LogicalPlan.canonicalized`, which
   * normalises exprIds and operand orderings so that semantically-equal
   * plans hash equal (same contract that `LogicalPlan.sameResult` relies on).
   *
   * The value set is a `ConcurrentHashMap.KeySetView` so that the index is
   * thread-safe with the rest of the cache. Multiple cteIds can map to the
   * same canonicalized plan in two cases:
   *   1. Two CTE definitions with identical bodies in different queries -
   *      cross-query reuse will collapse them via the Guava cache, but the
   *      index keeps both ids until eviction.
   *   2. Hash collisions between semantically-different plans (extremely
   *      rare; `recordAccessByPlan` still verifies via `cache.getIfPresent`).
   */
  private val planIndex: java.util.concurrent.ConcurrentHashMap[
      LogicalPlan,
      java.util.Set[java.lang.Long]] =
    new java.util.concurrent.ConcurrentHashMap()

  private val cache: Cache[java.lang.Long, AutoCTEEntry] = {
    val builder = CacheBuilder.newBuilder()
      .removalListener((notification: RemovalNotification[java.lang.Long, AutoCTEEntry]) => {
        if (notification.wasEvicted()) {
          val entry = notification.getValue
          pendingUncache.add(entry.plan)
          // Clean up the secondary index. Use the pre-computed indexKey
          // captured at trackEntry time, NOT a fresh recomputation - the
          // removalListener may run on a Guava cleanup thread where
          // SparkSession.active is null or the wrong session, which would
          // produce a different canonical form and leak the bucket.
          // If the cached key was null (normalisation failed at trackEntry
          // time), there is no index entry to clean up.
          val key = entry.indexKey
          if (key != null) {
            val ids = planIndex.get(key)
            if (ids != null) {
              ids.remove(notification.getKey)
              if (ids.isEmpty) {
                planIndex.remove(key, ids)
              }
            }
          }
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

  /**
   * Computes the index key for a plan. MUST use the same normalization as
   * `CacheManager.lookupCachedData` (which calls `QueryExecution.normalize`),
   * otherwise `recordAccessByPlan` may fail to find an entry that
   * `cacheManager.lookupCachedData` did find - leading to a TTL refresh
   * being silently dropped.
   *
   * The result is a `LogicalPlan` whose `equals`/`hashCode` are structural
   * (case-class semantics) over the canonicalized form, so it is safe to use
   * directly as a `ConcurrentHashMap` key.
   *
   * Returns `null` if normalization is not possible: either there is no
   * active SparkSession (the method was called from a context outside any
   * query, e.g. a synthetic test) or a normalization rule throws. Callers
   * MUST handle null by skipping the index operation; we never silently
   * substitute a different normalization, because that would break the
   * lookup contract with `CacheManager.lookupCachedData`.
   */
  private def indexKey(plan: LogicalPlan): LogicalPlan = {
    val session = SparkSession.getActiveSession.orNull
    if (session == null) return null
    try {
      QueryExecution.normalize(session, plan).canonicalized
    } catch {
      // NonFatal swallows ordinary exceptions but lets fatal errors
      // (OOM, StackOverflow, ThreadDeath) propagate as they should.
      case scala.util.control.NonFatal(_) => null
    }
  }

  def trackEntry(cteId: Long, plan: LogicalPlan): Unit = {
    val key = indexKey(plan)
    cache.put(cteId, AutoCTEEntry(plan = plan, tableName = s"auto_cte_$cteId", indexKey = key))
    if (key != null) {
      // Maintain the secondary index. computeIfAbsent is atomic; the
      // ConcurrentHashMap.newKeySet view supports concurrent add/remove.
      planIndex
        .computeIfAbsent(
          key,
          _ => java.util.concurrent.ConcurrentHashMap.newKeySet[java.lang.Long]())
        .add(cteId)
    }
  }

  /**
   * Best-effort TTL refresh for the entry whose plan matches the given plan.
   *
   * Returns silently in three cases (none are errors):
   *   1. `indexKey(plan)` is null (no active session, normalization threw).
   *   2. The index has no bucket for the key (cache miss).
   *   3. All ids in the bucket are stale (Guava evicted, removalListener
   *      pending). The next call to `evictStaleEntries` will run the
   *      removalListener and clean the bucket.
   *
   * O(1) average case via the `planIndex`. The lookup is keyed on
   * `QueryExecution.normalize(plan).canonicalized` so two semantically-equal
   * plans hit the same bucket regardless of exprId variation - and the
   * normalization matches what `CacheManager.lookupCachedData` does.
   *
   * Stale entries are NOT removed from the index inside this method:
   *   1. The removalListener will clean them up shortly (it has the
   *      pre-computed key on `AutoCTEEntry`).
   *   2. In-band cleanup races with concurrent `trackEntry` calls. Specifically:
   *      thread A drains a bucket, thread B inserts a fresh cteId into the
   *      same bucket, thread A's `remove(key, ids)` checks `ids.equals(ids)`
   *      (which is trivially true for the same Set instance), and erases
   *      thread B's insert. Letting `removalListener` own bucket lifecycle
   *      avoids the race entirely.
   */
  def recordAccessByPlan(plan: LogicalPlan): Unit = {
    val key = indexKey(plan)
    if (key == null) return
    val ids = planIndex.get(key)
    if (ids == null) return
    val it = ids.iterator()
    while (it.hasNext) {
      val cteId = it.next()
      val entry = cache.getIfPresent(cteId)
      if (entry != null) {
        // Found a live match. getIfPresent already refreshed the access time
        // via Guava's expireAfterAccess. Done.
        return
      }
      // Stale entry. Do not touch the index here - removalListener owns it.
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
    planIndex.clear()
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

  /** Test-only: number of distinct buckets in the secondary plan index. */
  private[sql] def planIndexSize: Int = planIndex.size()
}

/**
 * @param plan      The CTE definition's logical plan as it was passed to
 *                  `trackEntry`. Held for `pendingUncache` and for diagnostic
 *                  logging on eviction.
 * @param tableName The synthetic auto_cte_<id> name used by `CacheManager`.
 * @param indexKey  The pre-computed key used to insert into `planIndex`.
 *                  Snapshotted at `trackEntry` time so the removalListener
 *                  (which may run on a Guava cleanup thread where
 *                  `SparkSession.active` is null or wrong) can locate the
 *                  same bucket without re-normalising the plan.
 */
private[sql] case class AutoCTEEntry(
    plan: LogicalPlan,
    tableName: String,
    indexKey: LogicalPlan)
