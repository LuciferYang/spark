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

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, In, InSet}
import org.apache.spark.sql.catalyst.expressions.{IsNotNull, IsNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.{Not, Or, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.ReplaceCTERefWithRepartition
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.columnar.{CachedRDDBuilder, InMemoryRelation}
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
object ReplaceCTERefWithCache extends Rule[LogicalPlan] with PredicateHelper with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.AUTO_REUSED_CTE_ENABLED)) {
      // Sweep anyway. This rule is the only trigger for expiry, so gating it on the flag means
      // that switching the feature off mid-session freezes every entry materialised while it
      // was on: they keep being handed to new plans for the rest of the SparkContext's life,
      // and switching the feature off is precisely when an operator expects that to stop.
      // `everTracked` keeps a process that never enabled the feature from touching
      // `sharedState` at all.
      if (AutoCTECacheManager.everTracked) {
        val spark = SparkSession.active
        spark.sharedState.autoCTECacheManager.evictStaleEntries(spark)
      }
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
   * Five gates, all of which must pass. The fourth, `!hasDivergentPredicates`, is a
   * placeholder that always answers true: it is kept as a named method so the reasoning below
   * about why divergent non-correlated predicates ARE safe to cache stays attached to its
   * subject, and so a future hand-rolled divergence check has an obvious slot. Read it as
   * documentation, not as a gate that rejects anything today.
   *
   *   1. `cteDef.deterministic` - the CTE body must be deterministic. Caching
   *      a CTE containing `rand()`, `current_timestamp()`, etc. would let a
   *      later query reuse the previously-materialised values via the shared
   *      `CacheManager.lookupCachedData`, changing SQL semantics across query
   *      boundaries. Within-query reuse of a non-deterministic multi-ref CTE
   *      remains correct because `ReplaceCTERefWithRepartition` (the fallback
   *      when the auto-cache carve-out is skipped) shares the single
   *      materialisation via shuffle within the query.
   *
   *   2. `isExpensiveEnough` - the CTE body must contain a Join, Aggregate,
   *      Sort, or Window. Cheap scan-only CTEs are not worth materialising.
   *
   *   3. `!cteDef.correlatedSubqueryRef` - the CTE must NOT have any reference
   *      that originally appeared inside a correlated subquery expression.
   *      `TagCorrelatedCTERefs` populates this flag in an early optimizer
   *      batch BEFORE `RewriteCorrelatedScalarSubquery` decorrelates the
   *      subquery into a join. Without the tag, q1/q31/q39a-style queries
   *      cannot be distinguished structurally from q24a's user-written
   *      `cs1 join cs2` self-join at this point in the pipeline.
   *
   *   4. `!hasDivergentPredicates` - placeholder, see method doc.
   *
   *   5. `!cteDef.underSubquery` - a def synthesised by `MergeSubplans` to host a merged
   *      scalar subquery. Its batch (`SparkOptimizer`) runs AFTER both tagging batches and
   *      BEFORE this rule, so such a def arrives with `correlatedSubqueryRef = false` and
   *      `pruningVeto = false` -- neither tagging rule ever saw it, and the flags say
   *      "eligible" only because nothing was in a position to say otherwise.
   *      `ReplaceCTERefWithRepartition` deliberately skips the shuffle for these, so this rule
   *      declining them keeps the two consistent. `InlineCTE` needs no mirror: it runs before
   *      `MergeSubplans` and cannot see such a def.
   */
  private def shouldAutoCache(cteDef: CTERelationDef): Boolean = {
    cteDef.deterministic &&
      !cteDef.correlatedSubqueryRef &&
      !cteDef.pruningVeto &&
      !hasDivergentPredicates(cteDef) &&
      !cteDef.underSubquery &&
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
    if (!isStructurallyExpensive(plan)) return false

    // Row-expanding bodies are never cached; see `isRowExpanding`.
    if (isRowExpanding(plan)) return false

    val sizeOpt = bodySizeEstimate(plan)
    isBigEnough(sizeOpt) && !exceedsBodySizeCeiling(sizeOpt)
  }

  private def isStructurallyExpensive(plan: LogicalPlan): Boolean = plan.exists {
    case _: Join => true
    // `AggregateBase`, not `Aggregate`: `Aggregate`, `PartialAggregate` and `FinalAggregate`
    // are siblings under `AggregateBase` with no subtyping between them, and
    // `Batch("Partial Aggregation Optimization")` sits between `Batch("Inline CTE")` and this
    // rule. With `spark.sql.optimizer.partialAggregationOptimization.enabled` on --
    // which is how the cluster is configured, while the `SQLConf` default is off --
    // `PushPartialAggregationThroughJoin` rewrites an `Aggregate` above a join into
    // `FinalAggregate` over `PartialAggregate`. Matching `Aggregate` alone therefore sees the
    // body at `InlineCTE` and no longer sees it here: `InlineCTE` keeps the def, this rule
    // declines it, and it falls into the round-robin path -- exactly the state the shared
    // structural gate exists to prevent. Measured on 3.5.5: q23a's cache entries went 4 -> 2
    // with 2 `RepartitionByExpression` appearing, q23b 6 -> 2 with 4.
    case _: AggregateBase => true
    case _: Sort => true
    case _: Window => true
    case _ => false
  }

  /**
   * The body's estimated size, or None when the estimate is unavailable.
   *
   * Compare BigInt to BigInt; never call .toLong. Without CBO + column stats, the size estimate
   * for multi-way joins compounds via SizeInBytesOnlyStatsPlanVisitor and routinely exceeds
   * Long.MaxValue. BigInt#toLong silently wraps (sometimes negative, sometimes positive) which
   * would either reject very large CTEs or accept absurd sizes depending on the wrap.
   *
   * NonFatal, not Throwable: a stats-provider bug or an unbound subquery should not fail the
   * query, but a fatal error has to reach the driver. The two bounds below then decide what an
   * unavailable estimate means, and they answer differently on purpose.
   */
  private def bodySizeEstimate(plan: LogicalPlan): Option[BigInt] = {
    try {
      Some(plan.stats.sizeInBytes)
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }

  /**
   * Lower bound: permissive on an unavailable estimate. The structural gate has already passed,
   * so the body is at least job-shaped, and refusing here would silently disable caching for
   * every plan whose stats are unreliable -- a regression for queries that cached on default
   * config.
   */
  private def isBigEnough(sizeOpt: Option[BigInt]): Boolean =
    sizeOpt.forall(_ >= BigInt(conf.getConf(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES)))

  /**
   * Upper bound: restrictive on an unavailable estimate, and never true when the ceiling is
   * disabled with -1 (the default, so this changes nothing until an operator opts in).
   */
  private def exceedsBodySizeCeiling(sizeOpt: Option[BigInt]): Boolean = {
    val maxBodySize = conf.getConf(SQLConf.AUTO_CTE_CACHE_MAX_BODY_SIZE_BYTES)
    maxBodySize >= 0 && !sizeOpt.exists(_ <= BigInt(maxBodySize))
  }

  /**
   * True when the body has a Join with nothing above it that can collapse rows -- so caching it
   * can only store more rows than it reads, and the cache is a net loss however big the body is.
   *
   * This replaced an estimate-based admission cap, and the history matters because the obvious
   * fix is the one that failed. Capping inside `isExpensiveEnough` alone sent the rejected body
   * to `ReplaceCTERefWithRepartition`: measured 22 min on TPC-DS q95 against 8 min with no cap
   * at all and 1.4 min for the pre-feature baseline. Mirroring the cap into
   * `InlineCTE.isAutoCacheEligible` instead killed the caching of q14a/q14b/q24a/q24b, because
   * `InlineCTE` runs before predicate pushdown and its estimates are systematically larger --
   * q14a was rejected even at a 1000 TiB threshold. A structural signal has neither problem: it
   * reads no statistics, so both sides reach the same verdict despite running at different
   * points in the pipeline.
   *
   * `AggregateBase`, not `Aggregate`: the three are siblings with no subtyping between them and
   * `Batch("Partial Aggregation Optimization")` runs between `InlineCTE` and this rule. See
   * `isExpensiveEnough`.
   *
   * `private[sql]` so `ReplaceCTERefWithRepartition`'s side of the gate can be reviewed against
   * this one; the catalyst copy in `InlineCTE` MUST stay identical, and no test catches a
   * divergence directly.
   */
  private[sql] def isRowExpanding(plan: LogicalPlan): Boolean = {
    def collapsesRows(p: LogicalPlan): Boolean = p match {
      case _: AggregateBase => true
      case _: Distinct => true
      case _: Intersect => true
      case _: Except => true
      case _ => false
    }
    plan.exists(_.isInstanceOf[Join]) && !plan.exists(collapsesRows)
  }

  /**
   * True when `shouldAutoCache` declined this def for the body-size ceiling and nothing else.
   * Such a def is tagged with `ReplaceCTERefWithRepartition.SKIP_EXTRA_REPARTITION` so that rule
   * plugs the body in per reference instead of wrapping it in a round-robin shuffle, which on an
   * oversized body is the most expensive of the three available shapes.
   *
   * This is the normal path for an oversized body, not a corner case:
   * `InlineCTE.isAutoCacheEligible` deliberately does not mirror the ceiling (see
   * `isRowExpanding`), so it keeps such a def for this rule, this rule then declines it, and
   * without the tag it would land on the round-robin shuffle. The tag carries this rule's
   * post-pushdown verdict forward to that fallback, which `InlineCTE` could not have reached:
   * it runs before `V2ScanRelationPushDown`, so its estimate is of the unpruned table.
   */
  private def declinedOnlyForBodySize(cteDef: CTERelationDef): Boolean = {
    if (!cteDef.deterministic ||
        cteDef.correlatedSubqueryRef ||
        cteDef.pruningVeto ||
        !isStructurallyExpensive(cteDef.child) ||
        isRowExpanding(cteDef.child)) {
      return false
    }
    val sizeOpt = bodySizeEstimate(cteDef.child)
    isBigEnough(sizeOpt) && exceedsBodySizeCeiling(sizeOpt)
  }

  /**
   * The def to hand to `ReplaceCTERefWithRepartition`, carrying the `SKIP_EXTRA_REPARTITION` tag
   * when the only reason we declined to cache it was the body-size ceiling. `copy` is the
   * case-class copy, which does NOT carry tree-node tags over, so the tag is set on the new def
   * rather than the old one.
   */
  private def skippedDef(cteDef: CTERelationDef, resolvedChild: LogicalPlan): CTERelationDef = {
    val newDef = cteDef.copy(child = resolvedChild)
    if (declinedOnlyForBodySize(cteDef)) {
      newDef.setTagValue(ReplaceCTERefWithRepartition.SKIP_EXTRA_REPARTITION, ())
    }
    newDef
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
          skippedDefs += skippedDef(cteDef, resolvedChild)
        } else {
          // Cache the *pre-pushdown* body so the cache shape stays stable
          // across queries with different per-reference predicates.
          // PushdownPredicatesAndPruneColumnsForCTEDef merges per-reference
          // filters via OR and pushes the result into the body as a
          // top-level Filter. Caching that Filter-wrapped body would yield
          // a query-specific canonical plan and prevent cross-query reuse
          // (e.g. q39a uses d_moy IN (1,2), q39b uses d_moy IN (4,5);
          // cached bodies diverge so each query rebuilds the cache).
          //
          // The pre-pushdown body is preserved by
          // PushdownPredicatesAndPruneColumnsForCTEDef in
          // `cteDef.originalPlanWithPredicates._1`. We use it as the cache
          // body and Project to `cteDef.output` to retain column pruning.
          // Per-reference predicates remain above the CTERelationRef
          // (substituted with the InMemoryRelation), so the cache scan
          // pushes them down at execution time -- semantically equivalent
          // to the previous behaviour, with a possibly larger cache and
          // genuine cross-query reuse.
          val resolvedCacheBody = replaceWithCache(spark, prePushdownBody(cteDef), cteMap)

          // If the body still references sibling CTEs that we did NOT
          // cache (e.g. a runtime-filter CTE from InjectRuntimeFilter or a
          // merged-subplan CTE from MergeSubplans, sitting alongside the
          // cached one in q24a), wrap the body with WithCTE before handing
          // it to CacheManager. CacheManager.cacheQuery rebuilds the plan
          // via InMemoryRelation -> optimizedPlan -> Analyzer -> InlineCTE
          // and InlineCTE.buildCTEMap throws `key not found: <id>` when it
          // walks an unresolved CTERelationRef whose CTERelationDef is not
          // in scope. The wrapper restores scope; InlineCTE then inlines
          // the sibling defs into the cached body.
          //
          // Defensive guard: when the body (or a sibling we'd put in the
          // wrapper) references CTE defs from outside this WithCTE scope --
          // e.g. an outer-scope skipped def in a nested WithCTE -- the
          // wrap is insufficient and the rebuild would still crash. In
          // that case skip caching this cteDef and let
          // ReplaceCTERefWithRepartition handle it via shuffle reuse.
          // Common case (q24a flat WithCTE): guard does not fire.
          // Only the skipped siblings this body can actually REACH go into the wrapper.
          // The wrapped plan is the cache key, so passing every skipped def would let an
          // unrelated sibling declared earlier in the same `WithCTE` change the key: the
          // same body would then miss `lookupCachedData` and materialise a second copy of
          // the same data, which is exactly what `prePushdownBody` exists to prevent.
          // Transitive, because a sibling the body needs may reference another skipped one.
          val neededSkipped = reachableSkippedDefs(resolvedCacheBody, skippedDefs.toSeq)
          val unresolved = collectCTERefIds(resolvedCacheBody) -- cteMap.keys
          val skippedIds = skippedDefs.iterator.map(_.id).toSet
          val transitiveRefs =
            neededSkipped.flatMap(d => collectCTERefIds(d.child)).toSet
          val outOfScope =
            (unresolved ++ transitiveRefs) -- cteMap.keys -- skippedIds

          if (outOfScope.nonEmpty) {
            skippedDefs += skippedDef(cteDef, resolvedChild)
          } else {
            // `neededSkipped.nonEmpty` rather than `unresolved.nonEmpty`: on this branch
            // `outOfScope` is empty, so every unresolved id IS a skipped sibling and the two
            // conditions coincide. Keying the wrap on what actually goes into it keeps the
            // two lines from drifting apart.
            val cacheBody: LogicalPlan = if (neededSkipped.nonEmpty) {
              // `originalPlanWithPredicates` is dropped from the copies that go into the
              // wrapper. That field is optimizer bookkeeping rather than a child plan, so
              // `QueryPlan.canonicalized` never normalizes the ExprIds inside it: the same
              // sibling body canonicalizes differently in every query (measured on one body:
              // `key#14L` against `key#141L`), which alone is enough to make the cache key
              // unmatchable across queries. The wrapper exists only to restore scope for the
              // re-analysis inside `cacheQuery`, and that sub-optimization re-snapshots the
              // field itself if it pushes anything down.
              //
              // The sibling bodies are stripped of `DynamicPruningSubquery` for the same
              // reason `prePushdownBody` strips the cached body: they come from
              // `cteDef.child`, i.e. after the `PartitionPruning` batch, and the whole
              // wrapper is re-analyzed inside `prepareCachedData`, where a subquery
              // referencing the outer query's build side fails `CheckAnalysis`.
              WithCTE(
                resolvedCacheBody,
                neededSkipped.map(d => d.copy(
                  child = ReplaceCTERefWithRepartition.stripDynamicPruning(d.child),
                  originalPlanWithPredicates = None)))
            } else {
              resolvedCacheBody
            }

            val cacheManager = spark.sharedState.cacheManager
            val autoCTEManager = spark.sharedState.autoCTECacheManager
            val cachedPlan: Option[LogicalPlan] = cacheManager
              .lookupCachedData(spark, cacheBody)
              .map { cached =>
                // Cache hit -- refresh TTL for the matching entry
                autoCTEManager.recordAccessByPlan(cacheBody)
                cached.cachedRepresentation.withOutput(resolvedCacheBody.output)
              }
              .orElse {
                // `checkValues` accepts NONE because it is a valid `StorageLevel` name, so an
                // operator can reach this. Signal "do not cache" rather than returning the
                // body: the def then falls through to `skippedDefs` and
                // `ReplaceCTERefWithRepartition` still shares it via shuffle, whereas
                // returning the body would inline it at every reference and lose that reuse.
                val configuredLevel = StorageLevel
                  .fromString(conf.getConf(SQLConf.AUTO_CTE_CACHE_STORAGE_LEVEL))
                if (configuredLevel == StorageLevel.NONE) {
                  None
                } else {
                  cacheManager.prepareCachedData(
                    spark,
                    cacheBody,
                    tableName = Some(s"auto_cte_${cteDef.id}"),
                    configuredLevel) match {
                    case Some(cd) =>
                      // Publication waits for execution; see `AutoCTECacheManager.publishPending`.
                      // Tracking waits with it, so `numEntries` counts materialisations rather
                      // than plans and the TTL clock starts when the data does.
                      autoCTEManager.deferPublish(cteDef.id, cd)
                      Some(cd.cachedRepresentation.withOutput(resolvedCacheBody.output))
                    case None => None
                  }
                }
              }

            cachedPlan match {
              case Some(p) => cteMap.put(cteDef.id, p)
              case None => skippedDefs += skippedDef(cteDef, resolvedChild)
            }
          }
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

  /**
   * Returns the CTE body to cache, stripped of the OR-merged per-reference
   * predicate that `PushdownPredicatesAndPruneColumnsForCTEDef` injects
   * into `cteDef.child`. This keeps the cached canonical plan stable
   * across queries that reference the CTE with different downstream
   * filters, enabling cross-query reuse via
   * `CacheManager.lookupCachedData`.
   *
   * Column pruning is retained via a `Project` to `cteDef.output` so the
   * cache footprint is no larger than the columns the references actually
   * need. When `cteDef.originalPlanWithPredicates` is `None`, the rule
   * never fired, so `cteDef.child` is itself the un-pushdown body.
   *
   * Either body may carry `DynamicPruningSubquery`s that `PartitionPruning`
   * injected in an earlier batch and that reference the OUTER query's build
   * side; they cannot survive the re-analysis `cacheQuery` performs, so they
   * are stripped here. See `ReplaceCTERefWithRepartition.stripDynamicPruning`
   * for why only that node may be dropped, and `AutoCteDppReproSuite` for the
   * TPC-DS q5a shape that fails without it.
   */
  private def prePushdownBody(cteDef: CTERelationDef): LogicalPlan = {
    val body = cteDef.originalPlanWithPredicates match {
      // The pre-pushdown body may contain non-deterministic expressions
      // (e.g. `rand()`) that column pruning later strips out, leaving
      // `cteDef.child` deterministic but `originalPlan` not. Caching the
      // non-deterministic body would let two queries see different rand
      // seeds in the cached canonical plan, defeating cross-query reuse.
      // Fall back to `cteDef.child` (post-pushdown) in that case -- the
      // existing pre-fix behaviour, where pruning had already eliminated
      // the non-deterministic columns.
      case Some((originalPlan, preds)) if originalPlan.deterministic =>
        // Pruning is expressed as a `Project` over whatever body we end up with, so that an
        // injected predicate sits BELOW it and is therefore always resolvable.
        def pruned(plan: LogicalPlan): LogicalPlan =
          if (originalPlan.output == cteDef.output) plan else Project(cteDef.output, plan)
        pushablePartOfMergedPredicate(cteDef, originalPlan, preds) match {
          case Some(pushable)
              if conf.getConf(SQLConf.AUTO_CTE_CACHE_INJECT_IMPLIED_PREDICATE) &&
                canInject(cteDef, pushable, originalPlan) =>
            logInfo(s"Injecting $pushable into the cached body of CTE ${cteDef.id}: the part " +
              s"of the merged predicate that is implied by it and reads only " +
              s"grouping-derived columns, so it is a single top-level conjunct that " +
              s"`PushPredicateThroughNonJoin` can move below the body's aggregate. The rest " +
              s"of the merged predicate is NOT kept -- each reference still applies its own " +
              s"predicate above the cache. Set " +
              s"${SQLConf.AUTO_CTE_CACHE_INJECT_IMPLIED_PREDICATE.key} to false to cache the " +
              s"whole merged predicate instead.")
            pruned(Filter(pushable, originalPlan))
          case Some(pushable) =>
            logInfo(s"Caching the post-pushdown body of CTE ${cteDef.id}: $pushable, the part " +
              s"of the merged predicate that can be pushed below its aggregate, so the first " +
              s"execution reads fewer rows. NOTE the cached body carries the WHOLE merged " +
              s"predicate, not just this part. This trades cross-query reuse for a cheaper " +
              s"first execution; set " +
              s"${SQLConf.AUTO_CTE_CACHE_KEEP_MERGED_PREDICATE.key} to false to keep the reuse.")
            // Keep `cteDef.child`, i.e. the body WITH the merged predicate. Column pruning is
            // already part of it, so no extra `Project` is needed.
            cteDef.child
          case None => pruned(originalPlan)
        }
      case _ => cteDef.child
    }
    ReplaceCTERefWithRepartition.stripDynamicPruning(body)
  }

  /**
   * The part of the OR-merged per-reference predicate that can reach the body's scans, or
   * `None` when no part can.
   *
   * `Some` means the caller should stop caching the plain pre-pushdown body. WHAT it caches
   * instead is the caller's choice between two shapes -- inject just this part, or keep the
   * whole merged predicate -- see `prePushdownBody`.
   *
   * ONE signal, structural: `groupingDerivedOutput` requires the part to read only columns
   * derived from the body aggregate's grouping expressions, and only non-aggregate ones. That
   * is SUFFICIENT for `PushPredicateThroughNonJoin` to push it, but not necessary, so a `None`
   * is not proof that nothing could have been pushed. A predicate over an aggregate OUTPUT
   * (`sum(...) > 0`) genuinely cannot move; when nothing can move, touching the body would
   * shrink the cached relation while still reading and aggregating everything -- all of the
   * cost of losing reuse, none of the saving.
   *
   * Taken from `preds` rather than from the `Filter` inside `cteDef.child`, because by the time
   * this rule runs `PushDownPredicates` has moved that filter below the aggregate and the join,
   * so extracting "the part over grouping columns" from it yields nothing. That is why the
   * first version of this gate never fired.
   *
   * Fails CLOSED: no aggregate, a reference without a predicate, a residual that cannot remove
   * rows, or any error yields `None`, i.e. the pre-pushdown body.
   */
  private def pushablePartOfMergedPredicate(
      cteDef: CTERelationDef,
      originalPlan: LogicalPlan,
      preds: Seq[Expression]): Option[Expression] = {
    if (!conf.getConf(SQLConf.AUTO_CTE_CACHE_KEEP_MERGED_PREDICATE)) return None
    // `isTruePredicate` recognises `PushdownPredicatesAndPruneColumnsForCTEDef`'s exact
    // "no predicate" sentinel; a `TrueLiteral` among real predicates would make `reduce(Or)`
    // yield `true`, which `narrowsRows` rejects as well.
    if (preds.isEmpty || isTruePredicate(preds) || preds.exists(_ == TrueLiteral)) return None
    try {
      extractPushablePart(preds.reduce(Or), originalPlan)
    } catch {
      // Same reasoning as `bodySizeEstimate`: a rule bug or an unbound subquery must not fail
      // the query, and the safe direction is the old behaviour.
      case scala.util.control.NonFatal(e) =>
        logWarning(s"Could not decide whether to keep the pushed predicate for CTE " +
          s"${cteDef.id}; caching the pre-pushdown body.", e)
        None
    }
  }

  /**
   * Whether `PushdownPredicatesAndPruneColumnsForCTEDef` is signalling "no predicate". Copies
   * that rule's own private `isTruePredicate`, whose sentinel -- exactly `Seq(TrueLiteral)` --
   * is part of its contract.
   */
  private def isTruePredicate(preds: Seq[Expression]): Boolean =
    preds.length == 1 && preds.head == TrueLiteral
  /**
   * Whether `pushable` is safe to inject as a top-level conjunct over `originalPlan`.
   *
   * TWO substantive tests: it must hold no `SubqueryExpression`, and injecting must not drop
   * runtime Bloom filters the body already carries. Resolvability needs no test -- the injected
   * `Filter` goes BELOW the pruning `Project`, over `originalPlan` itself, and `pushable`'s
   * references are within `groupingDerivedOutput(originalPlan)` by construction.
   *
   * WHY SUBQUERIES ARE EXCLUDED, and why the sibling `keepMergedPredicate` shape needs no such
   * exclusion. `SubqueryExpression.references` is only its `outerAttrs`, empty for an
   * uncorrelated subquery, so `extractPredicatesWithinOutputSet` admits `dyear IN (SELECT ...)`
   * whenever `dyear` is grouping-derived, and `narrowsRows` does not reject it either (an
   * `Unevaluable` is not foldable). Two consequences, both bad: `collectCTERefIds` walks
   * subqueries, so an injected condition can add CTE ids the body did not reference, and if one
   * names a def outside this `WithCTE` scope the def is not cached at all; and `preds` is a
   * snapshot from catalyst's own batch, whereas `RewritePredicateSubquery` runs after
   * `operatorOptimizationBatch`, so `cteDef.child`'s copy of the same predicate has already
   * become a join while this snapshot is still a raw `InSubquery`. The two shapes are NOT
   * equivalent here, which is exactly the reasoning error that had this guard deleted once.
   *
   * Determinism is deliberately NOT tested: a `rand()` leaf has an empty reference set, so
   * `subsetOf` admits it vacuously -- but `cteDef.child` carries the same leaf inside the merged
   * predicate, so both shapes evaluate it twice. That is a pre-existing property of the
   * `keepMergedPredicate` path, not something injection introduces.
   */
  private def canInject(
      cteDef: CTERelationDef,
      pushable: Expression,
      originalPlan: LogicalPlan): Boolean = {
    if (pushable.exists(_.isInstanceOf[SubqueryExpression])) {
      logWarning(s"Not injecting $pushable into the cached body of CTE ${cteDef.id}: it holds " +
        s"a subquery, whose CTE references would enter the cached body's scope and could " +
        s"force this def out of the cache entirely. Falling back to caching the whole merged " +
        s"predicate.")
      return false
    }
    // Cannot fail by construction; checked so that a change to `groupingDerivedOutput` or to
    // the pushdown rule surfaces as a log line and a missed optimization rather than as an
    // unresolved plan inside `prepareCachedData`.
    if (!pushable.references.subsetOf(originalPlan.outputSet)) {
      logWarning(s"Not injecting $pushable into the cached body of CTE ${cteDef.id}: its " +
        s"references are not all in the body's output. This should be impossible; it means " +
        s"the extracted part and the body have drifted apart. Falling back to caching the " +
        s"whole merged predicate.")
      return false
    }
    if (wouldLoseRuntimeFilters(cteDef, originalPlan)) {
      logWarning(s"Not injecting $pushable into the cached body of CTE ${cteDef.id}: the " +
        s"post-pushdown body carries runtime Bloom filters that the pre-pushdown snapshot " +
        s"predates, and re-optimizing inside `prepareCachedData` does not re-derive all of " +
        s"them. Caching the whole merged predicate keeps them.")
      return false
    }
    true
  }
  /**
   * Whether injecting would DROP runtime Bloom filters that `cteDef.child` already carries.
   *
   * `InjectRuntimeFilter` runs in `SparkOptimizer`'s own batch, long after
   * `PushdownPredicatesAndPruneColumnsForCTEDef` wrote `originalPlanWithPredicates` in
   * catalyst's operator-optimization fixed point. It reaches `cteDef.child` -- defs are children
   * of `WithCTE`, so `transformUp` descends -- but can never reach the snapshot, which is a
   * case-class FIELD, not a child. So the snapshot structurally predates every Bloom filter in
   * the body. Re-optimizing inside `prepareCachedData` re-derives SOME of them, but the
   * per-key-pair dedup in `InjectRuntimeFilter` skips a join whose either side already carries a
   * filter on its key, so one fresh pass cannot reproduce what two accumulated passes had.
   *
   * Measured on local sf100, TPC-DS q64: `cteDef.child` carries 6 filters, the injected body 2,
   * and the `cs_ui` aggregate's input goes from 107K rows back to 143,997,065 (1345x), first
   * execution 11382ms -> 36270ms.
   *
   * A COUNT comparison rather than `count(cteDef.child) > 0`, because the snapshot is not
   * unconditionally bloom-free: `OptimizeSubqueries` runs the whole optimizer on each subquery
   * expression's plan BEFORE the snapshot is written, so a body holding a scalar/IN subquery
   * with a bloom-eligible join arrives with that filter already in place, and
   * `collectWithSubqueries` counts it on both sides, where it cancels.
   *
   * The direction is safe either way: a false positive falls back to the whole-predicate shape.
   */
  private def wouldLoseRuntimeFilters(
      cteDef: CTERelationDef,
      originalPlan: LogicalPlan): Boolean =
    // 3.5.5 guards this with `containsPattern(BLOOM_FILTER)` as a cheap bit-test fast path.
    // This branch's catalyst has no such tree pattern, so the two walks always run. The
    // comparison alone is equivalent -- `count(child) > count(snapshot)` already implies the
    // child has at least one -- the fast path only saved the walks.
    countRuntimeFilters(cteDef.child) > countRuntimeFilters(originalPlan)

  private def countRuntimeFilters(plan: LogicalPlan): Int =
    plan.collectWithSubqueries { case p =>
      p.expressions.map(_.collect { case _: BloomFilterMightContain => 1 }.sum).sum
    }.sum

  /**
   * The part of `condition` that can be pushed below `body`'s aggregate, or `None` when no part
   * can be.
   *
   * A PART of the predicate is enough, and that is the whole point. DO NOT tighten this to "the
   * whole predicate must push": that was tried and reverted. q4's references filter on a
   * grouping column AND on aggregate outputs (`dyear = 2001 AND year_total > 0`), so requiring
   * the whole predicate excludes exactly the query the gate was written for. The tightening
   * looked justified on local sf100, where the year predicate already reaches the fact scans
   * through the date_dim join -- sf100 cannot see the effect this gate targets. On the 100TB
   * non-partitioned ORC cluster the cached body's date_dim scan carries only
   * `isnotnull(d_date_sk)`, so all 73049 rows are read and 504e9 fact rows enter a partial
   * aggregate costing 896.6h against 170.4h of scan, for a query that needs two years.
   */
  private def extractPushablePart(
      condition: Expression,
      body: LogicalPlan): Option[Expression] = {
    val pushableCols = groupingDerivedOutput(body)
    if (pushableCols.isEmpty) {
      None
    } else {
      extractPredicatesWithinOutputSet(condition, AttributeSet(pushableCols))
        .filter(narrowsRows)
        // ORDER MATTERS, and not only for cost. This rejection and `canInject`'s have DIFFERENT
        // fallbacks: a `None` here yields the pre-pushdown body, while any `canInject` failure
        // yields `cteDef.child` with the whole merged predicate. q74 trips both -- its
        // extraction is implied AND its body carries Bloom filters the snapshot lacks -- and the
        // measured shapes are 20236ms pre-pushdown, 39559ms whole-predicate, 45934ms injected.
        // So the implied test must win, which it does by running first. Folding it into
        // `canInject`, or hoisting `canInject` ahead of it as a cheap early-out, would hand q74
        // the 2x.
        .filterNot(impliedByBody(_, body))
    }
  }
  /**
   * Whether `pred` is already guaranteed by `body`, i.e. it cannot remove a single row from it.
   *
   * `narrowsRows` asks whether a predicate can remove rows AT ALL; this asks whether it can
   * remove rows FROM THIS BODY. TPC-DS q74 is the case that needs the second question: its body
   * already says `d_year IN (2001, 2002)`, and the part extracted from its references is
   * `year = 2001 OR year = 2002` -- a tautology over that body, so it pays the full price of a
   * query-specific entry for zero rows removed (measured: 20236ms against 39559ms and 45934ms).
   *
   * Contrast the queries where the same extraction pays: q4 and q11 join `date_dim` with no date
   * predicate at all, so their bodies read all 73049 rows and the injected part takes that to
   * 20000. q39a/q39b DO pin `d_year` in the body -- but their references filter on `d_moy`,
   * which the body does not constrain, so the extraction is not implied. That is why this test is
   * per ATTRIBUTE and not "does the body have a date filter".
   *
   * Implemented as value-set containment rather than via `constraints.contains`, because the two
   * sides are canonically DIFFERENT for exactly the shape that matters: the body's constraint is
   * `In(year, [2001, 2002])` while the predicate is `Or(EqualTo(year, 2001), EqualTo(year,
   * 2002))`. That is also why `PruneFilters` does not already delete the injected filter.
   *
   * Fails CLOSED: any shape this cannot reduce to (attribute -> value set) yields false, so the
   * predicate is kept.
   */
  private def impliedByBody(pred: Expression, body: LogicalPlan): Boolean = {
    valueSetOf(pred) match {
      case Some((attr, predValues)) =>
        // The body implies `pred` when the values the body admits for that attribute are a
        // SUBSET of the values `pred` admits: every surviving row already satisfies it.
        body.constraints.exists { c =>
          valueSetOf(c) match {
            case Some((cAttr, bodyValues)) =>
              cAttr.semanticEquals(attr) && bodyValues.nonEmpty && bodyValues.subsetOf(predValues)
            case None => false
          }
        }
      case None => false
    }
  }

  /**
   * `pred` as (attribute, the set of literal values it admits), for the shapes an
   * equality-style restriction takes: `In`/`InSet`, `EqualTo` against a literal, an `Or` of
   * those on ONE attribute, and an `And` mixing them with null checks. `None` for anything else,
   * since the caller treats `None` as "not implied", which keeps the predicate.
   *
   * `IsNotNull` CONJUNCTS ARE DROPPED, and that is load-bearing. The extracted predicate arrives
   * with the analyzer's `IsNotNull` conjuncts baked into every disjunct, so an implementation
   * handling only the bare `Or` of `EqualTo` returns `None` and the guard silently never fires.
   * That was the first version.
   *
   * `IsNull` IS NOT DROPPED, and the asymmetry is the whole correctness of this function. The two
   * call sites need different things: on the BODY side an over-large set is safe (it makes
   * `subsetOf` harder, so implication is claimed less often), but on the PREDICATE side every
   * value in the returned set must actually make `pred` TRUE. Dropping `IsNull(a)` from
   * `And(IsNull(a), b = 2)` would return `(b, {2})` and claim implication against a body pinning
   * `b = 2` -- declining to inject a predicate that removes every row where `a` is NOT null.
   * `narrowsRows` draws the same line, and the two must agree.
   *
   * An `And` of two value-set restrictions on the same attribute is an INTERSECTION. A
   * conjunction across DIFFERENT attributes yields `None`: it restricts more than one column, so
   * it is not a value set on one, and treating either side alone as the whole predicate would
   * claim implication the body does not provide.
   */
  private def valueSetOf(pred: Expression): Option[(Expression, Set[Any])] = pred match {
    case In(value, list) if list.forall(_.isInstanceOf[Literal]) =>
      Some((value, list.map { case l: Literal => l.value }.toSet))
    case InSet(value, hset) => Some((value, hset.toSet[Any]))
    case EqualTo(a, l: Literal) => Some((a, Set(l.value)))
    case EqualTo(l: Literal, a) => Some((a, Set(l.value)))
    case _: IsNotNull | _: IsNull => None
    case And(l, r) =>
      // `IsNotNull` conjuncts are noise here; any OTHER conjunct this cannot read makes the
      // whole predicate unreadable, because ignoring it would overstate what `pred` admits.
      (dropIsNotNullConjuncts(l), dropIsNotNullConjuncts(r)) match {
        case (None, None) => None
        case (Some(only), None) => valueSetOf(only)
        case (None, Some(only)) => valueSetOf(only)
        case (Some(le), Some(re)) =>
          for {
            (la, lv) <- valueSetOf(le)
            (ra, rv) <- valueSetOf(re)
            if la.semanticEquals(ra)
          } yield (la, lv intersect rv)
      }
    case Or(l, r) =>
      // Both sides must restrict the SAME attribute, or the union is not a value set on one
      // column: `Or(a = 1, b = 2)` admits rows where `a` is anything.
      for {
        (la, lv) <- valueSetOf(l)
        (ra, rv) <- valueSetOf(r)
        if la.semanticEquals(ra)
      } yield (la, lv ++ rv)
    case _ => None
  }
  /**
   * `e` with `IsNotNull` conjuncts removed, or `None` when nothing else remains. `IsNull` is
   * deliberately NOT removed -- see `valueSetOf`.
   */
  private def dropIsNotNullConjuncts(e: Expression): Option[Expression] = {
    val kept = splitConjunctivePredicates(e).filterNot(_.isInstanceOf[IsNotNull])
    kept.reduceOption(And)
  }

  /**
   * Whether `pred` can actually remove rows, i.e. it is worth giving up cross-query reuse for.
   *
   * Two shapes are worthless here and both survive extraction: `IsNotNull`, which the analyzer
   * adds to nearly every join and comparison (on a `count(*)` body those were all that was left,
   * and the gate accepted them); and a foldable leaf, since nothing guarantees
   * `PruneFilters`/`ConstantFolding` have run on `preds` -- it is a snapshot the pushdown rule
   * stored, and those rules are excludable.
   *
   * The test is per DISJUNCT, not over all leaves. The merged predicate is an OR across
   * references, so `Or(a, b)` only narrows when BOTH sides do: `Or(d_year = 2001, isnotnull(k))`
   * keeps every non-null row. An earlier version asked whether ANY leaf anywhere was
   * non-`IsNotNull`, which admitted exactly that shape. Conjunction is the opposite: `And(a, b)`
   * narrows if EITHER side does.
   *
   * A `Not` over `And`/`Or`/`Not` is pushed through by De Morgan rather than falling to the
   * default, so `NOT (a IS NULL OR b IS NULL)` is rejected like any other pure null check. Every
   * OTHER `Not` does fall to the default, which is right: `Not(IsNotNull(x))` is `IsNull(x)` and
   * DOES narrow. All three shapes are reachable because `BooleanSimplification` normalises them
   * away but is not in `nonExcludableRules`.
   */
  private def narrowsRows(pred: Expression): Boolean = pred match {
    case Or(l, r) => narrowsRows(l) && narrowsRows(r)
    case And(l, r) => narrowsRows(l) || narrowsRows(r)
    // Both `Not` arms recurse on a NEWLY BUILT `Not` rather than on a subexpression, so
    // termination is by total node count, which strictly decreases.
    case Not(Or(l, r)) => narrowsRows(Not(l)) || narrowsRows(Not(r))
    case Not(And(l, r)) => narrowsRows(Not(l)) && narrowsRows(Not(r))
    case Not(Not(e)) => narrowsRows(e)
    case _: IsNotNull => false
    case Not(_: IsNull) => false
    case e if e.foldable => false
    case _ => true
  }

  /**
   * The output attributes that are functions of the aggregate's grouping expressions only.
   *
   * Both tests are load-bearing: `count(cust)` over `GROUP BY cust` reads only a grouping column
   * and still cannot be pushed below the aggregate, because `getAliasMap` skips aliases holding
   * an `AggregateExpression`, so `PushPredicateThroughNonJoin` cannot move `n = 25` below it.
   * Stated over the output expressions rather than only over the aggregate functions, so it holds
   * for this fork's `PartialAggregate`/`FinalAggregate` split as well.
   *
   * `references.nonEmpty` excludes a reference-free output -- `count(1)`, or a literal alias such
   * as q4's `'s' AS sale_type`. Both satisfy `subsetOf` vacuously, so without the guard they are
   * reported as grouping-derived even though neither is a function of the grouping keys. This is
   * STRICTER than `PushPredicateThroughNonJoin`'s own guard, and the direction is conservative: a
   * column left out can only make `extractPushablePart` decline.
   *
   * A `Union` body (q4's three channels, q74's two) passes a position only when every branch
   * does, which is what `PushPredicateThroughNonJoin` requires to push into all of them.
   */
  private def groupingDerivedOutput(plan: LogicalPlan): Seq[Attribute] = plan match {
    case agg: AggregateBase =>
      val groupingAttrs = AttributeSet(agg.groupingExpressions.flatMap(_.references)) ++
        AttributeSet(agg.groupingExpressions.collect { case n: NamedExpression =>
          n.toAttribute
        })
      agg.aggregateExpressions.zipWithIndex.collect {
        case (e, i) if e.references.nonEmpty && e.references.subsetOf(groupingAttrs) &&
            e.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
          agg.output(i)
      }

    case u: Union =>
      val perBranch = u.children.map(groupingDerivedOutput)
      if (perBranch.isEmpty || perBranch.exists(_.isEmpty)) {
        Nil
      } else {
        // Positions every branch passes, expressed in the union's own attributes.
        val positions = u.children.zip(perBranch).map { case (c, ok) =>
          val okSet = AttributeSet(ok)
          c.output.zipWithIndex.collect { case (a, i) if okSet.contains(a) => i }.toSet
        }.reduce(_ intersect _)
        u.output.zipWithIndex.collect { case (a, i) if positions.contains(i) => a }
      }

    case p: Project =>
      val childOk = AttributeSet(groupingDerivedOutput(p.child))
      p.projectList.zipWithIndex.collect {
        case (e, i) if e.references.nonEmpty && e.references.subsetOf(childOk) => p.output(i)
      }

    case f: Filter => groupingDerivedOutput(f.child)

    case _ => Nil
  }

  /**
   * The subset of `skipped` that `body` can reach, transitively, in declaration order.
   *
   * Used to decide what goes into the `WithCTE` wrapper around a body about to be cached.
   * Declaration order is preserved because `WithCTE` requires a def to precede the defs that
   * reference it, and `skipped` is accumulated in declaration order.
   */
  private def reachableSkippedDefs(
      body: LogicalPlan,
      skipped: Seq[CTERelationDef]): Seq[CTERelationDef] = {
    if (skipped.isEmpty) {
      return Nil
    }
    val byId = skipped.map(d => d.id -> d).toMap
    val needed = mutable.HashSet.empty[Long]
    val pending = mutable.Queue.empty[Long]
    pending ++= collectCTERefIds(body)
    while (pending.nonEmpty) {
      val id = pending.dequeue()
      // Ids that are not skipped siblings are either already cached or out of scope; the
      // caller's `outOfScope` check handles the latter.
      byId.get(id).foreach { d =>
        if (needed.add(id)) {
          pending ++= collectCTERefIds(d.child)
        }
      }
    }
    skipped.filter(d => needed.contains(d.id))
  }

  /**
   * Collects all `CTERelationRef.cteId`s reachable from `plan`, including
   * those inside subquery expressions (scalar subqueries, IN/EXISTS, etc.).
   * Used by `replaceWithCache` to decide whether the cached body needs a
   * `WithCTE` wrapper.
   */
  private def collectCTERefIds(plan: LogicalPlan): Set[Long] = {
    val ids = scala.collection.mutable.HashSet.empty[Long]
    plan.foreach {
      case r: CTERelationRef => ids += r.cteId
      case _ =>
    }
    plan.foreach { p =>
      p.expressions.foreach { e =>
        e.foreach {
          case sq: SubqueryExpression => ids ++= collectCTERefIds(sq.plan)
          case _ =>
        }
      }
    }
    ids.toSet
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

  // Plans whose tracker entry expired and which must stop being reused by NEW plans. Drained by
  // `evictStaleEntries` via `CacheManager.stopReusing`, which unregisters WITHOUT unpersisting.
  private val pendingExpiry = new java.util.concurrent.ConcurrentLinkedQueue[LogicalPlan]()

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

  /** Guava key generator: one id per materialisation. See `trackEntry` for why not `cteId`. */
  private val nextEntryId = new java.util.concurrent.atomic.AtomicLong()

  private val cache: Cache[java.lang.Long, AutoCTEEntry] = {
    val builder = CacheBuilder.newBuilder()
      .removalListener((notification: RemovalNotification[java.lang.Long, AutoCTEEntry]) => {
        if (notification.wasEvicted()) {
          val entry = notification.getValue
          pendingExpiry.add(entry.plan)
          // Deliberately NOT unpersisting here. Expiry is not `UNCACHE TABLE`: a DataFrame is
          // lazy, so one built an hour ago and collected now still holds the `InMemoryRelation`
          // its plan was rewritten to. Unpersisting because an idle timer fired turns the next
          // `collect()` into a full recompute of a body that was already materialised --
          // strictly worse than never having cached it, and reachable on the default 1h TTL
          // with nothing configured. Dropping the entry already stops NEW plans from reusing it;
          // the blocks go to `ContextCleaner`, which frees them once the last plan referencing
          // them is unreachable. `clearAll` still unpersists, because its callers
          // (`spark.catalog.clearCache()`, test teardown) asked for exactly that.
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
          logInfo(log"Evicted auto-cached CTE " +
            log"${MDC(LogKeys.TABLE_NAME, entry.tableName)}")
        }
      })
    if (ttlMs > 0) {
      builder.expireAfterAccess(ttlMs, java.util.concurrent.TimeUnit.MILLISECONDS)
    }
    if (maxSizeBytes > 0) {
      builder
        .maximumWeight(maxSizeBytes)
        // Guava enforces `maximumWeight` per SEGMENT, and a handful of entries spread over the
        // default 4 segments makes the budget mean a quarter of what it says. This cache is
        // touched once per query and holds few entries, so one segment costs nothing.
        .concurrencyLevel(1)
        .weigher(new Weigher[java.lang.Long, AutoCTEEntry] {
          override def weigh(key: java.lang.Long, value: AutoCTEEntry): Int =
            weighEntry(value.plan)
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

  /**
   * Tracks one freshly materialised body.
   *
   * The Guava key is a fresh id per materialisation, NOT `cteId`. Keying by `cteId` loses
   * entries: two queries that cache DIFFERENT bodies routinely arrive with the same
   * `cteDef.id` (measured: two bodies whose normalized cache keys were 1584311006 and
   * 595407320 both came in as id 2), and `Cache.put` then replaces the first row. The
   * replacement is not an eviction, so `removalListener` -- gated on `wasEvicted()` -- does
   * not run: the first body keeps its `CachedData` registered but is no longer tracked here,
   * so its TTL never fires, `clearAll` never unpersists it, its `planIndex` bucket still
   * points at the id now owned by the second body (so `recordAccessByPlan` refreshes the
   * wrong row), and `numEntries` under-reports what is actually materialised.
   *
   * `trackEntry` is only reached on a `lookupCachedData` miss, so one fresh id per call means
   * one entry per materialisation, not one per query.
   */
  def trackEntry(cteId: Long, plan: LogicalPlan): Unit = {
    val key = indexKey(plan)
    val entryId = nextEntryId.getAndIncrement()
    cache.put(entryId, AutoCTEEntry(plan = plan, tableName = s"auto_cte_$cteId", indexKey = key))
    AutoCTECacheManager.everTracked = true
    if (key != null) {
      // Maintain the secondary index. computeIfAbsent is atomic; the
      // ConcurrentHashMap.newKeySet view supports concurrent add/remove.
      planIndex
        .computeIfAbsent(
          key,
          _ => java.util.concurrent.ConcurrentHashMap.newKeySet[java.lang.Long]())
        .add(entryId)
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

  /**
   * Triggers Guava's lazy eviction, then stops NEW plans from reusing whatever expired.
   *
   * `stopReusing`, not `uncacheQuery`: the expired entries' `InMemoryRelation`s are left
   * persisted, because a lazy DataFrame built while the entry was live still holds one -- see
   * the `removalListener` and `CacheManager.stopReusing`.
   */
  def evictStaleEntries(spark: SparkSession): Unit = {
    cache.cleanUp()

    var plan = pendingExpiry.poll()
    while (plan != null) {
      spark.sharedState.cacheManager.stopReusing(plan)
      plan = pendingExpiry.poll()
    }
  }

  /**
   * Drops every entry AND unpersists its relation, unlike expiry. Callers
   * (`spark.catalog.clearCache()`, test teardown) asked for exactly that.
   */
  def clearAll(spark: SparkSession): Unit = {
    val plans = new java.util.ArrayList[LogicalPlan]()
    cache.asMap().values().forEach(e => plans.add(e.plan))
    cache.invalidateAll()
    planIndex.clear()
    // Prepared-but-unpublished entries go too: nothing was materialised, and a caller asking for
    // a clean slate (`spark.catalog.clearCache()`, test teardown) means this one as well.
    pendingPublish.clear()
    var plan = pendingExpiry.poll()
    while (plan != null) {
      plans.add(plan)
      plan = pendingExpiry.poll()
    }
    plans.forEach { p =>
      spark.sharedState.cacheManager.uncacheQuery(spark, p, cascade = false)
    }
  }

  def numEntries: Int = cache.asMap().size()

  /**
   * Guava weight for one entry, in bytes of estimated body size.
   *
   * Stays in the `BigInt` domain. Without CBO and column statistics a multi-way join estimate
   * is a product of its children's sizes and routinely runs past `Long.MaxValue`, where
   * `BigInt#toLong` wraps silently and to either sign; a negative weight makes Guava throw
   * `IllegalStateException: Weights must be non-negative` from inside `cache.put`, after this
   * method has already returned, where no catch here can help. Same reasoning as
   * `ReplaceCTERefWithCache.isExpensiveEnough`.
   *
   * Three cases, and `1` is deliberate for the first: `SQLConf.defaultSizeInBytes` is
   * `Long.MaxValue` unless overridden, and any plan over a relation without statistics reports
   * at least that, so weighing an unknown size as the maximum would make `cache.put` evict it
   * on the spot for every realistic budget -- auto-CTE caching would silently stop working as
   * soon as a `maxSize` was configured. Undercounting an unknown size only risks overshooting
   * the budget, which is the safe direction and matches the fallback below.
   */
  private[sql] def weighEntry(plan: LogicalPlan): Int = {
    try {
      val size = plan.stats.sizeInBytes
      if (size <= 0 || size >= BigInt(SQLConf.get.defaultSizeInBytes)) {
        1
      } else if (size >= BigInt(Int.MaxValue)) {
        // A real estimate above 2GiB: the largest weight Guava can represent. Undercounts,
        // but monotonically.
        Int.MaxValue
      } else {
        size.toInt
      }
    } catch {
      case scala.util.control.NonFatal(_) => 1
    }
  }

  /** Test-only: number of distinct buckets in the secondary plan index. */
  private[sql] def planIndexSize: Int = planIndex.size()

  /**
   * Entries that `ReplaceCTERefWithCache` built but has not published to `CacheManager` yet.
   *
   * Keyed by the identity of the relation's `CachedRDDBuilder`, because that is what the plan
   * carries: `publishPending` can then tell one query's entries from another's and publish only
   * the ones actually about to run. Bounded, and deliberately so -- a plan that is never executed
   * (`EXPLAIN`, one read of `optimizedPlan`) leaves its entry here with nobody to claim it, and
   * dropping the oldest is the right outcome: nothing was materialised, so nothing leaks except
   * the entry itself.
   */
  private val pendingPublish: java.util.Map[CachedRDDBuilder, (Long, CachedData)] =
    java.util.Collections.synchronizedMap(
      new java.util.LinkedHashMap[CachedRDDBuilder, (Long, CachedData)](16, 0.75f, false) {
        override def removeEldestEntry(
            eldest: java.util.Map.Entry[CachedRDDBuilder, (Long, CachedData)]): Boolean = {
          size() > AutoCTECacheManager.MaxPendingPublish
        }
      })

  private[sql] def deferPublish(cteId: Long, cd: CachedData): Unit = {
    pendingPublish.put(cd.cachedRepresentation.cacheBuilder, (cteId, cd))
  }

  /** Test-only: how many prepared-but-unpublished entries are waiting. */
  def numPending: Int = pendingPublish.size()

  /**
   * Publishes the prepared entries that `plan` actually references, and tracks them for TTL.
   *
   * Called once per execution from `SQLExecution.withNewExecutionId`, which is the boundary that
   * separates "a query ran" from "a plan was built": `EXPLAIN` and a bare `optimizedPlan` read
   * never reach it, so they no longer register anything other sessions can hit.
   */
  def publishPending(spark: SparkSession, plan: LogicalPlan): Unit = {
    if (pendingPublish.isEmpty) {
      return
    }
    val builders = plan.collectWithSubqueries {
      case r: InMemoryRelation => r.cacheBuilder
    }
    builders.foreach { b =>
      val claimed = pendingPublish.remove(b)
      if (claimed != null) {
        val (cteId, cd) = claimed
        spark.sharedState.cacheManager.registerCachedData(cd)
        trackEntry(cteId, cd.plan)
      }
    }
  }
}

object AutoCTECacheManager {
  /**
   * Cap on prepared-but-unpublished entries. Only reachable by planning without executing, so a
   * small number is plenty; the alternative is an unbounded map fed by every `EXPLAIN`.
   */
  private[execution] val MaxPendingPublish = 64

  /**
   * True once any manager in this process has tracked an entry.
   *
   * `ReplaceCTERefWithCache` sweeps expired entries even when the feature is switched off, and
   * this flag is what keeps that from costing anything in a process that never switched it on:
   * without it, the off path would have to reach `spark.sharedState` on every query just to
   * find an empty tracker. Never reset, and process-wide rather than per-SparkContext, which
   * errs in the only direction that matters -- an occasional sweep of an empty tracker.
   */
  @volatile private[execution] var everTracked: Boolean = false
}

/**
 * @param plan      The CTE definition's logical plan as it was passed to
 *                  `trackEntry`. Held so `clearAll` can unpersist the relation, and for
 *                  diagnostic logging on eviction.
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
