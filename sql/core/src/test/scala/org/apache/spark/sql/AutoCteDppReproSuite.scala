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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.DynamicPruning
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * Regression tests for the Auto-CTE and DynamicPruning interaction.
 *
 * `PartitionPruning` runs in an earlier optimizer batch than the batch hosting
 * `ReplaceCTERefWithCache`, so by the time the auto-cache rule takes a CTE body
 * it can already contain a `DynamicPruningSubquery` whose `buildQuery` is the
 * OUTER query's build side. `CacheManager.cacheQuery` re-runs
 * `sessionState.executePlan` on that body, and `CheckAnalysis` used to reject the
 * out-of-context dynamic-pruning filter with
 * INTERNAL_ERROR "Found the unresolved operator" (observed on tpcds-v2.7.0/q5a).
 *
 * `ReplaceCTERefWithCache.prePushdownBody` now strips `DynamicPruningSubquery`
 * out of the body before handing it to `cacheQuery`, mirroring the `TrueLiteral`
 * fallback `PlanDynamicPruningFilters` already uses when a broadcast cannot be
 * reused. It must strip nothing else -- see the last test.
 *
 * The TPC-DS parts use the same partitioned tables and injected sf100 stats as
 * `PlanStabilitySuite`, which is the configuration the failure was found in.
 * Those tables have no rows, so plan-shape assertions are all they can support.
 */
class AutoCteDppReproSuite extends QueryTest with TPCDSBase {

  override def injectStats: Boolean = true

  override protected def afterEach(): Unit = {
    try {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterEach()
    }
  }

  /**
   * `minSizeBytes` overrides `AUTO_CTE_CACHE_MIN_SIZE_BYTES` when the test needs the size
   * heuristic out of the way. It is per-call rather than suite-wide because the floor decides
   * WHICH def gets cached, and the q14a test below depends on the stock floor leaving its
   * DPP-bearing body uncached.
   */
  private def optimize(
      group: String,
      name: String,
      autoCte: Boolean,
      minSizeBytes: Option[String] = None): LogicalPlan = {
    val q = resourceToString(s"$group/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    // `withSQLConf` returns Unit on this branch, so capture the plan out of the body.
    var plan: LogicalPlan = null
    val confs = Seq(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> autoCte.toString,
      SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "false",
      SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
      SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") ++
      minSizeBytes.map(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> _)
    withSQLConf(confs: _*) {
      plan = spark.sql(q).queryExecution.optimizedPlan
    }
    plan
  }

  private def countInMemoryRelations(plan: LogicalPlan): Int =
    plan.collect { case r: InMemoryRelation => r }.size

  private def countDppNodes(plan: LogicalPlan): Int =
    plan.collect {
      case p if p.expressions.exists(_.exists(_.isInstanceOf[DynamicPruning])) => p
    }.size

  /**
   * Identities of the `CachedRDDBuilder`s behind every `InMemoryRelation` in the
   * query's optimized plan. A body reused from an earlier query shows up as the
   * same identity in both plans; a re-materialised one is an identity the earlier
   * plan did not have. `AUTO_CTE_CACHE_MIN_SIZE_BYTES=0` keeps the size gate out
   * of the picture.
   */
  private def cachedBuilderIds(group: String, name: String): Set[Int] = {
    val q = resourceToString(s"$group/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    var ids: Set[Int] = Set.empty
    withSQLConf(
        SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
        SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "false",
        SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0",
        SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
        SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE.key -> "true") {
      // `collect()` before reading the plan: publication happens at execution
      // (`SQLExecution.withNewExecutionId` -> `AutoCTECacheManager.publishPending`), so a
      // plan-only run leaves nothing registered for the next query to reuse. The tables are
      // empty, so the action costs nothing.
      val df = spark.sql(q)
      df.collect()
      ids = df.queryExecution.optimizedPlan.collect {
        case r: InMemoryRelation => System.identityHashCode(r.cacheBuilder)
      }.toSet
    }
    ids
  }

  test("q5a: DPP fires inside the CTE body when auto-CTE is off") {
    // Establishes that this query really is a DPP-bearing shape, so the next
    // test is exercising the interaction rather than a DPP-free plan.
    val plan = optimize("tpcds-v2.7.0", "q5a", autoCte = false, minSizeBytes = Some("0"))
    assert(countInMemoryRelations(plan) == 0,
      "auto-CTE is off, so nothing should be materialised")
    assert(countDppNodes(plan) > 0,
      "expected PartitionPruning to inject DynamicPruning into this query")
  }

  test("q5a: auto-CTE caches the body instead of failing analysis") {
    // Before the fix this threw
    //   INTERNAL_ERROR Found the unresolved operator: 'Filter (... dynamicpruning#... )
    // from CheckAnalysis, because cacheQuery re-analyzed a body still carrying a
    // DynamicPruningSubquery that referenced the outer query's build side.
    //
    // The size floor is pinned to 0 because the subject here is the DPP interaction, not
    // the size heuristic. It is not decorative: on the injected sf100 stats q5a's `results`
    // body estimates at 251 KiB -- three narrow aggregates over wide fact tables shrink the
    // estimate hard -- so at the stock 1 MiB floor `InlineCTE` inlines the def and the plan
    // never reaches the cache rule at all.
    val plan = optimize("tpcds-v2.7.0", "q5a", autoCte = true, minSizeBytes = Some("0"))
    assert(countInMemoryRelations(plan) > 0,
      s"expected the CTE body to be auto-cached, plan was:\n${plan.treeString}")
    // The cached body must not retain the outer query's pruning predicates.
    assert(countDppNodes(plan) == 0,
      s"expected DynamicPruning to be stripped from the cached body, plan was:\n" +
        plan.treeString)
  }

  test("q14a: auto-CTE caching leaves an unrelated query's DPP intact") {
    // Guards against over-stripping: q14a caches cleanly and its DPP nodes live
    // outside the cached bodies, so they must survive.
    //
    // Keep the stock size floor here. Pinning it to 0 makes the outer body cacheable too,
    // the DPP nodes then sit INSIDE a cached body and are stripped legitimately, and the
    // test fails while nothing is wrong -- the over-stripping guard needs a query whose DPP
    // is outside the cache, which at this floor is what q14a is.
    val plan = optimize("tpcds-v2.7.0", "q14a", autoCte = true)
    assert(countInMemoryRelations(plan) > 0,
      "expected q14a's CTE bodies to be auto-cached")
    assert(countDppNodes(plan) > 0,
      s"expected q14a to keep its DynamicPruning nodes, plan was:\n${plan.treeString}")
  }

  /**
   * Cross-query reuse guard for the strip's reach into subquery plans.
   *
   * `stripDynamicPruning` guards on `plan.containsPattern(DYNAMIC_PRUNING_SUBQUERY)`,
   * which DOES see into subquery plans (`PlanExpression.treePatternBits` unions its
   * inner plan's bits). It used to strip with `transformAllExpressionsWithPruning`,
   * which deliberately does NOT ("this method skips expressions inside subqueries").
   * A `DynamicPruningSubquery` nested inside a scalar subquery therefore passed the
   * guard and survived the strip.
   *
   * That is not merely cosmetic. `DynamicPruningSubquery.canonicalized` keeps
   * `buildQuery.canonicalized` -- the OUTER query's build side -- so two queries
   * sharing a CTE body get different canonical bodies, and because
   * `orderCommutative` sorts operands by `hashCode`, the difference also reorders
   * every enclosing commutative operand list. `lookupCachedData` can then never
   * match and the second query re-materialises the same data.
   *
   * q23a/q23b are the shape that exposes it: their shared `best_ss_customer` body
   * has the DPP subquery inside the HAVING scalar subquery. Both declare two
   * identical CTEs (`item_sk` and `c_customer_sk`); before the fix only `item_sk`
   * was reused. Tables are empty, so this asserts cache-key identity, not runtime.
   */
  test("q23b reuses both of q23a's cached CTE bodies") {
    spark.sharedState.autoCTECacheManager.clearAll(spark)
    val a = cachedBuilderIds("tpcds", "q23a")
    val b = cachedBuilderIds("tpcds", "q23b")

    assert(a.size == 2, s"expected q23a to cache both shared CTEs, got ${a.size}")
    assert(b.size == 2, s"expected q23b to reference both shared CTEs, got ${b.size}")
    assert(b == a,
      s"q23b must reuse q23a's cached bodies rather than re-materialise them; " +
        s"q23a builders=$a q23b builders=$b")
  }

  /**
   * The narrow counterpart to the two q5a tests: only `DynamicPruningSubquery`
   * may be dropped from a body about to be cached.
   *
   * This fork's `PartitionPruning.prune` has a second producer that rewrites a
   * user-written conjunct over a partition column in place:
   * `Filter(part = ScalarSubquery(...))` becomes
   * `Filter(DynamicPruningExpression(part = ScalarSubquery(...)))`. That wrapper
   * is not a hint -- `DynamicPruningExpression.eval` delegates to its child, and
   * the predicate has no counterpart above the CTE reference. Dropping it caches
   * every partition instead of the selected one, so the query returns different
   * rows, AND two bodies differing only by that conjunct canonicalize identically,
   * letting one query read the other's cache.
   *
   * Verified discriminating by mutation: widening `stripDynamicPruning` to match
   * the `DynamicPruning` trait makes the cached run return all four partitions
   * (8 rows) against the baseline's one partition (2 rows), and this test fails.
   *
   * The assertion is on the returned rows rather than on plan shape, because
   * that is what a widened cache body actually changes. The TPC-DS tables this
   * suite creates are empty, so this test brings its own partitioned table.
   *
   * `spark.sql.unionOutputPartitioning=false` works around a defect in this fork's
   * `UnionExec`, not in auto-CTE. `supportCodegen` is gated on
   * `outputPartitioning.isInstanceOf[UnknownPartitioning]` and `metrics` registers
   * `numOutputRows` only when that gate passes, but `outputPartitioning` is derived from
   * the children, and a cached child's partitioning changes when AQE materialises the
   * cache: `InMemoryTableScanExec.outputPartitioning` reads `cachedPlan.outputPartitioning`,
   * which the inner `AdaptiveSparkPlanExec` only reports concretely once the cache stage
   * has run. AQE then rebuilds the union through `withNewChildren` without re-running
   * `CollapseCodegenStages`, so the fused `WholeStageCodegenExec` ends up around an
   * instance that now answers `supportCodegen = false`, and `doProduce` dies with
   * `NoSuchElementException: key not found: numOutputRows`. Two cached scans directly
   * under a `Union` is the shape that reaches it, which is why auto-CTE finds it and the
   * baseline run does not. Remove the pin once `UnionExec` is fixed.
   */
  test("a DynamicPruningExpression wrapping a real conjunct is not stripped") {
    withTable("auto_cte_dpe_fact") {
      spark.range(0, 400)
        .selectExpr("id AS fact_id", "cast(id AS double) AS value", "id % 4 AS part")
        .write.partitionBy("part").saveAsTable("auto_cte_dpe_fact")

      // UNION ALL of two bare references, rather than a self-join: both
      // references read every CTE output column and carry no filter, so
      // `PushdownPredicatesAndPruneColumnsForCTEDef` has nothing to push and
      // nothing to prune. `cteDef.originalPlanWithPredicates` therefore stays
      // None and `prePushdownBody` falls through to `cteDef.child` -- the only
      // body that can still carry the `DynamicPruningExpression` that the
      // earlier `PartitionPruning` batch injected. A self-join would instead let
      // `InferFiltersFromConstraints` add an `isnotnull` above each reference,
      // which triggers the pushdown rule and makes `prePushdownBody` return the
      // pre-DPP `originalPlan`, leaving nothing to over-strip.
      //
      // `part = (SELECT max(part) ...)` is a real filter on the partition
      // column, which is the shape the fork's ScalarSubquery producer rewrites.
      val sqlText =
        """WITH cte AS (
          |  SELECT part, sum(value) AS total
          |  FROM auto_cte_dpe_fact
          |  WHERE part = (SELECT max(part) FROM auto_cte_dpe_fact)
          |  GROUP BY part
          |)
          |SELECT part, total FROM cte
          |UNION ALL
          |SELECT part, total FROM cte""".stripMargin

      var baseline: Seq[Row] = null
      withSQLConf(
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false",
          SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "false") {
        baseline = spark.sql(sqlText).collect().toSeq
      }
      assert(baseline.nonEmpty, "fixture must produce rows for the comparison to mean anything")

      var cachedPlan: LogicalPlan = null
      var cached: Seq[Row] = null
      withSQLConf(
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false",
          SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "false",
          SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0") {
        val df = spark.sql(sqlText)
        cached = df.collect().toSeq
        cachedPlan = df.queryExecution.optimizedPlan
      }
      assert(countInMemoryRelations(cachedPlan) > 0,
        s"the CTE must actually be cached, otherwise this test proves nothing:\n" +
          cachedPlan.treeString)
      assert(cached.sortBy(_.toString) == baseline.sortBy(_.toString),
        s"caching must not widen the body: with auto-CTE off the query returned " +
          s"$baseline, with auto-CTE on it returned $cached")
    }
  }
}
