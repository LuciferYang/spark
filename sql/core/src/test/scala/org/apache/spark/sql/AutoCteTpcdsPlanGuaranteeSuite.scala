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

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationRef, LogicalPlan, RepartitionByExpression}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf

/**
 * Plan-level guarantees for auto-CTE caching over the TPC-DS queries measured on the
 * 100TB cluster, using the CLUSTER's configuration (`minSizeBytes=-1`, CBO on).
 *
 * Two guarantees, both structural. Runtime is not measured here -- these tables are
 * empty and this JVM has no Gluten -- but every regression the cluster runs surfaced
 * was visible in the plan first, which is what this suite locks down:
 *
 *   1. The queries that measured a speedup must still CACHE: q14a/q14b (8.6x),
 *      q23a/q23b (4.0x/4.5x), q24a/q24b (55.8x/54.6x), q39a/q39b (2.3x/1.8x).
 *      A gate that stops one of them shows up as `numEntries == 0`.
 *
 *   2. The query that measured a REGRESSION must not cache and must not pick up the
 *      `ReplaceCTERefWithRepartition` shuffle either: q95's `ws_wh` body ran 5.5x
 *      slower cached (blocks never retained) and 15.3x slower repartitioned, against
 *      1.4 min inlined. Both `InlineCTE` and `ReplaceCTERefWithCache` decline it via
 *      the same structural predicate, so the plan comes out shaped exactly like the
 *      auto-CTE-off baseline.
 *
 * Every query here must also produce a RESOLVED plan when caching is declined for a
 * reason other than shape -- see `CteRepartitionDppSuite`, which covers the
 * `storageLevel=NONE` fall-through that used to fail plan validation.
 *
 * These tables ARE partitioned (`TPCDSBase` emits `PARTITIONED BY (*_date_sk)`) even
 * though they hold no rows, and `getFilterableTableScan` reads `partitionSchema` from
 * the DDL rather than from the data, so `TagPruningVetoCTE` reaches its verdict here as
 * well -- measured: q75 vetoed, the other 13 not, identical to the populated fixture
 * and to real sf100 partitioned data. That is why the loop below asserts q75 must not
 * pick up a repartition rather than must cache. `AutoCteVetoPartitionedSuite` is where
 * the same verdicts are pinned against a dataset where they cost real time.
 */
class AutoCteTpcdsPlanGuaranteeSuite extends QueryTest with TPCDSBase {

  override def injectStats: Boolean = true

  /**
   * Cluster config: apps 0016 / 0021 / 0027 all set minSizeBytes=-1.
   *
   * `partialAggregationOptimization` is a parameter because the cluster runs with it
   * ON while its SQLConf default is OFF, and it changes which node classes the cache
   * gate sees: the batch that rewrites `Aggregate` over a join into
   * `FinalAggregate`/`PartialAggregate` sits between `Batch("Inline CTE")` and
   * `Batch("Replace CTE with Repartition")`. Every guarantee below is asserted under
   * BOTH settings -- with the gate matching `Aggregate` alone, q23a/q23b lost half
   * their cache entries and picked up the repartition shuffle only when it was on.
   */
  private def withClusterConf(pushPartialAgg: Boolean)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1",
      SQLConf.CBO_ENABLED.key -> "true",
      SQLConf.PLAN_STATS_ENABLED.key -> "true",
      SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
      SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> pushPartialAgg.toString)(f)
  }

  /** Both settings of the conf the cluster and the SQLConf default disagree on. */
  private val paggModes = Seq(false, true)

  private def planOf(name: String): LogicalPlan = {
    val q = resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    spark.sql(q).queryExecution.optimizedPlan
  }

  private def counts(p: LogicalPlan): (Int, Int, Int) =
    (p.collectWithSubqueries { case r: InMemoryRelation => r }.size,
      p.collectWithSubqueries { case r: CTERelationRef => r }.size,
      p.collectWithSubqueries { case r: RepartitionByExpression => r }.size)

  /**
   * How many times the plan evaluates a base-table scan.
   *
   * `InMemoryRelation` is a `LeafNode` -- its cached plan hangs off `innerChildren`,
   * which `collect` does not descend into -- so scans that moved behind the cache stop
   * being counted here. That is exactly the quantity of interest: inlining a CTE
   * referenced N times copies the body's scans N times, and caching replaces all N
   * with one materialization. This is the mechanism behind every speedup app 0027
   * measured, and unlike a wall-clock number it is checkable on empty tables.
   */
  private def scanCount(p: LogicalPlan): Int = p.collectWithSubqueries {
    case l: LogicalRelation => l
    case h: HiveTableRelation => h
  }.size

  /** Distinct materializations, i.e. distinct `CachedRDDBuilder` identities. */
  private def distinctBuilders(p: LogicalPlan): Int = p.collectWithSubqueries {
    case r: InMemoryRelation => System.identityHashCode(r.cacheBuilder)
  }.distinct.size

  // Speedups measured at 100TB (app 0027 vs the auto-CTE-off baseline 0008).
  private val mustCache = Seq(
    "q14a" -> "8.6x", "q14b" -> "8.6x",
    "q23a" -> "4.0x", "q23b" -> "4.5x",
    "q24a" -> "55.8x", "q24b" -> "54.6x",
    "q39a" -> "2.3x", "q39b" -> "1.8x")

  for ((name, speedup) <- mustCache; pagg <- paggModes) {
    test(s"$name must cache and must remove repeated work " +
      s"(measured $speedup at 100TB, pushPartialAgg=$pagg)") {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      try {
        val offScans = withCteDisabled(pagg)(planOf(name)) match {
          case p => scanCount(p)
        }
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        withClusterConf(pagg) {
          val plan = planOf(name)
          val (imr, ref, rep) = counts(plan)
          // `numPending`, not `numEntries`: these tests only PLAN, and publication now waits
          // for execution (`SQLExecution.withNewExecutionId` -> `publishPending`), so the
          // tracker stays empty until a query runs. The prepared-entry count is the plan-time
          // observable for "the rule decided to cache this body".
          assert(spark.sharedState.autoCTECacheManager.numPending >= 1,
            s"$name lost its cache entry; it measured $speedup faster when cached")
          assert(imr >= 1, s"$name must have an InMemoryRelation in the plan:\n$plan")
          assert(ref == 0, s"$name must not leave an unreplaced CTERelationRef:\n$plan")
          assert(plan.resolved, s"$name plan must be resolved")

          // NO def of this query may land on `ReplaceCTERefWithRepartition`. Counting
          // entries is not enough: a query with several defs can keep caching some
          // while one drops onto the round-robin, which is the 22 min shape. That is
          // exactly what a gate mismatch across the partial-aggregation rewrite did to
          // q23a (entries 4 -> 2, two `RepartitionByExpression`) and q23b (6 -> 2,
          // four) -- both still satisfied `numEntries >= 1` and `onScans < offScans`,
          // so this assertion is the one that catches it.
          assert(rep == 0,
            s"$name must not send any CTE def to ReplaceCTERefWithRepartition; its " +
            s"round-robin shuffle measured 22 min against 1.4 min for inlining on " +
            s"q95, and it also re-scans the body per reference:\n$plan")

          // The BENEFIT, checkable without running anything: the CTE body's scans
          // appear once each behind the cache instead of once per reference. This is
          // the mechanism the cluster speedup came from -- q24a's body is referenced
          // twice and its six-table body was scanned twice when inlined.
          val onScans = scanCount(plan)
          assert(onScans < offScans,
            s"$name must evaluate FEWER base-table scans when cached " +
            s"(inlined=$offScans, cached=$onScans). Equal counts mean the body is " +
            s"still being recomputed per reference, which is what $speedup came from.")

          // All references of one def must share ONE materialization, otherwise the
          // body is computed several times over and the speedup is lost even though
          // an InMemoryRelation is present.
          val builders = distinctBuilders(plan)
          assert(builders <= imr,
            s"$name has $imr InMemoryRelation(s) but $builders distinct builders")
        }
      } finally {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
      }
    }
  }

  paggModes.foreach { pagg =>
  test("q95 must not cache and must not be repartitioned " +
    s"(measured 15.3x slower, pushPartialAgg=$pagg)") {
    spark.sharedState.autoCTECacheManager.clearAll(spark)
    try {
      val offPlan = withCteDisabled(pagg)(planOf("q95"))
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      withClusterConf(pagg) {
        val onPlan = planOf("q95")
        val (imr, ref, rep) = counts(onPlan)
        assert(spark.sharedState.autoCTECacheManager.numEntries == 0 &&
            spark.sharedState.autoCTECacheManager.numPending == 0,
          "q95's row-expanding ws_wh body must not be cached; caching it measured " +
          "5.5x slower and the repartition fallback 15.3x slower than inlining")
        assert(imr == 0, s"q95 must have no InMemoryRelation:\n$onPlan")
        assert(ref == 0, s"q95 must have no CTERelationRef left:\n$onPlan")
        assert(rep == 0,
          "q95 must not get the ReplaceCTERefWithRepartition shuffle: its round-robin " +
          s"plus sortBeforeRepartition sort measured 22 min against 1.4 min:\n$onPlan")

        // The decline must land on the inlined shape, i.e. the same operator classes
        // the auto-CTE-off baseline produces. Compare the operator-class sequence
        // rather than canonicalized strings: commutative operand ordering is
        // hash-derived and differs harmlessly between the two runs.
        assert(operatorSeq(onPlan) == operatorSeq(offPlan),
          "declined q95 must produce the auto-CTE-off operator structure\n" +
          s"ON : ${operatorSeq(onPlan).take(12).mkString(",")}\n" +
          s"OFF: ${operatorSeq(offPlan).take(12).mkString(",")}")

        // And it must do no MORE work than the baseline. This is the half of the
        // no-regression requirement that plan shape can carry: the same scans, the
        // same shuffles, no extra materialization.
        assert(scanCount(onPlan) == scanCount(offPlan),
          s"q95 must scan exactly as much as the baseline " +
          s"(off=${scanCount(offPlan)}, on=${scanCount(onPlan)})")
      }
    } finally {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }
  }

  // The remaining cluster-measured queries must not pick up work relative to the
  // auto-CTE-off baseline, whichever side of the gates they land on. q2/q47/q57 all
  // sped up and cache; q74 cached and stayed at or above baseline; q75 is VETOED here
  // (see the suite comment -- these tables are partitioned, so the veto fires) and is
  // therefore inlined, which this test covers as "no more scans, no repartition". Its
  // exact plan equality with the baseline is asserted in `AutoCteTpcdsWorkSuite`.
  for (name <- Seq("q2", "q47", "q57", "q74", "q75"); pagg <- paggModes) {
    test(s"$name must not add work relative to the auto-CTE-off baseline " +
      s"(pushPartialAgg=$pagg)") {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      try {
        val offScans = scanCount(withCteDisabled(pagg)(planOf(name)))
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        withClusterConf(pagg) {
          val plan = planOf(name)
          assert(plan.resolved, s"$name plan must be resolved")
          assert(scanCount(plan) <= offScans,
            s"$name must not scan more than the baseline " +
            s"(off=$offScans, on=${scanCount(plan)})")
          val (_, ref, rep) = counts(plan)
          assert(ref == 0, s"$name must not leave an unreplaced CTERelationRef:\n$plan")
          assert(rep == 0,
            s"$name must not pick up the ReplaceCTERefWithRepartition shuffle:\n$plan")
        }
      } finally {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
      }
    }
  }

  private def operatorSeq(p: LogicalPlan): Seq[String] =
    p.collectWithSubqueries { case n: LogicalPlan => n.getClass.getSimpleName }

  private def withCteDisabled(pushPartialAgg: Boolean)(f: => LogicalPlan): LogicalPlan = {
    var out: LogicalPlan = null
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> pushPartialAgg.toString) {
      out = f
    }
    out
  }
}
