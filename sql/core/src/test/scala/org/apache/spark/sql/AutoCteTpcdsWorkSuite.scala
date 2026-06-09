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

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf

/**
 * Measures the BENEFIT of auto-CTE caching by EXECUTING the real TPC-DS queries the
 * way the cluster benchmark measures them: WARM, i.e. with the cache already
 * populated, which is what "min of 5 iterations" reports.
 *
 * Measuring the first (cold) run instead is misleading and was the reason an earlier
 * version of this suite could not show any benefit. The cold run pays the whole
 * materialization on top of the work it will later save, so it processes MORE records
 * than inlining -- on q24a at 240 fact rows, 1244 against 270. That cost is paid once
 * and amortized across every subsequent execution; the cluster's reported number
 * never includes it.
 *
 * Records processed (read + shuffled), not wall-clock: at these data sizes job
 * startup dominates the clock, but records are deterministic and are the quantity the
 * cluster speedups are made of. A body referenced N times is processed N times when
 * inlined and read from cache when warm.
 *
 * Tables carry the real TPC-DS schema (`TPCDSBase`) with a small amount of synthetic
 * data from `TPCDSSmallDataFixture`, plus sf100 statistics from `injectStats` so the
 * OPTIMIZER makes the same decisions it makes on the cluster while EXECUTION stays
 * fast.
 *
 * Every test here also compares the ANSWER, cold and warm, against the inlined
 * baseline -- see `assertSameAnswer` -- because the work counters alone cannot
 * distinguish "shares one materialisation" from "wrongly dropped a join": both show up
 * as fewer records. The runs already execute every variant, so the comparison costs
 * only the memory to hold the rows.
 *
 * That comparison is VACUOUS for six queries, and knowingly so. This fixture cannot
 * make q14b/q23b/q24a/q24b/q39a/q39b return any rows -- their filters need
 * cross-column value overlaps that independently generated columns do not have. Their
 * work counters and cache verdicts are still valid on an empty result, which is why
 * they stay here; their ANSWERS are covered by `AutoCteTpcdsRealDataSuite`, which runs
 * the same 14 queries against a sampled dsdgen dataset.
 *
 * The cache VERDICTS here do match the cluster, including the veto. `TPCDSBase`
 * declares the seven fact tables `PARTITIONED BY (*_date_sk)` and `INSERT OVERWRITE`
 * creates the partition directories (measured: `SHOW PARTITIONS store_sales` = 90), so
 * `PartitionPruning.getFilterableTableScan` accepts these scans and
 * `TagPruningVetoCTE` reaches its decision -- q75 is vetoed here exactly as it is on
 * sf100 partitioned data, and the other 13 are not. What this fixture cannot reproduce
 * is the MAGNITUDE: 240 fact rows across 90 partitions make pruning worth nothing, so
 * the counters see only scheduling noise from the DPP subqueries (see the q75 test
 * below). `AutoCteVetoPartitionedSuite` is where the veto's verdict is pinned against
 * a real 34 GiB partitioned dataset with computed column statistics.
 */
class AutoCteTpcdsWorkSuite extends QueryTest with TPCDSBase
  with TPCDSSmallDataFixture {

  override def injectStats: Boolean = true

  /** Records read plus shuffled across every task, i.e. the work one run did. */
  private class WorkCounter extends SparkListener {
    private val total = new java.util.concurrent.atomic.AtomicLong(0)
    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
      Option(e.taskMetrics).foreach { m =>
        total.addAndGet(m.inputMetrics.recordsRead + m.shuffleReadMetrics.recordsRead)
      }
    }
    def value: Long = total.get()
  }

  private def measure(body: => Unit): Long = {
    val counter = new WorkCounter
    spark.sparkContext.addSparkListener(counter)
    try {
      body
      spark.sparkContext.listenerBus.waitUntilEmpty(60000)
      counter.value
    } finally {
      spark.sparkContext.removeSparkListener(counter)
    }
  }

  /** One run of a query: its ExprId-normalised plan text, its rows, and the cache size. */
  private case class PlanRun(text: String, rows: Seq[Row], entries: Int)

  /**
   * Runs `name` with auto-CTE off and then on, and returns both runs. For the queries whose
   * claim is "the plan is exactly the auto-CTE-off plan" (q75 vetoed, q95 declined) this is
   * the measurement to make; their work counters cannot decide it, for the reasons in q75's
   * scaladoc.
   */
  private def planProfile(name: String, pagg: Boolean): (PlanRun, PlanRun) = {
    val q = resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)

    def run(on: Boolean): PlanRun = {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      var text: String = null
      var rows: Seq[Row] = Nil
      var entries = 0
      withSQLConf((SQLConf.AUTO_REUSED_CTE_ENABLED.key -> on.toString) +:
        clusterConf(pagg): _*) {
        val df = spark.sql(q)
        text = df.queryExecution.optimizedPlan.toString.replaceAll("#[0-9]+L?", "#X")
        rows = df.collect().toSeq
        entries = spark.sharedState.autoCTECacheManager.numEntries
      }
      PlanRun(text, rows, entries)
    }

    try {
      (run(false), run(true))
    } finally {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }

  /** Cluster config: apps 0016 / 0021 / 0027 all set minSizeBytes=-1. */
  /**
   * `pushPartialAgg` is a parameter because the cluster runs with
   * `partialAggregationOptimization` ON while its SQLConf default is OFF, and the
   * batch it enables rewrites `Aggregate` over a join into
   * `FinalAggregate`/`PartialAggregate` BETWEEN the two gates. Every measurement
   * below is taken under both settings -- with the gate matching `Aggregate` alone,
   * q23a/q23b stopped caching and picked up the repartition shuffle only when on.
   */
  private def clusterConf(pushPartialAgg: Boolean): Seq[(String, String)] = Seq(
    SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1",
    SQLConf.CBO_ENABLED.key -> "true",
    SQLConf.PLAN_STATS_ENABLED.key -> "true",
    SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
    SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE.key -> "true",
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> pushPartialAgg.toString)

  /** Both settings of the conf the cluster and the SQLConf default disagree on. */
  private val paggModes = Seq(false, true)

  /**
   * (inlined, cold, warm, cacheEntries) for one query, where `inlined` and `warm` are
   * MINIMA over several executions and `cold` is the first cached execution.
   *
   * Minima, because a single execution is not reproducible: measured over four
   * consecutive identical runs with the feature OFF, q95 alternates between 1795 and
   * 2035 records (an AQE/DPP effect unrelated to caching -- entries=0 throughout).
   * The cluster benchmark reports min of 5 iterations for the same reason, so this
   * matches it.
   */
  private def workProfile(name: String, pushPartialAgg: Boolean): Profile = {
    val q = resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    val runs = 4

    spark.sharedState.autoCTECacheManager.clearAll(spark)
    var inlined = Long.MaxValue
    var inlinedRows: Seq[Row] = Nil
    withSQLConf((SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") +:
      clusterConf(pushPartialAgg): _*) {
      (1 to runs).foreach { _ =>
        var rows: Array[Row] = Array.empty
        inlined = math.min(inlined, measure { rows = spark.sql(q).collect() })
        inlinedRows = rows.toSeq
      }
    }

    spark.sharedState.autoCTECacheManager.clearAll(spark)
    var cold = 0L
    var warm = Long.MaxValue
    var entries = 0
    var coldRows: Seq[Row] = Nil
    var warmRows: Seq[Row] = Nil
    try {
      withSQLConf((SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") +:
        clusterConf(pushPartialAgg): _*) {
        var rows: Array[Row] = Array.empty
        cold = measure { rows = spark.sql(q).collect() }
        coldRows = rows.toSeq
        (2 to runs).foreach { _ =>
          warm = math.min(warm, measure { rows = spark.sql(q).collect() })
          warmRows = rows.toSeq
        }
        entries = spark.sharedState.autoCTECacheManager.numEntries
      }
    } finally {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
    Profile(inlined, cold, warm, entries, inlinedRows, coldRows, warmRows)
  }

  /**
   * One query's work profile plus the rows each variant returned.
   *
   * The rows are carried out of `workProfile` on purpose: these tests already execute
   * the query on all three paths, so comparing the answers costs nothing beyond
   * holding them, and "processes less work" is NOT by itself evidence of correctness
   * -- a plan that wrongly dropped a join or a predicate would also process less.
   */
  private case class Profile(
      inlined: Long,
      cold: Long,
      warm: Long,
      entries: Int,
      inlinedRows: Seq[Row],
      coldRows: Seq[Row],
      warmRows: Seq[Row])

  /**
   * Asserts the cached (cold and warm) answers equal the inlined answer.
   *
   * Compared as sorted string forms rather than as ordered `Seq[Row]`: several of
   * these queries have no total ordering (a `LIMIT` over a partial `ORDER BY`), and
   * caching legitimately changes partitioning, so row ORDER may differ without the
   * answer differing. This is the same normalisation `AutoCTECacheCorrectnessSuite`
   * uses. Cold and warm are both checked -- cold reads the freshly materialised
   * blocks, warm reads them back from the cache, and only warm exercises the
   * cross-execution reuse path.
   */
  private def assertSameAnswer(name: String, pagg: Boolean, p: Profile): Unit = {
    def norm(rows: Seq[Row]): Seq[String] = rows.map(_.toString).sorted
    assert(norm(p.coldRows) == norm(p.inlinedRows),
      s"$name (pushPartialAgg=$pagg): cold cached answer differs from inlined " +
      s"(inlined=${p.inlinedRows.size} rows, cold=${p.coldRows.size} rows)")
    assert(norm(p.warmRows) == norm(p.inlinedRows),
      s"$name (pushPartialAgg=$pagg): warm cached answer differs from inlined " +
      s"(inlined=${p.inlinedRows.size} rows, warm=${p.warmRows.size} rows)")
  }

  // Speedups measured at 100TB (app 0027 vs the auto-CTE-off baseline 0008). Those
  // runtimes come from a different build and are quoted only so a future failure
  // states what is at stake; the assertion below is what this suite proves.
  private val mustBenefit = Seq(
    "q14a" -> "8.6x", "q14b" -> "8.6x",
    "q23a" -> "4.0x", "q23b" -> "4.5x",
    "q24a" -> "55.8x", "q24b" -> "54.6x",
    "q39a" -> "2.3x", "q39b" -> "1.8x")

  for ((name, clusterSpeedup) <- mustBenefit; pagg <- paggModes) {
    test(s"$name: warm cache must process less work than inlining " +
      s"(cluster measured $clusterSpeedup, pushPartialAgg=$pagg)") {
      populateSmallData()
      val p = workProfile(name, pagg)
      assert(p.inlined > 0, s"$name processed nothing with auto-CTE off; test is vacuous")
      assert(p.entries >= 1, s"$name must have cached its CTE body, got ${p.entries} entries")
      // Correctness alongside the work measurement: "processes less work" is not
      // evidence of a correct answer, and these runs already have all three answers in
      // hand. NOTE this check is only meaningful for the queries this fixture can make
      // return rows -- q14b/q23b/q24a/q24b/q39a/q39b come out EMPTY here, because their
      // filters need cross-column value overlaps that independently generated columns
      // do not have (`i_color = 'pale'`, `s_zip = ca_zip`, `HAVING count(*) > 4` per
      // (item, date)). For those six the comparison below is vacuously true, and the
      // real answer coverage lives in `AutoCteTpcdsRealDataSuite`, which runs on a
      // sampled dsdgen dataset. Deliberately NOT asserted non-empty here: that would
      // fail six tests whose actual subject -- the work counters and the cache
      // verdict -- is still valid on empty results.
      assertSameAnswer(name, pagg, p)
      // Strictly less, not a fixed factor: how much is saved depends on how many
      // times the body is referenced and how much it collapses, which differs per
      // query (q24a saves >99% of the records, q14b about 40%). Asserting "less"
      // is the claim that generalises; the magnitude is what the cluster reports.
      assert(p.warm < p.inlined,
        s"$name warm cache must do less work than inlining " +
        s"(inlined=${p.inlined}, cold=${p.cold}, warm=${p.warm}). The cluster reported " +
        s"$clusterSpeedup for this query, which is this effect at 100TB. Note the " +
        s"COLD run legitimately does MORE work than inlining -- it pays the " +
        s"materialization once -- so a regression here means the warm path stopped " +
        s"reading from cache.")
    }
  }

  // q95 measured 15.3x SLOWER at 100TB when its row-expanding body was declined into
  // the repartition fallback, and 5.5x slower when cached with blocks never retained.
  // Both gates decline it structurally now, so the plan must be the inlined baseline.
  //
  // Asserted as plan equality, not as equal work. On this fork min-of-4 does not damp the
  // DPP/AQE variation out: consecutive runs of this suite produced (inlined=2035,
  // warm=1795) and, running q95 alone, (inlined=1795, warm=2035) -- the same +-240 records
  // in both directions, with `entries == 0` holding throughout, so nothing was ever
  // materialised and the counters were measuring scheduling. See q75's scaladoc below for
  // the full argument; this is the case it says q95 also has.
  paggModes.foreach { pagg =>
  test("q95: declining the row-expanding body reproduces the baseline exactly " +
    s"(pushPartialAgg=$pagg)") {
    populateSmallData()
    val (off, on) = planProfile("q95", pagg)
    assert(on.entries == 0,
      s"q95's row-expanding body must not be cached, got ${on.entries}. Caching it " +
      s"measured 5.5x slower at 100TB, and sending it to the repartition fallback " +
      s"measured 15.3x slower.")
    assert(on.text == off.text,
      s"q95 (pushPartialAgg=$pagg) must plan exactly like the auto-CTE-off baseline " +
      s"once declined. A difference means it picked up a materialization or the " +
      s"round-robin repartition that measured 22 min against 1.4 min for inlining at " +
      s"100TB:\n=== off ===\n${off.text}\n=== on ===\n${on.text}")
    assert(on.rows.map(_.toString).sorted == off.rows.map(_.toString).sorted,
      s"q95 (pushPartialAgg=$pagg) answer differs from the auto-CTE-off baseline " +
      s"(off=${off.rows.size} rows, on=${on.rows.size} rows)")
  }
  }

  // q74 caches on the cluster and stayed at or above baseline. Warm must not be worse
  // than inlining; unlike the eight above, no minimum benefit is claimed. q75 is NOT
  // in this loop -- it is vetoed and therefore inlined, and its work counter is
  // unusable on this fixture; see the dedicated test below.
  for (name <- Seq("q74"); pagg <- paggModes) {
    test(s"$name: warm cache must not do more work than inlining " +
      s"(pushPartialAgg=$pagg)") {
      populateSmallData()
      val p = workProfile(name, pagg)
      assert(p.inlined > 0, s"$name processed nothing with auto-CTE off; test is vacuous")
      assertSameAnswer(name, pagg, p)
      assert(p.warm <= p.inlined,
        s"$name warm cache must not do MORE work than inlining " +
        s"(inlined=${p.inlined}, cold=${p.cold}, warm=${p.warm})")
    }
  }

  /**
   * q75 is the only one of the 14 that `TagPruningVetoCTE` vetoes, so its plan IS the
   * auto-CTE-off plan -- and that is what this test asserts, instead of a work
   * comparison.
   *
   * Work counters cannot decide q75 on this fixture. Measured over 8 consecutive
   * identical runs: 454..1106 records with the feature OFF and 330..2100 with it ON
   * (pushPartialAgg=true). The two ranges overlap and each spans 2.4x-6.4x, so
   * `warm <= inlined` is a coin flip -- an earlier version of this test asserted it and
   * failed with `776 was not less than or equal to 330` while both plans were identical
   * modulo ExprIds. This is the same DPP/AQE variation documented for q95 on
   * `workProfile`, amplified because q75 has three partitioned fact scans behind
   * `dynamicpruning` subqueries where q95 has one, which is also why min-of-4 does not
   * damp it out.
   *
   * Plan equality is the stronger claim anyway: identical plans cannot differ in work
   * except by the scheduling noise the counters were picking up. Compared as ExprId-
   * normalised `toString`, NOT as `canonicalized`: canonicalization rewrites the DPP
   * filter's `And` chain through `MultiCommutativeOp`, which orders operands by a hash
   * that moves with ExprIds, so `off.canonicalized == on.canonicalized` is false here
   * for a reason unrelated to caching -- the only differing line is the conjunct order
   * inside `'Filter multicommutativeop(dynamicpruning#0 [none#22], isnotnull(none#22),
   * ...)`. The normalised text form was stable across 5 repeats under both settings.
   */
  paggModes.foreach { pagg =>
  test(s"q75: the vetoed body reproduces the auto-CTE-off plan (pushPartialAgg=$pagg)") {
    populateSmallData()
    val (off, on) = planProfile("q75", pagg)
    assert(on.entries == 0,
      s"q75 (pushPartialAgg=$pagg) must not cache: TagPruningVetoCTE vetoes its body " +
      s"because caching it loses the outer `d_year = 2002` predicate's partition " +
      s"pruning -- measured at 100TB as six SubqueryBroadcast nodes going to zero and " +
      s"the first execution going 59.5s -> 118.7s. Got ${on.entries} cache entries.")
    assert(on.text == off.text,
      s"q75 (pushPartialAgg=$pagg) must plan exactly like the auto-CTE-off baseline " +
      s"once vetoed. A difference means the veto stopped reaching the decision, or " +
      s"the CTE landed on ReplaceCTERefWithRepartition:\n=== off ===\n${off.text}" +
      s"\n=== on ===\n${on.text}")
    assert(on.rows.map(_.toString).sorted == off.rows.map(_.toString).sorted,
      s"q75 (pushPartialAgg=$pagg) answer differs from the auto-CTE-off baseline " +
      s"(off=${off.rows.size} rows, on=${on.rows.size} rows)")
  }
  }

  // The rest of the 14 queries app 0027 cached. They are not in `mustBenefit`
  // because their cluster speedups came with a caching shape this fixture cannot
  // reproduce faithfully at this scale, but they DO cache here and they must be
  // answer-checked: together with the loops above this covers all 14, which is the
  // point -- every query the feature touches on the benchmark gets its answer
  // compared against the inlined baseline, under both settings of
  // `partialAggregationOptimization`.
  for (name <- Seq("q2", "q47", "q57"); pagg <- paggModes) {
    test(s"$name: cached answer matches inlining and does no more work " +
      s"(pushPartialAgg=$pagg)") {
      populateSmallData()
      val p = workProfile(name, pagg)
      assert(p.inlined > 0, s"$name processed nothing with auto-CTE off; test is vacuous")
      assertSameAnswer(name, pagg, p)
      assert(p.warm <= p.inlined,
        s"$name warm cache must not do MORE work than inlining " +
        s"(inlined=${p.inlined}, cold=${p.cold}, warm=${p.warm})")
    }
  }
}
