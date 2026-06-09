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

import java.io.File

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.CTERelationDef
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.dynamicpruning.TagPruningVetoCTE
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * The veto's verdict on the 14 cluster-measured queries, against a REAL partitioned
 * sf100 dataset with computed column statistics.
 *
 * Why a second TPC-DS suite when `AutoCteTpcdsPlanGuaranteeSuite` and
 * `AutoCteTpcdsWorkSuite` already run these queries: those two use `TPCDSBase` tables,
 * which ARE partitioned (`PARTITIONED BY (*_date_sk)`) and do let
 * `PruningEligibility.isFilterableScan` succeed -- `getFilterableTableScan` reads
 * `HadoopFsRelation.partitionSchema`, which comes from the DDL, so the veto reaches its
 * decision there too and reaches the SAME verdict (measured: q75 vetoed, the other 13
 * not, on both the empty and the populated fixture). What those suites cannot produce
 * is a case where the verdict MATTERS: 240 fact rows over 90 partitions make pruning
 * worth nothing, statistics are injected rather than computed so `AggregateEstimation`
 * sees numbers that do not describe the data, and the work counters swamp the effect
 * with DPP scheduling noise.
 *
 * This suite registers the real sf100 tree at `spark.test.tpcds.partitionedPath` (a
 * dsdgen-produced dataset written with `PARTITIONED BY (*_sold_date_sk)`, matching the
 * cluster layout) as external tables over the existing parquet directories, so no data
 * is copied.
 *
 * Column statistics are computed on purpose, not for the veto but for
 * `partialAggregationOptimization`: `PushPartialAggregationThroughJoin`'s cost gate
 * is `pushPartialAggHasBenefit`, which divides an `Aggregate`'s estimated row count by
 * its child's. `AggregateEstimation` needs count statistics on every grouping
 * attribute and returns `None` without them, at which point the ratio defaults to 1.0
 * and `1.0 <= benefitRatio` is false for any sane ratio -- the whole pagg chain would
 * silently not fire and the `pagg=true` half of the matrix would be a copy of the
 * `pagg=false` half. Histograms stay off: they cost an extra scan per column and no
 * gate here reads them.
 */
class AutoCteVetoPartitionedSuite extends QueryTest with SharedSparkSession
  with TPCDSSchema {

  private def dataPath: String =
    spark.conf.getOption("spark.test.tpcds.partitionedPath")
      .orElse(sys.env.get("SPARK_TPCDS_PARTITIONED"))
      .getOrElse("/Users/yangjie01/Tools/tpcds-sf-100-parquet-partitioned")

  private val tables: Iterable[String] = tableColumns.keys

  private def hasData: Boolean = {
    val root = new File(dataPath)
    root.isDirectory && tables.forall(t => new File(root, t).isDirectory)
  }

  private var registered = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (hasData) {
      registerTables()
      registered = true
    }
  }

  override def afterAll(): Unit = {
    try {
      if (registered) {
        tables.foreach(t => spark.sql(s"DROP TABLE IF EXISTS `$t`"))
      }
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Declares each table over its existing parquet directory and recovers the
   * partitions, then computes statistics.
   *
   * `LOCATION` rather than a copy: the dataset is 34 GiB and copying it into managed
   * tables would dominate the suite. `RECOVER PARTITIONS` is what populates
   * `partitionSchema` from the `col=value` directory names -- without it the table is
   * declared partitioned but has no partitions, and `getFilterableTableScan` would
   * still refuse.
   */
  private def registerTables(): Unit = {
    withSQLConf(SQLConf.HISTOGRAM_ENABLED.key -> "false") {
      tables.foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS `$t`")
        val loc = new File(dataPath, t).getAbsolutePath
        // The schema comes from the parquet files, not from `TPCDSSchema.tableColumns`:
        // this dataset was written with different physical types for some columns
        // (`time_dim.t_time_sk` is INT64 where that DDL says int), and a declared
        // schema which disagrees fails the scan with
        // SchemaColumnConvertNotSupportedException.
        //
        // `spark.read.parquet` runs partition discovery, so its schema has the
        // partition column LAST and typed as the directory names imply. A declared
        // table needs the same ordering, and needs the partition column named in
        // PARTITIONED BY -- `CREATE TABLE ... LOCATION` with no column list does NOT
        // pick partitioning up here (it reported 0 partitions), which is why this
        // builds the DDL explicitly and then recovers the partition metadata.
        val schema = spark.read.parquet(loc).schema
        val partCols = tablePartitionColumns.getOrElse(t, Nil)
          .map(_.stripPrefix("`").stripSuffix("`"))
        val dataCols = schema.fields.filterNot(f => partCols.contains(f.name))
        val allCols = dataCols ++ partCols.flatMap(pc => schema.fields.find(_.name == pc))
        val colDdl = allCols.map(f => s"`${f.name}` ${f.dataType.catalogString}")
          .mkString(", ")
        val partClause =
          if (partCols.isEmpty) "" else s"PARTITIONED BY (${partCols.mkString(", ")})"
        spark.sql(
          s"""
             |CREATE TABLE `$t` ($colDdl)
             |USING parquet
             |$partClause
             |LOCATION '$loc'
           """.stripMargin)
        if (partCols.nonEmpty) {
          spark.sql(s"ALTER TABLE `$t` RECOVER PARTITIONS")
        }
        val statCols = schema.fieldNames.mkString(", ")
        spark.sql(s"ANALYZE TABLE `$t` COMPUTE STATISTICS FOR COLUMNS $statCols")
      }
    }
  }

  /** Cluster configuration, minus the auto-CTE switch which each test sets. */
  private def clusterConf(pagg: Boolean): Seq[(String, String)] = Seq(
    SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1",
    SQLConf.CBO_ENABLED.key -> "true",
    SQLConf.PLAN_STATS_ENABLED.key -> "true",
    SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "true",
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> pagg.toString)

  /**
   * How many `CTERelationDef`s `TagPruningVetoCTE` tags for this query.
   *
   * Applied to the analyzed plan directly rather than read off the optimized plan: a
   * vetoed CTE is inlined away and leaves no `CTERelationDef` behind, so the optimized
   * plan cannot distinguish "vetoed" from "never a candidate". Not derived from
   * `TagPruningVetoCTE.vetoCount` either -- that is a process-wide `LongAdder` and sbt
   * runs suites in one JVM.
   */
  private def vetoedDefs(sqlText: String): Int = {
    val analyzed = spark.sql(sqlText).queryExecution.analyzed
    TagPruningVetoCTE.apply(analyzed)
      .collectWithSubqueries { case d: CTERelationDef if d.pruningVeto => d }.size
  }

  private def sqlOf(name: String): String =
    resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)

  /** The 14 queries app 0027 cached at 100TB, before either structural gate existed. */
  private val allCached = Seq(
    "q2", "q14a", "q14b", "q23a", "q23b", "q24a", "q24b",
    "q39a", "q39b", "q47", "q57", "q74", "q75", "q95")

  /**
   * The two of the 14 that must NOT be cached now, and which gate refuses each. They
   * are refused for unrelated reasons and by different rules, which is why "not
   * vetoed" and "cached" are not interchangeable:
   *
   *   - q95 is refused by `ReplaceCTERefWithCache.isRowExpanding`: its body is a join
   *     with no aggregate above it (7.2e10 rows in, 8.04e11 out), so materialising it
   *     can only ever store more rows than it reads. It is never vetoed -- the veto
   *     rule does not even look at it -- yet it does not cache.
   *   - q75 is refused by `TagPruningVetoCTE`: caching costs it six DPP nodes.
   */
  private val mustNotCache = Map(
    "q95" -> "isRowExpanding (join with no aggregate above it)",
    "q75" -> "TagPruningVetoCTE (caching loses the outer year predicate's DPP)")

  /** Whether the optimized plan actually reads an auto-CTE cache. */
  private def cachesBody(sqlText: String): Boolean = {
    val plan = spark.sql(sqlText).queryExecution.optimizedPlan
    plan.toString.contains("InMemoryRelation")
  }

  test("q75's body is vetoed, so the outer d_year predicate keeps driving DPP") {
    assume(hasData, s"no partitioned TPC-DS dataset at $dataPath")
    // The measured defect this pins: caching q75's body turned six SubqueryBroadcast
    // DPP nodes into zero at 100TB, because the outer `d_year = 2002` could no longer
    // reach the fact scans through the InMemoryRelation barrier. The three fact tables
    // went from 195.4e9 pruned rows to 490.3e9 unpruned and the first execution went
    // 59.5s -> 118.7s. The veto exists to refuse exactly this trade, and it only
    // reaches the decision now that `Distinct` is dataflow-transparent in
    // `PruningEligibility.transformSurvival` -- q75's body is
    // `Aggregate(GROUP BY d_year, ...) over Distinct(Union(3 join chains))`.
    Seq(false, true).foreach { pagg =>
      withSQLConf((SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") +:
          clusterConf(pagg): _*) {
        assert(vetoedDefs(sqlOf("q75")) >= 1,
          s"q75 (pagg=$pagg) must be vetoed: its body has a partitioned fact scan " +
          s"joined to date_dim with no in-body date filter, so caching it loses the " +
          s"outer year predicate's partition pruning and buys nothing back")
      }
    }
  }

  test("q75 is inlined end to end and keeps its dynamic pruning") {
    assume(hasData, s"no partitioned TPC-DS dataset at $dataPath")
    Seq(false, true).foreach { pagg =>
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      withSQLConf((SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") +:
          clusterConf(pagg): _*) {
        val plan = spark.sql(sqlOf("q75")).queryExecution.optimizedPlan
        val text = plan.toString
        assert(!text.contains("InMemoryRelation"),
          s"q75 (pagg=$pagg) must not be cached after the veto:\n$text")
        // The point of vetoing: DPP comes back. `DynamicPruningSubquery` is what
        // `PartitionPruning` injects and what the InMemoryRelation barrier destroyed.
        assert(text.contains("dynamicpruning"),
          s"q75 (pagg=$pagg) was inlined but carries no dynamic pruning, so the veto " +
          s"bought nothing -- the whole reason to refuse the cache:\n$text")
        // And it must not land on the round-robin fallback, which measured 22 min
        // against 1.4 min for inlining on q95 at 100TB.
        assert(!text.contains("RoundRobinPartitioning"),
          s"q75 (pagg=$pagg) fell through to ReplaceCTERefWithRepartition:\n$text")
      }
    }
  }

  test("exactly 12 of the 14 cache; q95 and q75 are refused by different gates") {
    assume(hasData, s"no partitioned TPC-DS dataset at $dataPath")
    // Pins the whole picture rather than one query, because the two facts that matter
    // are easy to conflate: "not vetoed" is not the same as "cached" (q95 is never
    // vetoed and still does not cache), and the count is what a benchmark run sees.
    Seq(false, true).foreach { pagg =>
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      withSQLConf((SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") +:
          clusterConf(pagg): _*) {
        val cached = allCached.filter(q => cachesBody(sqlOf(q)))
        val notCached = allCached.filterNot(cached.contains)
        assert(notCached.toSet == mustNotCache.keySet,
          s"pagg=$pagg: expected exactly ${mustNotCache.keySet.toSeq.sorted} to be " +
          s"refused (${mustNotCache.values.mkString("; ")}), but the ones not caching " +
          s"were ${notCached.sorted}. Cached: ${cached.sorted}")
        assert(cached.size == 12,
          s"pagg=$pagg: expected 12 of the 14 to cache, got ${cached.size}: " +
          s"${cached.sorted}")
      }
    }
  }

  test("the 12 that cache are not vetoed, and q95 is refused without being vetoed") {
    assume(hasData, s"no partitioned TPC-DS dataset at $dataPath")
    // The constraint on any change to the veto: 12 of the 14 still cache, and most of
    // their bodies ALSO join date_dim on a partition column. What keeps them cached is
    // the in-body date filter -- `d_year BETWEEN 1999 AND 2001` in q14a,
    // `d_year IN (2001, 2002)` in q74 -- which makes `hasInBodyDPPOpportunity` true and
    // short-circuits the veto. q75 is the only one of the 14 whose body has no in-body
    // date filter at all; its year predicate lives in the outer query.
    Seq(false, true).foreach { pagg =>
      withSQLConf((SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") +:
          clusterConf(pagg): _*) {
        val vetoed = allCached.filter(_ != "q75").filter(q => vetoedDefs(sqlOf(q)) > 0)
        assert(vetoed.isEmpty,
          s"pagg=$pagg: only q75 may be vetoed, but these were too: " +
          s"${vetoed.mkString(", ")}. Vetoing a query that benefits from caching " +
          s"forces it back to inlining and gives up the measured speedup.")
        // q95 specifically: refused by the row-expanding gate, NOT by the veto. If a
        // future change makes the veto claim it, the two gates have started to overlap
        // and the reason a query is not cached stops being attributable.
        assert(vetoedDefs(sqlOf("q95")) == 0,
          s"pagg=$pagg: q95 must be refused by isRowExpanding, not by the veto")
      }
    }
  }

  test("partitioning and column statistics are actually in place") {
    assume(hasData, s"no partitioned TPC-DS dataset at $dataPath")
    // Both are load-bearing and both fail silently. Without partitions
    // `isFilterableScan` is false for every query and every veto test above passes
    // vacuously; without column statistics `pushPartialAggHasBenefit` divides by a
    // missing row count, defaults its ratio to 1.0, and the whole pagg chain does not
    // fire -- making the pagg=true half of this suite a copy of the pagg=false half.
    val fact = "store_sales"
    val parts = spark.sql(s"SHOW PARTITIONS `$fact`").count()
    assert(parts > 1000,
      s"$fact reports $parts partitions; RECOVER PARTITIONS did not populate them and " +
      s"getFilterableTableScan will refuse every scan")

    val stats = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier(fact)).stats
    assert(stats.exists(_.rowCount.exists(_ > 0)),
      s"$fact has no row count; AggregateEstimation returns None and the " +
      s"partial-aggregation cost gate defaults to a ratio of 1.0")
    assert(stats.exists(_.colStats.nonEmpty),
      s"$fact has no column statistics; same consequence as a missing row count")
  }
}

