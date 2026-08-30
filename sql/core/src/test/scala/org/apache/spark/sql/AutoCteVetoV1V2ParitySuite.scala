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

import org.apache.spark.sql.catalyst.plans.logical.CTERelationDef
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.dynamicpruning.TagPruningVetoCTE
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Measures whether routing parquet through the V2 `FileScan` path changes the auto-CTE
 * pruning veto's verdict, against a real partitioned sf100 dataset.
 *
 * IGNORED ON PURPOSE -- a manual measurement harness, not a behavioural guard. Flip
 * `ignore` to `test` to run it. Two reasons it does not belong in a normal run:
 * `beforeAll` declares and ANALYZEs 24 tables (~12 minutes), and the question it answers
 * is already answered. It is committed so the measurement can be REPRODUCED on a
 * connector TPC-DS cannot represent (see "Not covered" below), not so it runs daily.
 * Committing it also keeps it compiling with the tree: a rename of `pruningVeto` or
 * `TagPruningVetoCTE` then breaks the build instead of rotting silently.
 *
 * ## What it measures
 *
 * `looksLikeMaterializationNotWorthIt` is an empirical shape fingerprint, calibrated on
 * V1/Hive TPC-DS q75 (100TB, 59.5s -> 118.7s) and on Lance. `90f13ea7d43` added the
 * `ExtractV2Scan(scan: FileScan)` branch to `PartitionPruning.getFilterableTableScan`,
 * which generalises that fingerprint to every V2 file source at once. Both veto
 * predicates move with it, in OPPOSITE directions:
 *
 *  - more bodies look like they have in-body DPP -> `hasInBodyDPPOpportunity` is true
 *    more often -> FEWER vetoes;
 *  - more bodies look not worth materialising -> `looksLikeMaterializationNotWorthIt` is
 *    true more often -> MORE vetoes.
 *
 * The veto is `!A && B`, so which direction wins is per query and cannot be settled by
 * reading the code. This harness takes the verdict under V1 and under V2 on the same
 * data and reports the queries whose verdict flips.
 *
 * ## Result, 2026-08-31 (sf100 partitioned, 14 queries, plan-only)
 *
 * Zero flips. q75 vetoed and not cached on both sides, which is the calibrated behaviour
 * holding; q95 not cached on both sides, rejected by `isRowExpanding` and unrelated to
 * the veto; the other 12 cached and not vetoed on both sides. Under V1 these tables are
 * already filterable scans via `HadoopFsRelation`, so the V2 branch only gives the V2
 * reader the same recognition -- the set of filterable scans does not change, so the
 * predicates' input does not change. Recorded in
 * `docs/rebase-90f13ea7d43-autocte-followups.md` item 2, whose disposition is: do not
 * narrow the heuristic by connector type.
 *
 * ## What this does NOT show
 *
 * It reads the COMPOSITE verdict, not the two predicates separately. Zero flips proves
 * the behaviour is unchanged; it does not rule out both predicates flipping and
 * cancelling inside `!A && B`. Telling them apart means calling `PruningEligibility`'s
 * two predicates directly per CTE def, which is worth doing only once a flip shows up
 * here.
 *
 * ## Not covered: Lance and Iceberg
 *
 * TPC-DS parquet exercises the `dpp` half only. The `dfp` half of the calibration comes
 * from Lance, and Iceberg is the other connector the veto reaches. Narrowing by
 * connector type stays blocked on measuring one of those two, which is also the open
 * question behind followups item 3.
 *
 * ## Two methodology traps, both hit while writing this
 *
 *  - `vetoedDefs` reads the ANALYZED plan, not the optimized one. A vetoed CTE gets
 *    inlined and leaves no `CTERelationDef` behind, so on the optimized plan "vetoed" and
 *    "never a candidate" are indistinguishable.
 *  - it does not use `TagPruningVetoCTE.vetoCount`. That is a process-level `LongAdder`
 *    and sbt runs many suites in one JVM, so the count bleeds across suites.
 */
class AutoCteVetoV1V2ParitySuite extends QueryTest with SharedSparkSession
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
      if (registered) tables.foreach(t => spark.sql(s"DROP TABLE IF EXISTS `$t`"))
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Same registration as `AutoCteVetoPartitionedSuite`: external tables declared over the
   * existing parquet directories, so the 34 GiB dataset is not copied. `RECOVER
   * PARTITIONS` is what populates `partitionSchema` from the `col=value` directory names;
   * without it the table is declared partitioned but has none, and
   * `getFilterableTableScan` refuses on both read paths, which would make every row of
   * the table below read "not filterable" for the wrong reason.
   */
  private def registerTables(): Unit = {
    withSQLConf(SQLConf.HISTOGRAM_ENABLED.key -> "false") {
      tables.foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS `$t`")
        val loc = new File(dataPath, t).getAbsolutePath
        val schema = spark.read.parquet(loc).schema
        val partCols = tablePartitionColumns.getOrElse(t, Nil)
          .map(_.stripPrefix("`").stripSuffix("`"))
        val dataCols = schema.fields.filterNot(f => partCols.contains(f.name))
        val allCols = dataCols ++ partCols.flatMap(pc => schema.fields.find(_.name == pc))
        val colDdl = allCols.map(f => s"`${f.name}` ${f.dataType.catalogString}").mkString(", ")
        val partClause =
          if (partCols.isEmpty) "" else s"PARTITIONED BY (${partCols.mkString(", ")})"
        spark.sql(
          s"""CREATE TABLE `$t` ($colDdl) USING parquet $partClause LOCATION '$loc'""")
        if (partCols.nonEmpty) spark.sql(s"ALTER TABLE `$t` RECOVER PARTITIONS")
        spark.sql(
          s"ANALYZE TABLE `$t` COMPUTE STATISTICS FOR COLUMNS ${schema.fieldNames.mkString(", ")}")
      }
    }
  }

  /** The cluster's auto-CTE configuration, with only the read path differing. */
  private def clusterConf(useV1: Boolean): Seq[(String, String)] = Seq(
    SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1",
    SQLConf.CBO_ENABLED.key -> "true",
    SQLConf.PLAN_STATS_ENABLED.key -> "true",
    SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "true",
    SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
    SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
    // "" routes parquet through the V2 FileScan path; the default list keeps it on V1.
    SQLConf.USE_V1_SOURCE_LIST.key -> (if (useV1) "avro,csv,json,kafka,orc,parquet,text" else ""))

  /** Counts the CTE defs the veto tags. Analyzed plan on purpose -- see the class doc. */
  private def vetoedDefs(sqlText: String): Int = {
    val analyzed = spark.sql(sqlText).queryExecution.analyzed
    TagPruningVetoCTE.apply(analyzed)
      .collectWithSubqueries { case d: CTERelationDef if d.pruningVeto => d }.size
  }

  private def cachesBody(sqlText: String): Boolean =
    spark.sql(sqlText).queryExecution.optimizedPlan.toString.contains("InMemoryRelation")

  private def sqlOf(name: String): String =
    resourceToString(s"tpcds/$name.sql",
      classLoader = Thread.currentThread().getContextClassLoader)

  /** The 14 queries auto-CTE hits, i.e. the ones whose verdict can differ at all. */
  private val queries = Seq(
    "q2", "q14a", "q14b", "q23a", "q23b", "q24a", "q24b",
    "q39a", "q39b", "q47", "q57", "q74", "q75", "q95")

  ignore("veto verdicts are identical under the V1 and V2 read paths") {
    assume(hasData, s"no partitioned TPC-DS dataset at $dataPath")
    val rows = queries.map { q =>
      val sqlText = sqlOf(q)
      val (v1Veto, v1Cache) = withSQLConf(clusterConf(useV1 = true): _*) {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        (vetoedDefs(sqlText), cachesBody(sqlText))
      }
      val (v2Veto, v2Cache) = withSQLConf(clusterConf(useV1 = false): _*) {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        (vetoedDefs(sqlText), cachesBody(sqlText))
      }
      (q, v1Veto, v1Cache, v2Veto, v2Cache)
    }
    val flipped = rows.collect {
      case (q, v1Veto, v1Cache, v2Veto, v2Cache) if v1Veto != v2Veto || v1Cache != v2Cache => q
    }
    // The table is the deliverable: a flip needs the per-query numbers to be read, not
    // just the fact that one happened.
    // scalastyle:off println
    println("PARITY query  v1Veto v1Cache  v2Veto v2Cache  flipped")
    rows.foreach { case (q, v1v, v1c, v2v, v2c) =>
      val mark = if (v1v != v2v || v1c != v2c) "YES" else ""
      println(f"PARITY $q%-6s $v1v%6d $v1c%7s  $v2v%6d $v2c%7s  $mark")
    }
    // scalastyle:on println
    assert(flipped.isEmpty,
      s"the veto verdict differs between the V1 and V2 read paths for ${flipped.mkString(", ")}" +
        " -- followups item 2's conclusion no longer holds. Read the two predicates" +
        " separately before touching the heuristic; the composite !A && B cannot say which" +
        " one moved.")
  }
}
