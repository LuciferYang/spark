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

import org.apache.spark.sql.internal.SQLConf

/**
 * Registers a real (dsdgen-produced) TPC-DS dataset as temporary views, so a suite can
 * answer-check the benchmark queries against actual data instead of synthetic rows.
 *
 * Why real data. The synthetic fixture cannot make six of the fourteen queries the
 * benchmark caches return any rows at all -- q14b/q23b/q24a/q24b/q39a/q39b came out
 * empty -- because their filters need value distributions that are not reproducible by
 * generating columns independently: `i_color = 'pale'` needs the literal itself,
 * q24a needs `s_zip = ca_zip` (101 overlapping values at sf100) AND
 * `c_birth_country = upper(ca_country)` (exactly ONE overlapping value), q23b needs
 * `HAVING count(*) > 4` per (item, date), and q39a needs `stddev/mean > 1` which
 * requires several rows per group. An answer comparison on an empty result proves
 * nothing, so this trait exists to make those six checkable.
 *
 * The dataset is external and OPTIONAL: [[hasRealTpcdsData]] is false when the path is
 * absent, and suites are expected to `cancel` in that case rather than fail. Point
 * `spark.test.tpcds.dataPath` (or the `SPARK_TPCDS_DATA` environment variable) at a
 * directory holding one subdirectory of parquet per TPC-DS table.
 *
 * Sampling. Full sf100 is 34 GiB, too slow to execute a query four times per test.
 * [[SampleRealTpcdsData]] writes a subset keyed on `i_item_sk`, which is the only axis
 * that keeps all three properties the six queries need: dimension literals survive
 * (item rows are kept whole), the store_sales/store_returns pairing on
 * (ticket_number, item_sk) survives, and per-(item, date) group counts are EXACTLY
 * preserved, which sampling by date would destroy.
 *
 * Statistics. Suites keep `injectStats = false` here and instead run ANALYZE, or rely
 * on the parquet row counts: unlike the synthetic fixture, the data volume is real, so
 * injecting sf100 statistics over a 1/21 sample would make the optimizer estimate 21x
 * the rows that exist. Plan-shape guarantees under cluster-like statistics stay in
 * `AutoCteTpcdsPlanGuaranteeSuite`, which uses empty tables plus `injectStats` and is
 * unaffected by any of this.
 */
trait RealTpcdsData { self: org.apache.spark.sql.test.SharedSparkSession =>

  /** Every TPC-DS table, matching the subdirectory names dsdgen output produces. */
  protected val realTpcdsTables: Seq[String] = Seq(
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
    "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site")

  /** Where the sampled dataset lives; overridable per suite. */
  protected def realTpcdsDataPath: String =
    spark.conf.getOption("spark.test.tpcds.dataPath")
      .orElse(sys.env.get("SPARK_TPCDS_DATA"))
      .getOrElse(SampleRealTpcdsData.defaultPath)

  /** True when the dataset is present and holds every table. */
  protected def hasRealTpcdsData: Boolean = {
    val root = new File(realTpcdsDataPath)
    root.isDirectory && realTpcdsTables.forall { t =>
      val d = new File(root, t)
      d.isDirectory && d.listFiles() != null &&
        d.listFiles().exists(_.getName.endsWith(".parquet"))
    }
  }

  /**
   * Registers each table as a temporary view and computes table + column statistics.
   *
   * ANALYZE rather than `injectStats`: the row counts here are real, so the optimizer
   * must see the real ones. `NOSCAN` is not used because the cost gates this suite
   * exercises (`partialAggregationOptimization.benefitRatio`,
   * `AUTO_CTE_CACHE_MIN_SIZE_BYTES`) read column statistics, not just table size.
   *
   * Histograms stay OFF (`spark.sql.statistics.histogram.enabled` defaults to false
   * and is pinned here): they cost an extra full scan per column and none of the gates
   * under test read them.
   */
  protected def registerRealTpcdsTables(): Unit = {
    withSQLConf(SQLConf.HISTOGRAM_ENABLED.key -> "false") {
      realTpcdsTables.foreach { t =>
        spark.read.parquet(s"$realTpcdsDataPath/$t").write
          .mode("overwrite").format("parquet").saveAsTable(t)
        val cols = spark.table(t).schema.fieldNames.mkString(", ")
        spark.sql(s"ANALYZE TABLE `$t` COMPUTE STATISTICS FOR COLUMNS $cols")
      }
    }
  }

  protected def dropRealTpcdsTables(): Unit =
    realTpcdsTables.foreach(t => spark.sql(s"DROP TABLE IF EXISTS `$t`"))

}
