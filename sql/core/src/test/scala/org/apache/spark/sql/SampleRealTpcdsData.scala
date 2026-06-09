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

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Writes the sampled TPC-DS dataset that [[RealTpcdsData]] consumes, from a full
 * dsdgen-produced sf100 tree. Run it by hand; it is `ignore`d so a normal test run
 * never pays for it:
 *
 * {{{
 * build/sbt -Pyarn \
 *   -Dspark.test.tpcds.sourcePath=/path/to/tpcds-sf100-parquet \
 *   -Dspark.test.tpcds.dataPath=/path/to/output \
 *   "sql/testOnly org.apache.spark.sql.SampleRealTpcdsData -- -z sample"
 * }}}
 *
 * and change `ignore` to `test` for the one run. Takes a couple of minutes and writes
 * about 1.6 GiB.
 *
 * The sampling rule is `pmod(i_item_sk, 97) = 1` UNION every item whose `i_color` is a
 * literal q24a/q24b filter on, with the fact tables reduced by a semi-join against the
 * resulting key set.
 *
 * On why the colour exemption is needed rather than just a coarser rate: q24b filters
 * `i_color = 'chiffon'`, of which sf100 has 488 items, and its body is a six-way join
 * whose `c_birth_country = upper(ca_country)` has exactly ONE overlapping value at
 * sf100. Measured: plain 1-in-97 keeps 7 chiffon items and the body produces 0 rows;
 * 1-in-31 keeps 18 and still produces 0; 1-in-11 keeps 36 and produces 939 -- but a
 * 1-in-11 sample of the facts is 5.6 GiB. Keeping the two colours whole costs 536 extra
 * items (1/21 of the facts, 1.6 GiB) and gets q24b to 29 rows.
 *
 * On why `i_item_sk` is the sampling axis, see [[RealTpcdsData]]'s scaladoc: it is the
 * only one that preserves dimension literals, the store_sales/store_returns pairing,
 * and per-(item, date) group counts simultaneously.
 */
class SampleRealTpcdsData extends QueryTest with SharedSparkSession {

  /** Fact tables and the item column each is sampled on. */
  private val itemCol = Map(
    "store_sales" -> "ss_item_sk", "store_returns" -> "sr_item_sk",
    "catalog_sales" -> "cs_item_sk", "catalog_returns" -> "cr_item_sk",
    "web_sales" -> "ws_item_sk", "web_returns" -> "wr_item_sk",
    "inventory" -> "inv_item_sk")

  private val tables = Seq(
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
    "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site")

  private def sourcePath: String =
    spark.conf.getOption("spark.test.tpcds.sourcePath")
      .orElse(sys.env.get("SPARK_TPCDS_SOURCE"))
      .getOrElse(sys.error("set spark.test.tpcds.sourcePath to a full sf100 parquet tree"))

  private def outPath: String =
    spark.conf.getOption("spark.test.tpcds.dataPath")
      .orElse(sys.env.get("SPARK_TPCDS_DATA"))
      .getOrElse(SampleRealTpcdsData.defaultPath)

  ignore("sample a full sf100 tree down to an item-keyed subset") {
    assume(new File(sourcePath).isDirectory, s"no source dataset at $sourcePath")

    val item = spark.read.parquet(s"$sourcePath/item")
      .where(s"pmod(i_item_sk, ${SampleRealTpcdsData.itemModulus}) = 1 OR " +
        s"i_color IN (${SampleRealTpcdsData.keptColors.map("'" + _ + "'").mkString(", ")})")
    item.write.mode("overwrite").parquet(s"$outPath/item")

    val keys = spark.read.parquet(s"$outPath/item").select("i_item_sk").distinct().cache()
    assert(keys.count() > 0, "sampling produced no item keys")

    tables.filter(_ != "item").foreach { t =>
      val src = spark.read.parquet(s"$sourcePath/$t")
      val out = itemCol.get(t) match {
        // Semi-join, not `pmod` on the fact column: the key set is the sampled items
        // UNION the colour-exempt ones, so it is not expressible as a modulus.
        case Some(c) =>
          src.join(keys.hint("broadcast"), src(c) === keys("i_item_sk"), "left_semi")
        case None => src
      }
      out.write.mode("overwrite").parquet(s"$outPath/$t")
    }
    keys.unpersist()
  }
}

object SampleRealTpcdsData {
  /**
   * Where the sampled dataset is written and read by default.
   *
   * Not under `/tmp`: macOS and most Linux distributions clear it periodically, and
   * regenerating costs a couple of minutes plus a full sf100 tree to sample from.
   * Override with `spark.test.tpcds.dataPath` or `SPARK_TPCDS_DATA`.
   */
  val defaultPath = "/Users/yangjie01/Tools/tpcds-sf100-item-sampled"

  /** 1-in-97 on `i_item_sk`; see the class scaladoc for how this rate was picked. */
  val itemModulus = 97

  /** Colours kept whole because q24a/q24b filter on them literally. */
  val keptColors = Seq("pale", "chiffon")
}
