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

import org.apache.spark.sql.catalyst.TableIdentifier

trait TPCDSBase extends TPCBase with TPCDSSchema {

  // The TPCDS queries below are based on v1.4
  private val tpcdsAllQueries: Seq[String] = Seq("q24a", "q24b")

  // Since `tpcdsQueriesV2_7_0` has almost the same queries with these ones below,
  // we skip them in the TPCDS-related tests.
  // NOTE: q6" and "q75" can cause flaky test results, so we must exclude them.
  // For more details, see SPARK-35327.
  private val excludedTpcdsQueries: Set[String] = Set("q6", "q34", "q64", "q74", "q75", "q78")

  val tpcdsQueries: Seq[String] = tpcdsAllQueries.filterNot(excludedTpcdsQueries.contains)

  // This list only includes TPCDS v2.7 queries that are different from v1.4 ones
  val tpcdsQueriesV2_7_0 = Seq(
    "q5a", "q6", "q10a", "q11", "q12", "q14", "q14a", "q18a",
    "q20", "q22", "q22a", "q24", "q27a", "q34", "q35", "q35a", "q36a", "q47", "q49",
    "q51a", "q57", "q64", "q67a", "q70a", "q72", "q74", "q75", "q77a", "q78",
    "q80a", "q86a", "q98")

  // These queries are from https://github.com/cloudera/impala-tpcds-kit/tree/master/queries
  val modifiedTPCDSQueries = Seq(
    "q3", "q7", "q10", "q19", "q27", "q34", "q42", "q43", "q46", "q52", "q53", "q55", "q59",
    "q63", "q65", "q68", "q73", "q79", "q89", "q98", "ss_max")

  protected def partitionedByClause(tableName: String): String = {
    tablePartitionColumns.get(tableName) match {
      case Some(cols) if cols.nonEmpty => s"PARTITIONED BY (${cols.mkString(", ")})"
      case _ => ""
    }
  }

  val tableNames: Iterable[String] = tableColumns.keys

  def createTable(
      spark: SparkSession,
      tableName: String,
      format: String = "parquet",
      options: Seq[String] = Nil): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |${partitionedByClause(tableName)}
         |${options.mkString("\n")}
       """.stripMargin)
  }

  override def createTables(): Unit = {
    tableNames.foreach { tableName =>
      createTable(spark, tableName)
      if (injectStats) {
        // To simulate plan generation on actual TPC-DS data, injects data stats here
        spark.sessionState.catalog.alterTableStats(
          TableIdentifier(tableName), Some(TPCDSTableStats.sf100TableStats(tableName)))
      }
    }
  }

  override def dropTables(): Unit = {
    tableNames.foreach { tableName =>
      spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
    }
  }
}
