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

import org.apache.spark.sql.types._

/**
 * Fills the empty TPC-DS tables created by [[TPCDSBase]] with a small amount of
 * synthetic data, chosen so the benchmark queries actually return rows.
 *
 * Two properties matter and neither is automatic:
 *
 *   - Join keys line up. Every `*_sk` column is filled with `id % keySpace`, and the
 *     dimension tables' own surrogate keys use the same space, so a fact row always
 *     finds its dimension row. `d_date_sk` is the exception: `date_dim` numbers its
 *     rows `0 until dateRows`, and fact tables' `*_date_sk` columns draw from the
 *     same range.
 *   - Dimension values fall inside the filters. `d_year` covers 1998-2002 and
 *     `d_moy` 1-12 because q14/q23/q39 filter on both; `d_date` starts at 1999-01-01
 *     and steps daily so q95's two-month window matches; `i_category`/`i_class` and
 *     `s_store_sk` take a handful of distinct values so grouping keys are not
 *     degenerate.
 *
 * Everything else is filled by type. Row counts are tiny on purpose -- the point is
 * to execute the real query plans, not to be representative. Statistics come from
 * `TPCDSBase.injectStats`, so the OPTIMIZER still sees sf100 and makes cluster-like
 * decisions while EXECUTION stays fast.
 */
trait TPCDSSmallDataFixture { self: TPCDSBase =>

  /** Distinct values for surrogate keys other than dates. */
  protected def keySpace: Int = 12

  /** Rows in `date_dim`, and the size of the date-key space. */
  protected def dateRows: Int = 90

  /** Rows in each fact table. */
  protected def factRows: Int = 240

  private def rowsFor(table: String): Int = table match {
    case "date_dim" => dateRows
    case "store_sales" | "store_returns" | "catalog_sales" | "catalog_returns" |
         "web_sales" | "web_returns" | "inventory" => factRows
    case _ => keySpace
  }

  /** SQL for one column of the generated row, given the loop variable `id`. */
  private def valueExpr(table: String, field: StructField): String = {
    val name = field.name
    val idx = s"CAST(id AS INT)"
    def num(e: String) = s"CAST($e AS ${field.dataType.sql})"

    // Date dimension: the values queries filter on.
    if (table == "date_dim") {
      name match {
        case "d_date_sk" => return num(idx)
        case "d_date" => return s"date_add(DATE '1999-01-01', $idx)"
        case "d_year" => return num(s"1998 + (id % 5)")
        case "d_moy" => return num(s"1 + (id % 12)")
        case "d_dom" => return num(s"1 + (id % 28)")
        case "d_qoy" => return num(s"1 + (id % 4)")
        case "d_month_seq" => return num(s"1200 + (id % 24)")
        case "d_week_seq" | "d_quarter_seq" => return num(s"id % 30")
        case _ =>
      }
    }
    // Surrogate keys: the table's own key is dense, foreign keys wrap.
    if (name.endsWith("_date_sk")) return num(s"id % $dateRows")
    if (name.endsWith("_sk")) return num(s"id % $keySpace")
    // Grouping columns that must have a few distinct values.
    // Columns the queries group or filter on need a few distinct values. Each is
    // capped to the column's declared length, or skipped when the column is too
    // narrow to hold the intended literal.
    val declaredLen = field.dataType match {
      case c: CharType => c.length
      case v: VarcharType => v.length
      case _ => Int.MaxValue
    }
    def literal(v: String): Option[String] =
      if (v.length <= declaredLen) Some(s"CAST('$v' AS ${field.dataType.sql})") else None
    def prefixed(prefix: String, mod: Int): Option[String] =
      if (prefix.length + 1 <= declaredLen) {
        Some(s"CAST(concat('$prefix', CAST(id % $mod AS STRING)) AS ${field.dataType.sql})")
      } else {
        None
      }
    val named: Option[String] = name match {
      case "i_category" => prefixed("cat", 3)
      case "i_class" => prefixed("cls", 4)
      case "i_brand" => prefixed("br", 5)
      // q24a filters `i_color = 'pale'` and q24b `i_color = 'chiffon'`. The default
      // one-character value matches neither, so the `item` scan returns nothing, DPP
      // prunes every fact partition behind it, and both queries end up reading zero
      // records -- which the work suite reports as a vacuous test rather than a
      // measurement. Carry both literals so each query has rows to process.
      case "i_color" if declaredLen >= 7 =>
        Some(s"CAST(CASE $idx % 2 WHEN 0 THEN 'pale' ELSE 'chiffon' END " +
          s"AS ${field.dataType.sql})")
      case "i_manufact_id" | "i_brand_id" | "i_class_id" | "i_category_id" =>
        Some(num(s"id % $keySpace"))
      case "ca_state" => literal("IL")
      case "web_company_name" => literal("pri")
      case "s_state" => literal("TN")
      case "w_warehouse_name" => prefixed("wh", 3)
      case _ => None
    }
    if (named.isDefined) return named.get
    field.dataType match {
      case _: IntegerType | _: LongType | _: ShortType | _: ByteType => num(s"1 + (id % 9)")
      case _: DecimalType => num("1 + (id % 9)")
      case _: DoubleType | _: FloatType => num("1 + (id % 9)")
      case _: DateType => s"date_add(DATE '1999-01-01', CAST(id % $dateRows AS INT))"
      case _: TimestampType => "TIMESTAMP '1999-01-01 00:00:00'"
      // One character fits every CHAR(n)/VARCHAR(n) in the schema, including the many
      // CHAR(1) flag columns. A longer value fails the write with EXCEED_LIMIT_LENGTH,
      // and `spark.table(...).schema` reports StringType so the length is not visible
      // here.
      case _ => "CAST(id % 9 AS STRING)"
    }
  }

  private def stringOfLength(len: Int, sqlType: String): String =
    if (len <= 1) s"CAST(CAST(id % 9 AS STRING) AS $sqlType)"
    else s"CAST(concat('v', CAST(id % 9 AS STRING)) AS $sqlType)"

  /** Populates every TPC-DS table. Idempotent: uses INSERT OVERWRITE. */
  protected def populateSmallData(): Unit = {
    tableNames.foreach { table =>
      // `spark.table(...).schema` erases CHAR(n)/VARCHAR(n) to StringType, which
      // would hide the length limits and make every write fail with
      // EXCEED_LIMIT_LENGTH. `CatalogTable.schema` keeps the declared types.
      val schema = org.apache.spark.sql.types.StructType(
        spark.sessionState.catalog
          .getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(table))
          .schema.fields)
      // Partition columns must come last in an INSERT OVERWRITE select list, and
      // `spark.table(...).schema` already orders them that way for a partitioned
      // table created by TPCDSBase.
      val select = schema.fields.map { f => s"${valueExpr(table, f)} AS `${f.name}`" }
      spark.sql(
        s"INSERT OVERWRITE TABLE `$table` SELECT ${select.mkString(", ")} " +
        s"FROM range(${rowsFor(table)})")
    }
  }
}
