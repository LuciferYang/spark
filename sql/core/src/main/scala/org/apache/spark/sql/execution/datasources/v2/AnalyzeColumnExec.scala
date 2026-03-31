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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.execution.command.CommandUtils

/**
 * Physical plan for ANALYZE TABLE ... FOR COLUMNS on V2
 * file tables. Computes column-level statistics and
 * persists them as table properties via
 * [[TableCatalog.alterTable()]].
 *
 * Column stats property key format:
 * `spark.sql.statistics.colStats.<col>.<stat>`
 */
case class AnalyzeColumnExec(
    catalog: TableCatalog,
    ident: Identifier,
    table: FileTable,
    columnNames: Option[Seq[String]],
    allColumns: Boolean)
    extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val relation = DataSourceV2Relation.create(
      table, Some(catalog), Some(ident))

    val columnsToAnalyze = if (allColumns) {
      relation.output
    } else {
      columnNames.getOrElse(Seq.empty).map { name =>
        relation.output.find(
          _.name.equalsIgnoreCase(name)).getOrElse(
          throw new IllegalArgumentException(
            s"Column '$name' not found"))
      }
    }

    val (rowCount, colStats) =
      CommandUtils.computeColumnStats(
        session, relation, columnsToAnalyze)

    // Refresh fileIndex for accurate size
    table.fileIndex.refresh()
    val totalSize = table.fileIndex.sizeInBytes

    val changes =
      scala.collection.mutable.ArrayBuffer(
        TableChange.setProperty(
          "spark.sql.statistics.totalSize",
          totalSize.toString),
        TableChange.setProperty(
          "spark.sql.statistics.numRows",
          rowCount.toString))

    // Store column stats as table properties
    val prefix = "spark.sql.statistics.colStats."
    colStats.foreach { case (attr, stat) =>
      val catalogStat = stat.toCatalogColumnStat(
        attr.name, attr.dataType)
      catalogStat.toMap(attr.name).foreach {
        case (k, v) =>
          changes += TableChange.setProperty(
            prefix + k, v)
      }
    }

    catalog.alterTable(ident, changes.toSeq: _*)
    Seq.empty
  }
}
