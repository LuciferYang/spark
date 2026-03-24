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

/**
 * Physical plan for ANALYZE TABLE on V2 file tables.
 * Computes table statistics and persists them as table
 * properties via [[TableCatalog.alterTable()]].
 *
 * Statistics property keys:
 * - `spark.sql.statistics.totalSize`
 * - `spark.sql.statistics.numRows`
 */
case class AnalyzeTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    table: FileTable,
    partitionSpec: Map[String, Option[String]],
    noScan: Boolean) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    table.fileIndex.refresh()
    val totalSize = table.fileIndex.sizeInBytes

    val changes =
      scala.collection.mutable.ArrayBuffer(
        TableChange.setProperty(
          "spark.sql.statistics.totalSize",
          totalSize.toString))

    if (!noScan) {
      val relation = DataSourceV2Relation.create(
        table, Some(catalog), Some(ident))
      val df = session.internalCreateDataFrame(
        session.sessionState.executePlan(
          relation).toRdd,
        relation.schema)
      val rowCount = df.count()
      changes += TableChange.setProperty(
        "spark.sql.statistics.numRows",
        rowCount.toString)
    }

    catalog.alterTable(ident, changes.toSeq: _*)
    Seq.empty
  }
}
