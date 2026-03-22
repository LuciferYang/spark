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
package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCDatabaseMetadata, JDBCPartition, JDBCRelation}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

case class JDBCScan(
    relation: JDBCRelation,
    prunedSchema: StructType,
    pushedPredicates: Array[Predicate],
    pushedAggregateColumn: Array[String] =
      Array(),
    groupByColumns: Option[Array[String]],
    tableSample: Option[TableSampleInfo],
    pushedLimit: Int,
    sortOrders: Array[String],
    pushedOffset: Int,
    aggregation: Option[Aggregation] = None,
    originalSortOrders: Array[SortOrder] =
      Array.empty,
    originalLimit: Int = 0,
    originalOffset: Int = 0)
    extends Scan with Batch {

  /** External engine query for explain output. */
  def getExternalEngineQuery: Option[String] = {
    val noPartition = JDBCPartition(
      whereClause = null, idx = 0)
    Some(buildSqlForPartition(noPartition))
  }

  /** Database metadata for the JDBC connection. */
  def getDatabaseMetadata
      : JDBCDatabaseMetadata = {
    val dialect =
      JdbcDialects.get(relation.jdbcOptions.url)
    val connFactory =
      dialect.createConnectionFactory(
        relation.jdbcOptions)
    JDBCDatabaseMetadata
      .fromJDBCConnectionFactory(connFactory)
  }

  override def readSchema(): StructType =
    prunedSchema

  override def toBatch: Batch = this

  private def buildSqlForPartition(
      part: JDBCPartition): String = {
    val dialect =
      JdbcDialects.get(relation.jdbcOptions.url)
    val columns =
      if (groupByColumns.isEmpty) {
        prunedSchema.map(_.name).toArray.map(
          dialect.quoteIdentifier)
      } else {
        pushedAggregateColumn
      }
    var builder = dialect
      .getJdbcSQLQueryBuilder(relation.jdbcOptions)
      .withPredicates(pushedPredicates, part)
      .withColumns(columns)
      .withSortOrders(sortOrders)
      .withLimit(pushedLimit)
      .withOffset(pushedOffset)
    groupByColumns.foreach { gbc =>
      builder = builder.withGroupByColumns(gbc)
    }
    tableSample.foreach { ts =>
      builder = builder.withTableSample(ts)
    }
    builder.build()
  }

  override def planInputPartitions()
      : Array[InputPartition] = {
    relation.parts.map { p =>
      val jp = p.asInstanceOf[JDBCPartition]
      val sql = buildSqlForPartition(jp)
      JDBCInputPartition(sql, jp.index)
    }
  }

  override def createReaderFactory()
      : PartitionReaderFactory = {
    JDBCPartitionReaderFactory(
      prunedSchema, relation.jdbcOptions)
  }

  // Generate description matching V1
  // RowDataSourceScanExec metadata format.
  override def description(): String = {
    val filtersStr = seqToString(
      pushedPredicates.map(_.describe())
        .toImmutableArraySeq)

    // Build metadata entries matching V1 format
    val entries =
      scala.collection.mutable.LinkedHashMap(
        "ReadSchema" ->
          prunedSchema.catalogString,
        "PushedFilters" -> filtersStr)

    aggregation.foreach { agg =>
      entries += "PushedAggregates" -> seqToString(
        agg.aggregateExpressions()
          .map(_.describe()).toImmutableArraySeq)
      entries +=
        "PushedGroupByExpressions" -> seqToString(
          agg.groupByExpressions()
            .map(_.describe()).toImmutableArraySeq)
    }

    if (originalLimit > 0 &&
        originalSortOrders.nonEmpty) {
      val sorts = seqToString(
        originalSortOrders.map(_.describe())
          .toImmutableArraySeq)
      entries += "PushedTopN" ->
        s"ORDER BY $sorts LIMIT $originalLimit"
    } else if (originalLimit > 0) {
      entries += "PushedLimit" ->
        s"LIMIT $originalLimit"
    }

    if (originalOffset > 0) {
      entries += "PushedOffset" ->
        s"OFFSET $originalOffset"
    }

    val metadataStr = entries.toSeq.sorted
      .map { case (k, v) => s"$k: $v" }
      .mkString(", ")
    super.description() + ", " + metadataStr
  }

  private def seqToString(seq: Seq[Any]): String =
    seq.mkString("[", ", ", "]")
}
