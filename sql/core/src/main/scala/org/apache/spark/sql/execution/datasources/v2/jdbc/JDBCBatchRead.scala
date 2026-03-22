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

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

/**
 * V2 InputPartition for JDBC. Carries the full SQL
 * query for this partition (including WHERE clause
 * for the partition's data range).
 */
case class JDBCInputPartition(
    sqlQuery: String,
    idx: Int) extends InputPartition

/**
 * Factory that creates JDBCPartitionReader on
 * executors.
 */
case class JDBCPartitionReaderFactory(
    schema: StructType,
    options: JDBCOptions)
    extends PartitionReaderFactory {

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val p =
      partition.asInstanceOf[JDBCInputPartition]
    new JDBCPartitionReader(
      schema, options, p.sqlQuery)
  }
}

/**
 * V2 PartitionReader that executes a JDBC query and
 * iterates over the ResultSet, converting each row
 * to InternalRow via JdbcUtils.
 */
class JDBCPartitionReader(
    schema: StructType,
    options: JDBCOptions,
    sqlQuery: String)
    extends PartitionReader[InternalRow]
    with Logging {

  private val dialect =
    JdbcDialects.get(options.url)
  private val conn: Connection =
    dialect.createConnectionFactory(options)(-1)
  private var stmt: PreparedStatement = _
  private var rs: ResultSet = _
  private var rowsIter: Iterator[InternalRow] = _
  private var currentRow: InternalRow = _

  {
    stmt = conn.prepareStatement(
      sqlQuery,
      ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    stmt.setQueryTimeout(options.queryTimeout)
    rs = stmt.executeQuery()
    val metrics = Option(
      org.apache.spark.TaskContext.get())
      .map(_.taskMetrics().inputMetrics)
      .getOrElse(
        new org.apache.spark.executor
          .InputMetrics())
    rowsIter =
      JdbcUtils.resultSetToSparkInternalRows(
        rs, dialect, schema, metrics)
  }

  override def next(): Boolean = {
    if (rowsIter.hasNext) {
      currentRow = rowsIter.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    try { if (rs != null) rs.close() }
    catch { case _: Exception => }
    try { if (stmt != null) stmt.close() }
    catch { case _: Exception => }
    try { conn.close() }
    catch { case _: Exception => }
  }
}
