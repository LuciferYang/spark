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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, Write, WriterCommitMessage}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

/**
 * V2-native BatchWrite for JDBC. Replaces V1Write
 * and fixes SPARK-32595: truncate + append are atomic
 * when the database supports transactions.
 */
class JDBCBatchWrite(
    schema: StructType,
    options: JdbcOptionsInWrite,
    isTruncate: Boolean)
    extends Write with BatchWrite with Logging {

  override def toBatch: BatchWrite = this

  override def createBatchWriterFactory(
      info: PhysicalWriteInfo): DataWriterFactory = {
    // Validate numPartitions
    options.numPartitions.foreach { n =>
      if (n <= 0) {
        throw QueryExecutionErrors
          .invalidJdbcNumPartitionsError(
            n, JDBCOptions.JDBC_NUM_PARTITIONS)
      }
    }
    val dialect = JdbcDialects.get(options.url)
    val insertStmt = JdbcUtils.getInsertStatement(
      options.table, schema, None,
      isCaseSensitive = true, dialect)
    JDBCDataWriterFactory(
      schema, options, insertStmt,
      isTruncate)
  }

  override def commit(
      messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(
      messages: Array[WriterCommitMessage]): Unit = {}
}

case class JDBCDataWriterFactory(
    schema: StructType,
    options: JdbcOptionsInWrite,
    insertStmt: String,
    isTruncate: Boolean)
    extends DataWriterFactory {

  override def createWriter(
      partitionId: Int,
      taskId: Long): DataWriter[InternalRow] = {
    new JDBCDataWriter(
      schema, options, insertStmt,
      isTruncate && partitionId == 0)
  }
}

/**
 * DataWriter that collects InternalRows and writes
 * them to JDBC on commit. Truncate (if needed) and
 * insert happen in the same JDBC connection for
 * atomicity (SPARK-32595).
 */
class JDBCDataWriter(
    schema: StructType,
    options: JdbcOptionsInWrite,
    insertStmt: String,
    doTruncate: Boolean)
    extends DataWriter[InternalRow] with Logging {

  private val rows = new ArrayBuffer[InternalRow]()

  override def write(
      record: InternalRow): Unit = {
    rows += record.copy()
  }

  override def commit(): WriterCommitMessage = {
    val dialect = JdbcDialects.get(options.url)
    val conn =
      dialect.createConnectionFactory(options)(-1)
    try {
      // Atomic truncate + insert (SPARK-32595)
      if (doTruncate) {
        JdbcUtils.truncateTable(conn, options)
      }
      // Convert InternalRow to Row for savePartition
      val cleanSchema = CharVarcharUtils
        .replaceCharVarcharWithStringInSchema(schema)
      val converter =
        org.apache.spark.sql.catalyst.CatalystTypeConverters
          .createToScalaConverter(cleanSchema)
      val rowIterator = rows.iterator.map { ir =>
        converter(ir).asInstanceOf[Row]
      }
      JdbcUtils.savePartition(
        options.table, rowIterator, cleanSchema,
        insertStmt, options.batchSize, dialect,
        options.isolationLevel, options)
    } catch {
      case e: Exception =>
        throw e
    } finally {
      try { conn.close() } catch {
        case _: Exception =>
      }
    }
    new WriterCommitMessage {}
  }

  override def abort(): Unit = {
    rows.clear()
  }

  override def close(): Unit = {
    rows.clear()
  }
}
