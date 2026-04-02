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

import org.apache.hadoop.mapreduce.Job

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.datasources.{WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.execution.streaming.ManifestFileCommitProtocol
import org.apache.spark.sql.execution.streaming.sinks.FileStreamSinkLog
import org.apache.spark.util.ArrayImplicits._

/**
 * A [[StreamingDataWriterFactory]] that delegates to a batch [[DataWriterFactory]].
 * The epochId parameter is ignored because file naming already uses UUIDs for uniqueness.
 */
private[v2] class FileStreamingWriterFactory(
    delegate: DataWriterFactory) extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    delegate.createWriter(partitionId, taskId)
  }
}

/**
 * A [[StreamingWrite]] implementation for V2 file-based tables.
 *
 * This is the streaming equivalent of [[FileBatchWrite]]. It uses
 * [[ManifestFileCommitProtocol]] to track committed files in a
 * [[FileStreamSinkLog]], providing exactly-once semantics via
 * idempotent batch commits.
 */
class FileStreamingWrite(
    job: Job,
    description: WriteJobDescription,
    committer: ManifestFileCommitProtocol,
    fileLog: FileStreamSinkLog) extends StreamingWrite with Logging {

  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    committer.setupJob(job)
    new FileStreamingWriterFactory(FileWriterFactory(description, committer))
  }

  override def useCommitCoordinator(): Boolean = true

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    // Idempotency: skip if batch already committed
    if (epochId <= fileLog.getLatestBatchId().getOrElse(-1L)) {
      logInfo(log"Skipping already committed batch ${MDC(LogKeys.BATCH_ID, epochId)}")
      return
    }
    // Set the real batchId before commitJob
    committer.setupManifestOptions(fileLog, epochId)
    // Messages are WriteTaskResult (not raw TaskCommitMessage).
    // Must extract .commitMsg -- same pattern as FileBatchWrite.commit().
    val results = messages.map(_.asInstanceOf[WriteTaskResult])
    committer.commitJob(job, results.map(_.commitMsg).toImmutableArraySeq)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    committer.abortJob(job)
  }
}
