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

import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Wraps a delegate [[FilePartitionReaderFactory]] and appends a single `_metadata` struct
 * column to each row, mirroring V1 `_metadata` semantics for V2 file scans.
 *
 * Only row-based reads are supported: `supportColumnarReads` returns false so Spark falls
 * back to the row path whenever the query references `_metadata.*` (a `ConstantColumnVector`
 * cannot represent a struct column, and a real struct vector would require a larger change).
 *
 * @param delegate the format-specific factory to wrap
 * @param fileSourceOptions options forwarded to the per-partition [[FilePartitionReader]]
 * @param requestedMetadataFields the pruned metadata struct (only the referenced sub-fields)
 * @param metadataExtractors functions that produce each metadata value from a
 *                           [[PartitionedFile]]; typically [[FileFormat.BASE_METADATA_EXTRACTORS]]
 */
private[v2] class MetadataAppendingFilePartitionReaderFactory(
    delegate: FilePartitionReaderFactory,
    fileSourceOptions: FileSourceOptions,
    requestedMetadataFields: StructType,
    metadataExtractors: Map[String, PartitionedFile => Any])
  extends FilePartitionReaderFactory {

  override protected def options: FileSourceOptions = fileSourceOptions

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val baseReader = delegate.buildReader(file)
    new MetadataAppendingRowReader(baseReader, buildMetadataRow(file))
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    throw new UnsupportedOperationException(
      "Columnar reads are not supported when `_metadata` columns are requested")
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = false

  /**
   * Build a single-field row `[_metadata: struct]` whose one field holds the inner struct
   * of metadata values for `file`. [[JoinedRow]] appends this after the base data+partition
   * row so the combined row matches [[FileScan.readSchema]].
   */
  private def buildMetadataRow(file: PartitionedFile): InternalRow = {
    val fieldNames = requestedMetadataFields.fields.map(_.name).toSeq
    val innerStruct = FileFormat.updateMetadataInternalRow(
      new GenericInternalRow(fieldNames.length), fieldNames, file, metadataExtractors)
    val outer = new GenericInternalRow(1)
    outer.update(0, innerStruct)
    outer
  }
}

/**
 * Wraps a row-based [[PartitionReader]], appending a constant metadata row (produced from the
 * [[PartitionedFile]]) to each row returned by the delegate. Reuses a single [[JoinedRow]]
 * instance per split to avoid per-row allocations, as recommended by [[JoinedRow]]'s contract.
 */
private[v2] class MetadataAppendingRowReader(
    delegate: PartitionReader[InternalRow],
    metadataRow: InternalRow) extends PartitionReader[InternalRow] {

  // Pre-bind the right side since the metadata row is constant for the whole split;
  // only the left (data) row changes per `get()`.
  private val joined = new JoinedRow().withRight(metadataRow)

  override def next(): Boolean = delegate.next()

  override def get(): InternalRow = joined.withLeft(delegate.get())

  override def close(): Unit = delegate.close()
}

