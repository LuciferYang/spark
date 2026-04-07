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
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateNamedStruct, Expression, FileSourceGeneratedMetadataStructField, Literal, UnsafeProjection}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Wraps a delegate [[FilePartitionReaderFactory]] and appends a single `_metadata` struct
 * column to each row, mirroring V1 `_metadata` semantics for V2 file scans.
 *
 * Supports both row-based and columnar reads. Constant sub-fields (file_path, file_name, ...)
 * are populated from the [[PartitionedFile]] via [[metadataExtractors]]. Generated sub-fields
 * (e.g., Parquet `row_index`) are read from internal columns that the format reader populates;
 * [[FileScanBuilder.pruneColumns]] adds those internal columns to the read schema, and this
 * wrapper projects them back into the visible `_metadata` struct.
 *
 * @param delegate the format-specific factory to wrap
 * @param fileSourceOptions options forwarded to the per-partition [[FilePartitionReader]]
 * @param requestedMetadataFields the pruned `_metadata` struct as the user requested it
 *                                (constant + generated sub-fields, in declared order)
 * @param readDataSchema the data schema actually passed to the format reader; includes the
 *                       user's data columns followed by any internal columns appended by
 *                       [[FileScanBuilder.pruneColumns]] for generated metadata sub-fields
 * @param readPartitionSchema the partition schema (used to compute the user-visible row layout)
 * @param metadataExtractors functions producing constant metadata values from a
 *                           [[PartitionedFile]]; typically [[FileFormat.BASE_METADATA_EXTRACTORS]]
 */
private[v2] class MetadataAppendingFilePartitionReaderFactory(
    delegate: FilePartitionReaderFactory,
    fileSourceOptions: FileSourceOptions,
    requestedMetadataFields: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    metadataExtractors: Map[String, PartitionedFile => Any])
  extends FilePartitionReaderFactory {

  override protected def options: FileSourceOptions = fileSourceOptions

  // For each metadata sub-field, where its value comes from:
  //   - Left(fieldName)    => constant; resolve via `metadataExtractors(fieldName)`
  //   - Right(internalName) => generated; read from `readDataSchema` at the position of
  //                            `internalName` (the column the format reader populates)
  private val metadataFieldSources: Array[Either[String, String]] =
    requestedMetadataFields.fields.map {
      case FileSourceGeneratedMetadataStructField(_, internalName) => Right(internalName)
      case f => Left(f.name)
    }

  // Index of each generated metadata sub-field's source within `readDataSchema`. Used by
  // both row and columnar paths to find the per-row value the format reader produced.
  private val internalColumnIndexInReadDataSchema: Map[String, Int] = {
    val byName = readDataSchema.fields.zipWithIndex.map { case (f, i) => f.name -> i }.toMap
    metadataFieldSources.collect { case Right(internalName) =>
      internalName -> byName.getOrElse(internalName,
        throw new IllegalStateException(
          s"internal metadata column `$internalName` is missing from readDataSchema; " +
            "FileScanBuilder.pruneColumns should have added it"))
    }.toMap
  }

  // Number of user-visible data columns (everything in readDataSchema that is NOT an internal
  // column for a generated metadata field).
  private val numVisibleDataCols: Int = {
    val internalNames = internalColumnIndexInReadDataSchema.keySet
    readDataSchema.fields.count(f => !internalNames.contains(f.name))
  }

  private val numPartitionCols: Int = readPartitionSchema.length

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val baseReader = delegate.buildReader(file)
    val projection = buildRowProjection(file)
    new ProjectingMetadataRowReader(baseReader, projection)
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val baseReader = delegate.buildColumnarReader(file)
    // Constants are stable for the whole split, so build the per-field column vectors once
    // and reuse across batches. The vectors' `getXxx` methods ignore rowId, so the same
    // instances work for any batch size returned by the format reader.
    val constantChildren = requestedMetadataFields.fields.zip(metadataFieldSources).map {
      case (field, Left(_)) => Some(constantColumnVectorFor(field, file))
      case (_, Right(_)) => None
    }
    new MetadataAppendingColumnarReader(baseReader, this, constantChildren)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean =
    delegate.supportColumnarReads(partition)

  // ---------------- Row path ----------------

  /**
   * Build a [[UnsafeProjection]] from the base reader's row layout
   * (`readDataSchema ++ readPartitionSchema`) to the wrapper's output layout
   * (`<user data> ++ <partition> ++ [_metadata: struct]`). Constant metadata values are baked
   * in as [[Literal]]s for this split; generated values come from [[BoundReference]]s pointing
   * at the internal columns the format reader populated.
   */
  private def buildRowProjection(file: PartitionedFile): UnsafeProjection = {
    val internalNames = internalColumnIndexInReadDataSchema.keySet

    // User data columns: every position in readDataSchema that isn't an internal column.
    val visibleDataRefs = readDataSchema.fields.zipWithIndex.collect {
      case (f, idx) if !internalNames.contains(f.name) =>
        BoundReference(idx, f.dataType, f.nullable).asInstanceOf[Expression]
    }

    // Partition columns sit after readDataSchema in the base row.
    val partitionRefs = readPartitionSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(readDataSchema.length + i, f.dataType, f.nullable).asInstanceOf[Expression]
    }

    // Build the metadata struct via CreateNamedStruct of (name, value) pairs. Constant values
    // are Literals carrying the field's declared dataType (so timestamps stay timestamps and
    // don't degrade to longs); generated values are BoundReferences into the base row.
    val metadataStructExpr = CreateNamedStruct(
      requestedMetadataFields.fields.zip(metadataFieldSources).flatMap {
        case (field, Left(_)) =>
          val rawValue = FileFormat.getFileConstantMetadataColumnValue(
            field.name, file, metadataExtractors).value
          Seq(Literal(field.name), Literal(rawValue, field.dataType))
        case (field, Right(internalName)) =>
          val idx = internalColumnIndexInReadDataSchema(internalName)
          Seq(
            Literal(field.name),
            BoundReference(idx, field.dataType, field.nullable))
      }.toIndexedSeq)

    val outputExprs: Seq[Expression] =
      visibleDataRefs.toIndexedSeq ++ partitionRefs.toIndexedSeq :+ metadataStructExpr
    UnsafeProjection.create(outputExprs)
  }

  // ---------------- Columnar path ----------------

  /**
   * Build the wrapper's output [[ColumnarBatch]] for one input batch. User-visible data and
   * partition columns are passed through by reference (zero-copy). The `_metadata` column is
   * a [[CompositeStructColumnVector]] whose children are pre-built [[ConstantColumnVector]]s
   * (for constant fields) and direct references to the format reader's internal column
   * vectors (for generated fields).
   *
   * Assumes the format reader keeps stable [[ColumnVector]] references across batches within
   * a split (Parquet's vectorized reader satisfies this contract by reusing one
   * [[ColumnarBatch]] instance per split).
   */
  private[v2] def buildOutputBatch(
      base: ColumnarBatch,
      constantChildren: Array[Option[ConstantColumnVector]]): ColumnarBatch = {
    val internalNames = internalColumnIndexInReadDataSchema.keySet

    // Visible data columns: positions in readDataSchema that aren't internal.
    val visibleDataCols = readDataSchema.fields.zipWithIndex.collect {
      case (f, idx) if !internalNames.contains(f.name) => base.column(idx)
    }

    // Partition columns sit after readDataSchema in the input batch.
    val partitionCols = (0 until numPartitionCols).map { i =>
      base.column(readDataSchema.length + i)
    }

    val metadataChildren = requestedMetadataFields.fields.indices.map { i =>
      metadataFieldSources(i) match {
        case Left(_) => constantChildren(i).get.asInstanceOf[ColumnVector]
        case Right(internalName) =>
          base.column(internalColumnIndexInReadDataSchema(internalName))
      }
    }.toArray
    val metadataColumn: ColumnVector = new CompositeStructColumnVector(
      requestedMetadataFields, metadataChildren)

    val output = (visibleDataCols ++ partitionCols :+ metadataColumn).toArray
    new ColumnarBatch(output, base.numRows())
  }

  /**
   * Build a per-split [[ConstantColumnVector]] for one constant metadata field. The vector's
   * value is read from the [[PartitionedFile]] via [[metadataExtractors]] and persists for the
   * whole split; `getXxx` ignores `rowId`, so the same instance is valid for any batch size.
   */
  private def constantColumnVectorFor(
      field: StructField,
      file: PartitionedFile): ConstantColumnVector = {
    val literal = FileFormat.getFileConstantMetadataColumnValue(
      field.name, file, metadataExtractors)
    // ConstantColumnVector ignores `rowId` in its getters, so a capacity-1 allocation is
    // sufficient regardless of how many rows the consuming batch holds.
    val vector = new ConstantColumnVector(1, field.dataType)
    if (literal.value == null) {
      vector.setNull()
    } else {
      val tmp = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(1)
      tmp.update(0, literal.value)
      org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.populate(vector, tmp, 0)
    }
    vector
  }
}

/**
 * Row-based wrapper that applies the per-split [[UnsafeProjection]] built by
 * [[MetadataAppendingFilePartitionReaderFactory.buildRowProjection]]. The projection produces
 * the wrapper's output row layout from the format reader's row.
 */
private[v2] class ProjectingMetadataRowReader(
    delegate: PartitionReader[InternalRow],
    projection: UnsafeProjection) extends PartitionReader[InternalRow] {

  override def next(): Boolean = delegate.next()

  override def get(): InternalRow = projection(delegate.get())

  override def close(): Unit = delegate.close()
}

/**
 * Columnar wrapper that delegates to the format reader and rewrites each [[ColumnarBatch]] to
 * the wrapper's output layout. The factory does the heavy lifting in
 * [[MetadataAppendingFilePartitionReaderFactory.buildOutputBatch]]; this class only forwards
 * and supplies the per-split constant column vectors.
 */
private[v2] class MetadataAppendingColumnarReader(
    delegate: PartitionReader[ColumnarBatch],
    factory: MetadataAppendingFilePartitionReaderFactory,
    constantChildren: Array[Option[ConstantColumnVector]])
  extends PartitionReader[ColumnarBatch] {

  override def next(): Boolean = delegate.next()

  override def get(): ColumnarBatch = factory.buildOutputBatch(delegate.get(), constantChildren)

  override def close(): Unit = delegate.close()
}
