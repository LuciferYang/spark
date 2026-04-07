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

import scala.collection.mutable

import org.apache.spark.sql.{sources, SparkSession}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Expression, FileSourceGeneratedMetadataStructField, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, DataSourceUtils, FileFormat, FileSourceStrategy, PartitioningAwareFileIndex, PartitioningUtils}
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.collection.BitSet

abstract class FileScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    val bucketSpec: Option[BucketSpec] = None)
  extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownCatalystFilters {
  private val partitionSchema = fileIndex.partitionSchema
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
  protected val supportsNestedSchemaPruning = false
  protected var requiredSchema = StructType(dataSchema.fields ++ partitionSchema.fields)
  protected var partitionFilters = Seq.empty[Expression]
  protected var dataFilters = Seq.empty[Expression]
  protected var pushedDataFilters = Array.empty[Filter]
  // Populated by `pruneColumns` when the query references `_metadata.*`. Concrete
  // builders pass this to their `Scan` so the reader factory can append metadata values.
  protected var requestedMetadataFields: StructType = StructType(Seq.empty)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // [SPARK-30107] While `requiredSchema` might have pruned nested columns,
    // the actual data schema of this scan is determined in `readDataSchema`.
    // File formats that don't support nested schema pruning,
    // use `requiredSchema` as a reference and prune only top-level columns.
    //
    // [SPARK-56335] Extract the `_metadata` struct (if present) so the format-specific
    // scan can wrap its reader factory with metadata appending. The `_metadata` field is
    // removed from `this.requiredSchema` so it does not leak into `readDataSchema`.
    //
    // [SPARK-56371] When the metadata struct contains generated sub-fields (e.g.
    // Parquet's `row_index` backed by `_tmp_metadata_row_index`), append the
    // corresponding internal columns to `this.requiredSchema` so the format reader
    // populates them. The metadata wrapper later projects them out of the visible
    // output and weaves their values into the `_metadata` struct. Only formats that
    // enable nested schema pruning currently surface generated metadata fields,
    // because `readDataSchema()` reads from `requiredSchema` only on the nested path;
    // the non-nested path filters from `dataSchema` and would silently drop the
    // appended internal columns. Today only Parquet declares generated metadata
    // fields, so this limitation is not reachable in practice.
    val (metaFields, dataFields) = requiredSchema.fields.partition(isMetadataField)
    val metaStruct = metaFields.headOption
      .map(_.dataType.asInstanceOf[StructType])
      .getOrElse(StructType(Seq.empty))
    this.requestedMetadataFields = metaStruct
    // Internal columns must be nullable so the Parquet reader treats them as
    // synthetic columns (added to `missingColumns`) instead of failing the
    // required-column check in `VectorizedParquetRecordReader.checkColumn`. The
    // wrapper restores the user-facing nullability inside the `_metadata` struct.
    val internalCols = metaStruct.fields.collect {
      case FileSourceGeneratedMetadataStructField(field, internalName) =>
        StructField(internalName, field.dataType, nullable = true)
    }
    this.requiredSchema = StructType(dataFields ++ internalCols)
  }

  private def isMetadataField(field: StructField): Boolean =
    field.name == FileFormat.METADATA_NAME

  protected def readDataSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val schema = if (supportsNestedSchemaPruning) requiredSchema else dataSchema
    val fields = schema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName) && !partitionNameSet.contains(colName)
    }
    StructType(fields)
  }

  def readPartitionSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = partitionSchema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName)
    }
    StructType(fields)
  }

  override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
    val (deterministicFilters, nonDeterminsticFilters) = filters.partition(_.deterministic)
    val (partitionFilters, dataFilters) =
      DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, deterministicFilters)
    this.partitionFilters = partitionFilters.filter { f =>
      // Python UDFs might exist because this rule is applied before ``ExtractPythonUDFs``.
      !SubqueryExpression.hasSubquery(f) && !f.exists(_.isInstanceOf[PythonUDF])
    }
    this.dataFilters = dataFilters
    val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
    for (filterExpr <- dataFilters) {
      val translated = DataSourceStrategy.translateFilter(filterExpr, true)
      if (translated.nonEmpty) {
        translatedFilters += translated.get
      }
    }
    pushedDataFilters = pushDataFilters(translatedFilters.toArray)
    dataFilters ++ nonDeterminsticFilters
  }

  override def pushedFilters: Array[Predicate] = pushedDataFilters.map(_.toV2)

  /*
   * Push down data filters to the file source, so the data filters can be evaluated there to
   * reduce the size of the data to be read. By default, data filters are not pushed down.
   * File source needs to implement this method to push down data filters.
   */
  protected def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = Array.empty[Filter]

  private def createRequiredNameSet(): Set[String] =
    requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet

  val partitionNameSet: Set[String] =
    partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet

  /**
   * Computes the optional bucket set for bucket pruning based on pushed data filters.
   * Returns None if bucket pruning is not applicable or no buckets can be pruned.
   */
  protected def computeBucketSet(): Option[BitSet] = {
    bucketSpec match {
      case Some(spec) if FileSourceStrategy.shouldPruneBuckets(Some(spec)) =>
        FileSourceStrategy.genBucketSet(dataFilters, spec)
      case _ => None
    }
  }
}
