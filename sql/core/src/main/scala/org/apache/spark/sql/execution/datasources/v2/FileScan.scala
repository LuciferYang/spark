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

import java.util.{Locale, OptionalLong}

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PATH, REASON}
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, ExpressionSet}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{SessionStateHelper, SQLConf}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

trait FileScan extends Scan
  with Batch
  with SupportsReportStatistics
  with SupportsMetadata
  with SQLConfHelper
  with Logging {
  /**
   * Returns whether a file with `path` could be split or not.
   */
  def isSplitable(path: Path): Boolean = {
    false
  }

  def sparkSession: SparkSession

  def fileIndex: PartitioningAwareFileIndex

  def dataSchema: StructType

  /**
   * Returns the required data schema
   */
  def readDataSchema: StructType

  /**
   * Returns the required partition schema
   */
  def readPartitionSchema: StructType

  def options: CaseInsensitiveStringMap

  /**
   * Returns the filters that can be use for partition pruning
   */
  def partitionFilters: Seq[Expression]

  /**
   * Returns the data filters that can be use for file listing
   */
  def dataFilters: Seq[Expression]

  /** Optional bucket specification from the catalog table. */
  def bucketSpec: Option[BucketSpec] = None

  /** When true, disables bucketed scan. Set by DisableUnnecessaryBucketedScan. */
  def disableBucketedScan: Boolean = false

  /** Optional set of bucket IDs to scan (bucket pruning). None = scan all. */
  def optionalBucketSet: Option[BitSet] = None

  /** Optional coalesced bucket count. Set by CoalesceBucketsInJoin. */
  def optionalNumCoalescedBuckets: Option[Int] = None

  /**
   * Whether this scan actually uses bucketed read.
   * Mirrors V1 FileSourceScanExec.bucketedScan.
   */
  lazy val bucketedScan: Boolean = {
    conf.bucketingEnabled && bucketSpec.isDefined && !disableBucketedScan && {
      val spec = bucketSpec.get
      val resolver = sparkSession.sessionState.conf.resolver
      val bucketColumns = spec.bucketColumnNames.flatMap(n =>
        readSchema().fields.find(f => resolver(f.name, n)))
      bucketColumns.size == spec.bucketColumnNames.size
    }
  }

  /** Returns a copy of this scan with bucketed scan disabled. Default is a no-op. */
  def withDisableBucketedScan(disable: Boolean): FileScan = this

  /** Returns a copy of this scan with the given coalesced bucket count. Default is a no-op. */
  def withNumCoalescedBuckets(numCoalescedBuckets: Option[Int]): FileScan = this

  /**
   * If a file with `path` is unsplittable, return the unsplittable reason,
   * otherwise return `None`.
   */
  def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    "undefined"
  }

  protected def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

  private lazy val (normalizedPartitionFilters, normalizedDataFilters) = {
    val partitionFilterAttributes = AttributeSet(partitionFilters).map(a => a.name -> a).toMap
    val normalizedPartitionFilters = ExpressionSet(partitionFilters.map(
      QueryPlan.normalizeExpressions(_, toAttributes(fileIndex.partitionSchema)
        .map(a => partitionFilterAttributes.getOrElse(a.name, a)))))
    val dataFiltersAttributes = AttributeSet(dataFilters).map(a => a.name -> a).toMap
    val normalizedDataFilters = ExpressionSet(dataFilters.map(
      QueryPlan.normalizeExpressions(_, toAttributes(dataSchema)
        .map(a => dataFiltersAttributes.getOrElse(a.name, a)))))
    (normalizedPartitionFilters, normalizedDataFilters)
  }

  override def equals(obj: Any): Boolean = obj match {
    case f: FileScan =>
      fileIndex == f.fileIndex && readSchema == f.readSchema &&
        normalizedPartitionFilters == f.normalizedPartitionFilters &&
        normalizedDataFilters == f.normalizedDataFilters &&
        bucketSpec == f.bucketSpec &&
        disableBucketedScan == f.disableBucketedScan &&
        optionalBucketSet == f.optionalBucketSet &&
        optionalNumCoalescedBuckets == f.optionalNumCoalescedBuckets

    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def conf: SQLConf = SessionStateHelper.getSqlConf(sparkSession)

  val maxMetadataValueLength = conf.maxMetadataStringLength

  override def description(): String = {
    val metadataStr = getMetaData().toSeq.sorted.map {
      case (key, value) =>
        val redactedValue =
          Utils.redact(conf.stringRedactionPattern, value)
        key + ": " + Utils.abbreviate(redactedValue, maxMetadataValueLength)
    }.mkString(", ")
    s"${this.getClass.getSimpleName} $metadataStr"
  }

  override def getMetaData(): Map[String, String] = {
    val locationDesc =
      fileIndex.getClass.getSimpleName +
        Utils.buildLocationMetadata(fileIndex.rootPaths, maxMetadataValueLength)
    val base = Map(
      "Format" -> s"${this.getClass.getSimpleName.replace("Scan", "").toLowerCase(Locale.ROOT)}",
      "ReadSchema" -> readDataSchema.catalogString,
      "PartitionFilters" -> seqToString(partitionFilters),
      "DataFilters" -> seqToString(dataFilters),
      "Location" -> locationDesc)
    if (bucketedScan) {
      base ++ Map(
        "BucketSpec" -> bucketSpec.get.toString,
        "BucketedScan" -> "true")
    } else {
      base
    }
  }

  protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    // For bucketed scans, use Long.MaxValue to avoid splitting bucket files across partitions.
    val maxSplitBytes = if (bucketedScan) Long.MaxValue
      else FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
    val partitionAttributes = toAttributes(fileIndex.partitionSchema)
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw QueryCompilationErrors.cannotFindPartitionColumnInPartitionSchemaError(
          readField, fileIndex.partitionSchema)
      )
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val splitFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }
      partition.files.flatMap { file =>
        val filePath = file.getPath
        PartitionedFileUtil.splitFiles(
          file = file,
          filePath = filePath,
          isSplitable = isSplitable(filePath),
          maxSplitBytes = maxSplitBytes,
          partitionValues = partitionValues
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    if (bucketedScan) {
      createBucketedPartitions(splitFiles)
    } else {
      if (splitFiles.length == 1) {
        val path = splitFiles(0).toPath
        if (!isSplitable(path) && splitFiles(0).length >
          SessionStateHelper.getSparkConf(sparkSession).get(IO_WARNING_LARGEFILETHRESHOLD)) {
          logWarning(
            log"Loading one large unsplittable file ${MDC(PATH, path.toString)} with only " +
            log"one partition, the reason is: ${MDC(REASON, getFileUnSplittableReason(path))}")
        }
      }

      FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
    }
  }

  /**
   * Groups split files by bucket ID and applies bucket pruning and coalescing.
   * Mirrors V1 FileSourceScanExec.createBucketedReadRDD.
   */
  private def createBucketedPartitions(
      splitFiles: Seq[PartitionedFile]): Seq[FilePartition] = {
    val spec = bucketSpec.get
    val filesGroupedToBuckets = splitFiles.groupBy { f =>
      BucketingUtils.getBucketId(new Path(f.toPath.toString).getName)
        .getOrElse(throw new IllegalStateException(s"Invalid bucket file: ${f.toPath}"))
    }
    val prunedFilesGroupedToBuckets = optionalBucketSet match {
      case Some(bucketSet) =>
        filesGroupedToBuckets.filter { case (id, _) => bucketSet.get(id) }
      case None => filesGroupedToBuckets
    }
    optionalNumCoalescedBuckets.map { numCoalescedBuckets =>
      val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
      Seq.tabulate(numCoalescedBuckets) { bucketId =>
        val files = coalescedBuckets.get(bucketId)
          .map(_.values.flatten.toArray)
          .getOrElse(Array.empty)
        FilePartition(bucketId, files)
      }
    }.getOrElse {
      Seq.tabulate(spec.numBuckets) { bucketId =>
        FilePartition(bucketId,
          prunedFilesGroupedToBuckets.getOrElse(bucketId, Seq.empty).toArray)
      }
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = conf.fileCompressionFactor
        val size = (compressionFactor * fileIndex.sizeInBytes /
          (dataSchema.defaultSize + fileIndex.partitionSchema.defaultSize) *
          (readDataSchema.defaultSize + readPartitionSchema.defaultSize)).toLong

        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = {
        // Try to read stored row count from table
        // properties (set by ANALYZE TABLE).
        storedNumRows.map(OptionalLong.of)
          .getOrElse(OptionalLong.empty())
      }
    }
  }

  /**
   * Stored row count from ANALYZE TABLE, if available.
   * Injected via FileTable.mergedOptions.
   */
  protected def storedNumRows: Option[Long] =
    Option(options.get(FileTable.NUM_ROWS_KEY)).map(_.toLong)

  override def toBatch: Batch = this

  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

  // Returns whether the two given arrays of [[Filter]]s are equivalent.
  protected def equivalentFilters(a: Array[Filter], b: Array[Filter]): Boolean = {
    a.sortBy(_.hashCode()).sameElements(b.sortBy(_.hashCode()))
  }

  private val isCaseSensitive = conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }
}
