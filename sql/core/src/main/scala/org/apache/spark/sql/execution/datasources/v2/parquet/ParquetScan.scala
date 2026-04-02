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
package org.apache.spark.sql.execution.datasources.v2.parquet

import java.util.Optional

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Or}
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, SupportsMerge}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration

case class ParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    pushedAggregate: Option[Aggregation] = None,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
    extends FileScan with SupportsMerge {
  override def isSplitable(path: Path): Boolean = {
    // If aggregate is pushed down, only the file footer will be read once,
    // so file should not be split across multiple tasks.
    pushedAggregate.isEmpty
  }

  override def readSchema(): StructType = {
    // If aggregate is pushed down, schema has already been pruned in `ParquetScanBuilder`
    // and no need to call super.readSchema()
    if (pushedAggregate.nonEmpty) readDataSchema else super.readSchema()
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val readDataSchemaAsJson = readDataSchema.json
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key,
      sparkSession.sessionState.conf.parquetInferTimestampNTZEnabled)
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sparkSession.sessionState.conf.legacyParquetNanosAsLong)

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val sqlConf = sparkSession.sessionState.conf
    ParquetPartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      pushedFilters,
      pushedAggregate,
      new ParquetOptions(options.asCaseSensitiveMap.asScala.toMap, sqlConf))
  }

  private def pushedDownAggEqual(p: ParquetScan) = {
    if (pushedAggregate.nonEmpty && p.pushedAggregate.nonEmpty) {
      AggregatePushDownUtils.equivalentAggregations(pushedAggregate.get, p.pushedAggregate.get)
    } else {
      pushedAggregate.isEmpty && p.pushedAggregate.isEmpty
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: ParquetScan =>
      val pushedDownAggEqual = if (pushedAggregate.nonEmpty && p.pushedAggregate.nonEmpty) {
        AggregatePushDownUtils.equivalentAggregations(pushedAggregate.get, p.pushedAggregate.get)
      } else {
        pushedAggregate.isEmpty && p.pushedAggregate.isEmpty
      }
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters) && pushedDownAggEqual
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions.toImmutableArraySeq),
      seqToString(pushedAggregate.get.groupByExpressions.toImmutableArraySeq))
  } else {
    ("[]", "[]")
  }

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters.toImmutableArraySeq)) ++
      Map("PushedAggregation" -> pushedAggregationsStr) ++
      Map("PushedGroupBy" -> pushedGroupByStr)
  }


  def sameScan(o: ParquetScan): Boolean =
    fileIndex == o.fileIndex && options == o.options && dataSchema == o.dataSchema &&
      normalizedPartitionFilters == o.normalizedPartitionFilters

  def sameScanStrictly(o: ParquetScan): Boolean = sameScan(o) &&
    equivalentFilters(pushedFilters, o.pushedFilters) &&
    pushedDownAggEqual(o) &&
    normalizedDataFilters == o.normalizedDataFilters

  def buildSameScan(o: ParquetScan, table: SupportsRead): Optional[SupportsMerge] = {
    // Reject merge if pushed aggregates differ — the merged scan
    // cannot correctly compute two different aggregations.
    if (!pushedDownAggEqual(o)) return Optional.empty()
    val builder = table.newScanBuilder(options) match {
      case pb: ParquetScanBuilder => pb
      case _ => return Optional.empty()
    }
    pushedAggregate.foreach(builder.pushAggregation)

    // Merge filter predicates: OR them together if both present,
    // or return empty (no filter) if either side has no filter.
    def mergePlanFilters(
        f1: Seq[Expression],
        f2: Seq[Expression]): Seq[Expression] =
      if (f1.nonEmpty && f2.nonEmpty) {
        Seq(Or(f1.reduce(And), f2.reduce(And)))
      } else {
        Seq.empty
      }

    val dataFilter = mergePlanFilters(dataFilters, o.dataFilters)
    val partFilter = mergePlanFilters(partitionFilters, o.partitionFilters)
    builder.pushFilters(dataFilter ++ partFilter)
    builder.pruneColumns(readSchema().merge(o.readSchema()))
    val scan = builder.build().asInstanceOf[ParquetScan]
    Optional.of(scan)
  }

  def buildSameScanStrictly(o: ParquetScan, table: SupportsRead): Optional[SupportsMerge] = {
    val builder = table.newScanBuilder(options) match {
      case pb: ParquetScanBuilder => pb
      case _ => return Optional.empty()
    }
    pushedAggregate.foreach(builder.pushAggregation)
    builder.pushFilters(dataFilters ++ partitionFilters)
    builder.pruneColumns(readSchema().merge(o.readSchema()))
    val scan = builder.build().asInstanceOf[ParquetScan]
    Optional.of(scan)
  }

  override def mergeWith(other: SupportsMerge, table: SupportsRead): Optional[SupportsMerge] =
    other match {
      case o: ParquetScan if sameScan(o) &&
        sparkSession.sessionState.conf.ignorePushedDataFilters =>
        buildSameScan(o, table)
      case o: ParquetScan if sameScanStrictly(o) =>
        buildSameScanStrictly(o, table)
      case _ => Optional.empty()
    }
}
