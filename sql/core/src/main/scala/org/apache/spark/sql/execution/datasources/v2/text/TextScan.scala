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
package org.apache.spark.sql.execution.datasources.v2.text

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.text.TextOptions
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

case class TextScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    override val bucketSpec: Option[BucketSpec] = None,
    override val disableBucketedScan: Boolean = false,
    override val optionalBucketSet: Option[BitSet] = None,
    override val optionalNumCoalescedBuckets: Option[Int] = None)
  extends TextBasedFileScan(sparkSession, options) {

  private val optionsAsScala = options.asScala.toMap
  private lazy val textOptions: TextOptions = new TextOptions(optionsAsScala)

  private def verifyReadSchema(schema: StructType): Unit = {
    if (schema.size > 1) {
      throw QueryCompilationErrors.textDataSourceWithMultiColumnsError(schema)
    }
  }

  override def isSplitable(path: Path): Boolean = {
    super.isSplitable(path) && !textOptions.wholeText
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    if (!super.isSplitable(path)) {
      super.getFileUnSplittableReason(path)
    } else {
      "the text datasource is set wholetext mode"
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    verifyReadSchema(readDataSchema)
    val hadoopConf = {
      val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      // Hadoop Configurations are case sensitive.
      sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    }
    val broadcastedConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    TextPartitionReaderFactory(conf, broadcastedConf, readDataSchema,
      readPartitionSchema, textOptions)
  }

  override def equals(obj: Any): Boolean = obj match {
    case t: TextScan => super.equals(t) && options == t.options

    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def withFileIndex(newFI: PartitioningAwareFileIndex): TextScan =
    copy(fileIndex = newFI)

  override def withDisableBucketedScan(disable: Boolean): TextScan =
    copy(disableBucketedScan = disable)

  override def withNumCoalescedBuckets(n: Option[Int]): TextScan =
    copy(optionalNumCoalescedBuckets = n)
}
