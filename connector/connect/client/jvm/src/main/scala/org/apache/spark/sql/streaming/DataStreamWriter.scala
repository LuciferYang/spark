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

package org.apache.spark.sql.streaming

import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.connect.proto
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.BinaryEncoder
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming._

/**
 * Interface used to write a streaming `Dataset` to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.writeStream` to access this.
 *
 * @since 3.5.0
 */
@Evolving
final class DataStreamWriter[T] private[sql] (ds: Dataset[T]) {
  import DataStreamWriter._

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink. <ul> <li>
   * `OutputMode.Append()`: only the new rows in the streaming DataFrame/Dataset will be written
   * to the sink.</li> <li> `OutputMode.Complete()`: all the rows in the streaming
   * DataFrame/Dataset will be written to the sink every time there are some updates.</li> <li>
   * `OutputMode.Update()`: only the rows that were updated in the streaming DataFrame/Dataset
   * will be written to the sink every time there are some updates. If the query doesn't contain
   * aggregations, it will be equivalent to `OutputMode.Append()` mode.</li> </ul>
   *
   * @since 3.5.0
   */
  def outputMode(outputMode: OutputMode): DataStreamWriter[T] = {
    this.outputMode = outputMode
    this
  }

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink. <ul> <li>
   * `append`: only the new rows in the streaming DataFrame/Dataset will be written to the
   * sink.</li> <li> `complete`: all the rows in the streaming DataFrame/Dataset will be written
   * to the sink every time there are some updates.</li> <li> `update`: only the rows that were
   * updated in the streaming DataFrame/Dataset will be written to the sink every time there are
   * some updates. If the query doesn't contain aggregations, it will be equivalent to `append`
   * mode.</li> </ul>
   *
   * @since 3.5.0
   */
  def outputMode(outputMode: String): DataStreamWriter[T] = {
    this.outputMode = InternalOutputModes(outputMode)
    this
  }

  /**
   * Set the trigger for the stream query. The default value is `ProcessingTime(0)` and it will
   * run the query as fast as possible.
   *
   * Scala Example:
   * {{{
   *   df.writeStream.trigger(ProcessingTime("10 seconds"))
   *
   *   import scala.concurrent.duration._
   *   df.writeStream.trigger(ProcessingTime(10.seconds))
   * }}}
   *
   * Java Example:
   * {{{
   *   df.writeStream().trigger(ProcessingTime.create("10 seconds"))
   *
   *   import java.util.concurrent.TimeUnit
   *   df.writeStream().trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
   * }}}
   *
   * @since 3.5.0
   */
  def trigger(trigger: Trigger): DataStreamWriter[T] = {
    this.trigger = trigger
    this
  }

  /**
   * Specifies the name of the [[StreamingQuery]] that can be started with `start()`. This name
   * must be unique among all the currently active queries in the associated SQLContext.
   *
   * @since 3.5.0
   */
  def queryName(queryName: String): DataStreamWriter[T] = {
    this.extraOptions += ("queryName" -> queryName)
    this.queryName = queryName
    this
  }

  /**
   * Specifies the underlying output data source.
   *
   * @since 3.5.0
   */
  def format(source: String): DataStreamWriter[T] = {
    this.source = source
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   *
   * <ul> <li> year=2016/month=01/</li> <li> year=2016/month=02/</li> </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout. It
   * provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number of
   * distinct values in each column should typically be less than tens of thousands.
   *
   * @since 3.5.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataStreamWriter[T] = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 3.5.0
   */
  def option(key: String, value: String): DataStreamWriter[T] = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 3.5.0
   */
  def option(key: String, value: Boolean): DataStreamWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 3.5.0
   */
  def option(key: String, value: Long): DataStreamWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 3.5.0
   */
  def option(key: String, value: Double): DataStreamWriter[T] = option(key, value.toString)

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * @since 3.5.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamWriter[T] = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * @since 3.5.0
   */
  def options(options: java.util.Map[String, String]): DataStreamWriter[T] = {
    this.options(options.asScala)
    this
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given path as new data arrives. The returned [[StreamingQuery]] object can be used to
   * interact with the stream.
   *
   * @since 3.5.0
   */
  def start(path: String): StreamingQuery = {
    startInternal(Some(path))
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given path as new data arrives. The returned [[StreamingQuery]] object can be used to
   * interact with the stream. Throws a `TimeoutException` if the following conditions are met:
   *   - Another run of the same streaming query, that is a streaming query sharing the same
   *     checkpoint location, is already active on the same Spark Driver
   *   - The SQL configuration `spark.sql.streaming.stopActiveRunOnRestart` is enabled
   *   - The active run cannot be stopped within the timeout controlled by the SQL configuration
   *     `spark.sql.streaming.stopTimeout`
   *
   * @since 3.5.0
   */
  @throws[TimeoutException]
  def start(): StreamingQuery = startInternal(None)

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given table as new data arrives. The returned [[StreamingQuery]] object can be used to
   * interact with the stream.
   *
   * For v1 table, partitioning columns provided by `partitionBy` will be respected no matter the
   * table exists or not. A new table will be created if the table not exists.
   *
   * For v2 table, `partitionBy` will be ignored if the table already exists. `partitionBy` will
   * be respected only if the v2 table does not exist. Besides, the v2 table created by this API
   * lacks some functionalities (e.g., customized properties, options, and serde info). If you
   * need them, please create the v2 table manually before the execution to avoid creating a table
   * with incomplete information.
   *
   * @since 3.5.0
   */
  @Evolving
  @throws[TimeoutException]
  def toTable(tableName: String): StreamingQuery = {
    this.tableName = tableName
    startInternal(path = None, tableName = Some(tableName))
  }

  private def startInternal(
      path: Option[String],
      tableName: Option[String] = None): StreamingQuery = {
    val builder = proto.WriteStreamOperationStart.newBuilder().setInput(ds.plan.getRoot)
    builder.setFormat(source)
    extraOptions.foreach { case (k, v) =>
      builder.putOptions(k, v)
    }
    partitioningColumns.foreach(cols => builder.addAllPartitioningColumnNames(cols.asJava))
    trigger match {
      case ProcessingTimeTrigger(intervalMs) =>
        builder.setProcessingTimeInterval(intervalMs.toString)
      case AvailableNowTrigger => builder.setAvailableNow(true)
      case OneTimeTrigger => builder.setOnce(true)
      case ContinuousTrigger(intervalMs) =>
        builder.setContinuousCheckpointInterval(intervalMs.toString)
    }
    builder.setOutputMode(outputMode.toString)
    builder.setQueryName(queryName)
    path.foreach(builder.setPath)
    tableName.foreach(builder.setTableName)
    val head = ds.sparkSession
      .execute(
        proto.Command.newBuilder().setWriteStreamOperationStart(builder).build(),
        BinaryEncoder)
      .toArray
      .head
    val result = proto.WriteStreamOperationStartResult.parseFrom(head)
    new StreamingQueryWrapper(ds.sparkSession, result)
  }

  /**
   * Sets the output of the streaming query to be processed using the provided writer object.
   * object. See [[org.apache.spark.sql.ForeachWriter]] for more details on the lifecycle and
   * semantics.
   * @since 3.5.0
   */
  def foreach(writer: ForeachWriter[T]): DataStreamWriter[T] = {
    this.source = SOURCE_NAME_FOREACH
    this.foreachWriter = if (writer != null) {
      writer
    } else {
      throw new IllegalArgumentException("foreach writer cannot be null")
    }
    this
  }

  /**
   * :: Experimental ::
   *
   * (Scala-specific) Sets the output of the streaming query to be processed using the provided
   * function. This is supported only in the micro-batch execution modes (that is, when the
   * trigger is not continuous). In every micro-batch, the provided function will be called in
   * every micro-batch with (i) the output rows as a Dataset and (ii) the batch identifier. The
   * batchId can be used to deduplicate and transactionally write the output (that is, the
   * provided Dataset) to external systems. The output Dataset is guaranteed to be exactly the
   * same for the same batchId (assuming all operations are deterministic in the query).
   *
   * @since 3.5.0
   */
  @Evolving
  def foreachBatch(function: (Dataset[T], Long) => Unit): DataStreamWriter[T] = {
    this.source = SOURCE_NAME_FOREACH_BATCH
    if (function == null) {
      throw new IllegalArgumentException("foreachBatch function cannot be null")
    }
    this.foreachBatchWriter = function
    this
  }

  /**
   * :: Experimental ::
   *
   * (Java-specific) Sets the output of the streaming query to be processed using the provided
   * function. This is supported only in the micro-batch execution modes (that is, when the
   * trigger is not continuous). In every micro-batch, the provided function will be called in
   * every micro-batch with (i) the output rows as a Dataset and (ii) the batch identifier. The
   * batchId can be used to deduplicate and transactionally write the output (that is, the
   * provided Dataset) to external systems. The output Dataset is guaranteed to be exactly the
   * same for the same batchId (assuming all operations are deterministic in the query).
   *
   * @since 3.5.0
   */
  @Evolving
  def foreachBatch(function: VoidFunction2[Dataset[T], java.lang.Long]): DataStreamWriter[T] = {
    foreachBatch((batchDs: Dataset[T], batchId: Long) => function.call(batchDs, batchId))
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = _

  private var queryName: String = _

  private var tableName: String = _

  private var outputMode: OutputMode = OutputMode.Append

  private var trigger: Trigger = Trigger.ProcessingTime(0L)

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

  private var foreachWriter: ForeachWriter[T] = _

  private var foreachBatchWriter: (Dataset[T], Long) => Unit = _

  private var partitioningColumns: Option[Seq[String]] = None
}

object DataStreamWriter {
  val SOURCE_NAME_MEMORY = "memory"
  val SOURCE_NAME_FOREACH = "foreach"
  val SOURCE_NAME_FOREACH_BATCH = "foreachBatch"
  val SOURCE_NAME_CONSOLE = "console"
  val SOURCE_NAME_TABLE = "table"
  val SOURCE_NAME_NOOP = "noop"

  // these writer sources are also used for one-time query, hence allow temp checkpoint location
  val SOURCES_ALLOW_ONE_TIME_QUERY = Seq(
    SOURCE_NAME_MEMORY,
    SOURCE_NAME_FOREACH,
    SOURCE_NAME_FOREACH_BATCH,
    SOURCE_NAME_CONSOLE,
    SOURCE_NAME_NOOP)
}
