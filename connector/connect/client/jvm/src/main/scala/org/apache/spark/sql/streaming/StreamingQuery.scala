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

import java.util.UUID
import java.util.concurrent.TimeoutException

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.SparkSession

/**
 * A handle to a query that is executing continuously in the background as new data arrives. All
 * these methods are thread-safe.
 * @since 3.5.0
 */
@Evolving
trait StreamingQuery {

  /**
   * Returns the user-specified name of the query, or null if not specified. This name can be
   * specified in the `org.apache.spark.sql.streaming.DataStreamWriter` as
   * `dataframe.writeStream.queryName("query").start()`. This name, if set, must be unique across
   * all active queries.
   *
   * @since 3.5.0
   */
  def name: String

  /**
   * Returns the unique id of this query that persists across restarts from checkpoint data. That
   * is, this id is generated when a query is started for the first time, and will be the same
   * every time it is restarted from checkpoint data. Also see [[runId]].
   *
   * @since 3.5.0
   */
  def id: UUID

  /**
   * Returns the unique id of this run of the query. That is, every start/restart of a query will
   * generate a unique runId. Therefore, every time a query is restarted from checkpoint, it will
   * have the same [[id]] but different [[runId]]s.
   */
  def runId: UUID

  /**
   * Returns the `SparkSession` associated with `this`.
   *
   * @since 3.5.0
   */
  def sparkSession: SparkSession

  /**
   * Returns `true` if this query is actively running.
   *
   * @since 3.5.0
   */
  def isActive: Boolean

  /**
   * Returns the current status of the query.
   *
   * @since 3.5.0
   */
  def status: StreamingQueryStatus

  /**
   * Returns an array of the most recent [[StreamingQueryProgress]] updates for this query. The
   * number of progress updates retained for each stream is configured by Spark session
   * configuration `spark.sql.streaming.numRecentProgressUpdates`.
   *
   * @since 3.5.0
   */
  def recentProgress: Array[StreamingQueryProgress]

  /**
   * Returns the most recent [[StreamingQueryProgress]] update of this streaming query.
   *
   * @since 3.5.0
   */
  def lastProgress: StreamingQueryProgress

  /**
   * Blocks until all available data in the source has been processed and committed to the sink.
   * This method is intended for testing. Note that in the case of continually arriving data, this
   * method may block forever. Additionally, this method is only guaranteed to block until data
   * that has been synchronously appended data to a
   * `org.apache.spark.sql.execution.streaming.Source` prior to invocation. (i.e. `getOffset` must
   * immediately reflect the addition).
   * @since 3.5.0
   */
  def processAllAvailable(): Unit

  /**
   * Stops the execution of this query if it is running. This waits until the termination of the
   * query execution threads or until a timeout is hit.
   *
   * By default stop will block indefinitely. You can configure a timeout by the configuration
   * `spark.sql.streaming.stopTimeout`. A timeout of 0 (or negative) milliseconds will block
   * indefinitely. If a `TimeoutException` is thrown, users can retry stopping the stream. If the
   * issue persists, it is advisable to kill the Spark application.
   *
   * @since 3.5.0
   */
  @throws[TimeoutException]
  def stop(): Unit

  /**
   * Prints the physical plan to the console for debugging purposes.
   * @since 3.5.0
   */
  def explain(): Unit

  /**
   * Prints the physical plan to the console for debugging purposes.
   *
   * @param extended
   *   whether to do extended explain or not
   * @since 3.5.0
   */
  def explain(extended: Boolean): Unit
}
