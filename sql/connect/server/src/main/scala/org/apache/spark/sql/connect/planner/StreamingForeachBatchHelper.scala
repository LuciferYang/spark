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
package org.apache.spark.sql.connect.planner

import java.io.EOFException
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.api.python.{PythonException, PythonWorkerUtils, SimplePythonFunction, SpecialLengths, StreamingPythonRunner}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{DATAFRAME_ID, PYTHON_EXEC, QUERY_ID, RUN_ID_STRING, SESSION_ID, USER_ID}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticEncoders}
import org.apache.spark.sql.connect.IllegalStateErrors
import org.apache.spark.sql.connect.common.ForeachWriterPacket
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.Utils

/**
 * A helper class for handling ForeachBatch related functionality in Spark Connect servers
 */
object StreamingForeachBatchHelper extends Logging {

  type ForeachBatchFnType = (DataFrame, Long) => Unit

  // Visible for testing.
  /** An AutoClosable to clean up resources on query termination. Stops Python worker. */
  private[connect] case class RunnerCleaner(runner: StreamingPythonRunner) extends AutoCloseable {
    override def close(): Unit = {
      try runner.stop()
      catch {
        case NonFatal(ex) => // Exception is not propagated.
          logWarning("Error while stopping streaming Python worker", ex)
      }
    }
  }

  private case class FnArgsWithId(dfId: String, df: DataFrame, batchId: Long)

  /**
   * Holds the cloned SessionHolder for a foreachBatch streaming query's micro-batch execution.
   * The MicroBatch execution creates a cloned SparkSession (`sparkSessionForStream`), so batch
   * DataFrames are bound to a different session than the original one used to start the query.
   * This holder lazily creates a corresponding cloned SessionHolder on the first batch and
   * provides methods for caching/removing DataFrames in the correct session context.
   *
   * IMPORTANT: For the Scala foreachBatch path (HACK), the cloned session holder is not yet used
   * because the user function directly operates on the classic DataFrame. For the Python path,
   * the Python worker currently connects back using the original session_id, so we cache in the
   * original SessionHolder for now. The cloned session holder will be fully utilized once the
   * Connect callback protocol (Subtask 2) is implemented and the Python worker is updated.
   */
  private class ForeachBatchSessionManager(
      parentSessionHolder: SessionHolder) extends Logging {

    // The cloned SessionHolder for the micro-batch execution session.
    // Lazily initialized on the first batch to avoid creating it if the query never produces data.
    @volatile private var _clonedSessionHolder: SessionHolder = null

    /**
     * Get or create the cloned SessionHolder for the given batch DataFrame's SparkSession.
     * Currently returns the parent session holder since the Python worker uses the original
     * session_id. This will be updated to return a cloned session holder when the callback
     * protocol is fully implemented.
     */
    def getSessionHolder(batchDf: DataFrame): SessionHolder = {
      // TODO(SPARK-44462): Once the callback protocol and Python worker are updated to support
      // cloned session IDs, this should create and return a proper cloned SessionHolder from
      // batchDf.sparkSession. For now, return the parent to maintain backward compatibility.
      parentSessionHolder
    }

    /**
     * Clean up the cloned session holder (if any) when the streaming query terminates.
     */
    def close(): Unit = {
      if (_clonedSessionHolder != null) {
        logInfo(
          log"[session: ${MDC(SESSION_ID, parentSessionHolder.sessionId)}] " +
            log"Cleaning up cloned session holder for foreachBatch")
        _clonedSessionHolder = null
      }
    }
  }

  /**
   * Return a new ForeachBatch function that wraps `fn`. It sets up DataFrame cache so that the
   * user function can access it. The cache is cleared once ForeachBatch returns.
   *
   * Uses a ForeachBatchSessionManager to handle session cloning for micro-batch execution.
   * The batch DataFrame's SparkSession may differ from the original session (due to
   * StreamExecution.sparkSessionForStream cloning), so the session manager provides the
   * appropriate SessionHolder for caching.
   *
   * @param queryIdRef A reference that will be populated with the streaming query ID after the
   *                   query starts. Used to track at most one active cached DataFrame per query.
   *                   If provided and set, a sanity check ensures no stale DataFrame from a
   *                   previous batch is still cached for this query.
   */
  private def dataFrameCachingWrapper(
      fn: FnArgsWithId => Unit,
      sessionHolder: SessionHolder,
      queryIdRef: AtomicReference[String] = new AtomicReference[String]())
      : ForeachBatchFnType = {

    val sessionManager = new ForeachBatchSessionManager(sessionHolder)

    (df: DataFrame, batchId: Long) => {
      val dfId = UUID.randomUUID().toString
      val queryId = Option(queryIdRef.get())
      // Use the session manager to get the appropriate SessionHolder for this batch.
      // This handles the session clone that MicroBatch execution creates.
      val effectiveSessionHolder = sessionManager.getSessionHolder(df)

      logInfo(
        log"[session: ${MDC(SESSION_ID, effectiveSessionHolder.sessionId)}] " +
          log"[queryId: ${MDC(QUERY_ID, queryId.getOrElse("unknown"))}] " +
          log"Caching DataFrame with id ${MDC(DATAFRAME_ID, dfId)}")

      // Sanity check: ensure there is no other active DataFrame cached for this query.
      // If a previous batch's DataFrame was not properly cleaned up, log a warning
      // and remove the stale entry before caching the new one.
      queryId.foreach { qId =>
        Option(effectiveSessionHolder.dataFrameQueryIndex.put(qId, dfId)).foreach { staleDfId =>
          logWarning(
            log"[session: ${MDC(SESSION_ID, effectiveSessionHolder.sessionId)}] " +
              log"Found stale cached DataFrame ${MDC(DATAFRAME_ID, staleDfId)} " +
              log"for query ${MDC(QUERY_ID, qId)} while caching new DataFrame. " +
              log"Removing stale entry.")
          effectiveSessionHolder.removeCachedDataFrame(staleDfId)
        }
      }

      effectiveSessionHolder.cacheDataFrameById(dfId, df)
      try {
        fn(FnArgsWithId(dfId, df, batchId))
      } finally {
        logInfo(
          log"[session: ${MDC(SESSION_ID, effectiveSessionHolder.sessionId)}] " +
            log"Removing DataFrame with id ${MDC(DATAFRAME_ID, dfId)} from the cache")
        effectiveSessionHolder.removeCachedDataFrame(dfId)
        // Remove query-to-dfId mapping only if it still points to this dfId
        queryId.foreach { qId =>
          effectiveSessionHolder.dataFrameQueryIndex.remove(qId, dfId)
        }
      }
    }
  }

  /**
   * Handles setting up Scala remote session and other Spark Connect environment and then runs the
   * provided foreachBatch function `fn`.
   *
   * HACK ALERT: This version does not actually set up Spark Connect session. Directly passes the
   * DataFrame, so the user code actually runs with legacy DataFrame and session..
   *
   * @return A tuple of (foreachBatchFn, queryIdRef). The caller should set queryIdRef to the
   *         streaming query's ID after starting the query.
   */
  def scalaForeachBatchWrapper(
      payloadBytes: Array[Byte],
      sessionHolder: SessionHolder): (ForeachBatchFnType, AtomicReference[String]) = {
    val foreachBatchPkt =
      Utils.deserialize[ForeachWriterPacket](payloadBytes, Utils.getContextOrSparkClassLoader)
    val fn = foreachBatchPkt.foreachWriter.asInstanceOf[(Dataset[Any], Long) => Unit]
    val encoder = foreachBatchPkt.datasetEncoder.asInstanceOf[AgnosticEncoder[Any]]
    val queryIdRef = new AtomicReference[String]()
    // TODO(SPARK-44462): Set up Spark Connect session.
    // Do we actually need this for the first version?
    val foreachBatchFn = dataFrameCachingWrapper(
      (args: FnArgsWithId) => {
        // dfId is not used, see hack comment above.
        try {
          val ds = if (AgnosticEncoders.UnboundRowEncoder == encoder) {
            // When the dataset is a DataFrame (Dataset[Row).
            args.df.asInstanceOf[Dataset[Any]]
          } else {
            // Recover the Dataset from the DataFrame using the encoder.
            args.df.as(encoder)
          }
          fn(ds, args.batchId)
        } catch {
          case t: Throwable =>
            logError(s"Calling foreachBatch fn failed", t)
            throw t
        }
      },
      sessionHolder,
      queryIdRef)
    (foreachBatchFn, queryIdRef)
  }

  /**
   * Creates a foreachBatch function that uses the callback protocol instead of deserializing
   * the user's lambda. The function caches the batch DataFrame and signals the client to
   * execute the user function via the StreamingForeachBatchCallbackHandler.
   *
   * This enables Scala 3 clients (and any other client that cannot serialize lambdas) to use
   * foreachBatch by keeping the function on the client side.
   *
   * @return A tuple of (foreachBatchFn, callbackHandler, queryIdRef). The caller should:
   *         - Register the callbackHandler in SessionHolder.foreachBatchCallbackHandlers
   *         - Set queryIdRef to the query ID after starting the query
   *         - Register the callbackHandler as a cleaner for the query
   */
  def scalaForeachBatchCallbackWrapper(
      sessionHolder: SessionHolder,
      callbackTimeoutMs: Long): (ForeachBatchFnType, StreamingForeachBatchCallbackHandler,
      AtomicReference[String]) = {

    val queryIdRef = new AtomicReference[String]()
    val handlerRef = new AtomicReference[StreamingForeachBatchCallbackHandler]()

    val foreachBatchFn = dataFrameCachingWrapper(
      (args: FnArgsWithId) => {
        val handler = handlerRef.get()
        if (handler == null) {
          throw new SparkException(
            "ForeachBatch callback handler not initialized. " +
              "Client must open a signal stream before the first batch.")
        }
        handler.signalBatchAndWait(args.dfId, args.batchId)
      },
      sessionHolder,
      queryIdRef)

    // The handler uses queryIdRef to resolve the real queryId lazily (set after query starts).
    val handler = new StreamingForeachBatchCallbackHandler(
      sessionHolder,
      queryIdRef,
      callbackTimeoutMs)
    handlerRef.set(handler)

    (foreachBatchFn, handler, queryIdRef)
  }

  /**
   * Starts up Python worker and initializes it with Python function. Returns a foreachBatch
   * function that sets up the session and Dataframe cache and and interacts with the Python
   * worker to execute user's function. In addition, it returns an AutoClosable and a queryId
   * reference. The caller must ensure the AutoClosable is closed so that worker process and
   * related resources are released, and should set the queryIdRef after the query starts.
   */
  def pythonForeachBatchWrapper(
      pythonFn: SimplePythonFunction,
      sessionHolder: SessionHolder)
      : (ForeachBatchFnType, AutoCloseable, AtomicReference[String]) = {

    val port = SparkConnectService.localPort
    var connectUrl = s"sc://localhost:$port/;user_id=${sessionHolder.userId}"
    Connect.getAuthenticateToken.foreach { token =>
      connectUrl = s"$connectUrl;token=$token"
    }
    val runner = StreamingPythonRunner(
      pythonFn,
      connectUrl,
      sessionHolder.sessionId,
      "pyspark.sql.connect.streaming.worker.foreach_batch_worker")

    logInfo(
      log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
        log"[userId: ${MDC(USER_ID, sessionHolder.userId)}] Initializing Python runner, " +
        log"pythonExec: ${MDC(PYTHON_EXEC, pythonFn.pythonExec)})")

    val (dataOut, dataIn) = runner.init()

    val foreachBatchRunnerFn: FnArgsWithId => Unit = (args: FnArgsWithId) => {

      // TODO(SPARK-44462): A new session id pointing to args.df.sparkSession needs to be created.
      //     This is because MicroBatch execution clones the session during start.
      //     The session attached to the foreachBatch dataframe is different from the one the one
      //     the query was started with. `sessionHolder` here contains the latter.
      //     Another issue with not creating new session id: foreachBatch worker keeps
      //     the session alive. The session mapping at Connect server does not expire and query
      //     keeps running even if the original client disappears. This keeps the query running.

      PythonWorkerUtils.writeUTF(args.dfId, dataOut)
      dataOut.writeLong(args.batchId)
      dataOut.flush()

      try {
        dataIn.readInt() match {
          case 0 =>
            logInfo(
              log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
                log"[userId: ${MDC(USER_ID, sessionHolder.userId)}] " +
                log"Python foreach batch for dfId ${MDC(DATAFRAME_ID, args.dfId)} " +
                log"completed (ret: 0)")
          case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
            val traceback = PythonWorkerUtils.readUTF(dataIn)
            val msg =
              s"[session: ${sessionHolder.sessionId}] [userId: ${sessionHolder.userId}] " +
                s"Found error inside foreachBatch Python process"
            throw new PythonException(
              errorClass = "PYTHON_EXCEPTION",
              messageParameters = Map("msg" -> msg, "traceback" -> traceback))
          case otherValue =>
            throw IllegalStateErrors.streamingQueryUnexpectedReturnValue(
              sessionHolder.key.toString,
              otherValue,
              "foreachBatch function")
        }
      } catch {
        // TODO: Better handling (e.g. retries) on exceptions like EOFException to avoid
        // transient errors, same for StreamingQueryListenerHelper.
        case eof: EOFException =>
          throw new SparkException(
            s"[session: ${sessionHolder.sessionId}] [userId: ${sessionHolder.userId}] " +
              "Python worker exited unexpectedly (crashed)",
            eof)
      }
    }

    val queryIdRef = new AtomicReference[String]()

    (dataFrameCachingWrapper(foreachBatchRunnerFn, sessionHolder, queryIdRef),
      RunnerCleaner(runner), queryIdRef)
  }

  /**
   * This manages cache from queries to cleaner for runners used for streaming queries. This is
   * used in [[SessionHolder]].
   */
  class CleanerCache(sessionHolder: SessionHolder) {

    private case class CacheKey(queryId: String, runId: String)

    // Mapping from streaming (queryId, runId) to runner cleaner. Used for Python foreachBatch.
    private val cleanerCache: ConcurrentMap[CacheKey, AutoCloseable] = new ConcurrentHashMap()

    private lazy val streamingListener = { // Initialized on first registered query
      val listener = new StreamingRunnerCleanerListener
      sessionHolder.session.streams.addListener(listener)
      logInfo(
        log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
          log"[userId: ${MDC(USER_ID, sessionHolder.userId)}] " +
          log"Registered runner clean up listener.")
      listener
    }

    private[connect] def registerCleanerForQuery(
        query: StreamingQuery,
        cleaner: AutoCloseable): Unit = {

      streamingListener // Access to initialize
      val key = CacheKey(query.id.toString, query.runId.toString)

      Option(cleanerCache.putIfAbsent(key, cleaner)) match {
        case Some(_) =>
          throw IllegalStateErrors.cleanerAlreadySet(sessionHolder.key.toString, key.toString)
        case None => // Inserted. Normal.
      }
    }

    /** Cleans up all the registered runners. */
    private[connect] def cleanUpAll(): Unit = {
      // Clean up all remaining registered runners.
      cleanerCache.keySet().asScala.foreach(cleanupStreamingRunner(_))
    }

    private def cleanupStreamingRunner(key: CacheKey): Unit = {
      Option(cleanerCache.remove(key)).foreach { cleaner =>
        logInfo(
          log"Cleaning up runner for queryId ${MDC(QUERY_ID, key.queryId)} " +
            log"runId ${MDC(RUN_ID_STRING, key.runId)}.")
        cleaner.close()
      }
    }

    /**
     * An internal streaming query listener that cleans up Python runner (if there is any) when a
     * query is terminated.
     */
    private class StreamingRunnerCleanerListener extends StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        val key = CacheKey(event.id.toString, event.runId.toString)
        cleanupStreamingRunner(key)
      }
    }

    private[connect] def listEntriesForTesting(): Map[(String, String), AutoCloseable] = {
      cleanerCache
        .entrySet()
        .asScala
        .map { e =>
          (e.getKey.queryId, e.getKey.runId) -> e.getValue
        }
        .toMap
    }

    private[connect] def listenerForTesting: StreamingQueryListener = streamingListener
  }
}
