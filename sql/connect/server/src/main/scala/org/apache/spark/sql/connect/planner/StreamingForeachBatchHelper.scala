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
   * Return a new ForeachBatch function that wraps `fn`. It sets up DataFrame cache so that the
   * user function can access it. The cache is cleared once ForeachBatch returns.
   */
  private def dataFrameCachingWrapper(
      fn: FnArgsWithId => Unit,
      sessionHolder: SessionHolder,
      queryIdRef: AtomicReference[String]): ForeachBatchFnType = { (df: DataFrame, batchId: Long) =>
    {
      val dfId = UUID.randomUUID().toString
      logInfo(
        log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
          log"Caching DataFrame with id ${MDC(DATAFRAME_ID, dfId)}")

      // Sanity check: remove any stale DataFrame left over from a previous batch for this query.
      val queryId = queryIdRef.get()
      if (queryId != null) {
        Option(sessionHolder.dataFrameQueryIndex.put(queryId, dfId)).foreach { staleDfId =>
          logWarning(
            log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
              log"[queryId: ${MDC(QUERY_ID, queryId)}] " +
              log"Stale DataFrame ${MDC(DATAFRAME_ID, staleDfId)} found in cache. Removing it.")
          sessionHolder.removeCachedDataFrame(staleDfId)
        }
      }

      sessionHolder.cacheDataFrameById(dfId, df)
      try {
        fn(FnArgsWithId(dfId, df, batchId))
      } finally {
        logInfo(
          log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
            log"Removing DataFrame with id ${MDC(DATAFRAME_ID, dfId)} from the cache")
        sessionHolder.removeCachedDataFrame(dfId)
        // Clean up query-to-dfId mapping.
        if (queryId != null) {
          sessionHolder.dataFrameQueryIndex.remove(queryId, dfId)
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
   */
  def scalaForeachBatchWrapper(
      payloadBytes: Array[Byte],
      sessionHolder: SessionHolder): (ForeachBatchFnType, AtomicReference[String]) = {
    val foreachBatchPkt =
      Utils.deserialize[ForeachWriterPacket](payloadBytes, Utils.getContextOrSparkClassLoader)
    val fn = foreachBatchPkt.foreachWriter.asInstanceOf[(Dataset[Any], Long) => Unit]
    val encoder = foreachBatchPkt.datasetEncoder.asInstanceOf[AgnosticEncoder[Any]]
    val queryIdRef = new AtomicReference[String]()
    val wrappedFn = dataFrameCachingWrapper(
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
    (wrappedFn, queryIdRef)
  }

  /**
   * Starts up Python worker and initializes it with Python function. Returns a foreachBatch
   * function that sets up the session and Dataframe cache and and interacts with the Python
   * worker to execute user's function. In addition, it returns an AutoClosable and an
   * AtomicReference for setting the query id. The caller must ensure it is closed so that worker
   * process and related resources are released.
   */
  def pythonForeachBatchWrapper(
      pythonFn: SimplePythonFunction,
      sessionHolder: SessionHolder): (ForeachBatchFnType, AutoCloseable, AtomicReference[String]) =
  {

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

    val queryIdRef = new AtomicReference[String]()

    val foreachBatchRunnerFn: FnArgsWithId => Unit = (args: FnArgsWithId) => {

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

    (
      dataFrameCachingWrapper(foreachBatchRunnerFn, sessionHolder, queryIdRef),
      RunnerCleaner(runner),
      queryIdRef)
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
