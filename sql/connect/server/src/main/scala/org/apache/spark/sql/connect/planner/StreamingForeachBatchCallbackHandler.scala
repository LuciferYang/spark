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

import java.util.concurrent.{CompletableFuture, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import io.grpc.stub.StreamObserver

import org.apache.spark.SparkException
import org.apache.spark.connect.proto.{ExecutePlanResponse, ForeachBatchExecutionSignal}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{DATAFRAME_ID, QUERY_ID, SESSION_ID}
import org.apache.spark.sql.connect.service.SessionHolder

/**
 * Handles the foreachBatch callback protocol between server and client.
 *
 * When a streaming query uses the callback protocol (use_callback=true), the server does not
 * deserialize and execute the user's lambda. Instead, it signals the client through a gRPC
 * response stream to execute the function, waits for the result, and then continues.
 *
 * Lifecycle:
 *   1. Client starts a streaming query with use_callback=true
 *   2. Client opens a signal stream via GetForeachBatchExecutionSignalStream
 *   3. For each batch, server calls signalBatchAndWait() which:
 *      a. Sends a ForeachBatchExecutionSignal to the client
 *      b. Blocks on a CompletableFuture waiting for the client's result
 *   4. Client executes the function and sends ForeachBatchExecutionResult
 *   5. Server calls completeExecution() to unblock the waiting future
 *   6. When query terminates, close() cleans up resources
 *
 * @param sessionHolder The session holder for this streaming query
 * @param queryIdRef An AtomicReference that will be populated with the real query ID after the
 *                   streaming query starts. Used in log/error messages. May resolve to null
 *                   before the query starts, in which case "pending" is used.
 * @param callbackTimeoutMs The timeout in milliseconds for waiting for a callback response
 */
private[connect] class StreamingForeachBatchCallbackHandler(
    sessionHolder: SessionHolder,
    queryIdRef: AtomicReference[String],
    callbackTimeoutMs: Long)
    extends AutoCloseable
    with Logging {

  // The gRPC response observer for sending signals to the client.
  @volatile private var signalObserver: StreamObserver[ExecutePlanResponse] = null

  // The future that blocks the foreachBatch execution thread while waiting for the client.
  @volatile private var pendingFuture: CompletableFuture[Option[String]] = null

  // Whether this handler has been closed.
  @volatile private var closed: Boolean = false

  private def queryId: String = {
    val id = queryIdRef.get()
    if (id != null) id else "pending"
  }

  /**
   * Register the signal stream observer. Called when the client opens a
   * GetForeachBatchExecutionSignalStream.
   */
  def setSignalObserver(observer: StreamObserver[ExecutePlanResponse]): Unit = synchronized {
    if (closed) {
      throw new SparkException(
        s"Cannot register signal observer for closed handler (query: $queryId)")
    }
    if (signalObserver != null) {
      throw new SparkException(
        s"Signal observer already registered for query $queryId")
    }
    signalObserver = observer
    logInfo(
      log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
        log"[queryId: ${MDC(QUERY_ID, queryId)}] " +
        log"Signal observer registered for foreachBatch callback")
  }

  /**
   * Signal the client to execute the foreachBatch function and wait for the result.
   * This is called from the foreachBatch execution thread (within the streaming query's
   * micro-batch processing loop).
   *
   * The method uses a two-phase approach:
   *   Phase 1 (synchronized): Validate state, create the future, and send the signal.
   *   Phase 2 (unsynchronized): Wait on the CompletableFuture outside the lock so that
   *           completeExecution() and close() can proceed without deadlock.
   *
   * @param dfId The cached DataFrame ID for this batch
   * @param batchId The batch ID
   * @throws SparkException if the signal observer is not registered, the client disconnects,
   *                        or the timeout is exceeded
   */
  def signalBatchAndWait(dfId: String, batchId: Long): Unit = {
    // Phase 1: State validation and signal sending under the lock.
    val future = synchronized {
      if (closed) {
        throw new SparkException(
          s"Handler closed, cannot signal batch for query $queryId")
      }
      if (signalObserver == null) {
        throw new SparkException(
          s"No signal observer registered for query $queryId. " +
            s"Client must open a GetForeachBatchExecutionSignalStream first.")
      }
      if (pendingFuture != null && !pendingFuture.isDone) {
        throw new SparkException(
          s"Previous batch execution not yet completed for query $queryId")
      }

      val f = new CompletableFuture[Option[String]]()
      pendingFuture = f

      // Build and send the signal
      val signal = ForeachBatchExecutionSignal.newBuilder()
        .setDfId(dfId)
        .setBatchId(batchId)
        .build()

      val response = ExecutePlanResponse.newBuilder()
        .setSessionId(sessionHolder.sessionId)
        .setServerSideSessionId(sessionHolder.serverSessionId)
        .setForeachBatchExecutionSignal(signal)
        .build()

      try {
        signalObserver.onNext(response)
        logInfo(
          log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
            log"[queryId: ${MDC(QUERY_ID, queryId)}] " +
            log"Sent foreachBatch signal for dfId ${MDC(DATAFRAME_ID, dfId)}, " +
            log"waiting for client response")
      } catch {
        case ex: Exception =>
          f.completeExceptionally(new SparkException(
            s"Failed to send signal to client for query $queryId, batch $batchId: " +
              s"client may have disconnected",
            ex))
      }

      f
    }

    // Phase 2: Wait outside the synchronized block using CompletableFuture.get().
    // This avoids holding the lock while waiting and eliminates spurious wakeup issues.
    val result = try {
      future.get(callbackTimeoutMs, TimeUnit.MILLISECONDS)
    } catch {
      case _: TimeoutException =>
        throw new SparkException(
          s"Timed out waiting for foreachBatch callback response " +
            s"(query: $queryId, batch: $batchId, timeout: ${callbackTimeoutMs}ms)")
      case ex: java.util.concurrent.ExecutionException =>
        throw new SparkException(
          s"foreachBatch callback failed for query $queryId, batch $batchId",
          ex.getCause)
    }

    // If the client reported an error, throw it
    result.foreach { errorMsg =>
      throw new SparkException(
        s"Client-side foreachBatch function failed for query $queryId, " +
          s"batch $batchId: $errorMsg")
    }
  }

  /**
   * Complete the pending batch execution with the client's result.
   * Called when the client sends a ForeachBatchExecutionResult.
   */
  def completeExecution(errorMessage: Option[String]): Unit = synchronized {
    if (pendingFuture == null || pendingFuture.isDone) {
      logWarning(
        log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
          log"[queryId: ${MDC(QUERY_ID, queryId)}] " +
          log"Received execution result but no pending future")
      return
    }
    pendingFuture.complete(errorMessage)
  }

  override def close(): Unit = synchronized {
    if (closed) return
    closed = true

    // Complete any pending future with an error
    if (pendingFuture != null && !pendingFuture.isDone) {
      pendingFuture.completeExceptionally(
        new SparkException(
          s"Handler closed while waiting for callback response (query: $queryId)"))
    }

    // Close the signal stream
    if (signalObserver != null) {
      try {
        signalObserver.onCompleted()
      } catch {
        case ex: Exception =>
          logWarning(
            log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
              log"[queryId: ${MDC(QUERY_ID, queryId)}] " +
              log"Error closing signal stream", ex)
      }
      signalObserver = null
    }

    logInfo(
      log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
        log"[queryId: ${MDC(QUERY_ID, queryId)}] " +
        log"ForeachBatch callback handler closed")
  }
}
