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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.SparkException
import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.test.SharedSparkSession

class StreamingForeachBatchCallbackHandlerSuite extends SharedSparkSession {

  /** A simple StreamObserver that records sent responses (thread-safe). */
  private class RecordingObserver extends StreamObserver[ExecutePlanResponse] {
    val responses: ConcurrentLinkedQueue[ExecutePlanResponse] = new ConcurrentLinkedQueue()
    @volatile var completed: Boolean = false
    @volatile var error: Throwable = _

    override def onNext(value: ExecutePlanResponse): Unit = {
      responses.add(value)
    }
    override def onError(t: Throwable): Unit = {
      error = t
    }
    override def onCompleted(): Unit = {
      completed = true
    }
  }

  private def createHandler(
      timeoutMs: Long = 5000): StreamingForeachBatchCallbackHandler = {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val queryIdRef = new AtomicReference[String]("test-query-id")
    new StreamingForeachBatchCallbackHandler(sessionHolder, queryIdRef, timeoutMs)
  }

  test("signalBatchAndWait throws when no observer registered") {
    val handler = createHandler()
    val ex = intercept[SparkException] {
      handler.signalBatchAndWait("df-1", 0L)
    }
    assert(ex.getMessage.contains("No signal observer registered"))
  }

  test("signalBatchAndWait sends signal and completeExecution unblocks it") {
    val handler = createHandler()
    val observer = new RecordingObserver()
    handler.setSignalObserver(observer)

    val resultRef = new AtomicReference[Option[Throwable]](None)

    // Run signalBatchAndWait in a separate thread since it blocks.
    val signalThread = new Thread(() => {
      try {
        handler.signalBatchAndWait("df-1", 42L)
      } catch {
        case ex: Throwable => resultRef.set(Some(ex))
      }
    })
    signalThread.start()

    // Wait for the signal to be sent.
    Thread.sleep(200)

    // Verify the signal was sent to the observer.
    val responseList = responses(observer)
    assert(responseList.size == 1)
    val signal = responseList.head.getForeachBatchExecutionSignal
    assert(signal.getDfId == "df-1")
    assert(signal.getBatchId == 42L)

    // Complete the execution (success case).
    handler.completeExecution(None)

    signalThread.join(3000)
    assert(!signalThread.isAlive, "Signal thread should have completed")
    assert(resultRef.get().isEmpty, "Should not have thrown an exception")
  }

  test("signalBatchAndWait throws when client reports error") {
    val handler = createHandler()
    val observer = new RecordingObserver()
    handler.setSignalObserver(observer)

    val resultRef = new AtomicReference[Option[Throwable]](None)

    val signalThread = new Thread(() => {
      try {
        handler.signalBatchAndWait("df-2", 1L)
      } catch {
        case ex: Throwable => resultRef.set(Some(ex))
      }
    })
    signalThread.start()

    Thread.sleep(200)

    // Complete with an error.
    handler.completeExecution(Some("User function failed: division by zero"))

    signalThread.join(3000)
    assert(!signalThread.isAlive)
    val error = resultRef.get()
    assert(error.isDefined, "Should have thrown an exception")
    assert(error.get.getMessage.contains("Client-side foreachBatch function failed"))
    assert(error.get.getMessage.contains("division by zero"))
  }

  test("signalBatchAndWait times out") {
    val handler = createHandler(timeoutMs = 500)
    val observer = new RecordingObserver()
    handler.setSignalObserver(observer)

    val resultRef = new AtomicReference[Option[Throwable]](None)

    val signalThread = new Thread(() => {
      try {
        handler.signalBatchAndWait("df-3", 0L)
      } catch {
        case ex: Throwable => resultRef.set(Some(ex))
      }
    })
    signalThread.start()

    // Don't complete the execution - let it time out.
    signalThread.join(5000)
    assert(!signalThread.isAlive)
    val error = resultRef.get()
    assert(error.isDefined, "Should have thrown a timeout exception")
    assert(error.get.getMessage.contains("Timed out"))
  }

  test("close completes pending future and closes signal stream") {
    val handler = createHandler()
    val observer = new RecordingObserver()
    handler.setSignalObserver(observer)

    val resultRef = new AtomicReference[Option[Throwable]](None)

    val signalThread = new Thread(() => {
      try {
        handler.signalBatchAndWait("df-4", 0L)
      } catch {
        case ex: Throwable => resultRef.set(Some(ex))
      }
    })
    signalThread.start()

    Thread.sleep(200)

    // Close the handler while a signal is pending.
    handler.close()

    signalThread.join(3000)
    assert(!signalThread.isAlive)
    assert(observer.completed, "Signal stream should have been completed")
  }

  test("setSignalObserver throws when handler is closed") {
    val handler = createHandler()
    handler.close()

    val observer = new RecordingObserver()
    val ex = intercept[SparkException] {
      handler.setSignalObserver(observer)
    }
    assert(ex.getMessage.contains("closed"))
  }

  test("setSignalObserver throws on duplicate registration") {
    val handler = createHandler()
    val observer1 = new RecordingObserver()
    val observer2 = new RecordingObserver()

    handler.setSignalObserver(observer1)
    val ex = intercept[SparkException] {
      handler.setSignalObserver(observer2)
    }
    assert(ex.getMessage.contains("already registered"))
  }

  test("signalBatchAndWait throws when handler is closed") {
    val handler = createHandler()
    val observer = new RecordingObserver()
    handler.setSignalObserver(observer)
    handler.close()

    val ex = intercept[SparkException] {
      handler.signalBatchAndWait("df-5", 0L)
    }
    assert(ex.getMessage.contains("closed"))
  }

  test("close is idempotent") {
    val handler = createHandler()
    val observer = new RecordingObserver()
    handler.setSignalObserver(observer)

    handler.close()
    handler.close() // Should not throw.
    assert(observer.completed)
  }

  test("queryIdRef resolves lazily in log/error messages") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val queryIdRef = new AtomicReference[String]()
    val handler = new StreamingForeachBatchCallbackHandler(sessionHolder, queryIdRef, 5000)
    handler.close()

    // Before setting the queryId, error messages should contain "pending"
    val ex1 = intercept[SparkException] {
      handler.setSignalObserver(new RecordingObserver())
    }
    assert(ex1.getMessage.contains("pending"))

    // After setting queryId, messages should reflect the real ID
    val handler2 = new StreamingForeachBatchCallbackHandler(sessionHolder, queryIdRef, 5000)
    queryIdRef.set("real-query-id-123")
    handler2.close()
    val ex2 = intercept[SparkException] {
      handler2.setSignalObserver(new RecordingObserver())
    }
    assert(ex2.getMessage.contains("real-query-id-123"))
  }

  /** Helper to snapshot ConcurrentLinkedQueue as a List. */
  private def responses(observer: RecordingObserver): List[ExecutePlanResponse] = {
    observer.responses.asScala.toList
  }
}
