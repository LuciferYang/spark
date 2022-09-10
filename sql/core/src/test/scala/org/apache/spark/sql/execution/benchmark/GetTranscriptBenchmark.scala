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

package org.apache.spark.sql.execution.benchmark

import test.org.apache.spark.sql.TestApis

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

object GetTranscriptBenchmark extends BenchmarkBase {

  import org.apache.spark.network.crypto.AuthMessage

  private def testAuthMessagesLength(input: Array[AuthMessage], valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test AuthMessage length with input size ${input.length}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Arrays.steam api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.authMessagesLengthUseStreamApi(input)
      }
    }

    benchmark.addCase("Use Loop api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.authMessagesLengthUseLoopApi(input)
      }
    }
    benchmark.run()
  }

  private def testEncodeAuthMessages(bufferLength: Int,
      input: Array[AuthMessage], valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test encode AuthMessages input size ${input.length}",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Use Arrays.steam api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.encodeAuthMessagesUseStreamApi(bufferLength, input)
      }
    }

    benchmark.addCase("Use Loop api") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        TestApis.encodeAuthMessagesUseLoopApi(bufferLength, input)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    import org.apache.commons.lang3.RandomStringUtils
    val valuesPerIteration = 100000
    val appId = RandomStringUtils.random(20)
    val salt = RandomStringUtils.random(128).getBytes()
    val ciphertext = RandomStringUtils.random(256).getBytes()

    val authMessage = new AuthMessage(appId, salt, ciphertext)

    Seq(1, 5, 10, 20, 50, 100, 500, 1000).foreach { i =>
      val messages = Array.fill(i) {authMessage}
      testAuthMessagesLength(messages, valuesPerIteration)
      val bufferLength = TestApis.authMessagesLengthUseLoopApi(messages)
      testEncodeAuthMessages(bufferLength, messages, valuesPerIteration)
    }
  }
}
