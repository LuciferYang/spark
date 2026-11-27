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

package org.apache.spark

import scala.collection.mutable.ArrayBuffer

import org.scalatest.{Args, DoNotDiscover, Filter, Reporter}
import org.scalatest.events.{Event, TestFailed, TestIgnored, TestStarting, TestSucceeded}

class TestVariantSuite extends SparkFunSuite {
  import TestVariantSuite._

  private def run(suite: SparkFunSuite): EventRecorder = {
    val reporter = new EventRecorder
    suite.run(None, Args(reporter))
    reporter
  }

  test("each variant runs the test body inside its own wrapper, under its own name") {
    val suite = new TwoModes
    val events = run(suite)
    assert(events.started === Seq("t (a)", "t (b)"))
    assert(events.succeeded === Seq("t (a)", "t (b)"))
    assert(suite.modes === Seq("a", "b"))
    assert(suite.expectedTestCount(Filter()) === 2)
  }

  test("a failing variant is reported under its own name") {
    val suite = new TwoModes {
      override protected def check(mode: String): Unit = assert(mode != "b")
    }
    val events = run(suite)
    assert(events.succeeded === Seq("t (a)"))
    assert(events.failed === Seq("t (b)"))
  }

  test("an excluded test is reported once, as ignored, and not run") {
    val suite = new TwoModes {
      override protected def excluded: Seq[String] = Seq("t")
    }
    val events = run(suite)
    assert(events.ignored === Seq("t"))
    assert(events.started.isEmpty && suite.modes.isEmpty)
    assert(suite.expectedTestCount(Filter()) === 0)
  }
}

object TestVariantSuite {
  /** Runs its one test, `t`, as the variants `t (a)` and `t (b)`. */
  @DoNotDiscover
  class TwoModes extends SparkFunSuite {
    val modes = ArrayBuffer.empty[String]
    private var mode: String = _

    protected def check(mode: String): Unit = {}

    override protected def testVariants(testName: String): Seq[TestVariant] =
      Seq("a", "b").map { m =>
        TestVariant(s"$testName ($m)", body => {
          mode = m
          try body finally mode = null
        })
      }

    test("t") {
      modes += mode
      check(mode)
    }
  }

  class EventRecorder extends Reporter {
    val started = ArrayBuffer.empty[String]
    val succeeded = ArrayBuffer.empty[String]
    val failed = ArrayBuffer.empty[String]
    val ignored = ArrayBuffer.empty[String]

    override def apply(event: Event): Unit = event match {
      case e: TestStarting => started += e.testName
      case e: TestSucceeded => succeeded += e.testName
      case e: TestFailed => failed += e.testName
      case e: TestIgnored => ignored += e.testName
      case _ =>
    }
  }
}
