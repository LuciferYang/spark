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

import org.scalatest.{Outcome, Reporter}
import org.scalatest.events._

/**
 * A run of a registered test, reported as `name`, with the test body run inside `around`.
 * See `SparkFunSuite.testVariants`.
 */
case class TestVariant(name: String, around: ( => Outcome) => Outcome = body => body)

object TestVariant {
  // The config map entry that tells `SparkTestSuite.withFixture` which variant is running.
  val ConfigKey: String = classOf[TestVariant].getName
}

/** Reports the events of the test `from` as events of the test `to`. */
private[spark] class TestRenamingReporter(delegate: Reporter, from: String, to: String)
  extends Reporter {

  private def renameText(text: String): String = if (text == from) to else text

  private def renameFormatter(formatter: Option[Formatter]): Option[Formatter] = formatter.map {
    case IndentedText(formatted, raw, level) =>
      IndentedText(formatted.replace(from, to), raw.replace(from, to), level)
    case other => other
  }

  private def renameInfo(nameInfo: Option[NameInfo]): Option[NameInfo] =
    nameInfo.map(info => info.copy(testName = info.testName.map(renameText)))

  private def renameEvents(events: IndexedSeq[RecordableEvent]): IndexedSeq[RecordableEvent] =
    events.map {
      case e: InfoProvided => e.copy(nameInfo = renameInfo(e.nameInfo))
      case e: MarkupProvided => e.copy(nameInfo = renameInfo(e.nameInfo))
      case other => other
    }

  override def apply(event: Event): Unit = delegate(event match {
    case e: TestStarting if e.testName == from =>
      e.copy(testName = to, testText = renameText(e.testText),
        formatter = renameFormatter(e.formatter))
    case e: TestSucceeded if e.testName == from =>
      e.copy(testName = to, testText = renameText(e.testText),
        formatter = renameFormatter(e.formatter), recordedEvents = renameEvents(e.recordedEvents))
    case e: TestFailed if e.testName == from =>
      e.copy(testName = to, testText = renameText(e.testText),
        formatter = renameFormatter(e.formatter), recordedEvents = renameEvents(e.recordedEvents))
    case e: TestCanceled if e.testName == from =>
      e.copy(testName = to, testText = renameText(e.testText),
        formatter = renameFormatter(e.formatter), recordedEvents = renameEvents(e.recordedEvents))
    case e: TestPending if e.testName == from =>
      e.copy(testName = to, testText = renameText(e.testText),
        formatter = renameFormatter(e.formatter), recordedEvents = renameEvents(e.recordedEvents))
    case e: InfoProvided => e.copy(nameInfo = renameInfo(e.nameInfo))
    case e: MarkupProvided => e.copy(nameInfo = renameInfo(e.nameInfo))
    case e: AlertProvided => e.copy(nameInfo = renameInfo(e.nameInfo))
    case e: NoteProvided => e.copy(nameInfo = renameInfo(e.nameInfo))
    case other => other
  })
}
