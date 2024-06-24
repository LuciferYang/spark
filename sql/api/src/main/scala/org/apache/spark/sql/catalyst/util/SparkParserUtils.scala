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
package org.apache.spark.sql.catalyst.util

import java.lang.{StringBuilder => JStringBuilder}

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode

import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

trait SparkParserUtils {

  /** Unescape backslash-escaped string enclosed by quotes. */
  def unescapeSQLString(b: String): String = {
    val sb = new JStringBuilder(b.length())

    def appendEscapedChar(n: Char): Unit = {
      n match {
        case '0' => sb.append('\u0000')
        case 'b' => sb.append('\b')
        case 'n' => sb.append('\n')
        case 'r' => sb.append('\r')
        case 't' => sb.append('\t')
        case 'Z' => sb.append('\u001A')
        // The following 2 lines are exactly what MySQL does TODO: why do we do this?
        case '%' => sb.append("\\%")
        case '_' => sb.append("\\_")
        case _ => sb.append(n)
      }
    }

    // Helper method to check if all characters in a substring are hexadecimal
    def allCharsAreHex(s: Array[Char], start: Int, length: Int): Boolean = {
      val end = start + length
      var i = start
      while (i < end) {
        val c = s(i)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
          return false
        }
        i += 1
      }
      true
    }

    // Helper method to check if the next three characters form a valid octal escape
    def isThreeDigitOctalEscape(s: Array[Char], start: Int): Boolean = {
      val firstChar = s(start)
      val secondChar = s(start + 1)
      val thirdChar = s(start + 2)
      (firstChar == '0' || firstChar == '1') &&
        (secondChar >= '0' && secondChar <= '7') &&
        (thirdChar >= '0' && thirdChar <= '7')
    }

    // Convert string to char array for faster access
    val chars = b.toCharArray
    val length = chars.length

    // Check for raw string
    val isRawString = chars(0) == 'r' || chars(0) == 'R'

    if (isRawString) {
      // Skip the 'r' or 'R' and the first and last quotations enclosing the string literal.
      new String(chars, 2, length - 3)
    } else if (b.indexOf('\\') == -1) {
      // Fast path for the common case where the string has no escaped characters.
      new String(chars, 1, length - 2)
    } else {
      // Skip the first and last quotations enclosing the string literal.
      var i = 1
      while (i < length - 1) {
        val c = chars(i)
        if (c != '\\' || i + 1 == length - 1) {
          // Either a regular character or a backslash at the end of the string:
          sb.append(c)
          i += 1
        } else {
          // A backslash followed by at least one character:
          i += 1
          val cAfterBackslash = chars(i)
          cAfterBackslash match {
            case 'u' if i + 4 < length && allCharsAreHex(chars, i + 1, 4) =>
              // \u0000 style 16-bit unicode character literals.
              sb.append(Integer.parseInt(new String(chars, i + 1, 4), 16).toChar)
              i += 5
            case 'U' if i + 8 < length && allCharsAreHex(chars, i + 1, 8) =>
              // \U00000000 style 32-bit unicode character literals.
              val codePoint = Integer.parseInt(new String(chars, i + 1, 8), 16)
              sb.appendCodePoint(codePoint)
              i += 9
            case _ if i + 2 < length && isThreeDigitOctalEscape(chars, i) =>
              // \000 style character literals.
              sb.append(Integer.parseInt(new String(chars, i, 3), 8).toChar)
              i += 3
            case _ =>
              appendEscapedChar(cAfterBackslash)
              i += 1
          }
        }
      }
      sb.toString
    }
  }

  /** Convert a string token into a string. */
  def string(token: Token): String = unescapeSQLString(token.getText)

  /** Convert a string node into a string. */
  def string(node: TerminalNode): String = unescapeSQLString(node.getText)

  /** Get the origin (line and position) of the token. */
  def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  /**
   * Register the origin of the context. Any TreeNode created in the closure will be assigned the
   * registered origin. This method restores the previously set origin after completion of the
   * closure.
   */
  def withOrigin[T](ctx: ParserRuleContext, sqlText: Option[String] = None)(f: => T): T = {
    val current = CurrentOrigin.get
    val text = sqlText.orElse(current.sqlText)
    if (text.isEmpty) {
      CurrentOrigin.set(position(ctx.getStart))
    } else {
      CurrentOrigin.set(positionAndText(ctx.getStart, ctx.getStop, text.get,
        current.objectType, current.objectName))
    }
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  def positionAndText(
      startToken: Token,
      stopToken: Token,
      sqlText: String,
      objectType: Option[String],
      objectName: Option[String]): Origin = {
    val startOpt = Option(startToken)
    val stopOpt = Option(stopToken)
    Origin(
      line = startOpt.map(_.getLine),
      startPosition = startOpt.map(_.getCharPositionInLine),
      startIndex = startOpt.map(_.getStartIndex),
      stopIndex = stopOpt.map(_.getStopIndex),
      sqlText = Some(sqlText),
      objectType = objectType,
      objectName = objectName)
  }

  /** Get the command which created the token. */
  def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }
}

object SparkParserUtils extends SparkParserUtils
