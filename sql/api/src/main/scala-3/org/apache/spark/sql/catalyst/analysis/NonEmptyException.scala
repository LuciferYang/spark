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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.QuotingUtils.quoted

/**
 * Thrown by a catalog when a namespace is not empty. The analyzer will rethrow the exception as
 * an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 *
 * The Scala 2.13 build declares this as a case class. Scala 3 rejects that, because the
 * synthesized `copy` and the inherited `AnalysisException.copy` would both have default
 * arguments, so this declares the members a case class would get, except `copy`.
 */
class NonEmptyNamespaceException(
    val namespace: Array[String],
    val details: String,
    override val cause: Option[Throwable] = None)
    extends AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_3103",
      messageParameters = Map("namespace" -> quoted(namespace), "details" -> details))
    with Product
    with Serializable {

  def this(namespace: Array[String]) = this(namespace, "", None)

  override def productArity: Int = 3

  override def productElement(n: Int): Any = n match {
    case 0 => namespace
    case 1 => details
    case 2 => cause
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productPrefix: String = "NonEmptyNamespaceException"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[NonEmptyNamespaceException]

  override def equals(that: Any): Boolean = that match {
    case other: NonEmptyNamespaceException =>
      (this eq other) || (namespace == other.namespace && details == other.details &&
        cause == other.cause && other.canEqual(this))
    case _ => false
  }

  override def hashCode(): Int = scala.util.hashing.MurmurHash3.productHash(this)
}

object NonEmptyNamespaceException {
  def apply(
      namespace: Array[String],
      details: String,
      cause: Option[Throwable] = None): NonEmptyNamespaceException =
    new NonEmptyNamespaceException(namespace, details, cause)

  def unapply(e: NonEmptyNamespaceException): Option[(Array[String], String, Option[Throwable])] =
    Some((e.namespace, e.details, e.cause))
}
