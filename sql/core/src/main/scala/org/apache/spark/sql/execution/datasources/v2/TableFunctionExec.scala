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

package org.apache.spark.sql.execution.datasources.v2

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BaseOrdering, BoundReference, GenericInternalRow, RowOrdering, SortOrder, UnsafeProjection}
import org.apache.spark.sql.connector.catalog.functions.TableFunctionEvaluatorFactory
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Physical node that executes a V2 catalog table-valued function which consumes a TABLE argument,
 * over its (already repartitioned + sorted) child.
 *
 * The child emits a single struct column `c` whose fields are the TABLE argument's columns in order
 * (possibly followed by internal `partition_by_N` marker columns the analyzer appended to carry the
 * PARTITION BY values). Rows arrive hash-partitioned by the partition keys and sorted by
 * (partition keys, ORDER BY), so this node segments adjacent rows on `partitionColumnIndexes` into
 * PARTITION BY groups and invokes the connector's evaluator once per group. This mirrors the
 * `MapPartitionsExec` pattern (and the Python UDTF exec nodes).
 *
 * IMPORTANT: this node holds ONLY the serializable [[TableFunctionEvaluatorFactory]] and plain
 * metadata -- NOT the bound `SupportsTableArgument` function, which may not be serializable and
 * stays driver-side in the logical `TableFunctionGenerator`. This is the driver/executor boundary
 * the factory API is designed around: the factory ships to executors and `create()`s the evaluator
 * lazily per task.
 *
 * @param factory the serializable per-partition evaluator factory
 * @param inputStructType the schema of the struct input column `c` (input columns + any marker
 *                        columns), used for key comparison and input slicing
 * @param inputColumnCount the number of leading fields that are the TABLE argument's own columns;
 *                         each row is sliced to these before being handed to the evaluator
 * @param partitionColumnIndexes ordinals (within the struct) of the PARTITION BY expressions; empty
 *                               means the whole task-partition is one group
 * @param functionName the qualified function name, for display only
 */
case class TableFunctionExec(
    factory: TableFunctionEvaluatorFactory,
    inputStructType: StructType,
    inputColumnCount: Int,
    partitionColumnIndexes: Seq[Int],
    functionName: String,
    generatorOutput: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = generatorOutput

  override protected def withNewChildInternal(newChild: SparkPlan): TableFunctionExec =
    copy(child = newChild)

  override def simpleString(maxFields: Int): String =
    s"TableFunctionExec $functionName${truncatedOutputString(maxFields)}"

  private def truncatedOutputString(maxFields: Int): String =
    output.take(maxFields).map(_.name).mkString(" [", ", ", "]")

  protected override def doExecute(): RDD[InternalRow] = {
    val ordinals = partitionColumnIndexes
    val fieldTypes = inputStructType.fields.map(_.dataType)
    val nFields = inputStructType.length
    val nInputCols = inputColumnCount
    val evaluatorFactory = factory
    val out = output
    child.execute().mapPartitionsInternal { iter =>
      // Each child row has one column: the struct `c`. Unwrap to the inner struct row.
      val structRows: Iterator[InternalRow] = iter.map(r => r.getStruct(0, nFields))
      val evaluator = evaluatorFactory.create()
      // Build a type-aware ordering over the partition-key ordinals. Comparing keys through an
      // ordering (rather than boxing each key to a `Seq[Any]` and using Scala `==`) gives correct
      // value equality for every orderable type -- notably BinaryType, whose JVM `byte[]` has only
      // reference equality, so `Seq`-based comparison would put every row in its own group.
      val keyOrdering: Option[BaseOrdering] = if (ordinals.isEmpty) {
        None
      } else {
        val sortOrder = ordinals.map { i =>
          SortOrder(BoundReference(i, fieldTypes(i), nullable = true), Ascending)
        }
        Some(RowOrdering.create(sortOrder, Nil))
      }
      val grouped = new GroupSegmentingIterator(structRows, keyOrdering, fieldTypes, nInputCols,
        group => evaluator.eval(group.asJava).asScala)
      // Physical operators must emit UnsafeRow; the evaluator yields GenericInternalRow.
      val toUnsafe = UnsafeProjection.create(out, out)
      grouped.map(toUnsafe)
    }
  }
}

/**
 * Lazily segments a sorted iterator of struct rows into PARTITION BY groups -- adjacent rows that
 * compare equal under `keyOrdering` form a group -- and flat-maps each group through `evaluate`.
 * Buffers only the current group. When `keyOrdering` is `None`, the whole partition is one group.
 *
 * Before a row is handed to `evaluate`, it is sliced to its leading `inputColumnCount` fields, so
 * the connector's evaluator sees exactly the TABLE argument's columns and not the internal
 * `partition_by_N` marker columns the analyzer may have appended for segmentation.
 *
 * @param keyOrdering compares two struct rows on the PARTITION BY ordinals; `None` means no
 *                    PARTITION BY, so the whole (task) partition is a single group. Using an
 *                    ordering rather than boxed value equality gives correct byte-wise comparison
 *                    for BinaryType and all nested types.
 * @param fieldTypes the data types of ALL struct fields (input columns + any marker columns), used
 *                   for slicing input rows.
 */
private class GroupSegmentingIterator(
    input: Iterator[InternalRow],
    keyOrdering: Option[BaseOrdering],
    fieldTypes: Array[DataType],
    inputColumnCount: Int,
    evaluate: Iterator[InternalRow] => Iterator[InternalRow]) extends Iterator[InternalRow] {

  private val src = input.buffered
  private var current: Iterator[InternalRow] = Iterator.empty

  private def sameKey(a: InternalRow, b: InternalRow): Boolean =
    keyOrdering.forall(_.compare(a, b) == 0)

  // Project a full struct row down to just the TABLE argument's own leading columns.
  private def sliceInput(r: InternalRow): InternalRow = {
    if (inputColumnCount == fieldTypes.length) {
      r
    } else {
      val values = new Array[Any](inputColumnCount)
      var i = 0
      while (i < inputColumnCount) {
        values(i) = r.get(i, fieldTypes(i))
        i += 1
      }
      new GenericInternalRow(values)
    }
  }

  private def nextGroup(): Iterator[InternalRow] = {
    if (!src.hasNext) return Iterator.empty
    val head = src.next().copy()
    val group = scala.collection.mutable.ArrayBuffer[InternalRow](head)
    while (src.hasNext && sameKey(head, src.head)) {
      group += src.next().copy()
    }
    evaluate(group.iterator.map(sliceInput))
  }

  override def hasNext: Boolean = {
    while (!current.hasNext && src.hasNext) {
      current = nextGroup()
    }
    current.hasNext
  }

  override def next(): InternalRow = current.next()
}
