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

package org.apache.spark.sql

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow, UnsafeRow}
import org.apache.spark.sql.types._

/**
 * Benchmark for JoinedRow operations.
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. with sbt:
 *      build/sbt "catalyst/Test/runMain <this class>"
 *
 *   Results will be written to "benchmarks/JoinedRowBenchmark-results.txt".
 * }}}
 */
object JoinedRowBenchmark extends BenchmarkBase {

  private val numRows = 10000000
  private val numFields = 10

  def joinedRowCreation(): Unit = {
    val benchmark = new Benchmark("JoinedRow Creation", numRows, output = output)

    val row1 = new GenericInternalRow(numFields)
    val row2 = new GenericInternalRow(numFields)
    for (i <- 0 until numFields) {
      row1.setInt(i, i)
      row2.setInt(i, i + numFields)
    }

    benchmark.addCase("new JoinedRow(row1, row2)") { _ =>
      var sum = 0L
      for (_ <- 0L until numRows) {
        val joined = new JoinedRow(row1, row2)
        sum += joined.numFields
      }
    }

    benchmark.addCase("joinedRow.apply(row1, row2)") { _ =>
      var sum = 0L
      val joined = new JoinedRow()
      for (_ <- 0L until numRows) {
        joined.apply(row1, row2)
        sum += joined.numFields
      }
    }

    benchmark.run()
  }

  def joinedRowAccess(): Unit = {
    val benchmark = new Benchmark("JoinedRow Field Access", numRows, output = output)

    val row1 = new GenericInternalRow(numFields)
    val row2 = new GenericInternalRow(numFields)
    for (i <- 0 until numFields) {
      row1.setInt(i, i)
      row2.setInt(i, i + numFields)
    }

    val joined = new JoinedRow(row1, row2)

    benchmark.addCase("access left fields") { _ =>
      var sum = 0L
      for (_ <- 0L until numRows) {
        for (i <- 0 until numFields) {
          sum += joined.getInt(i)
        }
      }
    }

    benchmark.addCase("access right fields") { _ =>
      var sum = 0L
      for (_ <- 0L until numRows) {
        for (i <- 0 until numFields) {
          sum += joined.getInt(i + numFields)
        }
      }
    }

    benchmark.addCase("access mixed fields") { _ =>
      var sum = 0L
      for (_ <- 0L until numRows) {
        for (i <- 0 until joined.numFields) {
          sum += joined.getInt(i)
        }
      }
    }

    benchmark.run()
  }

  def joinedRowUpdate(): Unit = {
    val benchmark = new Benchmark("JoinedRow Update Operations", numRows / 10, output = output)

    benchmark.addCase("withLeft") { _ =>
      val row1 = new GenericInternalRow(numFields)
      val row2 = new GenericInternalRow(numFields)
      val newRow = new GenericInternalRow(numFields)
      for (i <- 0 until numFields) {
        row1.setInt(i, i)
        row2.setInt(i, i + numFields)
        newRow.setInt(i, i * 2)
      }
      val joined = new JoinedRow(row1, row2)

      var sum = 0L
      for (_ <- 0L until numRows / 10) {
        joined.withLeft(newRow)
        sum += joined.numFields
      }
    }

    benchmark.addCase("withRight") { _ =>
      val row1 = new GenericInternalRow(numFields)
      val row2 = new GenericInternalRow(numFields)
      val newRow = new GenericInternalRow(numFields)
      for (i <- 0 until numFields) {
        row1.setInt(i, i)
        row2.setInt(i, i + numFields)
        newRow.setInt(i, i * 2)
      }
      val joined = new JoinedRow(row1, row2)

      var sum = 0L
      for (_ <- 0L until numRows / 10) {
        joined.withRight(newRow)
        sum += joined.numFields
      }
    }

    benchmark.addCase("apply") { _ =>
      val row1 = new GenericInternalRow(numFields)
      val row2 = new GenericInternalRow(numFields)
      val newRow1 = new GenericInternalRow(numFields)
      val newRow2 = new GenericInternalRow(numFields)
      for (i <- 0 until numFields) {
        row1.setInt(i, i)
        row2.setInt(i, i + numFields)
        newRow1.setInt(i, i * 2)
        newRow2.setInt(i, i * 3)
      }
      val joined = new JoinedRow(row1, row2)

      var sum = 0L
      for (_ <- 0L until numRows / 10) {
        joined.apply(newRow1, newRow2)
        sum += joined.numFields
      }
    }

    benchmark.run()
  }

  def joinedRowWithUnsafeRow(): Unit = {
    val benchmark = new Benchmark("JoinedRow with UnsafeRow", numRows, output = output)

    val schema = StructType(Seq.tabulate(numFields)(i => StructField(s"col$i", IntegerType)))
    val unsafeRow1 = {
      val row = new GenericInternalRow(numFields)
      for (i <- 0 until numFields) row.setInt(i, i)
      UnsafeProjection.create(schema).apply(row)
    }
    val unsafeRow2 = {
      val row = new GenericInternalRow(numFields)
      for (i <- 0 until numFields) row.setInt(i, i + numFields)
      UnsafeProjection.create(schema).apply(row)
    }

    benchmark.addCase("UnsafeRow + UnsafeRow") { _ =>
      val joined = new JoinedRow()
      var sum = 0L
      for (_ <- 0L until numRows) {
        joined.apply(unsafeRow1, unsafeRow2)
        sum += joined.getInt(0) + joined.getInt(numFields)
      }
    }

    val genericRow = new GenericInternalRow(numFields)
    for (i <- 0 until numFields) genericRow.setInt(i, i)

    benchmark.addCase("GenericRow + UnsafeRow") { _ =>
      val joined = new JoinedRow()
      var sum = 0L
      for (_ <- 0L until numRows) {
        joined.apply(genericRow, unsafeRow2)
        sum += joined.getInt(0) + joined.getInt(numFields)
      }
    }

    benchmark.run()
  }

  def joinedRowCopy(): Unit = {
    val benchmark = new Benchmark("JoinedRow Copy", numRows / 100, output = output)

    val row1 = new GenericInternalRow(numFields)
    val row2 = new GenericInternalRow(numFields)
    for (i <- 0 until numFields) {
      row1.setInt(i, i)
      row2.setInt(i, i + numFields)
    }

    benchmark.addCase("copy JoinedRow") { _ =>
      val joined = new JoinedRow(row1, row2)
      var sum = 0L
      for (_ <- 0L until numRows / 100) {
        val copied = joined.copy()
        sum += copied.numFields
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("JoinedRow Creation") {
      joinedRowCreation()
    }
    runBenchmark("JoinedRow Access") {
      joinedRowAccess()
    }
    runBenchmark("JoinedRow Update") {
      joinedRowUpdate()
    }
    runBenchmark("JoinedRow with UnsafeRow") {
      joinedRowWithUnsafeRow()
    }
    runBenchmark("JoinedRow Copy") {
      joinedRowCopy()
    }
  }
}

// Helper for UnsafeRow creation
object UnsafeProjection {
  def create(schema: StructType): GenericInternalRow => UnsafeRow = {
    val converter = org.apache.spark.sql.catalyst.expressions.UnsafeProjection.create(schema)
    (row: GenericInternalRow) => converter.apply(row).asInstanceOf[UnsafeRow]
  }
}
