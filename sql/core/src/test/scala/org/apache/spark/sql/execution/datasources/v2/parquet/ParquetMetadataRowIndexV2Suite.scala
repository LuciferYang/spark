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

package org.apache.spark.sql.execution.datasources.v2.parquet

import java.io.File

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for `_metadata.row_index` on V2 Parquet (SPARK-56371). Verifies that the
 * generated row_index sub-field is exposed via `metadataColumns()`, populated correctly per
 * row by the Parquet reader, and round-trips through the V2 metadata wrapper for both
 * vectorized and row-based reads.
 */
class ParquetMetadataRowIndexV2Suite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private def withV2Parquet(body: => Unit): Unit = {
    val v1List = SQLConf.get.getConf(SQLConf.USE_V1_SOURCE_LIST)
    val newV1List = v1List.split(",").filter(_.nonEmpty)
      .filterNot(_.equalsIgnoreCase("parquet")).mkString(",")
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> newV1List) {
      body
    }
  }

  private def withVectorized(enabled: Boolean)(body: => Unit): Unit = {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> enabled.toString) {
      body
    }
  }

  test("SPARK-56371: _metadata.row_index per-row values (vectorized)") {
    withV2Parquet {
      withVectorized(enabled = true) {
        withTempDir { dir =>
          val tablePath = new File(dir, "rowidx").getAbsolutePath
          (1 to 5).toDF("id").coalesce(1).write.parquet(tablePath)

          val rows = spark.read.parquet(tablePath)
            .selectExpr("id", "_metadata.row_index")
            .orderBy("id")
            .collect()
          assert(rows.length == 5)
          rows.zipWithIndex.foreach { case (row, expectedIdx) =>
            assert(row.getInt(0) == expectedIdx + 1)
            assert(row.getLong(1) == expectedIdx)
          }
        }
      }
    }
  }

  test("SPARK-56371: _metadata.row_index per-row values (row-based)") {
    withV2Parquet {
      withVectorized(enabled = false) {
        withTempDir { dir =>
          val tablePath = new File(dir, "rowidx_rb").getAbsolutePath
          (1 to 5).toDF("id").coalesce(1).write.parquet(tablePath)

          val rows = spark.read.parquet(tablePath)
            .selectExpr("id", "_metadata.row_index")
            .orderBy("id")
            .collect()
          assert(rows.length == 5)
          rows.zipWithIndex.foreach { case (row, expectedIdx) =>
            assert(row.getInt(0) == expectedIdx + 1)
            assert(row.getLong(1) == expectedIdx)
          }
        }
      }
    }
  }

  test("SPARK-56371: row_index resets per file across multiple files") {
    withV2Parquet {
      withVectorized(enabled = true) {
        withTempDir { dir =>
          val tablePath = new File(dir, "multi").getAbsolutePath
          (1 to 3).toDF("id").coalesce(1).write.parquet(tablePath + "/f1")
          (10 to 12).toDF("id").coalesce(1).write.parquet(tablePath + "/f2")

          val df = spark.read.parquet(tablePath + "/f1", tablePath + "/f2")
            .selectExpr("id", "_metadata.file_name", "_metadata.row_index")
          val rows = df.collect()
          assert(rows.length == 6)
          val byFile = rows.groupBy(_.getString(1))
          assert(byFile.size == 2)
          byFile.values.foreach { fileRows =>
            val sortedIndices = fileRows.map(_.getLong(2)).sorted
            assert(sortedIndices.toSeq == Seq(0L, 1L, 2L),
              s"row_index per file should be 0,1,2; got $sortedIndices")
          }
        }
      }
    }
  }

  test("SPARK-56371: combined constant + generated metadata fields (row-based)") {
    // Same shape as the vectorized variant - exercises the row-based projection path
    // (UnsafeProjection over BoundReferences + CreateNamedStruct).
    withV2Parquet {
      withVectorized(enabled = false) {
        withTempDir { dir =>
          val tablePath = new File(dir, "combined_rb").getAbsolutePath
          (1 to 3).toDF("id").coalesce(1).write.parquet(tablePath)

          val rows = spark.read.parquet(tablePath)
            .selectExpr(
              "id",
              "_metadata.file_name",
              "_metadata.file_modification_time",
              "_metadata.row_index")
            .orderBy("id")
            .collect()
          assert(rows.length == 3)
          rows.foreach { r =>
            // file_modification_time must be returned as a Timestamp, not a Long.
            // This guards against a `Literal.apply(longValue)` regression that would
            // produce a `LongType` literal in the metadata struct instead of `TimestampType`.
            assert(r.getAs[java.sql.Timestamp](2) != null)
          }
          assert(rows.map(_.getLong(3)).toSeq == Seq(0L, 1L, 2L))
        }
      }
    }
  }

  test("SPARK-56371: combined constant + generated metadata fields (vectorized)") {
    withV2Parquet {
      withVectorized(enabled = true) {
        withTempDir { dir =>
          val tablePath = new File(dir, "combined").getAbsolutePath
          (1 to 3).toDF("id").coalesce(1).write.parquet(tablePath)

          val rows = spark.read.parquet(tablePath)
            .selectExpr(
              "id",
              "_metadata.file_path",
              "_metadata.file_name",
              "_metadata.file_size",
              "_metadata.row_index")
            .orderBy("id")
            .collect()
          assert(rows.length == 3)
          val firstPath = rows.head.getString(1)
          rows.foreach { r =>
            // file_path should be identical for all rows (single file).
            assert(r.getString(1) == firstPath)
            // file_size should be positive.
            assert(r.getLong(3) > 0)
          }
          val rowIndices = rows.map(_.getLong(4)).toSeq
          assert(rowIndices == Seq(0L, 1L, 2L))
        }
      }
    }
  }

  test("SPARK-56371: filter on _metadata.row_index") {
    withV2Parquet {
      withVectorized(enabled = true) {
        withTempDir { dir =>
          val tablePath = new File(dir, "filtered").getAbsolutePath
          (1 to 10).toDF("id").coalesce(1).write.parquet(tablePath)

          val rows = spark.read.parquet(tablePath)
            .where("_metadata.row_index < 3")
            .selectExpr("id", "_metadata.row_index")
            .orderBy("id")
            .collect()
          assert(rows.length == 3)
          assert(rows.map(_.getLong(1)).toSeq == Seq(0L, 1L, 2L))
        }
      }
    }
  }

  test("SPARK-56371: row_index only (no data columns) projection") {
    withV2Parquet {
      withVectorized(enabled = true) {
        withTempDir { dir =>
          val tablePath = new File(dir, "metaonly").getAbsolutePath
          (1 to 4).toDF("id").coalesce(1).write.parquet(tablePath)

          val rows = spark.read.parquet(tablePath)
            .selectExpr("_metadata.row_index")
            .collect()
            .map(_.getLong(0))
            .sorted
          assert(rows.toSeq == Seq(0L, 1L, 2L, 3L))
        }
      }
    }
  }

  test("SPARK-56371: row_index with partitioned table (vectorized)") {
    withV2Parquet {
      withVectorized(enabled = true) {
        withTempDir { dir =>
          val tablePath = new File(dir, "partitioned").getAbsolutePath
          Seq((1, "a"), (2, "a"), (3, "b"), (4, "b")).toDF("id", "p")
            .write.partitionBy("p").parquet(tablePath)

          val df = spark.read.parquet(tablePath)
            .selectExpr("id", "p", "_metadata.row_index")
          val rows = df.collect()
          assert(rows.length == 4)
          // Each partition has its own file; row_index should reset per file.
          val byPartition = rows.groupBy(_.getString(1))
          byPartition.values.foreach { partRows =>
            val indices = partRows.map(_.getLong(2)).sorted
            assert(indices.head == 0L,
              s"row_index should start at 0 per partition file, got $indices")
          }
        }
      }
    }
  }

  test("SPARK-56371: EXPLAIN shows row_index in MetadataColumns") {
    withV2Parquet {
      withTempDir { dir =>
        val tablePath = new File(dir, "explain").getAbsolutePath
        (1 to 3).toDF("id").coalesce(1).write.parquet(tablePath)
        val df = spark.read.parquet(tablePath).selectExpr("_metadata.row_index")
        val plan = df.queryExecution.explainString(
          org.apache.spark.sql.execution.ExplainMode.fromString("simple"))
        assert(plan.contains("MetadataColumns"))
        assert(plan.contains("row_index"))
        // The internal column name `_tmp_metadata_row_index` is wrapper-internal and
        // must NOT leak into the user-facing plan output.
        assert(!plan.contains("_tmp_metadata_row_index"),
          s"plan should not expose the internal row_index column name:\n$plan")
      }
    }
  }
}
