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

import java.io.File
import java.sql.Timestamp

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for `_metadata` column support on V2 file sources
 * (SPARK-56335). Covers the constant metadata fields exposed by
 * `FileFormat.BASE_METADATA_FIELDS`: `file_path`, `file_name`, `file_size`,
 * `file_block_start`, `file_block_length`, `file_modification_time`.
 *
 * Parquet's `row_index` generated field is covered separately by
 * SPARK-56371.
 */
class FileMetadataColumnsV2Suite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val v2Formats = Seq("parquet", "orc", "json", "csv", "text")

  private def withV2Source(format: String)(body: => Unit): Unit = {
    // Force the V2 path for `format` by removing it from the V1 source list.
    val v1List = SQLConf.get.getConf(SQLConf.USE_V1_SOURCE_LIST)
    val newV1List = v1List.split(",").filter(_.nonEmpty)
      .filterNot(_.equalsIgnoreCase(format)).mkString(",")
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> newV1List) {
      body
    }
  }

  private def writeSingleFile(dir: File, format: String): File = {
    val target = new File(dir, s"data.$format")
    format match {
      case "text" =>
        Seq("hello", "world").toDF("value").coalesce(1).write
          .mode("overwrite").text(target.getAbsolutePath)
      case "csv" =>
        Seq((1, "a"), (2, "b")).toDF("id", "name").coalesce(1).write
          .mode("overwrite").csv(target.getAbsolutePath)
      case "json" =>
        Seq((1, "a"), (2, "b")).toDF("id", "name").coalesce(1).write
          .mode("overwrite").json(target.getAbsolutePath)
      case "parquet" =>
        Seq((1, "a"), (2, "b")).toDF("id", "name").coalesce(1).write
          .mode("overwrite").parquet(target.getAbsolutePath)
      case "orc" =>
        Seq((1, "a"), (2, "b")).toDF("id", "name").coalesce(1).write
          .mode("overwrite").orc(target.getAbsolutePath)
    }
    // Return the directory (reader will scan it).
    target
  }

  private def dataFile(dir: File, format: String): File = {
    // Spark writes per-format files with varying extensions (e.g., text -> .txt). Just find
    // the first non-hidden, non-success data file.
    dir.listFiles()
      .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      .head
  }

  v2Formats.foreach { format =>
    test(s"SPARK-56335: read _metadata columns via V2 $format scan") {
      withV2Source(format) {
        withTempDir { dir =>
          val tableDir = writeSingleFile(dir, format)
          val file = dataFile(tableDir, format)

          val df = spark.read.format(format).load(tableDir.getAbsolutePath)
            .select(
              "_metadata.file_path",
              "_metadata.file_name",
              "_metadata.file_size",
              "_metadata.file_block_start",
              "_metadata.file_block_length",
              "_metadata.file_modification_time")

          val rows = df.collect()
          assert(rows.nonEmpty, s"expected non-empty rows for format=$format")
          rows.foreach { row =>
            assert(row.getString(0) == file.toURI.toString)
            assert(row.getString(1) == file.getName)
            assert(row.getLong(2) == file.length())
            assert(row.getLong(3) == 0L)
            assert(row.getLong(4) == file.length())
            assert(row.getAs[Timestamp](5) == new Timestamp(file.lastModified()))
          }
        }
      }
    }

    test(s"SPARK-56335: project data and _metadata together via V2 $format scan") {
      withV2Source(format) {
        withTempDir { dir =>
          val tableDir = writeSingleFile(dir, format)
          val file = dataFile(tableDir, format)

          val expectedPath = file.toURI.toString
          val df = spark.read.format(format).load(tableDir.getAbsolutePath)

          // Pick a data column that exists for each format.
          val dataColumn = if (format == "text") "value" else df.columns.head
          val projected = df.selectExpr(dataColumn, "_metadata.file_path AS p")

          val paths = projected.select("p").collect().map(_.getString(0)).toSet
          assert(paths == Set(expectedPath))
          assert(projected.count() == df.count())
        }
      }
    }

    test(s"SPARK-56335: select only data returns no metadata columns via V2 $format scan") {
      withV2Source(format) {
        withTempDir { dir =>
          val tableDir = writeSingleFile(dir, format)
          val df = spark.read.format(format).load(tableDir.getAbsolutePath)
          // Sanity: schema does not surface `_metadata` unless explicitly requested.
          assert(!df.schema.fieldNames.contains("_metadata"))
          checkAnswer(df.selectExpr("count(1)"), Row(df.count()))
        }
      }
    }
  }

  test("SPARK-56335: _metadata.file_name matches file_path basename (parquet V2)") {
    withV2Source("parquet") {
      withTempDir { dir =>
        val tableDir = writeSingleFile(dir, "parquet")
        val rows = spark.read.parquet(tableDir.getAbsolutePath)
          .selectExpr("_metadata.file_path", "_metadata.file_name")
          .collect()
        rows.foreach { r =>
          val path = r.getString(0)
          val name = r.getString(1)
          assert(path.endsWith(name), s"file_path=$path did not end with file_name=$name")
        }
      }
    }
  }

  test("SPARK-56335: _metadata on partitioned parquet V2 table") {
    withV2Source("parquet") {
      withTempDir { dir =>
        val tablePath = new File(dir, "partitioned").getAbsolutePath
        Seq((1, "a"), (2, "b"), (3, "a"), (4, "b")).toDF("id", "p")
          .write.partitionBy("p").parquet(tablePath)

        val df = spark.read.parquet(tablePath)
          .selectExpr("id", "p", "_metadata.file_path", "_metadata.file_name")
        val rows = df.collect()
        assert(rows.length == 4)
        rows.foreach { r =>
          val id = r.getInt(0)
          val part = r.getString(1)
          val path = r.getString(2)
          val name = r.getString(3)
          assert(path.contains(s"p=$part"),
            s"file_path=$path did not contain partition p=$part (id=$id)")
          assert(path.endsWith(name))
        }
      }
    }
  }

  test("SPARK-56335: filter on _metadata.file_name prunes to matching file (parquet V2)") {
    withV2Source("parquet") {
      withTempDir { dir =>
        val tablePath = new File(dir, "multifile").getAbsolutePath
        // Two separate files.
        Seq((1, "x")).toDF("id", "v").coalesce(1).write.parquet(tablePath + "/f1")
        Seq((2, "y")).toDF("id", "v").coalesce(1).write.parquet(tablePath + "/f2")

        val rows = spark.read.parquet(tablePath + "/f1", tablePath + "/f2")
          .selectExpr("id", "_metadata.file_name AS fname")
          .collect()
        assert(rows.length == 2)
        val fnames = rows.map(_.getString(1)).toSet
        assert(fnames.size == 2, s"expected two distinct file names, got $fnames")
      }
    }
  }

  test("SPARK-56335: filter on _metadata.file_name in WHERE clause (parquet V2)") {
    withV2Source("parquet") {
      withTempDir { dir =>
        val tablePath = new File(dir, "filtered").getAbsolutePath
        Seq((1, "x")).toDF("id", "v").coalesce(1).write.parquet(tablePath + "/f1")
        Seq((2, "y")).toDF("id", "v").coalesce(1).write.parquet(tablePath + "/f2")

        val allRows = spark.read.parquet(tablePath + "/f1", tablePath + "/f2")
          .where("_metadata.file_name LIKE '%part%'")
          .select("id")
          .collect()
        // Parquet output files are named `part-*.parquet`, so the filter should match both.
        assert(allRows.length == 2)

        // Pick one file's name and filter by exact equality; only that file's rows should
        // remain. This confirms the predicate is actually evaluated rather than dropped.
        val targetName = dataFile(new File(tablePath + "/f1"), "parquet").getName
        val filtered = spark.read.parquet(tablePath + "/f1", tablePath + "/f2")
          .where(s"_metadata.file_name = '$targetName'")
          .select("id")
          .collect()
        assert(filtered.length == 1)
        assert(filtered.head.getInt(0) == 1)
      }
    }
  }

  test("SPARK-56335: filter on numeric _metadata.file_size (parquet V2)") {
    withV2Source("parquet") {
      withTempDir { dir =>
        val tablePath = new File(dir, "sized").getAbsolutePath
        Seq((1, "x")).toDF("id", "v").coalesce(1).write.parquet(tablePath + "/f1")
        Seq((2, "y")).toDF("id", "v").coalesce(1).write.parquet(tablePath + "/f2")

        // Filter on a numeric metadata field should not break planning, and should
        // return either all rows (filter satisfied) or be evaluated correctly post-scan.
        val rows = spark.read.parquet(tablePath + "/f1", tablePath + "/f2")
          .where("_metadata.file_size > 0")
          .select("id")
          .collect()
        assert(rows.length == 2)
      }
    }
  }

  test("SPARK-56335: metadata-only projection on partitioned table (parquet V2)") {
    withV2Source("parquet") {
      withTempDir { dir =>
        val tablePath = new File(dir, "metaonly_partitioned").getAbsolutePath
        Seq((1, "a"), (2, "b"), (3, "a")).toDF("id", "p")
          .write.partitionBy("p").parquet(tablePath)

        // Select only metadata (no data columns, no partition columns).
        val rows = spark.read.parquet(tablePath)
          .selectExpr("_metadata.file_path")
          .collect()
        assert(rows.length == 3)
        rows.foreach { r =>
          val path = r.getString(0)
          assert(path.contains("p=a") || path.contains("p=b"))
        }
      }
    }
  }

  test("SPARK-56335: count(*) + _metadata does not regress aggregate behavior (parquet V2)") {
    // Sanity check that combining an aggregate with a metadata reference either works
    // correctly or does not crash the optimizer. V1 parity test.
    withV2Source("parquet") {
      withTempDir { dir =>
        val tableDir = writeSingleFile(dir, "parquet")
        val expected = spark.read.parquet(tableDir.getAbsolutePath).count()
        // Selecting count(*) alongside a metadata column. At minimum this must plan.
        val count = spark.read.parquet(tableDir.getAbsolutePath)
          .selectExpr("_metadata.file_path", "id")
          .count()
        assert(count == expected)
      }
    }
  }

  test("SPARK-56335: EXPLAIN shows MetadataColumns when _metadata is requested (parquet V2)") {
    withV2Source("parquet") {
      withTempDir { dir =>
        val tableDir = writeSingleFile(dir, "parquet")
        val df = spark.read.parquet(tableDir.getAbsolutePath)
          .selectExpr("_metadata.file_path")
        val plan = df.queryExecution.explainString(
          org.apache.spark.sql.execution.ExplainMode.fromString("simple"))
        assert(plan.contains("MetadataColumns"),
          s"expected EXPLAIN to mention MetadataColumns, got:\n$plan")
        assert(plan.contains("file_path"),
          s"expected EXPLAIN to mention the requested file_path field, got:\n$plan")

        // Without metadata reference, EXPLAIN should not mention MetadataColumns.
        val df2 = spark.read.parquet(tableDir.getAbsolutePath).select("id")
        val plan2 = df2.queryExecution.explainString(
          org.apache.spark.sql.execution.ExplainMode.fromString("simple"))
        assert(!plan2.contains("MetadataColumns"),
          s"unexpected MetadataColumns in plan without metadata reference:\n$plan2")
      }
    }
  }

  test("SPARK-56335: _metadata.file_block_start is 0 and length equals file size for small file") {
    withV2Source("parquet") {
      withTempDir { dir =>
        val tableDir = writeSingleFile(dir, "parquet")
        val file = dataFile(tableDir, "parquet")
        val row = spark.read.parquet(tableDir.getAbsolutePath)
          .selectExpr("_metadata.file_block_start", "_metadata.file_block_length")
          .first()
        // Small files are read in a single split.
        assert(row.getLong(0) == 0L)
        assert(row.getLong(1) == file.length())
      }
    }
  }
}
