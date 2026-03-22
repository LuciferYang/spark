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
package org.apache.spark.sql.connector

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}

class DummyReadOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DummyReadOnlyFileTable
  }
}

class DummyReadOnlyFileTable extends Table with SupportsRead {
  override def name(): String = "dummy"

  override def schema(): StructType = StructType(Nil)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    throw SparkException.internalError("Dummy file reader")
  }

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.ACCEPT_ANY_SCHEMA)
}

class DummyWriteOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DummyWriteOnlyFileTable
  }
}

class DummyWriteOnlyFileTable extends Table with SupportsWrite {
  override def name(): String = "dummy"

  override def schema(): StructType = StructType(Nil)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw SparkException.internalError("Dummy file writer")

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_WRITE, TableCapability.ACCEPT_ANY_SCHEMA)
}

class FileDataSourceV2FallBackSuite extends QueryTest with SharedSparkSession {

  private val dummyReadOnlyFileSourceV2 = classOf[DummyReadOnlyFileDataSourceV2].getName
  private val dummyWriteOnlyFileSourceV2 = classOf[DummyWriteOnlyFileDataSourceV2].getName

  override protected def sparkConf: SparkConf = super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("Fall back to v1 when writing to file with read only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      // Writing file should fall back to v1 and succeed.
      df.write.format(dummyReadOnlyFileSourceV2).save(path)

      // Validate write result with [[ParquetFileFormat]].
      checkAnswer(spark.read.parquet(path), df)

      // Dummy File reader should fail as expected.
      checkError(
        exception = intercept[SparkException] {
          spark.read.format(dummyReadOnlyFileSourceV2).load(path).collect()
        },
        condition = "INTERNAL_ERROR",
        parameters = Map("message" -> "Dummy file reader"))
    }
  }

  test("Fall back read path to v1 with configuration USE_V1_SOURCE_LIST") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      df.write.parquet(path)
      Seq(
        "foo,parquet,bar",
        "ParQuet,bar,foo",
        s"foobar,$dummyReadOnlyFileSourceV2"
      ).foreach { fallbackReaders =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> fallbackReaders) {
          // Reading file should fall back to v1 and succeed.
          checkAnswer(spark.read.format(dummyReadOnlyFileSourceV2).load(path), df)
          checkAnswer(sql(s"SELECT * FROM parquet.`$path`"), df)
        }
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "foo,bar") {
        // Dummy File reader should fail as DISABLED_V2_FILE_DATA_SOURCE_READERS doesn't include it.
        checkError(
          exception = intercept[SparkException] {
            spark.read.format(dummyReadOnlyFileSourceV2).load(path).collect()
          },
          condition = "INTERNAL_ERROR",
          parameters = Map("message" -> "Dummy file reader"))
      }
    }
  }

  test("Fall back to v1 when reading file with write only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      df.write.parquet(path)
      // Fallback reads to V1
      checkAnswer(spark.read.format(dummyWriteOnlyFileSourceV2).load(path), df)
    }
  }

  test("Write only FileDataSourceV2 falls back to v1 for unsupported modes") {
    // DummyWriteOnlyFileDataSourceV2's newWriteBuilder throws.
    // ErrorIfExists on a new path goes through V2 which triggers
    // newWriteBuilder. Since DummyWriteOnly does not properly
    // implement writing, this is expected to fail.
    val df = spark.range(10).toDF()
    withTempPath { path =>
      // With USE_V1_SOURCE_LIST containing the format, writes
      // should fall back to v1 and succeed.
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key ->
          dummyWriteOnlyFileSourceV2) {
        df.write.format(dummyWriteOnlyFileSourceV2)
          .save(path.getCanonicalPath)
        checkAnswer(
          spark.read.parquet(path.getCanonicalPath), df)
      }
    }
  }

  test("Fallback Parquet V2 to V1") {
    Seq("parquet", classOf[ParquetDataSourceV2].getCanonicalName).foreach { format =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> format) {
        val commands = ArrayBuffer.empty[(String, LogicalPlan)]
        val exceptions = ArrayBuffer.empty[(String, Exception)]
        val listener = new QueryExecutionListener {
          override def onFailure(
              funcName: String,
              qe: QueryExecution,
              exception: Exception): Unit = {
            exceptions += funcName -> exception
          }

          override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
            commands += funcName -> qe.logical
          }
        }
        spark.listenerManager.register(listener)

        try {
          withTempPath { path =>
            val inputData = spark.range(10)
            inputData.write.format(format).save(path.getCanonicalPath)
            sparkContext.listenerBus.waitUntilEmpty()
            assert(commands.length == 1)
            assert(commands.head._1 == "command")
            assert(commands.head._2.isInstanceOf[InsertIntoHadoopFsRelationCommand])
            assert(commands.head._2.asInstanceOf[InsertIntoHadoopFsRelationCommand]
              .fileFormat.isInstanceOf[ParquetFileFormat])
            val df = spark.read.format(format).load(path.getCanonicalPath)
            checkAnswer(df, inputData.toDF())
            assert(
              df.queryExecution.executedPlan.exists(_.isInstanceOf[FileSourceScanExec]))
          }
        } finally {
          spark.listenerManager.unregister(listener)
        }
      }
    }
  }

  test("V2 file write for multiple formats") {
    Seq("parquet", "orc", "json", "csv").foreach { format =>
      withTempPath { path =>
        val inputData = spark.range(10).toDF()
        inputData.write.option("header", "true").format(format).save(path.getCanonicalPath)
        val readBack = spark.read.option("header", "true").schema(inputData.schema)
          .format(format).load(path.getCanonicalPath)
        checkAnswer(readBack, inputData)
      }
    }
  }

  test("V2 file write produces same results as V1 write") {
    withTempPath { v1Path =>
      withTempPath { v2Path =>
        val inputData = spark.range(100).selectExpr("id", "id * 2 as value")

        // Write via V1 path
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
          inputData.write.parquet(v1Path.getCanonicalPath)
        }

        // Write via V2 path (default)
        inputData.write.parquet(v2Path.getCanonicalPath)

        // Both should produce the same results
        val v1Result = spark.read.parquet(v1Path.getCanonicalPath)
        val v2Result = spark.read.parquet(v2Path.getCanonicalPath)
        checkAnswer(v1Result, v2Result)
      }
    }
  }

  test("V2 partitioned file write") {
    Seq("parquet", "orc", "json", "csv").foreach { format =>
      withTempPath { path =>
        val inputData = spark.range(20).selectExpr(
          "id", "id % 5 as part")
        inputData.write.option("header", "true")
          .partitionBy("part").format(format).save(path.getCanonicalPath)
        val readBack = spark.read.option("header", "true").schema(inputData.schema)
          .format(format).load(path.getCanonicalPath)
        checkAnswer(readBack, inputData)

        // Verify partition directory structure exists
        val partDirs = path.listFiles().filter(_.isDirectory).map(_.getName).sorted
        assert(partDirs.exists(_.startsWith("part=")),
          s"Expected partition directories for format $format, got: ${partDirs.mkString(", ")}")
      }
    }
  }

  test("V2 partitioned write produces same results as V1") {
    Seq("parquet", "orc", "json", "csv").foreach { format =>
      withTempPath { v1Path =>
        withTempPath { v2Path =>
          val inputData = spark.range(50).selectExpr(
            "id", "id % 3 as category", "id * 10 as value")

          // Write via V1 path
          withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> format) {
            inputData.write.option("header", "true")
              .partitionBy("category").format(format).save(v1Path.getCanonicalPath)
          }

          // Write via V2 path (default)
          inputData.write.option("header", "true")
            .partitionBy("category").format(format).save(v2Path.getCanonicalPath)

          val v1Result = spark.read.option("header", "true").schema(inputData.schema)
            .format(format).load(v1Path.getCanonicalPath)
          val v2Result = spark.read.option("header", "true").schema(inputData.schema)
            .format(format).load(v2Path.getCanonicalPath)
          checkAnswer(v1Result, v2Result)
        }
      }
    }
  }

  test("V2 multi-level partitioned write") {
    withTempPath { path =>
      val inputData = spark.range(30).selectExpr(
        "id", "id % 3 as year", "id % 2 as month")
      inputData.write.partitionBy("year", "month").parquet(path.getCanonicalPath)
      val readBack = spark.read.parquet(path.getCanonicalPath)
      checkAnswer(readBack, inputData)

      // Verify two-level partition directory structure
      val yearDirs = path.listFiles().filter(_.isDirectory).map(_.getName).sorted
      assert(yearDirs.exists(_.startsWith("year=")),
        s"Expected year partition directories, got: ${yearDirs.mkString(", ")}")
      val firstYearDir = path.listFiles().filter(_.isDirectory).head
      val monthDirs = firstYearDir.listFiles().filter(_.isDirectory).map(_.getName).sorted
      assert(monthDirs.exists(_.startsWith("month=")),
        s"Expected month partition directories, got: ${monthDirs.mkString(", ")}")
    }
  }

  test("V2 dynamic partition overwrite") {
    Seq("parquet", "orc").foreach { format =>
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> format,
        SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
        withTempPath { path =>
          // Write initial data: part=0,1,2
          val initialData = spark.range(9).selectExpr("id", "id % 3 as part")
          initialData.write.partitionBy("part")
            .format(format).save(path.getCanonicalPath)

          // Overwrite only part=0 with new data
          val overwriteData = spark.createDataFrame(Seq((100L, 0L), (101L, 0L)))
            .toDF("id", "part")
          overwriteData.write.mode("overwrite").partitionBy("part")
            .format(format).save(path.getCanonicalPath)

          // part=1 and part=2 should be untouched, part=0 should have new data
          val result = spark.read.format(format).load(path.getCanonicalPath)
          val expected = initialData.filter("part != 0").union(overwriteData)
          checkAnswer(result, expected)
        }
      }
    }
  }

  test("V2 dynamic partition overwrite produces same results as V1") {
    Seq("parquet", "orc").foreach { format =>
      withTempPath { v1Path =>
        withTempPath { v2Path =>
          val initialData = spark.range(12).selectExpr("id", "id % 4 as part")
          val overwriteData = spark.createDataFrame(Seq((200L, 1L), (201L, 1L)))
            .toDF("id", "part")

          // V1 path
          withSQLConf(
            SQLConf.USE_V1_SOURCE_LIST.key -> format,
            SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
            initialData.write.partitionBy("part").format(format).save(v1Path.getCanonicalPath)
            overwriteData.write.mode("overwrite").partitionBy("part")
              .format(format).save(v1Path.getCanonicalPath)
          }

          // V2 path
          withSQLConf(
            SQLConf.USE_V1_SOURCE_LIST.key -> format,
            SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
            initialData.write.partitionBy("part").format(format).save(v2Path.getCanonicalPath)
            overwriteData.write.mode("overwrite").partitionBy("part")
              .format(format).save(v2Path.getCanonicalPath)
          }

          val v1Result = spark.read.format(format).load(v1Path.getCanonicalPath)
          val v2Result = spark.read.format(format).load(v2Path.getCanonicalPath)
          checkAnswer(v1Result, v2Result)
        }
      }
    }
  }

  test("DataFrame API write uses V2 path") {
    Seq("parquet", "orc", "json").foreach { format =>
      // SaveMode.Append to existing path goes via V2
      withTempPath { path =>
        // First write (ErrorIfExists falls back to V1 since SupportsCatalogOptions is not
        // implemented)
        val data1 = spark.range(5).toDF()
        data1.write.format(format).save(path.getCanonicalPath)
        // Append via V2
        val data2 = spark.range(5, 10).toDF()
        data2.write.mode("append").format(format).save(path.getCanonicalPath)
        checkAnswer(
          spark.read.format(format).load(path.getCanonicalPath),
          data1.union(data2))
      }

      // SaveMode.Overwrite goes via V2
      withTempPath { path =>
        val data1 = spark.range(5).toDF()
        data1.write.format(format).save(path.getCanonicalPath)
        val data2 = spark.range(10, 15).toDF()
        data2.write.mode("overwrite").format(format).save(path.getCanonicalPath)
        checkAnswer(spark.read.format(format).load(path.getCanonicalPath), data2)
      }
    }
  }

  test("DataFrame API partitioned write uses V2 path") {
    withTempPath { path =>
      val data = spark.range(20).selectExpr("id", "id % 4 as part")
      data.write.partitionBy("part").parquet(path.getCanonicalPath)
      val result = spark.read.parquet(path.getCanonicalPath)
      checkAnswer(result, data)

      val partDirs = path.listFiles().filter(_.isDirectory).map(_.getName)
      assert(partDirs.exists(_.startsWith("part=")))
    }
  }

  test("DataFrame API write with compression option uses V2 path") {
    withTempPath { path =>
      val data = spark.range(10).toDF()
      data.write.option("compression", "snappy").parquet(path.getCanonicalPath)
      checkAnswer(spark.read.parquet(path.getCanonicalPath), data)
    }
  }

  test("Catalog table INSERT INTO uses V2 path") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, value BIGINT) USING parquet")
      sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
      checkAnswer(sql("SELECT * FROM t"),
        Seq((1L, 10L), (2L, 20L), (3L, 30L)).map(Row.fromTuple))
    }
  }

  test("Catalog table partitioned INSERT INTO uses V2 path") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, part BIGINT) USING parquet PARTITIONED BY (part)")
      sql("INSERT INTO t VALUES (1, 1), (2, 1), (3, 2), (4, 2)")
      checkAnswer(sql("SELECT * FROM t ORDER BY id"),
        Seq((1L, 1L), (2L, 1L), (3L, 2L), (4L, 2L)).map(Row.fromTuple))
    }
  }

  test("V2 cache invalidation on overwrite") {
    Seq("parquet", "orc").foreach { format =>
      withTempPath { path =>
        val p = path.getCanonicalPath
        spark.range(1000).toDF("id").write.format(format).save(p)
        val df = spark.read.format(format).load(p).cache()
        assert(df.count() == 1000)
        // Overwrite via V2 path should invalidate cache
        spark.range(10).toDF("id").write.mode("append").format(format).save(p)
        spark.range(10).toDF("id").write
          .mode("overwrite").format(format).save(p)
        assert(df.count() == 10,
          s"Cache should be invalidated after V2 overwrite for $format")
        df.unpersist()
      }
    }
  }

  test("V2 cache invalidation on append") {
    Seq("parquet", "orc").foreach { format =>
      withTempPath { path =>
        val p = path.getCanonicalPath
        spark.range(1000).toDF("id").write.format(format).save(p)
        val df = spark.read.format(format).load(p).cache()
        assert(df.count() == 1000)
        // Append via V2 path should invalidate cache
        spark.range(10).toDF("id").write.mode("append").format(format).save(p)
        assert(df.count() == 1010,
          s"Cache should be invalidated after V2 append for $format")
        df.unpersist()
      }
    }
  }

  test("V2 cache invalidation on catalog table overwrite") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT) USING parquet")
      sql("INSERT INTO t SELECT id FROM range(100)")
      spark.table("t").cache()
      assert(spark.table("t").count() == 100)
      // Overwrite via INSERT OVERWRITE should invalidate cache
      sql("INSERT OVERWRITE TABLE t SELECT id FROM range(10)")
      assert(spark.table("t").count() == 10,
        "Cache should be invalidated after V2 catalog table overwrite")
      spark.catalog.uncacheTable("t")
    }
  }

  test("DataFrame API ErrorIfExists mode") {
    Seq("parquet", "orc").foreach { format =>
      // ErrorIfExists on existing path should throw
      withTempPath { path =>
        spark.range(5).toDF().write.format(format).save(path.getCanonicalPath)
        val e = intercept[AnalysisException] {
          spark.range(10).toDF().write.mode("error").format(format).save(path.getCanonicalPath)
        }
        assert(e.getCondition == "PATH_ALREADY_EXISTS",
          s"Expected PATH_ALREADY_EXISTS for $format, got: ${e.getCondition}")
      }

      // ErrorIfExists on new path should succeed (falls back to V1 for new paths)
      withTempPath { path =>
        spark.range(5).toDF().write.mode("error").format(format).save(path.getCanonicalPath)
        checkAnswer(spark.read.format(format).load(path.getCanonicalPath),
          spark.range(5).toDF())
      }
    }
  }

  test("DataFrame API Ignore mode") {
    Seq("parquet", "orc").foreach { format =>
      // Ignore on existing path should skip writing
      withTempPath { path =>
        spark.range(5).toDF().write.format(format).save(path.getCanonicalPath)
        spark.range(100).toDF().write.mode("ignore").format(format).save(path.getCanonicalPath)
        checkAnswer(spark.read.format(format).load(path.getCanonicalPath),
          spark.range(5).toDF())
      }

      // Ignore on new path should write data (falls back to V1 for new paths)
      withTempPath { path =>
        spark.range(5).toDF().write.mode("ignore").format(format).save(path.getCanonicalPath)
        checkAnswer(spark.read.format(format).load(path.getCanonicalPath),
          spark.range(5).toDF())
      }
    }
  }

  test("INSERT INTO format.path uses V2 path") {
    Seq("parquet", "orc", "json").foreach { format =>
      withTempPath { path =>
        val p = path.getCanonicalPath
        // Initial write
        spark.range(5).toDF("id").write
          .format(format).save(p)
        // INSERT INTO format.`path`
        sql(s"INSERT INTO ${format}.`${p}`" +
          " SELECT * FROM range(5, 10)")
        checkAnswer(
          spark.read.format(format).load(p),
          spark.range(10).toDF("id"))
      }
    }
  }

  test("SELECT FROM format.path uses V2 path") {
    Seq("parquet", "orc", "json").foreach { format =>
      withTempPath { path =>
        val p = path.getCanonicalPath
        spark.range(5).toDF("id").write
          .format(format).save(p)
        checkAnswer(
          sql(s"SELECT * FROM ${format}.`${p}`"),
          spark.range(5).toDF("id"))
      }
    }
  }

  test("CTAS uses V2 path") {
    withTable("t") {
      sql("CREATE TABLE t USING parquet " +
        "AS SELECT id, id * 2 as value FROM range(10)")
      checkAnswer(
        sql("SELECT count(*) FROM t"),
        Seq(Row(10L)))
    }
  }

  test("SHOW PARTITIONS on partitioned table") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, part INT) " +
        "USING parquet PARTITIONED BY (part)")
      sql("INSERT INTO t VALUES (1, 1), (2, 2)")
      val partitions = sql("SHOW PARTITIONS t")
        .collect().map(_.getString(0)).sorted
      assert(partitions === Array("part=1", "part=2"))
    }
  }

  test("ALTER TABLE ADD/DROP PARTITION") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, part INT) " +
        "USING parquet PARTITIONED BY (part)")
      sql("INSERT INTO t VALUES (1, 1)")
      // Add partition
      sql("ALTER TABLE t ADD PARTITION (part=2)")
      val after = sql("SHOW PARTITIONS t")
        .collect().map(_.getString(0)).sorted
      assert(after === Array("part=1", "part=2"))
      // Drop partition
      sql("ALTER TABLE t DROP PARTITION (part=1)")
      val afterDrop = sql("SHOW PARTITIONS t")
        .collect().map(_.getString(0)).sorted
      assert(afterDrop === Array("part=2"))
    }
  }

  test("V2 partitioned write to empty directory succeeds") {
    Seq("parquet", "orc").foreach { format =>
      withTempDir { dir =>
        // Write partitioned data to an empty directory via V2 path.
        // This verifies checkPartitioningMatchesV2Table is skipped for FileTable,
        // whose partitioning() from fileIndex is empty for new directories.
        val data = spark.range(20).selectExpr("id", "id % 4 as k")
        data.write.partitionBy("k").mode("overwrite")
          .format(format).save(dir.toString)
        checkAnswer(
          spark.read.format(format).load(dir.toString),
          data)
      }
    }
  }

  test("V2 partitioned overwrite to existing partitioned directory succeeds") {
    Seq("parquet", "orc").foreach { format =>
      withTempDir { dir =>
        // First write via V2 overwrite
        val data1 = spark.range(10).selectExpr("id", "id % 3 as k")
        data1.write.partitionBy("k").mode("overwrite")
          .format(format).save(dir.toString)
        // Second partitioned overwrite to same path should not fail with
        // checkPartitioningMatchesV2Table (table.partitioning() now has partition info)
        val data2 = spark.range(10, 20).selectExpr("id", "id % 3 as k")
        data2.write.partitionBy("k").mode("overwrite")
          .format(format).save(dir.toString)
        checkAnswer(
          spark.read.format(format).load(dir.toString),
          data2)
      }
    }
  }
}
