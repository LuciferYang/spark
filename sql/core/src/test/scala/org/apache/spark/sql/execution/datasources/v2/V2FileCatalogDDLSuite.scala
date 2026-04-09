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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for SPARK-56336: session-catalog file source `CREATE TABLE`,
 * `CREATE TABLE AS SELECT`, `REPLACE TABLE`, `REPLACE TABLE AS SELECT`, and
 * `TRUNCATE TABLE` operations route through V2 exec nodes instead of the
 * unconditional V1 fallbacks that SPARK-56175 added as transitional scaffolding.
 *
 * Covers:
 *   - DDL routing: CREATE TABLE / CTAS produce V2 `CreateTableExec` /
 *     `CreateTableAsSelectExec` under `USE_V1_SOURCE_LIST = ""`.
 *   - REPLACE TABLE / REPLACE TABLE AS SELECT no longer throws for file sources;
 *     non-atomic V2 `ReplaceTableExec` / `ReplaceTableAsSelectExec` handles them.
 *   - TRUNCATE TABLE via V2 `TruncateTableExec` (FileTable now implements
 *     `TruncatableTable`).
 *   - R2.F1 #3: non-partitioned file source catalog tables created via V2 have
 *     `tracksPartitionsInCatalog = false` (matches V1).
 *   - Default `USE_V1_SOURCE_LIST` remains a no-op: SPARK-56336 only changes behavior
 *     under opt-in V2 routing.
 *
 * Note: `LOAD DATA` is intentionally not tested here. V1 `LoadDataCommand.run()`
 * unconditionally rejects datasource tables via `loadDataNotSupportedForDatasourceTablesError`,
 * so LOAD DATA is effectively Hive-only regardless of V1 vs V2 routing. SPARK-56336
 * does not attempt to add V2 support for it.
 */
class V2FileCatalogDDLSuite extends QueryTest with SharedSparkSession {

  /**
   * SPARK-56336 activates when the provider is NOT in `USE_V1_SOURCE_LIST`. Tests opt
   * into V2 by emptying the list for the duration of the body. This mirrors the
   * pattern used by `V2FileCatalogReadSuite` in SPARK-56337 and other V2-routing
   * tests.
   */
  private def withV2FileSources(body: => Unit): Unit = {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      body
    }
  }

  private def analyzedPlanOf(sql: String): LogicalPlan =
    spark.sql(sql).queryExecution.analyzed

  private def containsV1CreateCommand(plan: LogicalPlan): Boolean = plan.exists {
    case _: CreateDataSourceTableCommand => true
    case _: CreateDataSourceTableAsSelectCommand => true
    case _ => false
  }

  // --------------------------------------------------------------------------
  // CREATE TABLE / CREATE TABLE AS SELECT routing
  // --------------------------------------------------------------------------

  test("SPARK-56336: CREATE TABLE USING parquet no longer lowers to V1 command") {
    withV2FileSources {
      withTable("t_v2_create_parquet") {
        val plan = analyzedPlanOf(
          "CREATE TABLE t_v2_create_parquet (id INT, v STRING) USING parquet")
        assert(!containsV1CreateCommand(plan),
          s"expected no V1 CreateDataSourceTableCommand under V2 routing, got:\n$plan")

        // The table should be created and usable.
        spark.sql("INSERT INTO t_v2_create_parquet VALUES (1, 'a'), (2, 'b')")
        checkAnswer(
          spark.sql("SELECT * FROM t_v2_create_parquet ORDER BY id"),
          Seq(Row(1, "a"), Row(2, "b")))
      }
    }
  }

  test("SPARK-56336: CREATE TABLE AS SELECT routes through V2 non-atomic path") {
    withV2FileSources {
      withTable("t_v2_source", "t_v2_ctas") {
        spark.sql("CREATE TABLE t_v2_source (id INT, v STRING) USING parquet")
        spark.sql("INSERT INTO t_v2_source VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        // `spark.sql` on a DDL command eagerly executes it, so check the plan shape
        // via `df.queryExecution.analyzed` *while* the CTAS runs, and verify the
        // resulting table afterwards.
        val df = spark.sql(
          "CREATE TABLE t_v2_ctas USING parquet AS SELECT * FROM t_v2_source")
        assert(!containsV1CreateCommand(df.queryExecution.analyzed),
          s"expected no V1 CTAS command under V2 routing, got:\n" +
            df.queryExecution.analyzed)

        checkAnswer(
          spark.sql("SELECT * FROM t_v2_ctas ORDER BY id"),
          Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
      }
    }
  }

  // --------------------------------------------------------------------------
  // REPLACE TABLE / REPLACE TABLE AS SELECT
  // --------------------------------------------------------------------------

  test("SPARK-56336: REPLACE TABLE USING parquet no longer throws") {
    withV2FileSources {
      withTable("t_v2_replace") {
        spark.sql("CREATE TABLE t_v2_replace (id INT) USING parquet")
        spark.sql("INSERT INTO t_v2_replace VALUES (1)")

        // Before SPARK-56336, this threw `unsupportedTableOperationError("REPLACE TABLE")`.
        // Now it should succeed by routing through V2 `ReplaceTableExec` (non-atomic:
        // drop + create).
        spark.sql("REPLACE TABLE t_v2_replace (id BIGINT, name STRING) USING parquet")

        // The new schema should be visible and the old data gone.
        val df = spark.sql("SELECT * FROM t_v2_replace")
        assert(df.schema.fieldNames.sameElements(Array("id", "name")),
          s"expected replaced schema (id, name), got ${df.schema.fieldNames.mkString(",")}")
        assert(df.collect().isEmpty,
          "expected replaced table to be empty (data from old schema dropped)")
      }
    }
  }

  test("SPARK-56336: REPLACE TABLE AS SELECT USING parquet no longer throws") {
    withV2FileSources {
      withTable("t_v2_source_for_rtas", "t_v2_rtas") {
        spark.sql("CREATE TABLE t_v2_source_for_rtas (id INT, v STRING) USING parquet")
        spark.sql("INSERT INTO t_v2_source_for_rtas VALUES (10, 'x'), (20, 'y')")

        // Seed target with the old schema to prove REPLACE actually replaces.
        spark.sql("CREATE TABLE t_v2_rtas (old_col BIGINT) USING parquet")
        spark.sql("INSERT INTO t_v2_rtas VALUES (999)")

        spark.sql(
          "REPLACE TABLE t_v2_rtas USING parquet AS SELECT * FROM t_v2_source_for_rtas")
        checkAnswer(
          spark.sql("SELECT * FROM t_v2_rtas ORDER BY id"),
          Seq(Row(10, "x"), Row(20, "y")))
      }
    }
  }

  // --------------------------------------------------------------------------
  // TRUNCATE TABLE (R2.F3)
  // --------------------------------------------------------------------------

  test("SPARK-56336: TRUNCATE TABLE on MANAGED V2 file source table empties the table") {
    withV2FileSources {
      withTable("t_v2_truncate_managed") {
        spark.sql("CREATE TABLE t_v2_truncate_managed (id INT) USING parquet")
        spark.sql("INSERT INTO t_v2_truncate_managed VALUES (1), (2), (3)")
        checkAnswer(
          spark.sql("SELECT count(*) FROM t_v2_truncate_managed"),
          Row(3))

        spark.sql("TRUNCATE TABLE t_v2_truncate_managed")
        checkAnswer(
          spark.sql("SELECT count(*) FROM t_v2_truncate_managed"),
          Row(0))

        // The table should still exist and be insertable after truncate.
        spark.sql("INSERT INTO t_v2_truncate_managed VALUES (42)")
        checkAnswer(
          spark.sql("SELECT * FROM t_v2_truncate_managed"),
          Row(42))
      }
    }
  }

  test("SPARK-56336: TRUNCATE TABLE on EXTERNAL V2 file source table throws") {
    withV2FileSources {
      withTempDir { dir =>
        withTable("t_v2_truncate_external") {
          spark.sql(
            s"CREATE TABLE t_v2_truncate_external (id INT) USING parquet " +
              s"LOCATION '${dir.toURI}'")
          spark.sql("INSERT INTO t_v2_truncate_external VALUES (1)")

          // Match V1 `TruncateTableCommand` behavior: truncate on external tables
          // is rejected.
          intercept[AnalysisException] {
            spark.sql("TRUNCATE TABLE t_v2_truncate_external")
          }
        }
      }
    }
  }

  test("SPARK-56336: TRUNCATE TABLE on partitioned V2 file source table empties all partitions") {
    withV2FileSources {
      withTable("t_v2_truncate_part") {
        spark.sql(
          """
            |CREATE TABLE t_v2_truncate_part (id INT, p STRING)
            |USING parquet
            |PARTITIONED BY (p)
          """.stripMargin)
        spark.sql(
          "INSERT INTO t_v2_truncate_part VALUES (1, 'a'), (2, 'a'), (3, 'b')")
        checkAnswer(
          spark.sql("SELECT count(*) FROM t_v2_truncate_part"),
          Row(3))

        spark.sql("TRUNCATE TABLE t_v2_truncate_part")
        checkAnswer(
          spark.sql("SELECT count(*) FROM t_v2_truncate_part"),
          Row(0))
      }
    }
  }

  // --------------------------------------------------------------------------
  // R2.F1 #3: tracksPartitionsInCatalog conditional logic
  // --------------------------------------------------------------------------

  test("SPARK-56336: non-partitioned V2 file source table has tracksPartitionsInCatalog=false") {
    withV2FileSources {
      withTable("t_v2_nonpart_tracks") {
        spark.sql("CREATE TABLE t_v2_nonpart_tracks (id INT) USING parquet")
        val meta = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier("t_v2_nonpart_tracks"))
        assert(!meta.tracksPartitionsInCatalog,
          "non-partitioned table should not track partitions in the catalog")
      }
    }
  }

  test("SPARK-56336: partitioned V2 file source table has tracksPartitionsInCatalog=true") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "",
      SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
      withTable("t_v2_part_tracks") {
        spark.sql(
          """
            |CREATE TABLE t_v2_part_tracks (id INT, p STRING)
            |USING parquet
            |PARTITIONED BY (p)
          """.stripMargin)
        val meta = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier("t_v2_part_tracks"))
        assert(meta.tracksPartitionsInCatalog,
          "partitioned table should track partitions in the catalog " +
            "(HIVE_MANAGE_FILESOURCE_PARTITIONS is enabled)")
      }
    }
  }

  // --------------------------------------------------------------------------
  // Default-config safety: SPARK-56336 is a no-op under default USE_V1_SOURCE_LIST
  // --------------------------------------------------------------------------

  test("SPARK-56336: under default USE_V1_SOURCE_LIST, CREATE TABLE still lowers to V1") {
    withTable("t_v1_default_create") {
      val plan = analyzedPlanOf(
        "CREATE TABLE t_v1_default_create (id INT) USING parquet")
      assert(containsV1CreateCommand(plan),
        s"expected V1 command under default USE_V1_SOURCE_LIST, got:\n$plan")
    }
  }
}
