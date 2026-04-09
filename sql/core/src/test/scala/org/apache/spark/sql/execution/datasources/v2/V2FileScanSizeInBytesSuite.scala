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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for SPARK-56337: sizeInBytes propagation from `CatalogTable.stats` into
 * V2 `FileScan` estimates.
 *
 * SPARK-56176 introduced V2-native ANALYZE TABLE stats propagation by injecting
 * `CatalogTable.stats.rowCount` into `FileTable.mergedOptions` via `NUM_ROWS_KEY`.
 * SPARK-56337 extends that mechanism with `SIZE_IN_BYTES_KEY` so the analyzed
 * `sizeInBytes` is also visible to `FileScan.estimateStatistics()`.
 *
 * For partitioned catalog tables that use `useCatalogFileIndex`, `sizeInBytes`
 * already flows through `CatalogFileIndex.sizeInBytes` directly (set by
 * `V2SessionCatalog.loadTable` -> `FileTable`). For non-partitioned tables that
 * use `InMemoryFileIndex`, the file-listing-based estimate would otherwise win;
 * this test guards the side-channel that closes the gap.
 */
class V2FileScanSizeInBytesSuite extends QueryTest with SharedSparkSession {

  /**
   * V2 routing for catalog file source tables activates only when the provider is
   * not in `USE_V1_SOURCE_LIST`. Tests opt into V2 by emptying the list for the
   * duration of the body so the catalog reads produce `DataSourceV2Relation` and
   * the V2 `FileScan` is the path that consumes the propagated stats.
   */
  private def withV2FileSources(body: => Unit): Unit = {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      body
    }
  }

  /**
   * Populates `CatalogTable.stats` directly via `SessionCatalog.alterTableStats`. This
   * sidesteps the V2 `ANALYZE TABLE` path (which writes stats as table properties via
   * `TableChange.setProperty` and relies on `HiveExternalCatalog.statsFromProperties`
   * to convert them back into `CatalogStatistics` on read). The conversion is not
   * present in `InMemoryCatalog`, which is what `SharedSparkSession` uses, so direct
   * population is the cleanest way to test the propagation pipeline:
   *   `CatalogTable.stats.sizeInBytes` -> `FileTable.mergedOptions` ->
   *   `FileTable.SIZE_IN_BYTES_KEY` -> `FileScan.storedSizeInBytes` ->
   *   `FileScan.estimateStatistics().sizeInBytes()`.
   */
  private def setAnalyzedSize(table: String, sizeInBytes: BigInt): Unit = {
    spark.sessionState.catalog.alterTableStats(
      TableIdentifier(table),
      Some(CatalogStatistics(sizeInBytes = sizeInBytes)))
  }

  test("Analyzed sizeInBytes flows into V2 scan estimate (non-partitioned)") {
    withV2FileSources {
      withTable("t_v2_size_nonpart") {
        spark.sql("CREATE TABLE t_v2_size_nonpart (id INT, v STRING) USING parquet")
        spark.sql("INSERT INTO t_v2_size_nonpart VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        // Use a deliberately distinctive size that cannot match the file-listing-based
        // estimate for a 3-row table, so the assertion is unambiguous.
        val analyzedSize = BigInt(123456789L)
        setAnalyzedSize("t_v2_size_nonpart", analyzedSize)

        // The V2 scan's optimized-plan stats should reflect the analyzed sizeInBytes
        // rather than the file-listing-based estimate. Without the SIZE_IN_BYTES_KEY
        // propagation, this assertion would fail because `InMemoryFileIndex` returns
        // the listing-based size for non-partitioned tables.
        val df = spark.sql("SELECT * FROM t_v2_size_nonpart")
        val planStats = df.queryExecution.optimizedPlan.stats
        assert(planStats.sizeInBytes == analyzedSize,
          s"expected V2 scan size to match analyzed stats $analyzedSize, " +
            s"got ${planStats.sizeInBytes}")
      }
    }
  }

  test("Analyzed sizeInBytes flows into V2 scan estimate (partitioned)") {
    // Partitioned tables go through `CatalogFileIndex.sizeInBytes` already, but the
    // SIZE_IN_BYTES_KEY propagation should be a no-op rather than a regression here.
    // This test guards that the partitioned path keeps producing analyzed stats end
    // to end.
    withV2FileSources {
      withTable("t_v2_size_part") {
        spark.sql(
          """
            |CREATE TABLE t_v2_size_part (id INT, v STRING, p STRING)
            |USING parquet
            |PARTITIONED BY (p)
          """.stripMargin)
        spark.sql(
          "INSERT INTO t_v2_size_part VALUES (1, 'a', 'x'), (2, 'b', 'x'), (3, 'c', 'y')")
        val analyzedSize = BigInt(987654321L)
        setAnalyzedSize("t_v2_size_part", analyzedSize)

        val df = spark.sql("SELECT * FROM t_v2_size_part")
        val planStats = df.queryExecution.optimizedPlan.stats
        assert(planStats.sizeInBytes == analyzedSize,
          s"expected V2 scan size to match analyzed stats $analyzedSize, " +
            s"got ${planStats.sizeInBytes}")
      }
    }
  }

  test("Without ANALYZE TABLE, sizeInBytes falls back to file-listing estimate") {
    // When `CatalogTable.stats` is empty, no SIZE_IN_BYTES_KEY is injected and
    // `FileScan.estimateStatistics` returns the file-listing-based estimate. The
    // estimate should be a positive number (the test files exist on disk) rather
    // than zero or `defaultSizeInBytes`.
    withV2FileSources {
      withTable("t_v2_size_unanalyzed") {
        spark.sql("CREATE TABLE t_v2_size_unanalyzed (id INT) USING parquet")
        spark.sql("INSERT INTO t_v2_size_unanalyzed VALUES (1), (2), (3)")

        val meta = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier("t_v2_size_unanalyzed"))
        assert(meta.stats.isEmpty,
          "precondition: stats should be empty before ANALYZE TABLE")

        val df = spark.sql("SELECT * FROM t_v2_size_unanalyzed")
        val planStats = df.queryExecution.optimizedPlan.stats
        assert(planStats.sizeInBytes > 0,
          s"expected positive listing-based size estimate, got ${planStats.sizeInBytes}")
      }
    }
  }
}
