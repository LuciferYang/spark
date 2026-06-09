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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.CTERelationDef
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier, SupportsRead}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{LocalScan, Scan, ScanBuilder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * `TagPruningVetoCTE` reaches into the connector: `PruningEligibility.shadowOptimize`
 * runs `V2ScanRelationPushDown`, which calls `newScanBuilder(...).build()` on every
 * `DataSourceV2Relation` in a CTE body. That is third-party code which loads
 * statistics and opens metadata -- `shadowOptimize`'s own scaladoc names Lance
 * zonemap loading as the motivating case -- and it can throw.
 *
 * Two facts make an escaping exception worse than it first looks. The rule is in
 * `SparkOptimizer.nonExcludableRules`, so a user cannot switch it off with
 * `spark.sql.optimizer.excludedRules`; and the veto is a heuristic that only ever
 * decides between caching and inlining, so its failure has no bearing on whether
 * the query can be answered.
 *
 * What is and is not in scope. A connector whose `build()` throws unconditionally
 * fails the query with or without the veto -- the real
 * `Batch("Early Filter and Projection Push-Down")` calls `build()` too, so there is
 * nothing to protect. The exposure the veto adds is a SECOND, EARLIER build, on a
 * plan shape the connector never sees in production (pre-pushdown, different
 * predicates, no column pruning). What a `NonFatal` guard buys is therefore
 * confined to connectors that fail on that probe and not on the real path:
 * timeouts, flaky metadata loads, and code paths only the shadow shape reaches.
 * `failFirstBuildOnly` models exactly that.
 *
 * The three tests must be read together. `probeIsReached` is the vacuity guard: if
 * a future change stops shadow-optimizing V2 relations, the other two stop proving
 * anything. Remove the `NonFatal` catch in `shouldVeto` and both of the others fail
 * with the connector's own exception.
 */
class TagPruningVetoConnectorFailureSuite extends QueryTest with SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.catalog.vetocat", classOf[ThrowingScanCatalog].getName)
  }

  override def afterAll(): Unit = {
    try {
      spark.conf.unset("spark.sql.catalog.vetocat")
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterAll()
    }
  }

  override protected def afterEach(): Unit = {
    try {
      ThrowingScanBuilder.reset()
    } finally {
      super.afterEach()
    }
  }

  /**
   * Two references so the body is a caching candidate at all, and a join in the
   * body so it passes the structural gate. `minSizeBytes = 0` keeps the size gate
   * out of the way.
   */
  private val cteSql =
    """WITH t AS (
      |  SELECT a.i AS i FROM vetocat.tbl a JOIN vetocat.tbl b ON a.i = b.i
      |)
      |SELECT x.i FROM t x JOIN t y ON x.i = y.i""".stripMargin

  private def vetoConf: Seq[(String, String)] = Seq(
    SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
    SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "true",
    SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0")

  test("the veto check really does invoke the connector's scan builder") {
    // Guard against the next test passing vacuously. If a future change stops
    // shadow-optimizing V2 relations for this shape, this fails and the
    // fail-closed test below stops proving anything -- both must be read together.
    withSQLConf(vetoConf: _*) {
      ThrowingScanBuilder.shouldThrow = false
      TagPruningVetoCTE.apply(spark.sql(cteSql).queryExecution.analyzed)
      assert(ThrowingScanBuilder.buildCount > 0,
        "expected TagPruningVetoCTE to build a V2 scan for this CTE body; if it no " +
        "longer does, the fail-closed test below is vacuous")
    }
  }

  test("a throwing scan builder does not fail the query; the CTE is not vetoed") {
    withSQLConf(vetoConf: _*) {
      ThrowingScanBuilder.shouldThrow = true
      val analyzed = spark.sql(cteSql).queryExecution.analyzed
      // Without the NonFatal catch in `shouldVeto` this throws
      // ThrowingScanBuilder.Failure straight out of the rule.
      val tagged = TagPruningVetoCTE.apply(analyzed)
      assert(ThrowingScanBuilder.buildCount > 0,
        "the connector was never asked to build a scan, so nothing was exercised")
      val vetoed = tagged.collectWithSubqueries {
        case d: CTERelationDef if d.pruningVeto => d
      }
      assert(vetoed.isEmpty,
        "a failed veto check must fall back to NOT vetoing, leaving the CTE a " +
        s"caching candidate, but ${vetoed.size} def(s) were tagged")
    }
  }

  test("a connector that fails only on the veto's probe still runs the query") {
    // End to end, and the only end-to-end case the guard can actually rescue: the
    // first `build()` is the veto's shadow probe, every later one is the real
    // pushdown batch. A connector that throws on EVERY build fails regardless of
    // the veto, so asserting on that would test Spark's own error propagation
    // rather than this fix.
    withSQLConf(vetoConf: _*) {
      ThrowingScanBuilder.failFirstBuildOnly = true
      checkAnswer(spark.sql(cteSql), ThrowingScanTable.data.map(Row(_)))
      assert(ThrowingScanBuilder.buildCount > 1,
        "expected the real pushdown batch to build a scan after the probe failed; " +
        s"only ${ThrowingScanBuilder.buildCount} build(s) happened, so this did not " +
        "exercise the veto-probe-versus-real-path distinction")
    }
  }
}

object ThrowingScanBuilder {
  /** Thrown from `build()`; a checked-style failure a connector could plausibly raise. */
  class Failure extends RuntimeException("connector refused to build a scan")

  /** Throw on every `build()`. Kills the query with or without the veto. */
  @volatile var shouldThrow: Boolean = false

  /**
   * Throw on the FIRST `build()` only -- i.e. on the veto's shadow probe, since that
   * runs in `Batch("Tag Pruning Veto CTE")`, ahead of
   * `Batch("Early Filter and Projection Push-Down")`. Models a connector that is
   * only upset by the probe (a timeout, a metadata load that the shadow shape
   * triggers and the real one does not).
   */
  @volatile var failFirstBuildOnly: Boolean = false

  @volatile var buildCount: Int = 0

  def reset(): Unit = {
    shouldThrow = false
    failFirstBuildOnly = false
    buildCount = 0
  }
}

class ThrowingScanCatalog extends BasicInMemoryTableCatalog {
  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val table = new ThrowingScanTable(ident.toString)
    tables.put(ident, table)
    table
  }

  override def loadTable(ident: Identifier): Table = new ThrowingScanTable(ident.toString)
}

class ThrowingScanTable(override val name: String) extends Table with SupportsRead {
  override def schema(): StructType = ThrowingScanTable.schema

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ScanBuilder {
      override def build(): Scan = {
        ThrowingScanBuilder.buildCount += 1
        if (ThrowingScanBuilder.shouldThrow ||
            (ThrowingScanBuilder.failFirstBuildOnly && ThrowingScanBuilder.buildCount == 1)) {
          throw new ThrowingScanBuilder.Failure
        }
        // `LocalScan` rather than a bare `Scan`: the end-to-end test has to reach
        // execution, and `Scan.toBatch` throws `UnsupportedOperationException` by
        // default, which would fail that test for a reason unrelated to the veto.
        new LocalScan {
          override def rows(): Array[InternalRow] =
            ThrowingScanTable.data.map(InternalRow(_)).toArray
          override def readSchema(): StructType = ThrowingScanTable.schema
        }
      }
    }
}

object ThrowingScanTable {
  val schema: StructType = new StructType().add("i", "int")
  val data: Seq[Int] = Seq(1, 2, 3)
}
