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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for [[TagPruningVetoCTE]] focused on:
 *   - Master switch gating.
 *   - Eligibility gates (deterministic, !correlatedSubqueryRef).
 *   - Idempotency.
 *   - End-to-end behavior on a Q4-shape query.
 *
 * Deeper detection logic (`PruningEligibility.shadowOptimize`,
 * `hasInBodyDPPOpportunity`, `looksLikeMaterializationNotWorthIt`) is validated empirically by
 * `DppAutoCteSmokeTest` against partitioned TPC-DS SF=10 data.
 */
class TagPruningVetoCTESuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: org.apache.spark.SparkConf =
    super.sparkConf
      .set(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key, "0")
      .set(SQLConf.CBO_ENABLED.key, "true")
      .set(SQLConf.PLAN_STATS_ENABLED.key, "true")

  override protected def afterEach(): Unit = {
    try {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterEach()
    }
  }

  private def withVetoCount[T](body: => T): (T, Long) = {
    val before = TagPruningVetoCTE.vetoCount.sum()
    val result = body
    val after = TagPruningVetoCTE.vetoCount.sum()
    (result, after - before)
  }

  test("master switch off: no veto fires") {
    withSQLConf(
        SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "false") {
      val (_, delta) = withVetoCount {
        spark.sql(
          """WITH t AS (SELECT id, id % 10 AS k FROM range(100))
            |SELECT a.id FROM t a JOIN t b ON a.id = b.id""".stripMargin)
          .queryExecution.optimizedPlan
      }
      assert(delta == 0, s"expected no veto when master switch is off, got delta=$delta")
    }
  }

  test("non-deterministic CTE: no veto fires") {
    withSQLConf(
        SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "true") {
      val (_, delta) = withVetoCount {
        spark.sql(
          """WITH t AS (SELECT id, rand() AS r FROM range(100))
            |SELECT a.id FROM t a JOIN t b ON a.id = b.id""".stripMargin)
          .queryExecution.optimizedPlan
      }
      assert(delta == 0, s"expected no veto on non-deterministic CTE, got delta=$delta")
    }
  }

  test("CTE with no partitioned fact scan: no veto fires") {
    // Plain range() CTE -- no HadoopFsRelation with partitionSchema, no V2 scan
    // with SupportsRuntimeV2Filtering. looksLikeMaterializationNotWorthIt should return false.
    withSQLConf(
        SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "true") {
      val (_, delta) = withVetoCount {
        spark.sql(
          """WITH t AS (
            |  SELECT id, sum(id) AS s
            |  FROM range(1000)
            |  GROUP BY id
            |)
            |SELECT a.id FROM t a JOIN t b ON a.id = b.id""".stripMargin)
          .queryExecution.optimizedPlan
      }
      assert(delta == 0,
        s"expected no veto when CTE has no partitioned fact scan, got delta=$delta")
    }
  }

  test("deterministic verdict across repeated runs of the same query") {
    // True idempotency on an already-tagged plan would require constructing
    // a tagged CTERelationDef directly, which crosses module boundaries
    // (case-class arity, internal fields). Instead we verify the verdict is
    // stable: running the same SQL twice produces the same veto-count delta.
    withSQLConf(
        SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "true") {
      val sql =
        """WITH t AS (SELECT id FROM range(100))
          |SELECT a.id FROM t a JOIN t b ON a.id = b.id""".stripMargin
      val (_, delta1) = withVetoCount {
        spark.sql(sql).queryExecution.optimizedPlan
      }
      val (_, delta2) = withVetoCount {
        spark.sql(sql).queryExecution.optimizedPlan
      }
      // Both queries either both veto or both not veto; counts should match.
      assert(delta1 == delta2,
        s"expected idempotent veto count across two runs of same query, " +
        s"got delta1=$delta1, delta2=$delta2")
    }
  }

  /**
   * Build a small partitioned managed table backed by parquet so
   * `PartitionPruning.getFilterableTableScan` recognises it as filterable.
   * The partition column IS the join key (`dim_fk`) -- mirrors the TPC-DS
   * Q4 shape where `store_sales.ss_sold_date_sk` (partition column) joins
   * `date_dim.d_date_sk`. The CTE aggregates the fact joined to a small dim,
   * GROUPing by a dim attribute so looksLikeMaterializationNotWorthIt returns true (dim_name
   * escapes to CTE output) while hasInBodyDPPOpportunity returns false
   * (no selective predicate inside the CTE body).
   */
  private def withQ4ShapeFixture(body: => Unit): Unit = {
    withTable("pa_fact", "pa_dim") {
      spark.range(0, 200)
        .selectExpr("id AS fact_id", "id % 10 AS dim_fk")
        .write.partitionBy("dim_fk").saveAsTable("pa_fact")
      spark.range(0, 10)
        .selectExpr("id AS dim_id", "concat('d', cast(id as string)) AS dim_name")
        .write.saveAsTable("pa_dim")
      body
    }
  }

  private val q4ShapeSql =
    """WITH t AS (
      |  SELECT dim_name, COUNT(*) AS cnt
      |  FROM pa_fact f JOIN pa_dim d ON f.dim_fk = d.dim_id
      |  GROUP BY dim_name
      |)
      |SELECT a.dim_name
      |FROM t a JOIN t b ON a.dim_name = b.dim_name
      |WHERE a.cnt > 0 AND b.cnt > 0""".stripMargin

  // Same body as `q4ShapeSql` except the join chain is duplicated under a deduplicating UNION.
  // `Distinct.output` IS `child.output`, so the survival walk must reach the same verdict; a
  // `Distinct` barrier would silently suppress the veto and cache the body.
  private val unionDistinctShapeSql =
    """WITH t AS (
      |  SELECT dim_name, SUM(v) AS cnt FROM (
      |    SELECT d.dim_name AS dim_name, 1 AS v
      |    FROM pa_fact f JOIN pa_dim d ON f.dim_fk = d.dim_id
      |    UNION
      |    SELECT d.dim_name AS dim_name, 2 AS v
      |    FROM pa_fact f JOIN pa_dim d ON f.dim_fk = d.dim_id
      |  ) u
      |  GROUP BY dim_name
      |)
      |SELECT a.dim_name
      |FROM t a JOIN t b ON a.dim_name = b.dim_name
      |WHERE a.cnt > 0 AND b.cnt > 0""".stripMargin

  test("UNION-distinct body: Distinct must not block the survival walk") {
    // Guards the fix for the q75 regression. With `Distinct` treated as a barrier this asserts
    // 0 vetoes and the CTE gets cached; the point of the fix is that a deduplicating UNION is
    // dataflow-transparent and must not change the verdict relative to the same body written
    // with a single join. Measured on 3.5.5 at 100TB: the barrier turned six `SubqueryBroadcast`
    // DPP nodes into zero and q75's first execution went 59.5s -> 118.7s.
    withQ4ShapeFixture {
      withSQLConf(
          SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "true",
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
          SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0") {
        val (plan, delta) = withVetoCount {
          spark.sql(unionDistinctShapeSql).queryExecution.optimizedPlan
        }
        assert(delta >= 1,
          s"a body whose only difference from q4ShapeSql is a deduplicating UNION must reach " +
          s"the same verdict; a Distinct barrier silently suppresses the veto and the body " +
          s"gets cached, losing the outer query's DPP. delta=$delta\nplan:\n$plan")
        assert(!plan.toString.contains("InMemoryRelation"),
          s"expected the vetoed CTE to be inlined but got:\n$plan")
      }
    }
  }

  test("UNION-distinct body: master switch still turns the veto off") {
    // The fix must not make the new verdict unconditional -- the escape hatch has to keep
    // working for the shape it now applies to.
    withQ4ShapeFixture {
      withSQLConf(
          SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "false",
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
          SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0") {
        val (plan, delta) = withVetoCount {
          spark.sql(unionDistinctShapeSql).queryExecution.optimizedPlan
        }
        assert(delta == 0, s"expected no veto when the master switch is off, got $delta")
        assert(plan.toString.contains("InMemoryRelation"),
          s"expected the CTE to be auto-cached when the veto is off but got:\n$plan")
      }
    }
  }

  test("Q4-shape, master=true + Auto-CTE on: veto fires and CTE is inlined") {
    withQ4ShapeFixture {
      withSQLConf(
          SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "true",
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
          SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0") {
        val (plan, delta) = withVetoCount {
          spark.sql(q4ShapeSql).queryExecution.optimizedPlan
        }
        assert(delta >= 1,
          s"expected at least one veto fire on Q4-shape, got delta=$delta\nplan:\n$plan")
        val planStr = plan.toString
        assert(!planStr.contains("InMemoryRelation"),
          s"expected CTE to be inlined (no InMemoryRelation) but got:\n$planStr")
      }
    }
  }

  test("Q4-shape, master=false + Auto-CTE on: no veto, CTE is auto-cached") {
    withQ4ShapeFixture {
      withSQLConf(
          SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE.key -> "false",
          SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
          SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0") {
        val (plan, delta) = withVetoCount {
          spark.sql(q4ShapeSql).queryExecution.optimizedPlan
        }
        assert(delta == 0,
          s"expected no veto when master switch off, got delta=$delta")
        val planStr = plan.toString
        assert(planStr.contains("InMemoryRelation"),
          s"expected CTE to be auto-cached (InMemoryRelation present) but got:\n$planStr")
      }
    }
  }
}
