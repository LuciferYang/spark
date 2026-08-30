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

package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * `UnionExec` derives three things from its children's `outputPartitioning`: whether whole-stage
 * codegen fusion applies, whether `numOutputRows` is registered, and which RDD `doExecute`
 * builds. That input is not stable over a node's lifetime -- `InMemoryTableScanExec` reads
 * `cachedPlan.outputPartitioning`, and an inner `AdaptiveSparkPlanExec` answers
 * `UnknownPartitioning` until its final plan exists -- and `CollapseCodegenStages` puts a
 * `withNewChildren` COPY of the node inside the shell it builds, so the copy used to re-derive
 * the decision and answer the opposite of what the shell was built from.
 *
 * The decision is now taken once and carried on the node, which is what these tests pin.
 */
class UnionFusedMetricsSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private def cachedAggregate(view: String): Unit = {
    spark.catalog.clearCache()
    spark.range(0, 200, 1, 4)
      .selectExpr("id % 10 AS k", "id AS v")
      .groupBy("k")
      .agg(sum("v").as("s"))
      .createOrReplaceTempView(view)
    spark.catalog.cacheTable(view)
  }

  /** Every node of the final plan, descending through AQE wrappers and query stages. */
  private def allNodes(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case a: AdaptiveSparkPlanExec => plan +: allNodes(a.executedPlan)
    case q: QueryStageExec => plan +: allNodes(q.plan)
    case other => other +: other.children.flatMap(allNodes)
  }

  private def fusedUnions(plan: SparkPlan): Seq[UnionExec] =
    allNodes(plan).collect {
      case w: WholeStageCodegenExec if w.child.isInstanceOf[UnionExec] =>
        w.child.asInstanceOf[UnionExec]
    }

  private def unions(plan: SparkPlan): Seq[UnionExec] =
    allNodes(plan).collect { case u: UnionExec => u }

  test("a fused union over cached scans keeps its numOutputRows metric") {
    // The asymmetry matters: an expression on one branch keeps a `ProjectExec` from being
    // collapsed away, so the two children are shaped differently, `comparePartitioning` rejects
    // the pair while the cache stages are unmaterialised, and the union looks plain and is
    // fused. Once the cache finalises its inner AQE both children report the same
    // `HashPartitioning`, and re-deriving the decision at that point used to leave
    // `metrics` empty -- `doProduce` then threw `NoSuchElementException: key not found:
    // numOutputRows`. `SELECT *` or a plain alias is collapsed away and does not reproduce it.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("v") {
        cachedAggregate("v")
        val df = spark.sql("SELECT k, abs(s) AS s FROM v UNION ALL SELECT k, s FROM v")
        // Execute THIS DataFrame, not a count over it: the plan being inspected has to be the
        // one that ran, and an AQE plan that never ran has no final plan to inspect.
        assert(df.collect().length == 20)
        val fused = fusedUnions(df.queryExecution.executedPlan)
        assert(fused.nonEmpty,
          "this shape must actually fuse, or the test is not exercising the defect")
        assert(fused.forall(_.metrics.contains("numOutputRows")),
          "a fused union must register the metric its generated code increments")
      }
    }
  }

  test("a fused union reports UnknownPartitioning, so no parent skips an exchange") {
    // The other half of the same decision. A fused union concatenates its children's
    // partitions, so if it went on claiming the children's `HashPartitioning` a parent would
    // satisfy a clustered distribution from an RDD that does not have it -- a wrong answer
    // rather than a crash.
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withTempView("v") {
        cachedAggregate("v")
        val df = spark.sql("SELECT k, abs(s) AS s FROM v UNION ALL SELECT k, s FROM v")
        df.collect()
        val fused = fusedUnions(df.queryExecution.executedPlan)
        assert(fused.nonEmpty, "this shape must actually fuse")
        fused.foreach { u =>
          assert(u.outputPartitioning.isInstanceOf[UnknownPartitioning],
            s"a fused union must not claim a concrete partitioning, got ${u.outputPartitioning}")
        }
      }
    }
  }

  test("SPARK-52921 pass-through still applies when the children agree up front") {
    // Guards against the fix disabling the partitioning-aware union in the case it was built
    // for: both children are aggregates over the same grouping key, so their partitioning is
    // known at planning time, the decision latches to "not plain", and the union passes the
    // partitioning through instead of being fused.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.UNION_OUTPUT_PARTITIONING.key -> "true") {
      val left = spark.range(0, 200, 1, 4).selectExpr("id % 10 AS k", "id AS v")
        .groupBy("k").agg(sum("v").as("s"))
      val right = spark.range(200, 400, 1, 4).selectExpr("id % 10 AS k", "id AS v")
        .groupBy("k").agg(sum("v").as("s"))
      val df = left.union(right)
      df.collect()
      val us = unions(df.queryExecution.executedPlan)
      assert(us.nonEmpty, "expected a UnionExec in this plan")
      us.foreach { u =>
        assert(!u.outputPartitioning.isInstanceOf[UnknownPartitioning],
          "a union whose children agree on partitioning must pass it through")
        assert(u.metrics.isEmpty,
          "a partitioning-aware union does not fuse, so it must not register a row-count metric")
      }
      assert(fusedUnions(df.queryExecution.executedPlan).isEmpty,
        "a partitioning-aware union must not be wrapped in WholeStageCodegenExec")
    }
  }
}
