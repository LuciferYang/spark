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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for [[SortKeyReordering]].
 *
 * Fixture: two synthetic tables joined on two or three keys. Left key NDV
 * is much smaller than the right key NDV, so the cost-formula predicts a
 * large `comparisons(orig)/comparisons(best)` ratio when the small-NDV key
 * sits first. With the rule on we expect the SMJ's `leftKeys` to be
 * permuted so the higher-NDV key is first.
 */
class SortKeyReorderingSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: org.apache.spark.SparkConf =
    super.sparkConf
      .set(SQLConf.CBO_ENABLED.key, "true")
      .set(SQLConf.PLAN_STATS_ENABLED.key, "true")
      // Force SMJ rather than broadcast hash join so the rule has an SMJ to act on.
      .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

  private def withTwoKeyTables(body: => Unit): Unit = {
    withTable("two_key_left", "two_key_right") {
      // 10,000 rows: small NDV on `a` (100), large NDV on `b` (10,000).
      spark.range(0, 10000).selectExpr(
          "id AS a",
          "id AS b")
        .selectExpr("a % 100 AS a", "b AS b")
        .write.saveAsTable("two_key_left")
      spark.range(0, 10000).selectExpr(
          "id % 100 AS a",
          "id AS b")
        .write.saveAsTable("two_key_right")
      spark.sql("ANALYZE TABLE two_key_left COMPUTE STATISTICS FOR ALL COLUMNS")
      spark.sql("ANALYZE TABLE two_key_right COMPUTE STATISTICS FOR ALL COLUMNS")
      body
    }
  }

  private def withThreeKeyTables(body: => Unit): Unit = {
    withTable("three_key_left", "three_key_right") {
      // 10,000 rows: NDV(a)=10, NDV(b)=100, NDV(c)=10,000.
      // Best key order is therefore [c, b, a] (highest NDV first).
      spark.range(0, 10000).selectExpr(
          "id % 10 AS a", "id % 100 AS b", "id AS c")
        .write.saveAsTable("three_key_left")
      spark.range(0, 10000).selectExpr(
          "id % 10 AS a", "id % 100 AS b", "id AS c")
        .write.saveAsTable("three_key_right")
      spark.sql("ANALYZE TABLE three_key_left COMPUTE STATISTICS FOR ALL COLUMNS")
      spark.sql("ANALYZE TABLE three_key_right COMPUTE STATISTICS FOR ALL COLUMNS")
      body
    }
  }

  private val joinSql =
    """SELECT l.a, l.b
      |FROM two_key_left l JOIN two_key_right r
      |  ON l.a = r.a AND l.b = r.b""".stripMargin

  private val threeKeyJoinSql =
    """SELECT l.a, l.b, l.c
      |FROM three_key_left l JOIN three_key_right r
      |  ON l.a = r.a AND l.b = r.b AND l.c = r.c""".stripMargin

  private def findSmjLeftKeyNames(plan: SparkPlan): Seq[String] = {
    plan.collectFirst { case smj: SortMergeJoinExec =>
      smj.leftKeys.collect { case a: Attribute => a.name }
    }.getOrElse(Seq.empty)
  }

  private def extractInitialPlan(plan: SparkPlan): SparkPlan = plan match {
    case aqe: AdaptiveSparkPlanExec => aqe.initialPlan
    case other => other
  }

  test("rule disabled: SMJ leftKeys remain in original [a, b] order") {
    withTwoKeyTables {
      withSQLConf(SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "false") {
        val plan = extractInitialPlan(spark.sql(joinSql).queryExecution.executedPlan)
        val keys = findSmjLeftKeyNames(plan)
        assert(keys.nonEmpty, s"expected an SMJ in plan, got:\n$plan")
        assert(keys == Seq("a", "b"),
          s"expected original key order [a, b] when rule disabled, got: $keys")
      }
    }
  }

  test("rule enabled + threshold met: SMJ leftKeys permuted so high-NDV b first") {
    withTwoKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        val plan = extractInitialPlan(spark.sql(joinSql).queryExecution.executedPlan)
        val rule = SortKeyReordering(spark)
        val transformed = rule.apply(plan)
        val keys = findSmjLeftKeyNames(transformed)
        assert(keys.nonEmpty, s"expected an SMJ in plan, got:\n$transformed")
        assert(keys == Seq("b", "a"),
          s"expected key order [b, a] (high-NDV first) when rule enabled, got: $keys")
      }
    }
  }

  test("rule enabled + threshold not met: SMJ leftKeys unchanged") {
    withTwoKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          // Threshold above any realistic 2-key ratio for this fixture.
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "1000000.0") {
        val plan = extractInitialPlan(spark.sql(joinSql).queryExecution.executedPlan)
        val rule = SortKeyReordering(spark)
        val transformed = rule.apply(plan)
        val keys = findSmjLeftKeyNames(transformed)
        assert(keys == Seq("a", "b"),
          s"expected original key order [a, b] when threshold not met, got: $keys")
      }
    }
  }

  test("rule enabled on 3-key SMJ: leftKeys permuted to descending-NDV [c, b, a]") {
    withThreeKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        val plan = extractInitialPlan(spark.sql(threeKeyJoinSql).queryExecution.executedPlan)
        val rule = SortKeyReordering(spark)
        val transformed = rule.apply(plan)
        val keys = findSmjLeftKeyNames(transformed)
        assert(keys.nonEmpty, s"expected an SMJ in plan, got:\n$transformed")
        assert(keys == Seq("c", "b", "a"),
          s"expected key order [c, b, a] for 3-key join, got: $keys")
      }
    }
  }

  test("result equivalence: permuted plan produces same rows as baseline") {
    withTwoKeyTables {
      val baseline = withSQLConf(SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "false") {
        spark.sql(joinSql).collect().toSet
      }
      val withRule = withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        spark.sql(joinSql).collect().toSet
      }
      assert(baseline == withRule,
        s"row sets differ after enabling rule:\nbaseline=${baseline.size}, " +
        s"with-rule=${withRule.size}")
    }
  }

  test("wiring: SortKeyReordering is registered in the plan-preparation pipeline") {
    val prepRuleClasses = QueryExecution.preparations(spark, None, subquery = false)
      .map(_.getClass.getSimpleName)
    assert(prepRuleClasses.contains(classOf[SortKeyReordering].getSimpleName),
      s"expected SortKeyReordering in QueryExecution.preparations, got: $prepRuleClasses")
  }

  test("idempotency: applying the rule twice yields the same plan as once") {
    withTwoKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        val plan = extractInitialPlan(spark.sql(joinSql).queryExecution.executedPlan)
        val rule = SortKeyReordering(spark)
        val once = rule.apply(plan)
        val twice = rule.apply(once)
        assert(findSmjLeftKeyNames(once) == findSmjLeftKeyNames(twice),
          "expected idempotent transformation, but key order changed on re-apply")
      }
    }
  }
}
