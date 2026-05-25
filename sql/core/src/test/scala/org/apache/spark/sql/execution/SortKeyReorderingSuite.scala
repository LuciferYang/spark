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
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.ValidateRequirements
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

  private def firstSmj(plan: SparkPlan): SortMergeJoinExec =
    plan.collectFirst { case smj: SortMergeJoinExec => smj }
      .getOrElse(fail(s"expected an SMJ in plan, got:\n$plan"))

  private def names(exprs: Seq[org.apache.spark.sql.catalyst.expressions.Expression]): Seq[String] =
    exprs.collect { case a: Attribute => a.name }

  private def findSmjLeftKeyNames(plan: SparkPlan): Seq[String] =
    plan.collectFirst { case smj: SortMergeJoinExec => names(smj.leftKeys) }.getOrElse(Seq.empty)

  private def extractInitialPlan(plan: SparkPlan): SparkPlan = plan match {
    case aqe: AdaptiveSparkPlanExec => aqe.initialPlan
    case other => other
  }

  /**
   * The plan AQE actually ran, rather than the one it started from.
   *
   * `stripAQEPlan` rather than reading `aqe.executedPlan` directly: that plan's root is a
   * `ResultQueryStageExec`, and every `QueryStageExec` keeps its wrapped plan in `plan`
   * rather than in `children`, so `collectFirst` walks straight past the SMJ underneath and
   * reports an empty key list instead of failing on the keys.
   */
  private def collectFinal(plan: SparkPlan): SparkPlan =
    AdaptiveSparkPlanHelper.stripAQEPlan(plan)

  private def prepared(sqlText: String): SparkPlan =
    extractInitialPlan(spark.sql(sqlText).queryExecution.executedPlan)

  /**
   * The plan as prepared with the rule OFF, so `apply` gets an un-permuted input.
   *
   * `prepared` under a conf-ON block returns a plan the pipeline has ALREADY permuted,
   * so calling `apply` on it exercises re-application rather than the first
   * permutation, and an assertion on the result is satisfied by the pipeline no matter
   * what the explicit `apply` does. Tests that mean to exercise the rule itself take
   * their input from here.
   */
  private def preparedUnpermuted(sqlText: String): SparkPlan = {
    var p: SparkPlan = null
    withSQLConf(SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "false") {
      p = prepared(sqlText)
    }
    p
  }

  test("rule disabled: SMJ leftKeys remain in original [a, b] order") {
    withTwoKeyTables {
      withSQLConf(SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "false") {
        val keys = findSmjLeftKeyNames(prepared(joinSql))
        assert(keys.nonEmpty, "expected an SMJ in plan")
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
        val transformed = SortKeyReordering(spark).apply(preparedUnpermuted(joinSql))
        val keys = findSmjLeftKeyNames(transformed)
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
        val transformed = SortKeyReordering(spark).apply(prepared(joinSql))
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
        val transformed = SortKeyReordering(spark).apply(preparedUnpermuted(threeKeyJoinSql))
        val keys = findSmjLeftKeyNames(transformed)
        assert(keys == Seq("c", "b", "a"),
          s"expected key order [c, b, a] for 3-key join, got: $keys")
      }
    }
  }

  /**
   * The permutation the key check above cannot see.
   *
   * `leftKeys` alone is not enough to prove the rewrite is sound: an
   * implementation that permuted the keys and left `SortExec.sortOrder` (or
   * `rightKeys`) alone would satisfy every assertion above while producing a
   * plan whose children are sorted on a different sequence than the merge
   * expects -- a wrong answer, not a slow one. This pins all three moving in
   * lock-step, which is exactly what makes the rewrite answer-preserving.
   */
  test("keys, rightKeys and both SortExec children are permuted in lock-step") {
    withThreeKeyTables {
      // The baseline must be planned with the conf OFF. `prepared` runs the real
      // preparation pipeline, so under conf-on the plan comes back ALREADY
      // permuted and a "before" taken there is the after.
      var before: SortMergeJoinExec = null
      withSQLConf(SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "false") {
        before = firstSmj(prepared(threeKeyJoinSql))
      }
      var after: SortMergeJoinExec = null
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        after = firstSmj(prepared(threeKeyJoinSql))
      }
      assert(names(before.leftKeys) == Seq("a", "b", "c"), "fixture assumption broken")

      val perm = names(after.leftKeys).map(names(before.leftKeys).indexOf(_))
      assert(perm == Seq(2, 1, 0), s"expected the reversing permutation, got $perm")
      // The right side must carry the SAME permutation -- pairing key i on the
      // left with key i on the right is what the merge relies on.
      assert(names(after.rightKeys) == perm.map(names(before.rightKeys)(_)),
        s"rightKeys permutation ${names(after.rightKeys)} does not match leftKeys' $perm " +
        s"(before: ${names(before.rightKeys)})")
      // And both children's sortOrder must follow, or the merge reads streams
      // that are not sorted the way it compares them.
      Seq(("left", before.left, after.left), ("right", before.right, after.right))
        .foreach { case (side, b, a) =>
          val bOrder = b.asInstanceOf[SortExec].sortOrder
          val aOrder = a.asInstanceOf[SortExec].sortOrder
          assert(names(aOrder.map(_.child)) == perm.map(names(bOrder.map(_.child))(_)),
            s"$side child's sortOrder was not permuted in lock-step: " +
            s"got ${names(aOrder.map(_.child))}, " +
            s"expected ${perm.map(names(bOrder.map(_.child))(_))}")
        }
    }
  }

  /**
   * The bug this rule shipped with, and the reason `apply` validates the whole plan.
   *
   * `SortMergeJoinExec.outputOrdering` is derived positionally from `leftKeys`, so
   * permuting a join's keys changes what it advertises; `EnsureRequirements` has already
   * run and already decided, against the OLD ordering, that the join above needed no
   * `SortExec`. `transformUp` then makes this the common case rather than a corner: the
   * lower join is permuted first, and when the upper join is visited its left child is an
   * SMJ instead of a `SortExec`, so `maybePermute`'s shape guard declines and the upper
   * join keeps comparing in its original key order over a stream sorted the new way.
   *
   * Measured before the fix, on exactly this fixture: `count(*)` came back 512 instead of
   * 2000, under AQE both on and off. No error, no warning. Asserted on the COUNT rather
   * than on the plan shape because the plan shape is what a future refactor might change
   * while keeping the bug -- the row count is the property that has to hold.
   */
  test("stacked SMJ: permuting the lower join must not corrupt the upper join") {
    withTable("s1", "s2", "s3") {
      // NDV(k1)=4, NDV(k2)=2000 -> ratio far above the default threshold, so the rule
      // wants [k2, k1] and the lower join is a fire candidate.
      Seq("s1", "s2", "s3").foreach { t =>
        spark.range(0, 2000).selectExpr("id % 4 AS k1", "id AS k2")
          .write.saveAsTable(t)
        spark.sql(s"ANALYZE TABLE $t COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      val stackedSql =
        """SELECT count(*) AS c
          |FROM s1 JOIN s2 ON s1.k1 = s2.k1 AND s1.k2 = s2.k2
          |        JOIN s3 ON s1.k1 = s3.k1 AND s1.k2 = s3.k2""".stripMargin
      Seq(false, true).foreach { aqe =>
        var off = -1L
        var on = -1L
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
            SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "false") {
          off = spark.sql(stackedSql).collect()(0).getLong(0)
        }
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
            SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
            SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
          on = spark.sql(stackedSql).collect()(0).getLong(0)
        }
        assert(off == 2000L,
          s"aqe=$aqe: fixture assumption broken -- the baseline count should be 2000, " +
          s"got $off; without that this test cannot detect dropped rows")
        assert(on == off,
          s"aqe=$aqe: enabling sortKeyReordering changed the ANSWER " +
          s"(off=$off, on=$on). Permuting the lower SMJ invalidated the upper SMJ's " +
          s"already-satisfied requiredChildOrdering and rows were silently dropped.")
      }

      // The count assertion above cannot tell "the gate reverted an unsafe permutation"
      // from "the rule never fired at all", and after the fix the stacked plan comes back
      // UN-permuted either way -- so a future change that makes `maybePermute` decline
      // (a stats path, a join-strategy switch, a tightened shape guard) would leave this
      // test green while testing nothing. Pin the fixture by checking the rule is still a
      // candidate on the same tables and confs, using the two-table join where the
      // permutation is safe and therefore visible.
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        val twoTableKeys = findSmjLeftKeyNames(
          prepared("SELECT s1.k1 FROM s1 JOIN s2 ON s1.k1 = s2.k1 AND s1.k2 = s2.k2"))
        assert(twoTableKeys == Seq("k2", "k1"),
          s"this fixture no longer makes the rule fire (got $twoTableKeys), so the " +
          s"stacked-join assertions above are vacuous: they would pass against a rule " +
          s"that declines everything")
      }
    }
  }

  test("the permuted plan still satisfies ValidateRequirements") {
    // Distribution half of the caveat on `SortKeyReordering`: permuting the keys changes
    // `requiredChildDistribution` (`ClusteredDistribution(leftKeys)`) while the
    // upstream shuffle used the original order. It passes today because
    // `requireAllClusterKeysForDistribution` defaults false and
    // `HashShuffleSpec.isCompatibleWith` compares positions derived from each
    // side's own clustering. If a future Spark version tightens either, this
    // fails HERE rather than silently shipping a plan AQE will not re-validate.
    withThreeKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        val transformed = SortKeyReordering(spark).apply(preparedUnpermuted(threeKeyJoinSql))
        assert(names(firstSmj(transformed).leftKeys) == Seq("c", "b", "a"),
          "test is vacuous unless the rule actually permuted something")
        assert(ValidateRequirements.validate(transformed, UnspecifiedDistribution),
          s"the permuted plan does not satisfy its own distribution/ordering " +
          s"requirements:\n$transformed")
      }
    }
  }

  test("result equivalence: permuted plan produces same rows as baseline") {
    withTwoKeyTables {
      // `withSQLConf` returns Unit on this line, so the rows come out through a var
      // rather than as the block's value.
      var baseline: Set[org.apache.spark.sql.Row] = Set.empty
      withSQLConf(SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "false") {
        baseline = spark.sql(joinSql).collect().toSet
      }
      var withRule: Set[org.apache.spark.sql.Row] = Set.empty
      var executed: SparkPlan = null
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        val df = spark.sql(joinSql)
        withRule = df.collect().toSet
        executed = df.queryExecution.executedPlan
      }
      assert(baseline.nonEmpty, "fixture returned no rows; this test would be vacuous")
      // Without this the test is vacuous in a way row counts cannot reveal: at this
      // data size AQE's `DynamicJoinSelection` can demote the SMJ to a
      // `ShuffledHashJoinExec`, which has no `SortExec` children, so the rule
      // declines and both sides execute the SAME plan. Verified by mutation --
      // with `rightKeys` left unpermuted (a wrong-answer bug) this test still
      // passed until the assertion below was added.
      val smjKeys = findSmjLeftKeyNames(collectFinal(executed))
      assert(smjKeys == Seq("b", "a"),
        s"expected the EXECUTED plan to contain the permuted SMJ, got keys $smjKeys in:\n" +
        s"${collectFinal(executed)}")
      assert(baseline == withRule,
        s"row sets differ after enabling rule:\nbaseline=${baseline.size}, " +
        s"with-rule=${withRule.size}")
    }
  }

  /**
   * End to end through the real pipeline, not `rule.apply` by hand.
   *
   * Every assertion above constructs the rule directly, which proves the
   * transformation but not that anything invokes it. This one only sets the conf
   * and reads the plan the session produced, so a rule that is correct but not
   * wired -- or wired after `RemoveRedundantSorts` has already collapsed the
   * `SortExec` children it needs -- fails here.
   */
  test("end to end: setting only the conf permutes the plan the session produces") {
    withThreeKeyTables {
      Seq(false, true).foreach { aqe =>
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
            SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
            SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
          val keys = findSmjLeftKeyNames(prepared(threeKeyJoinSql))
          assert(keys == Seq("c", "b", "a"),
            s"aqe=$aqe: the rule is not reached by the plan-preparation pipeline; " +
            s"expected [c, b, a], got: $keys")
        }
      }
    }
  }

  test("wiring: SortKeyReordering is registered in both preparation pipelines") {
    // `ruleName` rather than `getSimpleName`: for a Scala `object` the latter
    // carries a trailing `$` (`RemoveRedundantSorts$`), which makes an equality
    // comparison against the class name quietly never match.
    val prepRules = QueryExecution.preparations(spark, None, subquery = false).map(_.ruleName)
    val skrAt = prepRules.indexOf(classOf[SortKeyReordering].getName)
    assert(skrAt >= 0,
      s"expected SortKeyReordering in QueryExecution.preparations, got: $prepRules")
    // It must precede `RemoveRedundantSorts`, which can remove the direct
    // `SortExec` children the rule requires.
    val rrsAt = prepRules.indexOf(RemoveRedundantSorts.ruleName)
    assert(rrsAt >= 0, s"RemoveRedundantSorts not found in the pipeline: $prepRules")
    assert(skrAt < rrsAt,
      s"SortKeyReordering must run before RemoveRedundantSorts, got: $prepRules")
  }

  test("idempotency: applying the rule twice yields the same plan as once") {
    withTwoKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        val rule = SortKeyReordering(spark)
        val once = rule.apply(preparedUnpermuted(joinSql))
        val twice = rule.apply(once)
        // Assert the permutation HAPPENED before asserting it is stable: comparing
        // `once` against `twice` alone is green against a rule that declines everything.
        assert(findSmjLeftKeyNames(once) == Seq("b", "a"),
          s"nothing was permuted, so idempotency is vacuous: " +
          s"${findSmjLeftKeyNames(once)}")
        assert(findSmjLeftKeyNames(once) == findSmjLeftKeyNames(twice),
          "expected idempotent transformation, but key order changed on re-apply")
      }
    }
  }

  /**
   * `copyTagsFrom` on the two replacement `SortExec`s.
   *
   * The rule builds them with `.copy(sortOrder = ...)`, and `tags` is a per-instance
   * mutable map, so a copy starts empty. `TreeNode.transformUpWithPruning` copies tags
   * for the node the rule RETURNED (the SMJ) but never for nodes the rule constructed
   * inside itself, so without the explicit calls the Sorts silently lose whatever tag
   * state they carried -- `logicalLink` among it, which is what any later rule reading
   * statistics off those nodes depends on. Seeded with an explicit tag rather than
   * asserting on `logicalLink`, because the Sorts `EnsureRequirements` inserts in this
   * fixture have no link to begin with, which would make a link assertion vacuous.
   */
  test("the replacement SortExec children keep their tags") {
    withThreeKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "10.0") {
        val before = preparedUnpermuted(threeKeyJoinSql)
        val beforeSmj = firstSmj(before)
        val tag = TreeNodeTag[String]("skr-test-tag")
        beforeSmj.left.setTagValue(tag, "L")
        beforeSmj.right.setTagValue(tag, "R")

        val afterSmj = firstSmj(SortKeyReordering(spark).apply(before))
        assert(names(afterSmj.leftKeys) == Seq("c", "b", "a"),
          "test is vacuous unless the rule actually replaced the Sort children")
        assert(afterSmj.left.getTagValue(tag).contains("L"),
          "the permuted left SortExec lost its tags; copyTagsFrom is missing")
        assert(afterSmj.right.getTagValue(tag).contains("R"),
          "the permuted right SortExec lost its tags; copyTagsFrom is missing")
      }
    }
  }

  /**
   * The `perm == ndvs.indices` short-circuit, asserted on reference identity.
   *
   * `SORT_KEY_REORDERING_THRESHOLD` accepts 1.0, and at that value a key order that is
   * ALREADY descending-NDV has ratio exactly 1.0 and clears the ratio gate -- the
   * short-circuit is the only thing that then stops a no-op rewrite.
   *
   * `apply` is handed the SMJ ITSELF as the plan root, not the whole tree, and that is
   * load-bearing. Without the short-circuit the identity permutation rebuilds an
   * equal-but-not-identical SMJ; if that SMJ has a parent, `withNewChildren` sees
   * `childrenFastEquals` and returns the parent unchanged, so the rewrite collapses and
   * an `eq` assertion on the whole tree passes either way. With the SMJ as root there is
   * no parent to absorb it, so `eq` distinguishes "returned untouched" from "rebuilt".
   */
  test("already-optimal key order short-circuits instead of rebuilding the node") {
    withThreeKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "1.0") {
        // Keys written in descending-NDV order: NDV(c)=10000, NDV(b)=100, NDV(a)=10.
        val alreadyBest =
          """SELECT l.a, l.b, l.c
            |FROM three_key_left l JOIN three_key_right r
            |  ON l.c = r.c AND l.b = r.b AND l.a = r.a""".stripMargin
        val smj = firstSmj(preparedUnpermuted(alreadyBest))
        assert(names(smj.leftKeys) == Seq("c", "b", "a"),
          "fixture assumption broken: the keys should already be in descending-NDV order")
        // The rule also returns `smj` unchanged when `collectNdvs` finds no statistics,
        // so without this the `eq` assertion below would pass for the wrong reason.
        // Reversing the same three keys must permute, which proves the NDVs are visible
        // through this SMJ and that `eq` below is really the short-circuit talking.
        val reversed = firstSmj(preparedUnpermuted(threeKeyJoinSql))
        assert(names(reversed.leftKeys) == Seq("a", "b", "c"), "fixture assumption broken")
        assert(names(firstSmj(SortKeyReordering(spark).apply(reversed)).leftKeys) ==
            Seq("c", "b", "a"),
          "the rule cannot see this fixture's NDVs at all, so the identity assertion " +
          "below would hold vacuously")
        val after = SortKeyReordering(spark).apply(smj)
        assert(after eq smj,
          s"an already-optimal SMJ must come back by identity, but a new instance was " +
          s"built -- the perm == indices short-circuit is gone:\n$after")
      }
    }
  }

  test("single-key SMJ is left alone") {
    // `maybePermute` returns early on `leftKeys.size < 2`. This matters for the
    // target workload: TPC-DS q64 has four SMJs and only one is multi-key, so a
    // regression that started rewriting single-key joins would touch three more
    // joins than intended.
    withTwoKeyTables {
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "1.0") {
        val oneKeySql =
          """SELECT l.a FROM two_key_left l JOIN two_key_right r ON l.b = r.b"""
        val before = prepared(oneKeySql)
        val after = SortKeyReordering(spark).apply(before)
        assert(after.fastEquals(before),
          s"a single-key SMJ must not be rewritten, even at threshold 1.0:\n$after")
      }
    }
  }

  test("missing column statistics: no permutation") {
    // `collectNdvs` returns None when any key lacks a distinct count, and the
    // whole rule is a no-op then. Worth pinning because it is the difference
    // between "the rule declined" and "the rule permuted on a guessed NDV": on
    // the cluster the tables may or may not have been ANALYZEd, and a rule that
    // silently defaulted a missing NDV could reorder keys the wrong way.
    withTable("no_stats_left", "no_stats_right") {
      spark.range(0, 10000).selectExpr("id % 100 AS a", "id AS b")
        .write.saveAsTable("no_stats_left")
      spark.range(0, 10000).selectExpr("id % 100 AS a", "id AS b")
        .write.saveAsTable("no_stats_right")
      // Deliberately NO `ANALYZE ... FOR ALL COLUMNS`.
      withSQLConf(
          SQLConf.SORT_KEY_REORDERING_ENABLED.key -> "true",
          SQLConf.SORT_KEY_REORDERING_THRESHOLD.key -> "1.0") {
        val sqlText =
          """SELECT l.a, l.b FROM no_stats_left l JOIN no_stats_right r
            |  ON l.a = r.a AND l.b = r.b""".stripMargin
        val before = prepared(sqlText)
        assert(names(firstSmj(before).leftKeys) == Seq("a", "b"), "fixture assumption broken")
        val after = SortKeyReordering(spark).apply(before)
        assert(names(firstSmj(after).leftKeys) == Seq("a", "b"),
          s"without column statistics the rule must decline, but it permuted:\n$after")
      }
    }
  }
}
