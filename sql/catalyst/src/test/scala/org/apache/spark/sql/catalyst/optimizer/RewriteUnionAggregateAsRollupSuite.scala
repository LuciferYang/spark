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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, LongType, StringType}

class RewriteUnionAggregateAsRollupSuite extends PlanTest {

  // The rule ships disabled, so enable it for the suite; the one test that checks
  // the config gate overrides this with withSQLConf.
  private var enabledBefore: Boolean = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    enabledBefore = SQLConf.get.getConf(SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED)
    SQLConf.get.setConf(SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED, true)
  }

  protected override def afterAll(): Unit = {
    try {
      SQLConf.get.setConf(SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED, enabledBefore)
    } finally {
      super.afterAll()
    }
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace Operators", FixedPoint(100),
        RewriteUnionAggregateAsRollup) :: Nil
  }

  /** Assert the plan was rewritten to the ROLLUP shape:
   *  `Union(Project(Aggregate(Expand)), <grand-total branch>)`. The grand-total
   *  (GROUP BY ()) branch is unioned back as a separate global aggregate so it
   *  still yields one row on empty input; the ROLLUP side (levels [N..1]) is a
   *  remapped Aggregate-over-Expand. So the rewritten top-level is a 2-child
   *  Union whose first child is a Project over Aggregate(Expand), and which
   *  contains exactly one Expand.
   */
  private def assertRewrittenToRollup(optimized: LogicalPlan): Unit = {
    val shapeOk = optimized match {
      case Union(Seq(Project(_, Aggregate(_, _, _: Expand, _)), _), _, _) => true
      case _ => false
    }
    val expandCount = optimized.collect { case e: Expand => e }.size
    assert(shapeOk && expandCount == 1,
      s"Expected Union(Project(Aggregate(Expand)), grandTotal) with one Expand, " +
        s"got (expandCount=$expandCount):\n${optimized.treeString}")
  }

  /** Assert the rule did NOT fire. The rewrite ALWAYS introduces exactly one
   *  Expand (ROLLUP via Expand), so absence of any Expand is the reliable
   *  "not rewritten" signal. (Checking "a Union remains" is NOT reliable: the
   *  rewrite itself now produces a 2-child union-back, so a Union is present in
   *  both the rewritten and unchanged plans.)
   */
  private def assertNotRewritten(optimized: LogicalPlan): Unit = {
    val expandCount = optimized.collect { case e: Expand => e }.size
    assert(expandCount == 0,
      s"Expected NO rewrite (no Expand), but found $expandCount Expand(s):\n" +
        s"${optimized.treeString}")
  }

  // Test relation: 2 grouping cols + a measure column. A 3-branch
  // [c1, c2] -> [c1] -> [] union is then a COMPLETE prefix hierarchy
  // matching ROLLUP(c1, c2) semantics.
  private val rel = LocalRelation($"c1".int, $"c2".int, $"x".int)

  /** Build the inner aggregate: sum(x) GROUP BY [c1, c2]. */
  private def innerAgg(child: LogicalPlan = rel): LogicalPlan = {
    val sumX = Alias(
      AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sumx")()
    Aggregate(
      groupingExpressions = Seq($"c1", $"c2"),
      aggregateExpressions = Seq($"c1", $"c2", sumX),
      child = child)
  }

  /** Build an outer rollup-level branch: sum(sumx) GROUP BY [keepCols], with
   *  NULL in the dropped positions, over the inner aggregate. */
  private def outerBranch(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
    val innerSumx = inner.output.find(_.name == "sumx").get
    val sumSumx = Alias(
      AggregateExpression(Sum(innerSumx), Complete, isDistinct = false), "sumx")()
    val groupCols: Seq[Expression] =
      keepCols.map(n => inner.output.find(_.name == n).get)
    val allOutputCols = Seq("c1", "c2").map { n =>
      if (keepCols.contains(n)) {
        Alias(inner.output.find(_.name == n).get, n)()
      } else {
        Alias(Literal(null, IntegerType), n)()
      }
    } :+ sumSumx
    Aggregate(
      groupingExpressions = groupCols,
      aggregateExpressions = allOutputCols,
      child = inner)
  }

  /** Raw branch: the inner aggregate used directly as a Union branch (the
   *  rule's stripProject sees through any Project the analyzer adds above it). */
  private def rawBranch(inner: LogicalPlan): LogicalPlan = inner

  test("rewrites 3-level rollup pattern (raw + 1 level + grand total)") {
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      outerBranch(Seq("c1"), inner2),
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)

    // After rewrite, the top-level operator should be a Project wrapping a
    // single Aggregate (post-Rollup expansion produces Aggregate over Expand),
    // with the grand-total branch unioned back.
    assertRewrittenToRollup(optimized)
  }

  test("does NOT rewrite when fewer than 3 branches") {
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      outerBranch(Seq("c1"), inner2)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    // The Union should still be there (not rewritten).
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when branches read from different bases") {
    // Use a clearly distinct relation (different column types) so the
    // leaf-source signature differs after attribute-ID stripping.
    val rel2 = LocalRelation($"c1".long, $"c2".long, $"x".long)
    val sumX2 = Alias(
      AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sumx")()
    val inner2alt = Aggregate(
      groupingExpressions = Seq($"c1", $"c2"),
      aggregateExpressions = Seq($"c1", $"c2", sumX2),
      child = rel2)
    val sumX3 = Alias(
      AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sumx")()
    val inner3alt = Aggregate(
      groupingExpressions = Seq($"c1", $"c2"),
      aggregateExpressions = Seq($"c1", $"c2", sumX3),
      child = rel2)
    val inner1 = innerAgg(rel)
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      outerBranch(Seq("c1"), inner2alt),
      outerBranch(Seq.empty, inner3alt)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when outer aggregate is AVG (non-additive)") {
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    // outer branch uses AVG instead of SUM -- not safely foldable.
    val innerSumx2 = inner2.output.find(_.name == "sumx").get
    val avgSumx = Alias(
      AggregateExpression(Average(innerSumx2), Complete, isDistinct = false), "sumx")()
    val avgBranch = Aggregate(
      groupingExpressions = Seq(inner2.output.find(_.name == "c1").get),
      aggregateExpressions = Seq(
        Alias(inner2.output.find(_.name == "c1").get, "c1")(),
        Alias(Literal(null, IntegerType), "c2")(),
        avgSumx),
      child = inner2)
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      avgBranch,
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("idempotency: re-applying the rule yields the same plan") {
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      outerBranch(Seq("c1"), inner2),
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val once = Optimize.execute(analyzed)
    val twice = Optimize.execute(once)
    comparePlans(once, twice)
  }

  test("does NOT rewrite when outer SUM references a non-inner-SUM column") {
    // Outer branch's SUM argument is c1 (an inner GROUP BY column), not the
    // inner SUM's output -- sum-of-c1 across rollup levels is NOT equivalent
    // to sum(x) folded through ROLLUP.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val innerC1 = inner2.output.find(_.name == "c1").get
    val sumC1 = Alias(
      AggregateExpression(Sum(innerC1), Complete, isDistinct = false), "sumx")()
    val wrongColBranch = Aggregate(
      groupingExpressions = Seq(inner2.output.find(_.name == "c1").get),
      aggregateExpressions = Seq(
        Alias(inner2.output.find(_.name == "c1").get, "c1")(),
        Alias(Literal(null, IntegerType), "c2")(),
        sumC1),
      child = inner2)
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      wrongColBranch,
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when a branch has non-null literal output (level indicator)") {
    // q36a-shape: branches encode rollup level via per-branch literal
    // constants like `0 as t_class`. Pure ROLLUP cannot reconstruct these.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val innerSumx = inner2.output.find(_.name == "sumx").get
    val literalBranch = Aggregate(
      groupingExpressions = Seq(inner2.output.find(_.name == "c1").get),
      aggregateExpressions = Seq(
        Alias(inner2.output.find(_.name == "c1").get, "c1")(),
        Alias(Literal(1, IntegerType), "c2")(),  // <-- non-null literal: rejected
        Alias(AggregateExpression(Sum(innerSumx), Complete, isDistinct = false), "sumx")()),
      child = inner2)
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      literalBranch,
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when SQLConf disabled") {
    withSQLConf(SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED.key -> "false") {
      val inner1 = innerAgg()
      val inner2 = innerAgg()
      val inner3 = innerAgg()
      val unionQuery = Union(Seq(
        rawBranch(inner1),
        outerBranch(Seq("c1"), inner2),
        outerBranch(Seq.empty, inner3)))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertNotRewritten(optimized)
    }
  }

  test("does NOT rewrite when outer aggregate uses DISTINCT") {
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val innerSumx = inner2.output.find(_.name == "sumx").get
    val distinctBranch = Aggregate(
      groupingExpressions = Seq(inner2.output.find(_.name == "c1").get),
      aggregateExpressions = Seq(
        Alias(inner2.output.find(_.name == "c1").get, "c1")(),
        Alias(Literal(null, IntegerType), "c2")(),
        Alias(AggregateExpression(Sum(innerSumx), Complete, isDistinct = true), "sumx")()),
      child = inner2)
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      distinctBranch,
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when the INNER aggregate uses COUNT(DISTINCT) " +
      "folded by outer SUM") {
    // sum-of-count(distinct) is NOT decomposable: sum of per-subgroup distinct
    // counts != distinct count over the union when a value appears in more than
    // one finer subgroup. The inner DISTINCT must be rejected (the existing
    // !isDistinct guard only inspects the OUTER aggregate).
    def innerCountDistinct(): LogicalPlan = {
      val dc = Alias(
        AggregateExpression(Count(Seq($"x")), Complete, isDistinct = true), "dc")()
      Aggregate(Seq($"c1", $"c2"), Seq($"c1", $"c2", dc), rel)
    }
    def outerSumOfDc(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
      val innerDc = inner.output.find(_.name == "dc").get
      val sumDc = Alias(
        AggregateExpression(Sum(innerDc), Complete, isDistinct = false), "dc")()
      val keyOuts = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
        else Alias(Literal(null, IntegerType), n)()
      }
      Aggregate(keepCols.map(n => inner.output.find(_.name == n).get),
        keyOuts :+ sumDc, inner)
    }
    val unionQuery = Union(Seq(
      rawBranch(innerCountDistinct()),
      outerSumOfDc(Seq("c1"), innerCountDistinct()),
      outerSumOfDc(Seq.empty, innerCountDistinct())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when the outer fold is try_sum (TRY eval mode), " +
      "which the reconstructed fold would silently drop") {
    // try_sum(s) returns NULL on overflow; the inner-fold reconstructs the fold
    // function via Sum(attr) at the SESSION default eval mode, dropping TRY and
    // changing overflow behavior (wraparound in LEGACY / throw in ANSI). The
    // rule must reject a non-default (TRY) eval-mode foldable.
    //
    // The inners are session mode on purpose: a TRY inner bails at the foldable
    // census before any outer fold is examined, so the plan would never reach the
    // check this test is named for.
    def outerTrySum(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
      val innerSumx = inner.output.find(_.name == "sumx").get
      val sumS = Alias(AggregateExpression(
        Sum(innerSumx, NumericEvalContext(EvalMode.TRY)), Complete, isDistinct = false),
        "sumx")()
      val keyOuts = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
        else Alias(Literal(null, IntegerType), n)()
      }
      Aggregate(keepCols.map(n => inner.output.find(_.name == n).get), keyOuts :+ sumS, inner)
    }
    val unionQuery = Union(Seq(
      rawBranch(innerAgg()),
      outerTrySum(Seq("c1"), innerAgg()),
      outerTrySum(Seq.empty, innerAgg())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when a rollup level appears twice in the union") {
    // Levels [{c1,c2}, {c1}, {c1}, {}] -- {c1} is duplicated. UNION ALL keeps both
    // copies while buildRollupPlan emits each level once, so accepting this drops
    // a level's worth of rows with no error. isPrefixShrinkingHierarchy is the
    // only thing that looks at level multiplicity, so what this pins is a refactor
    // that dedups groupColSets before comparing.
    val unionQuery = Union(Seq(
      rawBranch(innerAgg()),
      outerBranch(Seq("c1"), innerAgg()),
      outerBranch(Seq("c1"), innerAgg()),
      outerBranch(Seq.empty, innerAgg())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when the grand-total level appears twice in the union") {
    // Same for the GROUP BY () level: buildRollupPlan unions back ONE grand-total
    // branch, so the second copy's row is dropped. This is the case that rests on
    // the size arm alone -- drop `size != n + 1` and the per-index comparison
    // accepts every level, since {} equals both take(0) and take(-1).
    val unionQuery = Union(Seq(
      rawBranch(innerAgg()),
      outerBranch(Seq("c1"), innerAgg()),
      outerBranch(Seq.empty, innerAgg()),
      outerBranch(Seq.empty, innerAgg())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("source-aggregate: does NOT rewrite when a branch re-emits a grouping key " +
      "at a position the reference fills with NULL") {
    // The middle branch emits c1 twice: at c1's own position and again where the
    // reference branch has a NULL filler. Every other per-position check accepts
    // that -- a key passthrough and a NULL filler both sign as "KEY" -- so only
    // the NULL-filler arm of verifyKeyPositionsAligned rejects it.
    def sumX: NamedExpression =
      Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "s")()
    val finest = Aggregate(
      Seq($"c1", $"c2"),
      Seq($"c1", $"c2", Alias(Literal(null, IntegerType), "n")(), sumX),
      rel)
    val level1 = Aggregate(
      Seq($"c1"),
      Seq($"c1", Alias(Literal(null, IntegerType), "n2")(), Alias($"c1", "c1")(), sumX),
      rel)
    val grandTotal = Aggregate(
      Nil,
      Seq(Alias(Literal(null, IntegerType), "a0")(),
        Alias(Literal(null, IntegerType), "n3")(),
        Alias(Literal(null, IntegerType), "n4")(), sumX),
      rel)
    val analyzed = Union(Seq(finest, level1, grandTotal)).analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("inner-fold: does NOT rewrite when a FOLD branch's top Project narrows the " +
      "folded measure") {
    // The narrowing cast sits above a FOLD branch (not the raw branch, which the
    // existing narrowing-cast test covers). buildRollupPlan rebuilds levels [N..1]
    // from the aligned inner, so this branch's top Project is discarded along with
    // its truncation.
    val level1 = outerBranch(Seq("c1"), innerAgg())
    val level1Narrowed = Project(
      Seq(level1.output.find(_.name == "c1").get,
        level1.output.find(_.name == "c2").get,
        Alias(Cast(level1.output.find(_.name == "sumx").get, IntegerType), "sumx")()),
      level1)
    val unionQuery = Union(Seq(
      rawBranch(innerAgg()),
      level1Narrowed,
      outerBranch(Seq.empty, innerAgg())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("inner-fold: DOES rewrite when the narrowing cast is inside the shared inner, " +
      "which the rewrite preserves verbatim") {
    // The truncation lives in the INNER aggregate's own outputs, and buildRollupPlan
    // keeps that aggregate as the Expand child -- so both plans truncate in the same
    // place and the rewrite is correct. This pins the narrow half of
    // verifyDiscardedWrapperCastsAreUpCasts: it must skip a raw branch's first
    // Aggregate, which IS the shared inner, rather than reject it for narrowing.
    def innerWithNarrowedMeasure(): LogicalPlan = {
      val sumX = Alias(
        Cast(AggregateExpression(Sum($"x"), Complete, isDistinct = false), IntegerType),
        "sumx")()
      Aggregate(Seq($"c1", $"c2"), Seq($"c1", $"c2", sumX), rel)
    }
    val unionQuery = Union(Seq(
      rawBranch(innerWithNarrowedMeasure()),
      outerBranch(Seq("c1"), innerWithNarrowedMeasure()),
      outerBranch(Seq.empty, innerWithNarrowedMeasure())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertRewrittenToRollup(optimized)
  }

  test("inner-fold: does NOT rewrite when a FOLD branch's top Project truncates the " +
      "folded measure and casts it back") {
    // The nastiest variant: CAST(CAST(sumx AS INT) AS BIGINT) truncates but ends at
    // the ORIGINAL type, so every branch agrees on bigint, the analyzer adds no
    // coercion Project, and the remap's canUpCast bail sees matching types. Only a
    // check that looks INSIDE the wrapper chain can see the truncation.
    val level1 = outerBranch(Seq("c1"), innerAgg())
    val roundTripped = Project(
      Seq(level1.output.find(_.name == "c1").get,
        level1.output.find(_.name == "c2").get,
        Alias(
          Cast(Cast(level1.output.find(_.name == "sumx").get, IntegerType), LongType),
          "sumx")()),
      level1)
    val unionQuery = Union(Seq(
      rawBranch(innerAgg()),
      roundTripped,
      outerBranch(Seq.empty, innerAgg())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("source-aggregate: does NOT rewrite when only SOME branches wrap a measure " +
      "in a narrowing cast") {
    // CAST(sum(x) AS INT) is narrowing (Sum over int is bigint) and sits ABOVE the
    // aggregate, where buildRollupPlan -- which replays only the aggregate's own
    // expressions -- drops it. The remap's canUpCast bail does not catch this:
    // with the cast on ONE branch the union type widens back to bigint, so the
    // remap is a type-level no-op and the truncation is lost for the rebuilt level.
    def sumX: NamedExpression =
      Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sx")()
    val finestAgg = Aggregate(Seq($"c1", $"c2"), Seq($"c1", $"c2", sumX), rel)
    val finest = Project(
      Seq(finestAgg.output.find(_.name == "c1").get,
        finestAgg.output.find(_.name == "c2").get,
        Alias(Cast(finestAgg.output.find(_.name == "sx").get, IntegerType), "sx")()),
      finestAgg)
    val level1 = Aggregate(
      Seq($"c1"), Seq($"c1", Alias(Literal(null, IntegerType), "c2")(), sumX), rel)
    val grandTotal = Aggregate(
      Nil,
      Seq(Alias(Literal(null, IntegerType), "c1")(),
        Alias(Literal(null, IntegerType), "c2")(), sumX),
      rel)
    val analyzed = Union(Seq(finest, level1, grandTotal)).analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when a Sum's captured eval mode differs from the " +
      "session eval mode (ANSI-captured sum under a LEGACY session)") {
    // A view body analyzed under ANSI captures Sum(evalMode=ANSI); read under a
    // LEGACY session the rule would rebuild Sum at LEGACY, changing overflow
    // behavior (ANSI throws, LEGACY wraps). The session here is LEGACY (ansi
    // off), so an explicitly ANSI-captured Sum must be rejected.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      def ansiSumAgg(): LogicalPlan = {
        val s = Alias(AggregateExpression(
          Sum($"x", NumericEvalContext(EvalMode.ANSI)), Complete, isDistinct = false), "sumx")()
        Aggregate(Seq($"c1", $"c2"), Seq($"c1", $"c2", s), rel)
      }
      def outerAnsiSum(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
        val innerSumx = inner.output.find(_.name == "sumx").get
        val sumS = Alias(AggregateExpression(
          Sum(innerSumx, NumericEvalContext(EvalMode.ANSI)), Complete, isDistinct = false),
          "sumx")()
        val keyOuts = Seq("c1", "c2").map { n =>
          if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
          else Alias(Literal(null, IntegerType), n)()
        }
        Aggregate(keepCols.map(n => inner.output.find(_.name == n).get), keyOuts :+ sumS, inner)
      }
      val unionQuery = Union(Seq(
        rawBranch(ansiSumAgg()),
        outerAnsiSum(Seq("c1"), ansiSumAgg()),
        outerAnsiSum(Seq.empty, ansiSumAgg())))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertNotRewritten(optimized)
    }
  }

  test("does NOT rewrite when an outer fold aggregate carries a FILTER") {
    // The outer fold is `sum(sumx) FILTER (WHERE sumx > 100)`. buildRollupPlan
    // emits the inner's UNFILTERED sum(x) for every level, dropping the FILTER
    // and changing results. The outer FILTER must be rejected.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    def outerFilteredFold(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
      val innerSumx = inner.output.find(_.name == "sumx").get
      val filteredSum = Alias(
        AggregateExpression(Sum(innerSumx), Complete, isDistinct = false,
          filter = Some(GreaterThan(innerSumx, Literal(100L)))), "sumx")()
      val keyOuts = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
        else Alias(Literal(null, IntegerType), n)()
      }
      Aggregate(keepCols.map(n => inner.output.find(_.name == n).get),
        keyOuts :+ filteredSum, inner)
    }
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      outerFilteredFold(Seq("c1"), inner2),
      outerFilteredFold(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when an inner fold aggregate carries a FILTER") {
    // Inner is `sum(x) FILTER (WHERE x > 5)`. The rule reuses the inner measure
    // for every level; to stay conservative (the filter's level-invariance is
    // not validated) a filtered inner foldable must be rejected.
    def innerFiltered(): LogicalPlan = {
      val s = Alias(
        AggregateExpression(Sum($"x"), Complete, isDistinct = false,
          filter = Some(GreaterThan($"x", Literal(5)))), "sumx")()
      Aggregate(Seq($"c1", $"c2"), Seq($"c1", $"c2", s), rel)
    }
    val unionQuery = Union(Seq(
      rawBranch(innerFiltered()),
      outerBranch(Seq("c1"), innerFiltered()),
      outerBranch(Seq.empty, innerFiltered())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when the full-detail branch applies a VALUE-CHANGING " +
      "cast to a measure (narrowing) that the rewrite would drop") {
    // The full-detail (raw) branch projects CAST(sumx AS decimal(17,0)) AS sumx
    // -- a NARROWING cast that rounds/changes the value. stripProject removes
    // this Project and the rewrite emits the inner's un-cast sumx, so the
    // finest-level value would differ. The rule must not silently drop it.
    // (Contrast the legitimate analyzer WIDENING coercion cast, which the rule
    // correctly looks through -- see the Cast-wrapper rewrite test below.)
    val decRel = LocalRelation($"c1".int, $"c2".int, $"amt".decimal(7, 2))
    def innerDec(): LogicalPlan = {
      val s = Alias(AggregateExpression(Sum($"amt"), Complete, isDistinct = false), "tot")()
      Aggregate(Seq($"c1", $"c2"), Seq($"c1", $"c2", s), decRel)
    }
    val inner1 = innerDec(); val inner2 = innerDec(); val inner3 = innerDec()
    val narrowingRaw = {
      val innerTot = inner1.output.find(_.name == "tot").get  // Decimal(17,2)
      Project(Seq(
        inner1.output.find(_.name == "c1").get,
        inner1.output.find(_.name == "c2").get,
        Alias(Cast(innerTot, DecimalType(17, 0)), "tot")()), inner1)  // narrow scale 2->0
    }
    def outerDecFold(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
      val innerTot = inner.output.find(_.name == "tot").get
      val sumTot = Alias(AggregateExpression(Sum(innerTot), Complete, isDistinct = false), "tot")()
      val keyOuts = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
        else Alias(Literal(null, IntegerType), n)()
      }
      Aggregate(keepCols.map(n => inner.output.find(_.name == n).get), keyOuts :+ sumTot, inner)
    }
    val unionQuery = Union(Seq(
      narrowingRaw,
      outerDecFold(Seq("c1"), inner2),
      outerDecFold(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    // The narrowing cast must not be silently dropped. Which check rejects it is
    // deliberately not named: disabling verifyConsistentRoleLayout and
    // verifyInnerFoldMeasurePositions, together or one at a time, all leave this test
    // green, so at least one further check rejects this shape on its own. Do not name
    // a rejector without disabling it and watching this test go red.
    assertNotRewritten(optimized)
  }

  test("does NOT rewrite when inner aggregate's source is non-deterministic") {
    // Inner aggregate's child has a non-deterministic Filter. Folding to a
    // single ROLLUP would evaluate the filter once instead of once per branch,
    // potentially producing different rows.
    val filteredRel = Filter(GreaterThan(Rand(0), Literal(0.5)), rel)
    def innerOverNonDet(): LogicalPlan = innerAgg(filteredRel)
    val inner1 = innerOverNonDet()
    val inner2 = innerOverNonDet()
    val inner3 = innerOverNonDet()
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      outerBranch(Seq("c1"), inner2),
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("rewrites when raw branch has Cast wrapper on SUM output (real-data shape)") {
    // Spark's Union type-coercion wraps the raw branch's SUM in a Cast when
    // sum-of-sum in other branches widens the type. The rule must look
    // through the Cast wrapper.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val rawWithCast = {
      val innerSumx = inner1.output.find(_.name == "sumx").get
      val widenedCast = Alias(
        Cast(innerSumx, DecimalType(38, 2)), "sumx")()
      Project(Seq(
        Alias(inner1.output.find(_.name == "c1").get, "c1")(),
        Alias(inner1.output.find(_.name == "c2").get, "c2")(),
        widenedCast), inner1)
    }
    val unionQuery = Union(Seq(
      rawWithCast,
      outerBranch(Seq("c1"), inner2),
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertRewrittenToRollup(optimized)  // rule should look through Cast wrapper
  }

  test("rewrites when outer aggregate has auto-named SUM (`sum(col)` style)") {
    // When SQL omits aliasing the outer SUM, Spark auto-names it "sum(col)".
    // The rule's acceptableNames must include these auto-names to validate
    // the branch's effective output.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val innerSumx2 = inner2.output.find(_.name == "sumx").get
    // Auto-named (no explicit alias) -> Alias child is unnamed expression
    // but the analyzer assigns a default name "sum(sumx)".
    val autoNamedSum = Alias(
      AggregateExpression(Sum(innerSumx2), Complete, isDistinct = false),
      "sum(sumx)")()
    val branchWithAutoName = Aggregate(
      groupingExpressions = Seq(inner2.output.find(_.name == "c1").get),
      aggregateExpressions = Seq(
        Alias(inner2.output.find(_.name == "c1").get, "c1")(),
        Alias(Literal(null, IntegerType), "c2")(),
        autoNamedSum),
      child = inner2)
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      branchWithAutoName,
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertRewrittenToRollup(optimized)  // rule recognizes `sum(sumx)` auto-name
  }

  /** Inner aggregate using MAX (or MIN if `useMin = true`) instead of SUM. */
  private def innerAggWith(
      aggFnName: String, child: LogicalPlan = rel): LogicalPlan = {
    val xCol = child.output.find(_.name == "x").get
    val aggFn = aggFnName match {
      case "max" => Max(xCol)
      case "min" => Min(xCol)
      case "sum" => Sum(xCol)
    }
    val aggAlias = Alias(
      AggregateExpression(aggFn, Complete, isDistinct = false), "aggx")()
    Aggregate(
      groupingExpressions = Seq($"c1", $"c2"),
      aggregateExpressions = Seq($"c1", $"c2", aggAlias),
      child = child)
  }

  private def outerBranchWith(
      aggFnName: String, keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
    val innerAggx = inner.output.find(_.name == "aggx").get
    val outerFn = aggFnName match {
      case "max" => Max(innerAggx)
      case "min" => Min(innerAggx)
      case "sum" => Sum(innerAggx)
    }
    val outerAggAlias = Alias(
      AggregateExpression(outerFn, Complete, isDistinct = false), "aggx")()
    val groupCols: Seq[Expression] =
      keepCols.map(n => inner.output.find(_.name == n).get)
    val allOutputCols = Seq("c1", "c2").map { n =>
      if (keepCols.contains(n)) {
        Alias(inner.output.find(_.name == n).get, n)()
      } else {
        Alias(Literal(null, IntegerType), n)()
      }
    } :+ outerAggAlias
    Aggregate(groupCols, allOutputCols, inner)
  }

  test("rewrites MAX rollup pattern") {
    val inner1 = innerAggWith("max")
    val inner2 = innerAggWith("max")
    val inner3 = innerAggWith("max")
    val unionQuery = Union(Seq(
      inner1,
      outerBranchWith("max", Seq("c1"), inner2),
      outerBranchWith("max", Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertRewrittenToRollup(optimized)  // MAX-of-MAX rollup rewrite
  }

  test("rewrites COUNT rollup pattern (outer SUM-of-COUNT fold)") {
    // Inner: count(x) AS aggx. Outer branches: sum(aggx) (NOT count-of-count).
    val xCol = rel.output.find(_.name == "x").get
    def innerCountAgg(): LogicalPlan = {
      val countAlias = Alias(
        AggregateExpression(Count(Seq(xCol)), Complete, isDistinct = false), "aggx")()
      Aggregate(
        groupingExpressions = Seq($"c1", $"c2"),
        aggregateExpressions = Seq($"c1", $"c2", countAlias),
        child = rel)
    }
    val inner1 = innerCountAgg()
    val inner2 = innerCountAgg()
    val inner3 = innerCountAgg()
    val unionQuery = Union(Seq(
      inner1,
      outerBranchWith("sum", Seq("c1"), inner2),  // outer SUM over inner Count
      outerBranchWith("sum", Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertRewrittenToRollup(optimized)  // SUM-of-COUNT rollup rewrite
  }

  test("does NOT rewrite when outer uses COUNT but inner uses COUNT (count-of-count)") {
    val xCol = rel.output.find(_.name == "x").get
    def innerCountAgg(): LogicalPlan = {
      val countAlias = Alias(
        AggregateExpression(Count(Seq(xCol)), Complete, isDistinct = false), "aggx")()
      Aggregate(
        groupingExpressions = Seq($"c1", $"c2"),
        aggregateExpressions = Seq($"c1", $"c2", countAlias),
        child = rel)
    }
    val inner1 = innerCountAgg()
    val inner2 = innerCountAgg()
    val inner3 = innerCountAgg()
    val innerAggx = inner2.output.find(_.name == "aggx").get
    val countOfCount = Alias(
      AggregateExpression(Count(Seq(innerAggx)), Complete, isDistinct = false), "aggx")()
    val invalidBranch = Aggregate(
      groupingExpressions = Seq(inner2.output.find(_.name == "c1").get),
      aggregateExpressions = Seq(
        Alias(inner2.output.find(_.name == "c1").get, "c1")(),
        Alias(Literal(null, IntegerType), "c2")(),
        countOfCount),
      child = inner2)
    val unionQuery = Union(Seq(
      inner1,
      invalidBranch,
      outerBranchWith("sum", Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("rewrites MIN rollup pattern") {
    val inner1 = innerAggWith("min")
    val inner2 = innerAggWith("min")
    val inner3 = innerAggWith("min")
    val unionQuery = Union(Seq(
      inner1,
      outerBranchWith("min", Seq("c1"), inner2),
      outerBranchWith("min", Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertRewrittenToRollup(optimized)  // MIN-of-MIN rollup rewrite
  }

  test("does NOT rewrite when outer uses SUM but inner uses MAX (function mismatch)") {
    // sum(max(x)) is NOT a valid fold; max-of-max would be the fold.
    val inner1 = innerAggWith("max")
    val inner2 = innerAggWith("max")
    val inner3 = innerAggWith("max")
    val unionQuery = Union(Seq(
      inner1,
      outerBranchWith("sum", Seq("c1"), inner2),  // wrong function!
      outerBranchWith("max", Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("rewrites regardless of branch order") {
    // Grand total first, full grouping last -- the rule should sort and
    // recognize the hierarchy.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val unionQuery = Union(Seq(
      outerBranch(Seq.empty, inner1),      // grand total first
      outerBranch(Seq("c1"), inner2),  // middle
      rawBranch(inner3)))                  // full grouping last
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertRewrittenToRollup(optimized)  // rewrites regardless of branch order
  }

  test("does NOT rewrite a STREAMING union of aggregates") {
    // The rewrite collapses N+1 stateful streaming aggregates into 2,
    // changing the checkpointed operator topology, and its firing depends on
    // session state (timezone/eval-mode gates) -- a restart under a different
    // session could mis-bind operator state. The rule must bail on streaming
    // plans like its topology-changing batch siblings do. This shape passes
    // every other guard (it is the plain firing shape), so the isStreaming
    // gate is the only rejector.
    val streamRel = LocalRelation(
      Seq($"c1".int, $"c2".int, $"x".int), Nil, isStreaming = true)
    def srcStream(keepCols: Seq[String]): LogicalPlan = {
      def attr(n: String): Attribute = streamRel.output.find(_.name == n).get
      val s = Alias(AggregateExpression(
        Sum(attr("x")), Complete, isDistinct = false), "sx")()
      val outs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias(attr(n), n)()
        else Alias(Literal(null, IntegerType), n)()
      } :+ s
      Aggregate(keepCols.map(n => attr(n): Expression), outs, streamRel)
    }
    val unionQuery = Union(Seq(
      srcStream(Seq("c1", "c2")),
      srcStream(Seq("c1")),
      srcStream(Seq.empty)))
    val optimized = Optimize.execute(unionQuery.analyze)
    assertNotRewritten(optimized)
  }

  test("source-aggregate: POSITIVE CONTROL -- the eval-mode builder shape fires " +
      "when all branches agree") {
    // Same builder shape as the eval-mode negative tests below, with every
    // branch LEGACY: it must FIRE. This pins that those negatives are
    // non-vacuous -- if a future guard change rejected the builder shape for an
    // unrelated reason, the negatives would pass without exercising the
    // signature components they exist to kill, and THIS test would fail.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      def srcSumCtl(keepCols: Seq[String]): LogicalPlan = {
        val s = Alias(AggregateExpression(
          Sum($"x", NumericEvalContext(EvalMode.LEGACY)), Complete, isDistinct = false), "sx")()
        val outs = Seq("c1", "c2").map { n =>
          if (keepCols.contains(n)) Alias($"$n", n)()
          else Alias(Literal(null, IntegerType), n)()
        } :+ s
        Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
      }
      val unionQuery = Union(Seq(
        srcSumCtl(Seq("c1", "c2")),
        srcSumCtl(Seq("c1")),
        srcSumCtl(Seq.empty)))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertRewrittenToRollup(optimized)
    }
  }

  test("source-aggregate: does NOT rewrite when branches' stddev_samp differ " +
      "only in the hidden nullOnDivideByZero flag") {
    // StddevSamp's stringArgs filters to Expressions only, hiding the
    // analysis-captured nullOnDivideByZero flag (NaN vs NULL on single-row
    // groups) from toString and so from every string-based comparator; a temp
    // view can freeze one branch's flag while inline branches differ. Without
    // the hiddenStateMarkers arm these branches collide and the rewrite
    // replays one flag for the other's level.
    def srcStd(nullOnDiv: Boolean, keepCols: Seq[String]): LogicalPlan = {
      val s = Alias(AggregateExpression(
        StddevSamp($"x", nullOnDivideByZero = nullOnDiv), Complete, isDistinct = false), "sd")()
      val outs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias($"$n", n)()
        else Alias(Literal(null, IntegerType), n)()
      } :+ s
      Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
    }
    val unionQuery = Union(Seq(
      srcStd(nullOnDiv = true, Seq("c1", "c2")),
      srcStd(nullOnDiv = false, Seq("c1")),
      srcStd(nullOnDiv = true, Seq.empty)))
    val optimized = Optimize.execute(unionQuery.analyze)
    assertNotRewritten(optimized)
  }

  test("source-aggregate: does NOT rewrite when branches' abs() differ only " +
      "in the hidden failOnError flag") {
    // Abs overrides flatArguments to Iterator(child), hiding the ANSI
    // failOnError flag (throw vs wrap on MinValue) from toString. Under
    // Max/Min/Count no other marker incidentally diverges, so only the
    // dedicated absfail marker rejects the mismatch.
    def srcAbs(fail: Boolean, keepCols: Seq[String]): LogicalPlan = {
      val m = Alias(AggregateExpression(
        Max(Abs($"x", failOnError = fail)), Complete, isDistinct = false), "m")()
      val outs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias($"$n", n)()
        else Alias(Literal(null, IntegerType), n)()
      } :+ m
      Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
    }
    val unionQuery = Union(Seq(
      srcAbs(fail = true, Seq("c1", "c2")),
      srcAbs(fail = false, Seq("c1")),
      srcAbs(fail = true, Seq.empty)))
    val optimized = Optimize.execute(unionQuery.analyze)
    assertNotRewritten(optimized)
  }

  test("source-aggregate: does NOT rewrite when branches' covar_pop differ " +
      "only in the hidden nullOnDivideByZero flag") {
    // Covariance's stringArgs filters to Expressions only, hiding
    // nullOnDivideByZero (NaN vs NULL) -- the covdiv0 marker arm. Same
    // mechanism as the stddev_samp test above; Corr (corrdiv0) is the same
    // arm family.
    def srcCov(nullOnDiv: Boolean, keepCols: Seq[String]): LogicalPlan = {
      val s = Alias(AggregateExpression(
        CovPopulation($"x", $"c1", nullOnDivideByZero = nullOnDiv),
        Complete, isDistinct = false), "cv")()
      val outs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias($"$n", n)()
        else Alias(Literal(null, IntegerType), n)()
      } :+ s
      Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
    }
    val unionQuery = Union(Seq(
      srcCov(nullOnDiv = true, Seq("c1", "c2")),
      srcCov(nullOnDiv = false, Seq("c1")),
      srcCov(nullOnDiv = true, Seq.empty)))
    assertNotRewritten(Optimize.execute(unionQuery.analyze))
  }

  test("source-aggregate: does NOT rewrite when branches' unary minus differ " +
      "only in the hidden failOnError flag") {
    // UnaryMinus renders as "-$child", hiding failOnError (ANSI throw vs wrap
    // on MinValue) -- the negfail marker arm. Under Max no other marker
    // diverges.
    def srcNeg(fail: Boolean, keepCols: Seq[String]): LogicalPlan = {
      val m = Alias(AggregateExpression(
        Max(UnaryMinus($"x", failOnError = fail)), Complete, isDistinct = false), "m")()
      val outs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias($"$n", n)()
        else Alias(Literal(null, IntegerType), n)()
      } :+ m
      Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
    }
    val unionQuery = Union(Seq(
      srcNeg(fail = true, Seq("c1", "c2")),
      srcNeg(fail = false, Seq("c1")),
      srcNeg(fail = true, Seq.empty)))
    assertNotRewritten(Optimize.execute(unionQuery.analyze))
  }

  test("source-aggregate: does NOT rewrite when branches' round() differ only " +
      "in the hidden ansiEnabled flag") {
    // Round overrides flatArguments to (child, scale), hiding ansiEnabled
    // (negative-scale integral overflow: throw vs wrap) -- the roundansi
    // marker arm. GetArrayItem (getitemfail) / ArrayExists (exists3vl) are the
    // same hidden-ANSI/flag mechanism.
    def srcRound(ansi: Boolean, keepCols: Seq[String]): LogicalPlan = {
      val m = Alias(AggregateExpression(
        Max(Round($"x", Literal(0), ansiEnabled = ansi)), Complete, isDistinct = false), "m")()
      val outs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias($"$n", n)()
        else Alias(Literal(null, IntegerType), n)()
      } :+ m
      Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
    }
    val unionQuery = Union(Seq(
      srcRound(ansi = true, Seq("c1", "c2")),
      srcRound(ansi = false, Seq("c1")),
      srcRound(ansi = true, Seq.empty)))
    assertNotRewritten(Optimize.execute(unionQuery.analyze))
  }

  test("source-aggregate: does NOT rewrite when branches' same-named udf " +
      "carries DIFFERENT function objects; fires when they are the same") {
    // ScalaUDF's toString renders only the udf NAME; the function closure is
    // a non-child constructor field invisible to every comparator. A temp
    // view freezes the resolved function object, so a same-named
    // re-registered udf in another branch must not be conflated. The marker
    // compares function-object IDENTITY: plan copies share the instance
    // (fires), different registrations differ (rejected).
    val f1: AnyRef = (x: Int) => x + 1
    val f2: AnyRef = (x: Int) => x * 100
    def srcUdf(fn: AnyRef, keepCols: Seq[String]): LogicalPlan = {
      val udf = ScalaUDF(fn, IntegerType, Seq($"x"), Nil, udfName = Some("f"))
      val m = Alias(AggregateExpression(
        Max(udf), Complete, isDistinct = false), "m")()
      val outs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias($"$n", n)()
        else Alias(Literal(null, IntegerType), n)()
      } :+ m
      Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
    }
    val mixed = Union(Seq(
      srcUdf(f1, Seq("c1", "c2")),
      srcUdf(f2, Seq("c1")),
      srcUdf(f1, Seq.empty)))
    assertNotRewritten(Optimize.execute(mixed.analyze))
    val uniform = Union(Seq(
      srcUdf(f1, Seq("c1", "c2")),
      srcUdf(f1, Seq("c1")),
      srcUdf(f1, Seq.empty)))
    assertRewrittenToRollup(Optimize.execute(uniform.analyze))
  }

  test("source-aggregate: does NOT rewrite when measures differ only in " +
      "same-class ARITY hidden beyond the toString truncation limit") {
    // concat_ws(a, concat_ws(b, z)) and concat_ws(a, concat_ws(b), z) share
    // the pre-order class sequence, attribute order, and literals, and differ
    // only in arity (2/2 vs 3/1) -- semantically "z" vs a+"z". Hidden behind
    // 25 leading concat arguments, toString truncation erases the difference;
    // only the class:ARITY encoding distinguishes them.
    val wideCols = (1 to 25).map(i => AttributeReference(s"s$i", StringType)())
    val wideRel = LocalRelation(
      Seq(AttributeReference("c1", IntegerType)(), AttributeReference("c2", IntegerType)(),
        AttributeReference("a", StringType)(), AttributeReference("b", StringType)(),
        AttributeReference("z", StringType)()) ++ wideCols)
    def attr(name: String): Attribute = wideRel.output.find(_.name == name).get
    def srcWide(nested: Boolean, keepCols: Seq[String]): LogicalPlan = {
      val cw =
        if (nested) {
          ConcatWs(Seq(attr("a"), ConcatWs(Seq(attr("b"), attr("z")))))
        } else {
          ConcatWs(Seq(attr("a"), ConcatWs(Seq(attr("b"))), attr("z")))
        }
      val arg = Concat(wideCols.map(c => c: Expression) :+ cw)
      val m = Alias(AggregateExpression(
        Max(Length(arg)), Complete, isDistinct = false), "m")()
      val outs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias(attr(n), n)()
        else Alias(Literal(null, IntegerType), n)()
      } :+ m
      Aggregate(keepCols.map(n => attr(n): Expression), outs, wideRel)
    }
    val unionQuery = Union(Seq(
      srcWide(nested = true, Seq("c1", "c2")),
      srcWide(nested = false, Seq("c1")),
      srcWide(nested = true, Seq.empty)))
    val optimized = Optimize.execute(unionQuery.analyze)
    assertNotRewritten(optimized)
  }

  test("source-aggregate: does NOT rewrite when branches' sums differ only in " +
      "eval mode (ANSI vs LEGACY) -- toString-blind signature must catch it") {
    // Source path reuses the reference branch's sum VERBATIM for all levels. If
    // a sibling branch's sum has a DIFFERENT captured eval mode (e.g. an
    // ANSI-analyzed view branch unioned with a LEGACY session branch over the
    // same source), reusing the reference's mode changes that level's overflow
    // behavior (ANSI throws / LEGACY wraps). Sum.toString hides the eval mode,
    // so aggMeasureSignature must encode it to reject the mismatch.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      def srcSum(mode: EvalMode.Value, keepCols: Seq[String]): LogicalPlan = {
        val s = Alias(AggregateExpression(
          Sum($"x", NumericEvalContext(mode)), Complete, isDistinct = false), "sx")()
        val outs = Seq("c1", "c2").map { n =>
          if (keepCols.contains(n)) Alias($"$n", n)()
          else Alias(Literal(null, IntegerType), n)()
        } :+ s
        Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
      }
      // refBranch (largest) LEGACY; mid branch ANSI; grand total LEGACY.
      val unionQuery = Union(Seq(
        srcSum(EvalMode.LEGACY, Seq("c1", "c2")),
        srcSum(EvalMode.ANSI, Seq("c1")),
        srcSum(EvalMode.LEGACY, Seq.empty)))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertNotRewritten(optimized)
    }
  }

  test("source-aggregate: does NOT rewrite when branches' sums differ only in " +
      "a NESTED CAST's eval mode (Cast.toString hides ANSI vs LEGACY)") {
    // sum(cast(x as long)) where the inner Cast is ANSI in one branch and LEGACY
    // in another. Cast.toString renders both as `cast(... as bigint)`, so a
    // toString-only signature collides and the source path would reuse one
    // branch's cast mode for all levels, changing overflow behavior. The
    // signature must encode nested cast eval modes.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      def srcSumCast(mode: EvalMode.Value, keepCols: Seq[String]): LogicalPlan = {
        val castX = Cast($"x", LongType, None, mode)
        val s = Alias(AggregateExpression(Sum(castX), Complete, isDistinct = false), "sx")()
        val outs = Seq("c1", "c2").map { n =>
          if (keepCols.contains(n)) Alias($"$n", n)()
          else Alias(Literal(null, IntegerType), n)()
        } :+ s
        Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
      }
      val unionQuery = Union(Seq(
        srcSumCast(EvalMode.LEGACY, Seq("c1", "c2")),
        srcSumCast(EvalMode.ANSI, Seq("c1")),
        srcSumCast(EvalMode.LEGACY, Seq.empty)))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertNotRewritten(optimized)
    }
  }

  test("source-aggregate: does NOT rewrite when branch SOURCES differ only in " +
      "a cast eval mode (below the aggregate, invisible to the measure sig)") {
    // The cast lives in a Project in the SOURCE (below the per-branch Aggregate),
    // so the measure signature (which inspects only aggregateExpressions) cannot
    // see it. canonicalHash(source) must be eval-mode-aware, else two sources
    // differing only in the cast's ANSI/LEGACY mode collide and the rule reuses
    // one source's mode for all levels (changing overflow behavior).
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      // NARROWING cast (bigint -> int) so ANSI vs LEGACY genuinely differ on
      // overflow (a widening cast would be eval-mode-irrelevant).
      val relL = LocalRelation($"c1".int, $"c2".int, $"xl".long)
      def srcWithCastMode(mode: EvalMode.Value, keepCols: Seq[String]): LogicalPlan = {
        val src = Project(
          Seq(relL.output.find(_.name == "c1").get, relL.output.find(_.name == "c2").get,
            Alias(Cast(relL.output.find(_.name == "xl").get, IntegerType, None, mode), "v")()),
          relL)
        val v = src.output.find(_.name == "v").get
        val s = Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sx")()
        val outs = Seq("c1", "c2").map { n =>
          if (keepCols.contains(n)) Alias(src.output.find(_.name == n).get, n)()
          else Alias(Literal(null, IntegerType), n)()
        } :+ s
        Aggregate(keepCols.map(n => src.output.find(_.name == n).get), outs, src)
      }
      val unionQuery = Union(Seq(
        srcWithCastMode(EvalMode.LEGACY, Seq("c1", "c2")),
        srcWithCastMode(EvalMode.ANSI, Seq("c1")),
        srcWithCastMode(EvalMode.LEGACY, Seq.empty)))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertNotRewritten(optimized)
    }
  }

  test("source-aggregate: does NOT rewrite when branch SOURCES differ only in " +
      "a needs-timezone cast's timeZoneId (hidden in toString/canonicalized)") {
    // A timestamp->date cast bucket key (needsTimeZone) carries a session
    // timeZoneId that Cast.toString omits and canonicalized PRESERVES (not
    // normalized). Two branches whose key cast uses different timezones bucket
    // rows into different dates, but canonicalHash(agg.child)/exprSignature would
    // collide without a tz marker -> the rule would reuse one timezone for all
    // levels, changing the date grouping. Must be rejected.
    val relTs = LocalRelation($"c1".int, $"ts".timestamp, $"x".int)
    def srcWithTz(tz: String, keepCols: Seq[String]): LogicalPlan = {
      val src = Project(Seq(
        relTs.output.find(_.name == "c1").get,
        Alias(Cast(relTs.output.find(_.name == "ts").get, DateType, Some(tz)), "d")(),
        relTs.output.find(_.name == "x").get), relTs)
      val s = Alias(AggregateExpression(
        Sum(src.output.find(_.name == "x").get), Complete, isDistinct = false), "sx")()
      val gcols = Seq("c1", "d")
      val outs = gcols.map { n =>
        if (keepCols.contains(n)) Alias(src.output.find(_.name == n).get, n)()
        else Alias(Literal(null, if (n == "d") DateType else IntegerType), n)()
      } :+ s
      Aggregate(keepCols.map(n => src.output.find(_.name == n).get), outs, src)
    }
    val unionQuery = Union(Seq(
      srcWithTz("America/Los_Angeles", Seq("c1", "d")),
      srcWithTz("UTC", Seq("c1")),
      srcWithTz("America/Los_Angeles", Seq.empty)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("source-aggregate: does NOT rewrite when branches' measures differ only " +
      "in a nested ARITHMETIC eval mode (sum(a+b) ANSI vs LEGACY)") {
    // BinaryArithmetic carries evalContext (eval mode) but BinaryOperator
    // toString renders only `(a + b)` -- the mode is hidden, like Cast. Two
    // branches sum(a+b)[ANSI] vs [LEGACY] would collide and the rule would reuse
    // one mode for all levels (ANSI throws on overflow, LEGACY wraps). The
    // signature must encode arithmetic eval modes.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val rel2 = LocalRelation($"c1".int, $"c2".int, $"a".long, $"b".long)
      def srcSumAdd(mode: EvalMode.Value, keepCols: Seq[String]): LogicalPlan = {
        val a = rel2.output.find(_.name == "a").get
        val b = rel2.output.find(_.name == "b").get
        val add = Add(a, b, NumericEvalContext(mode))
        val s = Alias(AggregateExpression(Sum(add), Complete, isDistinct = false), "sx")()
        val outs = Seq("c1", "c2").map { n =>
          if (keepCols.contains(n)) Alias(rel2.output.find(_.name == n).get, n)()
          else Alias(Literal(null, IntegerType), n)()
        } :+ s
        Aggregate(keepCols.map(n => rel2.output.find(_.name == n).get), outs, rel2)
      }
      val unionQuery = Union(Seq(
        srcSumAdd(EvalMode.LEGACY, Seq("c1", "c2")),
        srcSumAdd(EvalMode.ANSI, Seq("c1")),
        srcSumAdd(EvalMode.LEGACY, Seq.empty)))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertNotRewritten(optimized)
    }
  }

  test("source-aggregate: does NOT rewrite when branches' AVG differ only in " +
      "eval mode (Average.evalMode hidden in toString)") {
    // Average carries evalMode but its toString hides it (Spark's own comment).
    // avg(x)[ANSI] vs [LEGACY] collide; the rule would reuse one mode (the
    // internal running-sum Add's overflow behavior differs). Must reject.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      def srcAvg(mode: EvalMode.Value, keepCols: Seq[String]): LogicalPlan = {
        val s = Alias(AggregateExpression(
          Average($"x", mode), Complete, isDistinct = false), "av")()
        val outs = Seq("c1", "c2").map { n =>
          if (keepCols.contains(n)) Alias($"$n", n)()
          else Alias(Literal(null, IntegerType), n)()
        } :+ s
        Aggregate(keepCols.map(n => $"$n": Expression), outs, rel)
      }
      val unionQuery = Union(Seq(
        srcAvg(EvalMode.LEGACY, Seq("c1", "c2")),
        srcAvg(EvalMode.ANSI, Seq("c1")),
        srcAvg(EvalMode.LEGACY, Seq.empty)))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertNotRewritten(optimized)
    }
  }

  test("source-aggregate: does NOT rewrite when an OPAQUE measure (top-level " +
      "Add of aggregates) differs only in arithmetic eval mode") {
    // A top-level `sum(a) + sum(b)` is NOT an AggregateExpression, so it takes
    // the UNSUPPORTED signature branch. That branch must also encode hidden eval
    // state -- else try_add(...) [TRY] vs +(...) [LEGACY] collide and the rule
    // reuses one mode for all levels (TRY -> NULL on overflow vs LEGACY wrap).
    // Reachable in a single plain query: try_add rewrites to Add(...,TRY) before
    // this rule runs.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val rel2 = LocalRelation($"c1".int, $"c2".int, $"a".long, $"b".long)
      def srcOpaqueAdd(mode: EvalMode.Value, keepCols: Seq[String]): LogicalPlan = {
        val a = rel2.output.find(_.name == "a").get
        val b = rel2.output.find(_.name == "b").get
        val sa = AggregateExpression(Sum(a), Complete, isDistinct = false)
        val sb = AggregateExpression(Sum(b), Complete, isDistinct = false)
        // top-level Add of two aggregates -> not an AggregateExpression -> UNSUPPORTED
        val m = Alias(Add(sa, sb, NumericEvalContext(mode)), "m")()
        val outs = Seq("c1", "c2").map { n =>
          if (keepCols.contains(n)) Alias(rel2.output.find(_.name == n).get, n)()
          else Alias(Literal(null, IntegerType), n)()
        } :+ m
        Aggregate(keepCols.map(n => rel2.output.find(_.name == n).get), outs, rel2)
      }
      val unionQuery = Union(Seq(
        srcOpaqueAdd(EvalMode.LEGACY, Seq("c1", "c2")),
        srcOpaqueAdd(EvalMode.TRY, Seq("c1")),
        srcOpaqueAdd(EvalMode.LEGACY, Seq.empty)))
      val analyzed = unionQuery.analyze
      val optimized = Optimize.execute(analyzed)
      assertNotRewritten(optimized)
    }
  }

  test("source-aggregate: does NOT rewrite when a branch swaps measure/key " +
      "output positions") {
    // Source-aggregate branches over the same `rel`. The reference (fullest)
    // branch has output layout [c1, c2, sum(x)]. The middle branch places the
    // measure in position 1 and the NULL filler in position 2 -- a different
    // positional layout. buildRollupPlan would emit the reference layout for
    // every level, so accepting this branch would bind the measure to the
    // wrong output column. The position-sensitive signature must reject it.
    def srcBranch(aggs: Seq[NamedExpression], groupCols: Seq[Expression]): Aggregate =
      Aggregate(groupCols, aggs, rel)
    val full = srcBranch(
      Seq($"c1", $"c2",
        Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sx")()),
      Seq($"c1", $"c2"))
    val swapped = srcBranch(
      Seq($"c1",
        Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sx")(),
        Alias(Literal(null, IntegerType), "c2")()),  // measure/NULL swapped
      Seq($"c1"))
    val grand = srcBranch(
      Seq(Alias(Literal(null, IntegerType), "c1")(),
        Alias(Literal(null, IntegerType), "c2")(),
        Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sx")()),
      Seq.empty)
    val unionQuery = Union(Seq(full, swapped, grand))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT mis-bind when inner aggregate emits measure before keys " +
      "and the raw branch reorders to keys-first") {
    // The inner aggregate emits its outputs as [mx, c1, c2] (measure FIRST).
    // The raw branch projects them back to [c1, c2, mx] (keys-first), so the
    // Union output order is [c1, c2, mx] while the inner aggregate's
    // expression order is [mx, c1, c2]. Using MAX keeps the measure the same
    // type (int) as the keys, so a purely positional output remap would NOT be
    // caught by a type check and would silently bind c1<-max, c2<-c1, mx<-c2.
    // The rule must either reorder correctly or refuse to rewrite.
    def innerMeasureFirst(): LogicalPlan = {
      val maxX = Alias(
        AggregateExpression(Max($"x"), Complete, isDistinct = false), "mx")()
      Aggregate(
        groupingExpressions = Seq($"c1", $"c2"),
        aggregateExpressions = Seq(maxX, $"c1", $"c2"),  // measure FIRST
        child = rel)
    }
    val inner1 = innerMeasureFirst()
    val inner2 = innerMeasureFirst()
    val inner3 = innerMeasureFirst()
    // Raw branch reordered to keys-first [c1, c2, mx].
    val rawReordered = Project(
      Seq(inner1.output.find(_.name == "c1").get,
        inner1.output.find(_.name == "c2").get,
        inner1.output.find(_.name == "mx").get),
      inner1)
    def outerMaxFold(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
      val innerMx = inner.output.find(_.name == "mx").get
      val maxMx = Alias(
        AggregateExpression(Max(innerMx), Complete, isDistinct = false), "mx")()
      val keyOutputs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
        else Alias(Literal(null, IntegerType), n)()
      }
      Aggregate(
        groupingExpressions = keepCols.map(n => inner.output.find(_.name == n).get),
        aggregateExpressions = keyOutputs :+ maxMx,  // keys-first [c1, c2, mx]
        child = inner)
    }
    val unionQuery = Union(Seq(
      rawReordered,
      outerMaxFold(Seq("c1"), inner2),
      outerMaxFold(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    // If the rule rewrites, the output column->expression binding MUST be
    // correct despite the inner/union order mismatch: each remap Alias must
    // wrap an expanded attribute of the SAME name (e.g. the "mx" output must
    // carry the max measure, not a grouping value). A purely positional remap
    // would alias "c1" <- max(x), which is silent corruption. The rewritten
    // shape is Union(Project(remap, Aggregate(Expand)), grandTotal).
    optimized match {
      case Union(Seq(Project(projList, _), _), _, _) =>
        projList.foreach { ne =>
          val referenced = ne.collect { case a: AttributeReference => a.name }.distinct
          assert(referenced == Seq(ne.name),
            s"Output column '${ne.name}' is bound to $referenced (expected a " +
              s"same-named expression). Positional mis-binding detected:\n" +
              s"${optimized.treeString}")
        }
      case other =>
        // Refusing to rewrite is also acceptable (no corruption).
        assertNotRewritten(other)
    }
  }

  test("inner-fold decimal sum-of-sum rolls up OVER the inner aggregate, " +
      "preserving the original two-level decimal accumulation") {
    // Original is a TWO-level decimal accumulation: inner sum(amt: Dec(7,2))
    // -> s: Dec(17,2); outer sum(s) -> Dec(27,2). The rewrite must roll up the
    // OUTER fold (sum(s)) OVER the inner aggregate's output `s` (Dec(17,2)),
    // reproducing both levels exactly -- including the inner level's Dec(17,2)
    // overflow/NULL behavior. So the rewritten ROLLUP Sum reads a Dec(17,2)
    // input (the inner's `s`), producing Dec(27,2), and the Expand's grandchild
    // is the inner Aggregate (not the raw source).
    val decRel = LocalRelation($"c1".int, $"c2".int, $"amt".decimal(7, 2))
    def innerDecAgg(): LogicalPlan = {
      val sumAmt = Alias(
        AggregateExpression(Sum($"amt"), Complete, isDistinct = false), "s")()
      Aggregate(
        groupingExpressions = Seq($"c1", $"c2"),
        aggregateExpressions = Seq($"c1", $"c2", sumAmt),
        child = decRel)
    }
    def outerDecFold(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
      val innerS = inner.output.find(_.name == "s").get
      val sumS = Alias(
        AggregateExpression(Sum(innerS), Complete, isDistinct = false), "s")()
      val keyOutputs = Seq("c1", "c2").map { n =>
        if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
        else Alias(Literal(null, IntegerType), n)()
      }
      Aggregate(
        groupingExpressions = keepCols.map(n => inner.output.find(_.name == n).get),
        aggregateExpressions = keyOutputs :+ sumS,
        child = inner)
    }
    val unionQuery = Union(Seq(
      innerDecAgg(),
      outerDecFold(Seq("c1"), innerDecAgg()),
      outerDecFold(Seq.empty, innerDecAgg())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    // The rolled-up Sum reads the inner's Dec(17,2) output `s` (NOT the raw
    // Dec(7,2) `amt`), so it accumulates at Dec(27,2) = the original outer type.
    // Collect Sums ONLY from the rollup-side Aggregate (the one over the
    // Expand): a plan-wide collect would be satisfied by the verbatim
    // unioned-back grand-total branch (whose Sum also reads Dec(17,2))
    // regardless of what the Expand side reads, making the assertion vacuous.
    val rollupSums = optimized.collect {
      case a @ Aggregate(_, _, _: Expand, _) => a.aggregateExpressions.flatMap(_.collect {
        case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[Sum] =>
          ae.aggregateFunction.asInstanceOf[Sum]
      })
    }.flatten
    assert(rollupSums.exists(s => s.child.dataType == DecimalType(17, 2)),
      s"Expected the rolled-up Sum to read the inner's Decimal(17,2) output " +
        s"(two-level accumulation preserved); got Sum children " +
        s"${rollupSums.map(_.child.dataType)} in:\n${optimized.treeString}")
    // The Expand's grandchild on the ROLLUP side must be the inner Aggregate.
    val expandOverInnerAgg = optimized.collectFirst {
      case e: Expand => e.child
    }.exists(c => stripProjects(c).isInstanceOf[Aggregate])
    assert(expandOverInnerAgg,
      s"Expected the Expand to roll up over the inner Aggregate, got:\n" +
        s"${optimized.treeString}")
  }

  /** Strip leading Project nodes (test helper). */
  private def stripProjects(p: LogicalPlan): LogicalPlan = p match {
    case Project(_, child) => stripProjects(child)
    case other => other
  }

  test("source-aggregate: does NOT mis-bind when branch 0 is not the largest " +
      "branch and a key sits in a different output position") {
    // Union output schema/order comes from branch 0. Here branch 0 is the
    // 1-key branch with layout [NULL AS c1, c1 AS c2, sum]; the largest branch
    // (refBranch = byLen.head) is the 2-key branch with layout [c1, c2, sum].
    // buildRollupPlan builds from refBranch but binds to u.output (branch-0
    // order). All columns are int, so a positional bind would silently swap
    // the c1/c2 output values. The source path must name-align like the
    // inner-fold path.
    def srcAgg(aggs: Seq[NamedExpression], groupCols: Seq[Expression]): Aggregate =
      Aggregate(groupCols, aggs, rel)
    // branch 0: GROUP BY c1, output [NULL AS c1, c1 AS c2, sum(x) AS sx]
    val branch0 = srcAgg(
      Seq(Alias(Literal(null, IntegerType), "c1")(),
        Alias($"c1", "c2")(),
        Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sx")()),
      Seq($"c1"))
    // branch 1 (largest, refBranch): GROUP BY c1, c2, output [c1, c2, sum(x)]
    val branch1 = srcAgg(
      Seq($"c1", $"c2",
        Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sx")()),
      Seq($"c1", $"c2"))
    // branch 2 (grand total): output [NULL, NULL, sum(x)]
    val branch2 = srcAgg(
      Seq(Alias(Literal(null, IntegerType), "c1")(),
        Alias(Literal(null, IntegerType), "c2")(),
        Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sx")()),
      Seq.empty)
    val unionQuery = Union(Seq(branch0, branch1, branch2))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    // Branch 0 cross-labels grouping column c1 to output "c2" and nulls "c1".
    // A standard ROLLUP(c1, c2) places keys under their own names, so this
    // branch's intent cannot be reconstructed; the rule must refuse.
    assertNotRewritten(optimized)
  }

  test("source-aggregate: does NOT mis-bind when a branch swaps which column " +
      "each same-typed measure sums relative to its output name") {
    // rel2 has two same-typed measures a, b. Branch 0 (defines u.output names
    // m1, m2): m1=sum(a), m2=sum(b). Higher-level branches swap: m1=sum(b),
    // m2=sum(a). aggregateExpressionSignatures is NAME-preserving (it uses
    // stripIds(toString), NOT canonicalized which would rewrite both to "none"),
    // so sum(a) and sum(b) produce DIFFERENT "AGG:" markers; the swapped branch
    // fails allMatch and the rule refuses. (Were the swap accepted, buildRollupPlan
    // would emit the reference branch's [sum(a) AS m1, sum(b) AS m2] for every
    // level, changing the swapped branches' results.)
    val rel2 = LocalRelation($"g1".int, $"g2".int, $"a".long, $"b".long)
    def sum(col: String): AggregateExpression =
      AggregateExpression(Sum(rel2.output.find(_.name == col).get), Complete, isDistinct = false)
    def srcAgg(aggs: Seq[NamedExpression], groupCols: Seq[Expression]): Aggregate =
      Aggregate(groupCols, aggs, rel2)
    val g1 = rel2.output.find(_.name == "g1").get
    val g2 = rel2.output.find(_.name == "g2").get
    val branch0 = srcAgg(  // defines output names; m1=sum(a), m2=sum(b)
      Seq(g1, g2, Alias(sum("a"), "m1")(), Alias(sum("b"), "m2")()),
      Seq(g1, g2))
    val branch1 = srcAgg(  // swapped: m1=sum(b), m2=sum(a)
      Seq(g1, Alias(Literal(null, IntegerType), "g2")(),
        Alias(sum("b"), "m1")(), Alias(sum("a"), "m2")()),
      Seq(g1))
    val branch2 = srcAgg(  // swapped
      Seq(Alias(Literal(null, IntegerType), "g1")(),
        Alias(Literal(null, IntegerType), "g2")(),
        Alias(sum("b"), "m1")(), Alias(sum("a"), "m2")()),
      Seq.empty)
    val unionQuery = Union(Seq(branch0, branch1, branch2))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("inner-fold: does NOT rewrite when branches' inner aggregates compute " +
      "different expressions over the same source") {
    // Each branch has its own inner aggregate over the same source rel2, but
    // branch 0's inner computes sum(a) while branches 1/2 compute sum(b) (same
    // class Sum, same source, same output name "m"). innerHash only hashed the
    // source UNDER the inner, so the differing inner EXPRESSIONS slipped
    // through, and buildRollupPlan emitted branch 0's sum(a) for every rollup
    // level -> wrong rolled-up values. The rule must refuse.
    val rel2 = LocalRelation($"k1".int, $"k2".int, $"a".long, $"b".long)
    def innerSum(col: String): LogicalPlan = {
      val s = Alias(
        AggregateExpression(Sum(rel2.output.find(_.name == col).get),
          Complete, isDistinct = false), "m")()
      Aggregate(Seq($"k1", $"k2"), Seq($"k1", $"k2", s), rel2)
    }
    // Outer fold branch: sum(m) over the given inner, with NULL fillers.
    def outerFold(keepCols: Seq[String], inner: LogicalPlan): LogicalPlan = {
      val innerM = inner.output.find(_.name == "m").get
      val sumM = Alias(
        AggregateExpression(Sum(innerM), Complete, isDistinct = false), "m")()
      val keyOuts = Seq("k1", "k2").map { n =>
        if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
        else Alias(Literal(null, IntegerType), n)()
      }
      Aggregate(keepCols.map(n => inner.output.find(_.name == n).get),
        keyOuts :+ sumM, inner)
    }
    val unionQuery = Union(Seq(
      innerSum("a"),                       // raw branch, inner = sum(a)  (reference)
      outerFold(Seq("k1"), innerSum("b")), // inner = sum(b)  -- DIFFERS
      outerFold(Seq.empty, innerSum("b")))) // inner = sum(b)  -- DIFFERS
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("inner-fold: does NOT rewrite when an outer branch cross-labels a " +
      "grouping key to a different output name") {
    // Outer branch groups by c1 but outputs it as "c2" (Alias(c1, "c2")) while
    // nulling "c1". verifyKeyPassthroughNamesAligned must reject this on the
    // inner-fold path, not just the source path.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val innerSumx = inner2.output.find(_.name == "sumx").get
    val crossLabeled = Aggregate(
      groupingExpressions = Seq(inner2.output.find(_.name == "c1").get),
      aggregateExpressions = Seq(
        Alias(Literal(null, IntegerType), "c1")(),               // c1 nulled
        Alias(inner2.output.find(_.name == "c1").get, "c2")(),   // c1 -> "c2"
        Alias(AggregateExpression(Sum(innerSumx), Complete, isDistinct = false), "sumx")()),
      child = inner2)
    val unionQuery = Union(Seq(
      rawBranch(inner1),
      crossLabeled,
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("inner-fold: does NOT rewrite when a raw branch's measure differs from " +
      "the reference inner's measure") {
    // The raw branch computes max(x) while the reference inner computes sum(x)
    // (same source, same output name "sumx"). The rule must reject this, or
    // buildRollupPlan would emit sum(x) for the raw branch's level too. What
    // rejects it: a raw branch is canonically its own inner, so max(x) against
    // sum(x) makes innerKeys non-unique.
    val refInner = innerAgg()                       // sum(x) AS sumx
    val rawDifferent = {                            // max(x) AS sumx (raw)
      val maxX = Alias(
        AggregateExpression(Max($"x"), Complete, isDistinct = false), "sumx")()
      Aggregate(Seq($"c1", $"c2"), Seq($"c1", $"c2", maxX), rel)
    }
    val inner3 = innerAgg()
    val unionQuery = Union(Seq(
      rawDifferent,                                 // raw branch, max(x)
      outerBranch(Seq("c1"), refInner),             // outer over sum(x) inner
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  // Note: alignInnerToUnionOutput's duplicate-name bail (byName uniqueness) is
  // defensive only -- a plan with ambiguous duplicate output names is rejected
  // by the analyzer (AMBIGUOUS_REFERENCE) before this optimizer rule runs, so
  // it is not reachable via an analyzed plan and has no dedicated test.

  test("source-aggregate: branch grouping by a different attribute is rejected " +
      "by the name-based hierarchy check") {
    // A branch grouping on a DIFFERENT source attribute (idR) than the
    // reference's prefix (idL) is rejected: grouping keys carry their SOURCE
    // names (idL/idR), not output aliases, so isPrefixShrinkingHierarchy sees
    // {idR} != prefix {k} and bails. (Output aliasing idR AS "id" does not
    // rename the grouping key.)
    val rel3 = LocalRelation($"k".int, $"idL".int, $"idR".int, $"v".long)
    val k = rel3.output.find(_.name == "k").get
    val idL = rel3.output.find(_.name == "idL").get
    val idR = rel3.output.find(_.name == "idR").get
    def sumv(): AggregateExpression = AggregateExpression(Sum(rel3.output.find(_.name == "v").get),
      Complete, isDistinct = false)
    val refB = Aggregate(Seq(k, idL),
      Seq(k, Alias(idL, "id")(), Alias(sumv(), "s")()), rel3)
    val midB = Aggregate(Seq(idR),
      Seq(Alias(Literal(null, IntegerType), "k")(),
        Alias(idR, "id")(), Alias(sumv(), "s")()), rel3)
    val gtB = Aggregate(Seq.empty,
      Seq(Alias(Literal(null, IntegerType), "k")(),
        Alias(Literal(null, IntegerType), "id")(), Alias(sumv(), "s")()), rel3)
    val unionQuery = Union(Seq(refB, midB, gtB))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("does NOT mis-rewrite when a non-passthrough Project sits between the " +
      "Union branch and its Aggregate") {
    // The raw branch is Project([c1, c2, sumx], Project([c1, c2, sumx*2 AS
    // sumx], inner)). stripProject sees through BOTH Projects to the inner
    // Aggregate; if the rule rewrote, it would emit the inner's sum(x) and drop
    // the `*2`, changing results. The rule must not silently drop the middle
    // Project's computation.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    val rawDoubled = {
      val sx = inner1.output.find(_.name == "sumx").get
      val mid = Project(Seq(
        inner1.output.find(_.name == "c1").get,
        inner1.output.find(_.name == "c2").get,
        Alias(Multiply(sx, Literal(2L)), "sumx")()), inner1)
      Project(mid.output, mid)
    }
    val unionQuery = Union(Seq(
      rawDoubled,
      outerBranch(Seq("c1"), inner2),
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    // Either refuse (no Expand), or if it rewrote, the `*2` must be preserved
    // (a Multiply by 2 must still appear somewhere). Refusal is the safe
    // expectation since the rule reconstructs from the inner aggregate only.
    val hasExpand = optimized.collectFirst { case e: Expand => e }.isDefined
    if (hasExpand) {
      val hasMultiply = optimized.expressions.exists(_.exists(_.isInstanceOf[Multiply])) ||
        optimized.collect { case p => p.expressions }.flatten
          .exists(_.exists(_.isInstanceOf[Multiply]))
      assert(hasMultiply,
        s"Rewrote but dropped the middle Project's `* 2`:\n${optimized.treeString}")
    }
  }

  test("inner-fold: does NOT mis-bind when the Union output has DUPLICATE " +
      "column names (NULL filler colliding with a measure name)") {
    // The first branch (which defines u.output names) outputs [c1, NULL AS sumx,
    // sumx] -- two columns both named "sumx" (a NULL filler for dropped c2 and
    // the measure). alignInnerToUnionOutput's name-based mapping would resolve
    // both "sumx" positions to the single inner measure, dropping c2 and
    // duplicating the measure. The duplicate-name guard must make it bail.
    val inner1 = innerAgg()
    val inner2 = innerAgg()
    val inner3 = innerAgg()
    // Branch with output names [c1, sumx, sumx]: NULL filler for c2 aliased
    // "sumx", and the real measure also "sumx".
    val collidingBranch = {
      val innerSumx = inner1.output.find(_.name == "sumx").get
      Project(Seq(
        inner1.output.find(_.name == "c1").get,
        Alias(Literal(null, IntegerType), "sumx")(),  // NULL filler named "sumx"
        Alias(innerSumx, "sumx")()), inner1)            // measure also "sumx"
    }
    val unionQuery = Union(Seq(
      collidingBranch,
      outerBranch(Seq("c1"), inner2),
      outerBranch(Seq.empty, inner3)))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("inner-fold: does NOT mis-bind when a non-head branch swaps two " +
      "same-typed measures' positions relative to the reference inner") {
    // Inner has two BIGINT sum measures s, t. The head (full-detail) branch
    // outputs [.., SUM(s) AS s, SUM(t) AS t]; the level-2 branch SWAPS them:
    // [.., SUM(t) AS t, SUM(s) AS s] at positions 3,4. verifyOuterFoldsToInner
    // (forall/set semantics) and verifyConsistentRoleLayout (both -> AGG) are
    // position-insensitive, so the swap slips through and buildRollupPlan
    // recomputes levels in the reference (head) order -> the level-2 row's s/t
    // are swapped vs the original. The inner-fold path needs a positional
    // measure-identity check like the source path.
    val rel2 = LocalRelation($"a".int, $"b".int, $"x".long, $"y".long)
    def innerTwoSums(): LogicalPlan = {
      val s = Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "s")()
      val t = Alias(AggregateExpression(Sum($"y"), Complete, isDistinct = false), "t")()
      Aggregate(Seq($"a", $"b"), Seq($"a", $"b", s, t), rel2)
    }
    // outer fold: keyOuts then the two sum-of-sum measures in the given order.
    def outerFold(keepCols: Seq[String], inner: LogicalPlan, swap: Boolean): LogicalPlan = {
      val innerS = inner.output.find(_.name == "s").get
      val innerT = inner.output.find(_.name == "t").get
      val sumS = Alias(AggregateExpression(Sum(innerS), Complete, isDistinct = false), "s")()
      val sumT = Alias(AggregateExpression(Sum(innerT), Complete, isDistinct = false), "t")()
      val keyOuts = Seq("a", "b").map { n =>
        if (keepCols.contains(n)) Alias(inner.output.find(_.name == n).get, n)()
        else Alias(Literal(null, IntegerType), n)()
      }
      val measures = if (swap) Seq(sumT, sumS) else Seq(sumS, sumT)
      Aggregate(keepCols.map(n => inner.output.find(_.name == n).get),
        keyOuts ++ measures, inner)
    }
    val unionQuery = Union(Seq(
      outerFold(Seq("a", "b"), innerTwoSums(), swap = false),  // head: s, t
      outerFold(Seq("a"), innerTwoSums(), swap = true),        // level-2: SWAPPED t, s
      outerFold(Seq.empty, innerTwoSums(), swap = false)))     // grand total: s, t
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("inner-fold: does NOT mis-bind a non-head PASSTHROUGH/raw branch when a " +
      "coarser fold branch placed first defines a swapped measure name order") {
    // R35 wrong-results bug: the finest-level branch is a PASSTHROUGH of the CTE
    // placed AFTER a coarser fold branch. The coarser fold branch is branch 0, so
    // u.output takes ITS measure name order [.., t, s], while the passthrough's
    // positional order is [.., s, t]. alignInnerToUnionOutput is name-based, so the
    // rewrite emits the inner measure named "t" at the position the passthrough
    // fills with the "s" value -> the detail rows' s/t come out swapped vs the
    // original UNION. The passthrough/raw branch must NOT be exempt from the
    // positional measure-identity check (it was in the first R34 cut, which only
    // checked fold branches).
    val rel2 = LocalRelation($"a".int, $"b".int, $"x".long, $"y".long)
    def innerTwoSums(): LogicalPlan = {
      val s = Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "s")()
      val t = Alias(AggregateExpression(Sum($"y"), Complete, isDistinct = false), "t")()
      Aggregate(Seq($"a", $"b"), Seq($"a", $"b", s, t), rel2)
    }
    // Coarser fold (GROUP BY a) with measures in order t, s -> defines u.output.
    def coarserFoldTS(inner: LogicalPlan, keep: Boolean): LogicalPlan = {
      val innerS = inner.output.find(_.name == "s").get
      val innerT = inner.output.find(_.name == "t").get
      val a = if (keep) Alias(inner.output.find(_.name == "a").get, "a")()
        else Alias(Literal(null, IntegerType), "a")()
      Aggregate(
        if (keep) Seq(inner.output.find(_.name == "a").get) else Seq.empty,
        Seq(a, Alias(Literal(null, IntegerType), "b")(),
          Alias(AggregateExpression(Sum(innerT), Complete, isDistinct = false), "t")(),
          Alias(AggregateExpression(Sum(innerS), Complete, isDistinct = false), "s")()),
        inner)
    }
    // Finest-level passthrough: SELECT a, b, s, t FROM cte (positional s, t).
    def passthrough(inner: LogicalPlan): LogicalPlan =
      Project(Seq("a", "b", "s", "t").map(n => inner.output.find(_.name == n).get), inner)
    val unionQuery = Union(Seq(
      coarserFoldTS(innerTwoSums(), keep = true),   // branch 0: u.output = [a, b, t, s]
      passthrough(innerTwoSums()),                  // finest detail: positional s, t
      coarserFoldTS(innerTwoSums(), keep = false))) // grand total
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("source-aggregate: does NOT rewrite when a VALUE-CHANGING projection sits " +
      "above a branch aggregate (would be silently dropped)") {
    // R36 defense-in-depth: buildRollupPlan rolls up the aggregate's OWN
    // expressions and DISCARDS any Project above the aggregate. A value-changing
    // projection there (sum(x) + 1) would be dropped, changing results at every
    // level. The optimizer normally folds such an expression INTO the aggregate
    // (CollapseProject) before this rule runs, so this shape is not reachable from
    // SQL -- but the rule must be robust to its input regardless of upstream rule
    // ordering. aboveAggregateProjectsArePassthrough rejects it. (Fed un-folded:
    // the catalyst Optimize runs only this rule, no CollapseProject.)
    def srcBranchPlus1(
        groupCols: Seq[Expression], keyOuts: Seq[NamedExpression]): LogicalPlan = {
      val sx = Alias(AggregateExpression(Sum($"x"), Complete, isDistinct = false), "sx")()
      val agg = Aggregate(groupCols, keyOuts :+ sx, rel)
      val m = agg.output.find(_.name == "sx").get
      Project(
        keyOuts.map(k => agg.output.find(_.name == k.name).get: NamedExpression) :+
          Alias(Add(m, Literal(1L)), "sx")(),  // value-changing: sum(x) + 1
        agg)
    }
    val full = srcBranchPlus1(Seq($"c1", $"c2"), Seq($"c1", $"c2"))
    val mid = srcBranchPlus1(
      Seq($"c1"), Seq($"c1", Alias(Literal(null, IntegerType), "c2")()))
    val grand = srcBranchPlus1(Seq.empty,
      Seq(Alias(Literal(null, IntegerType), "c1")(),
        Alias(Literal(null, IntegerType), "c2")()))
    val unionQuery = Union(Seq(full, mid, grand))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }

  test("inner-fold: does NOT mis-bind when a branch places a MEASURE where the " +
      "reference places a (same-typed) grouping key") {
    // Pins the per-position KEY-vs-AGG family on the inner-fold path. Inner has
    // ONE int MAX measure m (max keeps m the same int type as the keys, so a
    // type check would NOT catch the swap). The head outputs [a(KEY), b(KEY),
    // m(AGG)]; the level-2 branch puts the AGG in position 1 (where the head
    // has grouping key b) and a NULL in position 2 -- a KEY<->AGG role swap
    // that buildRollupPlan would otherwise mis-bind. NOTE: the FIRST rejector
    // in guard order is verifyConsistentRoleLayout (layouts [KEY,KEY,AGG] vs
    // [KEY,AGG,KEY] genuinely differ); verifyInnerFoldMeasurePositions ALSO
    // rejects this shape independently (binding [None, Some("m"), None] vs
    // reference [None, None, Some("m")]), so the two guards back each other
    // up on this family.
    val rel3 = LocalRelation($"a".int, $"b".int, $"x".int)
    def innerMax(): LogicalPlan = {
      val m = Alias(AggregateExpression(Max($"x"), Complete, isDistinct = false), "m")()
      Aggregate(Seq($"a", $"b"), Seq($"a", $"b", m), rel3)
    }
    def headPassthrough(inner: LogicalPlan): LogicalPlan =
      Project(Seq("a", "b", "m").map(n => inner.output.find(_.name == n).get), inner)
    def roleSwapped(inner: LogicalPlan): LogicalPlan = {
      val innerM = inner.output.find(_.name == "m").get
      Aggregate(Seq(inner.output.find(_.name == "a").get),
        Seq(Alias(inner.output.find(_.name == "a").get, "a")(),
          Alias(AggregateExpression(Max(innerM), Complete, isDistinct = false), "b")(),
          Alias(Literal(null, IntegerType), "m")()),  // AGG in pos 1, NULL in pos 2
        inner)
    }
    def grandTotal(inner: LogicalPlan): LogicalPlan = {
      val innerM = inner.output.find(_.name == "m").get
      Aggregate(Seq.empty,
        Seq(Alias(Literal(null, IntegerType), "a")(),
          Alias(Literal(null, IntegerType), "b")(),
          Alias(AggregateExpression(Max(innerM), Complete, isDistinct = false), "m")()),
        inner)
    }
    val unionQuery = Union(Seq(
      headPassthrough(innerMax()), roleSwapped(innerMax()), grandTotal(innerMax())))
    val analyzed = unionQuery.analyze
    val optimized = Optimize.execute(analyzed)
    assertNotRewritten(optimized)
  }
}
