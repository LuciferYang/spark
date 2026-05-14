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

import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Attribute, AttributeMap, AttributeReference, Cast, Divide, EvalMode, GreaterThan, Literal, Rand, Substring}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, BitAndAgg, BitOrAgg, BitXorAgg, Complete, Count, First, Last, Max, Min, Partial, StddevSamp, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateHint, ColumnStat, FinalAggregate, LocalRelation, LogicalPlan, PartialAggregate, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}

object PushPartialAggregationThroughProjectSuite {
  case object TestAggregateHint extends AggregateHint
}

class PushPartialAggregationThroughProjectSuite extends StatsEstimationTestBase with PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Push Partial Aggregation Through Project", Once,
      PushPartialAggregationThroughProject) :: Nil
  }

  // --- Fixtures -------------------------------------------------------------

  // 1000 rows, k_distinct=10 (good for grouping), v/w/extra distinct=500 (no benefit alone).
  private def relation: (StatsTestPlan, AttributeReference, AttributeReference,
      AttributeReference, AttributeReference) = {
    val k = AttributeReference("k", IntegerType)()
    val v = AttributeReference("v", IntegerType)()
    val w = AttributeReference("w", IntegerType)()
    val extra = AttributeReference("extra", IntegerType)()
    val stats = AttributeMap(Seq(
      k -> ColumnStat(distinctCount = Some(10), nullCount = Some(0)),
      v -> ColumnStat(distinctCount = Some(500), nullCount = Some(0)),
      w -> ColumnStat(distinctCount = Some(500), nullCount = Some(0)),
      extra -> ColumnStat(distinctCount = Some(500), nullCount = Some(0))))
    val rel = StatsTestPlan(
      outputList = Seq(k, v, w, extra),
      rowCount = 1000,
      attributeStats = stats,
      size = Some(1000 * 16))
    (rel, k, v, w, extra)
  }

  /** True iff the optimized plan contains a `PartialAggregate` anywhere.
   *  The rule always drops the original Project layer in the rewrite, so a
   *  positive result also implies the matched Project did not survive. */
  private def hasPreAgg(plan: LogicalPlan): Boolean = plan.exists {
    case _: PartialAggregate => true
    case _ => false
  }

  private def withRuleEnabled[T](body: => T): T = withSQLConf(
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
    SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0"
  )(body)

  // --- Tests: per-function decomposition ------------------------------------

  test("Sum push through bare-Attribute Project") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized),
        s"Expected PartialAggregate below Project; got:\n$optimized")
    }
  }

  test("Count(*) push through Project") {
    withRuleEnabled {
      val (rel, k, _, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Count(Seq(Literal(1))), Complete, isDistinct = false),
            "cnt")()),
        child = Project(Seq(k), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  test("Count(v) push through Project") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Count(Seq(v)), Complete, isDistinct = false), "cnt_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  test("Min/Max push through Project") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Min(v), Complete, isDistinct = false), "min_v")(),
          Alias(AggregateExpression(Max(v), Complete, isDistinct = false), "max_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  test("First/Last push through Project (preserves ignoreNulls)") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(First(v, ignoreNulls = true), Complete,
            isDistinct = false), "first_v")(),
          Alias(AggregateExpression(Last(v, ignoreNulls = false), Complete,
            isDistinct = false), "last_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  test("BitAnd/BitOr/BitXor push through Project") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(BitAndAgg(v), Complete, isDistinct = false), "ba_v")(),
          Alias(AggregateExpression(BitOrAgg(v), Complete, isDistinct = false), "bo_v")(),
          Alias(AggregateExpression(BitXorAgg(v), Complete, isDistinct = false), "bx_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  test("Avg push through Project") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  test("Multiple aggregates mixed: Sum + Count + Min + Avg") {
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")(),
          Alias(AggregateExpression(Count(Seq(w)), Complete, isDistinct = false), "cnt_w")(),
          Alias(AggregateExpression(Min(w), Complete, isDistinct = false), "min_w")(),
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = Project(Seq(k, v, w), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  // --- Tests: end-to-end plan lock-in --------------------------------------

  test("comparePlans: Sum push produces FinalAggregate -> PartialAggregate (Project dropped)") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(optimized.isInstanceOf[FinalAggregate],
        s"Expected root FinalAggregate; got ${optimized.getClass.getSimpleName}")
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      // The bare-Attribute Project layer is dropped; PartialAggregate is FinalAggregate's
      // direct child. Verify no Project survives anywhere in the optimized tree -- locks
      // in the "Project dropped" contract documented on the rule.
      assert(optimized.collectFirst { case _: Project => () }.isEmpty,
        s"Expected no Project in optimized tree; got:\n$optimized")
      val partial = finalAgg.child.asInstanceOf[PartialAggregate]
      assert(partial.groupingExpressions.size == 1,
        s"Expected grouping [k]; got ${partial.groupingExpressions}")
      val partialAlias = partial.aggregateExpressions.last.asInstanceOf[Alias]
      assert(partialAlias.name.startsWith("_pushed_sum_"))
      // Outer Sum's input must reference the PartialAggregate's partial alias attr.
      val outerSumAlias = finalAgg.aggregateExpressions.last.asInstanceOf[Alias]
      outerSumAlias.child match {
        case AggregateExpression(s: Sum, Complete, false, None, _) =>
          val sumInputExprIds = s.child.references.map(_.exprId)
          val partialOutputExprIds = partial.output.map(_.exprId).toSet
          assert(sumInputExprIds.forall(partialOutputExprIds.contains),
            s"Outer Sum's input must reference partial.output; got refs $sumInputExprIds " +
              s"but partial.output is ${partial.output.map(a => (a.name, a.exprId))}")
        case other => fail(s"Expected outer Sum aggregate; got $other")
      }
    }
  }

  test("comparePlans: Avg push produces Divide(Cast, Cast) with LEGACY EvalMode") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Average(v), Complete, isDistinct = false), "avg_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      val outerAvgAlias = finalAgg.aggregateExpressions.last.asInstanceOf[Alias]
      outerAvgAlias.child match {
        case d @ Divide(Cast(_: AggregateExpression, _, _, _),
                        Cast(_: AggregateExpression, _, _, _), _) =>
          assert(d.evalMode == EvalMode.LEGACY,
            s"Avg replacement Divide must use EvalMode.LEGACY; got ${d.evalMode}")
        case other =>
          fail(s"Expected Divide(Cast(AggExp), Cast(AggExp)) for Avg replacement; got $other")
      }
    }
  }

  // --- Tests: cost gate / config ---------------------------------------------

  test("Cost gate rejects when grouping is high-cardinality") {
    // benefitRatio=0.001; k_distinct=10/1000 = ratio 0.01 > 0.001 -> reject.
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.001") {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized))
    }
  }

  test("Configuration disabled: rule is a no-op") {
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "false",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(optimized.fastEquals(agg))
    }
  }

  // --- Tests: ineligibility guards ------------------------------------------

  test("Project with Alias for derived expression: rejected") {
    // projectList contains `Alias(Add(k, 1), "x")` -- derived Project alias.
    // Per Synapse section 4.2: blocked unless we can substitute, which the simple
    // first-iteration rule rejects entirely.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val derived = Alias(Add(k, Literal(1)), "k_plus_one")()
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(derived, k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized),
        "Project with derived Alias must be rejected by the simple rule (Synapse section 4.2)")
    }
  }

  test("Project with renaming Alias of bare Attribute: rejected by simple rule") {
    // projectList contains `Alias(v, "v_renamed")` -- pure renaming Alias.
    // The simple first-iteration rule rejects any Alias (even identity).
    // A follow-up could handle renames specifically.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val renamed = Alias(v, "v_renamed")()
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(renamed.toAttribute), Complete, isDistinct = false),
            "sum_v")()),
        child = Project(Seq(k, renamed), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized),
        "Project with any Alias must be rejected by the simple rule (first-iteration scope)")
    }
  }

  test("Non-deterministic aggregate input: rejected") {
    withRuleEnabled {
      val (rel, k, _, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(Rand(Literal(1L))), Complete, isDistinct = false),
            "sum_rand")()),
        child = Project(Seq(k), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized))
    }
  }

  test("DISTINCT aggregate: rejected") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = true), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized))
    }
  }

  test("FILTER (WHERE ...) aggregate: rejected") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val aggFilter =
        GreaterThan(v, Literal(0))
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false,
            filter = Some(aggFilter)), "sum_v_filtered")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized))
    }
  }

  test("Eligibility guard: AggregateBase already below Project's child -> no-op") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val preWrapped = PartialAggregate(rel.output, rel.output, rel)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), preWrapped))
      val optimized = Optimize.execute(agg)
      assert(optimized.fastEquals(agg),
        s"Rule must be a strict no-op when Project's child is an AggregateBase; got:\n$optimized")
    }
  }

  test("Non-Attribute grouping expression: rule is a no-op (skipped)") {
    withRuleEnabled {
      val (rel, _, v, _, _) = relation
      val strV = Cast(v, StringType)
      val grouping = Substring(strV, Literal(1), Literal(3))
      val agg = Aggregate(
        groupingExpressions = Seq(grouping),
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized))
    }
  }

  test("Decimal Avg: excluded (matches Expand/Union/Filter rules)") {
    withRuleEnabled {
      val dv = AttributeReference("dv", DecimalType(17, 2))()
      val dk = AttributeReference("dk", IntegerType)()
      val rel = LocalRelation(dk, dv)
      val agg = Aggregate(
        groupingExpressions = Nil,
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Average(dv), Complete, isDistinct = false), "avg_dv")()),
        child = Project(Seq(dv), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized))
    }
  }

  test("No aggregate function (group-by only): rejected") {
    withRuleEnabled {
      val (rel, k, _, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k),
        child = Project(Seq(k), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized))
    }
  }

  test("Aggregate hint preserved on FinalAggregate after rewrite") {
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val hint = Some(PushPartialAggregationThroughProjectSuite.TestAggregateHint)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel),
        hint = hint)
      val optimized = Optimize.execute(agg)
      val finalAgg = optimized.asInstanceOf[FinalAggregate]
      assert(finalAgg.hint == hint)
    }
  }

  test("Stats missing: cost gate falls back to ratio=1.0") {
    val k = AttributeReference("k", IntegerType)()
    val v = AttributeReference("v", IntegerType)()
    val rel = LocalRelation(k, v)
    val agg = Aggregate(
      groupingExpressions = Seq(k),
      aggregateExpressions = Seq(k,
        Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
      child = Project(Seq(k, v), rel))

    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "1.0") {
      assert(hasPreAgg(Optimize.execute(agg)))
    }
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.5") {
      assert(!hasPreAgg(Optimize.execute(agg)))
    }
  }

  test("Project with subset of child columns: rule fires (Project is column-selection)") {
    // Project selects 2 of 4 child columns. Rule fires.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))  // omits w, extra
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  test("Project with extra columns beyond grouping + agg-inputs: extras are dropped") {
    // projectList = [k, v, w, extra], but outer Aggregate uses only k (grouping) and
    // v (sum input). The Project layer is dropped, and the new PartialAggregate's
    // output must contain only [k, partial_alias] -- not w or extra.
    withRuleEnabled {
      val (rel, k, v, w, extra) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v, w, extra), rel))  // includes unused w, extra
      val optimized = Optimize.execute(agg)
      val partial = optimized.asInstanceOf[FinalAggregate].child.asInstanceOf[PartialAggregate]
      val partialOutputIds = partial.output.map(_.exprId).toSet
      assert(partialOutputIds.contains(k.exprId),
        s"PartialAggregate must carry grouping key k; got ${partial.output}")
      assert(!partialOutputIds.contains(w.exprId),
        s"PartialAggregate must NOT carry unused projectList attr w; got ${partial.output}")
      assert(!partialOutputIds.contains(extra.exprId),
        s"PartialAggregate must NOT carry unused projectList attr extra; got ${partial.output}")
    }
  }

  test("Multi-column grouping: all keys preserved in PartialAggregate grouping") {
    // Grouping = [k, w]. Verify both ordering and presence. Also assert that
    // partial.output places grouping keys before partial aliases (per the
    // documented `g ++ partial_aliases` layout in the rule's ScalaDoc).
    withRuleEnabled {
      val (rel, k, v, w, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k, w),
        aggregateExpressions = Seq(k, w,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, w, v), rel))
      val optimized = Optimize.execute(agg)
      val partial = optimized.asInstanceOf[FinalAggregate].child.asInstanceOf[PartialAggregate]
      val groupingExprIds = partial.groupingExpressions.map(_.asInstanceOf[Attribute].exprId)
      assert(groupingExprIds == Seq(k.exprId, w.exprId),
        s"Expected partial grouping [k, w]; got $groupingExprIds")
      // Lock down output ordering: [k, w, partial_alias_for_sum_v]
      val outputExprIds = partial.output.map(_.exprId)
      assert(outputExprIds.take(2) == Seq(k.exprId, w.exprId),
        s"Expected partial.output to start with grouping keys [k, w]; got $outputExprIds")
      assert(partial.output.size == 3,
        s"Expected partial.output size 3 (2 grouping + 1 partial); got ${partial.output}")
    }
  }

  test("Duplicate attributes in projectList: rule still fires; duplicates harmless") {
    // projectList = [k, k, v] -- duplicate `k` (same ExprId). Plan resolution
    // uses ExprId, so the duplicate is structurally equivalent to [k, v]
    // downstream. Rule fires because all elements are bare Attributes; the
    // rule drops the Project layer so the duplicate is gone.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
      // "Harmless" = the duplicate does not leak into PartialAggregate.
      val partial = optimized.asInstanceOf[FinalAggregate].child.asInstanceOf[PartialAggregate]
      val partialOutputExprIds = partial.output.map(_.exprId)
      // PartialAggregate.output should contain k exactly once; duplicate `k`
      // from projectList is dropped together with the Project layer.
      assert(partialOutputExprIds.count(_ == k.exprId) == 1,
        s"Duplicate `k` must not leak; got partial.output=$partialOutputExprIds")
      // Output schema is preserved at the top-level FinalAggregate.
      assert(optimized.output.map(_.exprId) == agg.output.map(_.exprId),
        s"Output exprIds must match original Aggregate; got " +
          s"${optimized.output.map(_.exprId)} vs ${agg.output.map(_.exprId)}")
    }
  }

  test("Aggregate as Project's child: rule is a no-op (eligibility guard)") {
    // The inlined `!project.child.isInstanceOf[AggregateBase]` guard rejects
    // an `Aggregate`, `PartialAggregate`, or `FinalAggregate` sitting below
    // the matched Project. This test pins down that contract for the
    // `Aggregate` case (the other two arms are covered by the
    // "Eligibility guard: AggregateBase already below Project's child" test).
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val innerAgg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "inner_sum_v")()),
        child = Project(Seq(k, v), rel))
      val outerAgg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Min(v), Complete, isDistinct = false), "min_v")()),
        child = Project(Seq(k, v), innerAgg))
      val optimized = Optimize.execute(outerAgg)
      // Inner Aggregate's Project sits over a leaf relation -> rewrite fires.
      // Outer Aggregate's Project sits over the inner Aggregate -> blocked.
      // Result: exactly one PartialAggregate (from the inner rewrite), and the
      // root is the original outer Aggregate.
      val partialAggCount = optimized.collect { case _: PartialAggregate => () }.size
      assert(partialAggCount == 1,
        s"Expected exactly one PartialAggregate (inner only); got $partialAggCount in:\n$optimized")
      // `FinalAggregate` and `PartialAggregate` are siblings of `Aggregate`
      // under `AggregateBase`, so `isInstanceOf[Aggregate]` is already false
      // for them -- the single-arm assertion suffices.
      assert(optimized.isInstanceOf[Aggregate],
        s"Root must remain a regular Aggregate; got ${optimized.getClass.getSimpleName}")
    }
  }

  test("Empty grouping (Nil): rule fires when stats permit") {
    // groupingExpressions = Nil. `forall(_.isInstanceOf[Attribute])` is vacuously
    // true, so eligibility passes. The cost gate computes the ratio
    // `Aggregate(Nil, Nil, project.child).stats.rowCount / project.child.rowCount`,
    // which is `1 / 1000 = 0.001` -- well below the benefitRatio of 1.0 -- so
    // the rule fires. Pins down behavior parity with sibling
    // `PushPartialAggregationThroughFilterSuite.Empty grouping`.
    withRuleEnabled {
      val (rel, _, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Nil,
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
      val partial = optimized.asInstanceOf[FinalAggregate].child.asInstanceOf[PartialAggregate]
      assert(partial.groupingExpressions.isEmpty,
        s"Expected empty partial grouping; got ${partial.groupingExpressions}")
    }
  }

  test("Nested Project chain: outermost Aggregate -> Project -> Project -> rel") {
    // Two stacked Projects below the Aggregate. The rule's `transformUpWithPruning`
    // matches the Aggregate-over-Project pair regardless of what the inner Project
    // sits on; the inner Project (whose child is the leaf relation) is irrelevant
    // to whether the outer rewrite fires.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), Project(Seq(k, v), rel)))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
    }
  }

  test("Double execution: rule is idempotent on its own output") {
    // Run the optimizer twice and confirm the second pass produces the same tree.
    // The inlined `!project.child.isInstanceOf[AggregateBase]` guard sees the
    // new `PartialAggregate` and blocks any second-pass rewrite.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val once = Optimize.execute(agg)
      val twice = Optimize.execute(once)
      comparePlans(once, twice)
    }
  }

  test("Extras-dropped: PartialAggregate aggregateExpressions size locked down") {
    // Strengthens the "Project with extra columns" test by also pinning down
    // `partial.aggregateExpressions.size` (must be grouping + partial-aliases).
    withRuleEnabled {
      val (rel, k, v, w, extra) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v, w, extra), rel))
      val optimized = Optimize.execute(agg)
      val partial = optimized.asInstanceOf[FinalAggregate].child.asInstanceOf[PartialAggregate]
      // Sum has exactly one partial alias; plus one grouping key = 2 total.
      assert(partial.aggregateExpressions.size == 2,
        s"Expected partial.aggregateExpressions.size == 2; got " +
          s"${partial.aggregateExpressions.size} in:\n$partial")
    }
  }

  test("comparePlans: Count(*) push produces FinalAggregate -> PartialAggregate") {
    // Adds structural lock-in for Count(*) alongside the existing Sum and Avg
    // lock-in tests. Count(*) decomposes to Sum(partial Count) outer.
    withRuleEnabled {
      val (rel, k, _, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Count(Seq(Literal(1))), Complete, isDistinct = false),
            "count_star")()),
        child = Project(Seq(k), rel))
      val optimized = Optimize.execute(agg)
      assert(optimized.isInstanceOf[FinalAggregate],
        s"Expected root FinalAggregate; got ${optimized.getClass.getSimpleName}")
      val partial = optimized.asInstanceOf[FinalAggregate].child
      assert(partial.isInstanceOf[PartialAggregate],
        s"Expected PartialAggregate child; got ${partial.getClass.getSimpleName}")
      assert(optimized.collectFirst { case _: Project => () }.isEmpty,
        s"Expected no Project in optimized tree; got:\n$optimized")
    }
  }

  test("Repeated aggregate: sum(v) aliased twice -- both rewired, schema preserved") {
    // `SELECT sum(v) AS s1, sum(v) AS s2 FROM ... GROUP BY k`. Two
    // `AggregateExpression`s with the same input but distinct `resultId`s.
    // The rule's `replacements` map keys by `resultId`, so both outer
    // references are rewired correctly.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "s1")(),
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "s2")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
      // Output schema preserved: 3 columns with original names.
      val outputNames = optimized.output.map(_.name)
      assert(outputNames == Seq(k.name, "s1", "s2"),
        s"Expected output names [${k.name}, s1, s2]; got $outputNames")
      // Both Sum aliases have data types matching the original outer Sum.
      val outputDtypes = optimized.output.map(_.dataType)
      assert(outputDtypes == agg.output.map(_.dataType),
        s"Expected output dtypes ${agg.output.map(_.dataType)}; got $outputDtypes")
    }
  }

  test("Non-deterministic non-aggregate projection: rejected") {
    // The eligibility check `outerAgg.aggregateExpressions.forall(_.deterministic)`
    // applies to the entire NamedExpression list, not just aggregate inputs.
    // A bare `Rand()` alongside the agg should reject regardless of where in
    // the projection list it appears.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(Rand(Literal(42L)), "r")(),
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized),
        s"Expected rule no-op when non-aggregate projection is non-deterministic; got:\n$optimized")
    }
  }

  test("Empty projectList: vacuously eligible, rule fires") {
    // `Project(Nil, rel)` projects no columns. The bare-Attribute eligibility
    // check `forall(_.isInstanceOf[Attribute])` is vacuously true on `Nil`.
    // The outer Aggregate would have to consume from `project.child` directly
    // via its grouping/agg-inputs (here grouping = Nil, agg = Count(*)).
    // Verify the rule fires gracefully.
    withRuleEnabled {
      val (rel, _, _, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Nil,
        aggregateExpressions = Seq(
          Alias(AggregateExpression(Count(Seq(Literal(1))), Complete, isDistinct = false),
            "cnt")()),
        child = Project(Nil, rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized),
        s"Expected rule to fire on empty projectList with empty grouping; got:\n$optimized")
    }
  }

  test("Mixed supported + unsupported aggregate functions: rule rejects") {
    // `eligible` requires `aggExprs.forall(supportedAgg)`. A single unsupported
    // function (e.g., `StddevSamp`, which `PartialAggregationDecomposition`
    // does not handle) must reject the entire rewrite -- not just skip that
    // one function.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")(),
          Alias(AggregateExpression(StddevSamp(v), Complete, isDistinct = false), "std_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized),
        s"Expected rule no-op when any aggregate function is unsupported; got:\n$optimized")
    }
  }

  test("Non-Complete aggregate mode (Partial): rejected by supportedAgg") {
    // `supportedAgg` matches only `Complete` aggregate mode. An optimizer
    // earlier in the pipeline does not produce non-Complete modes here, but
    // the guard exists -- pin it down.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Partial, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized),
        s"Expected rule no-op when aggregate mode is not Complete; got:\n$optimized")
    }
  }

  test("Project with constant-literal Alias: rejected") {
    // `Alias(Literal(1), "c")` -- a constant column. `projectList.forall(
    // _.isInstanceOf[Attribute])` rejects any Alias regardless of payload.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v, Alias(Literal(1, IntegerType), "c")()), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized),
        s"Expected rule no-op when projectList contains a constant Alias; got:\n$optimized")
    }
  }

  test("Decimal Sum (not Avg): supported, rule fires") {
    // Decimal Avg is excluded; Decimal Sum is NOT excluded -- `supportedAgg`'s
    // Sum branch has no dtype guard. Pin this contract down.
    val decT = DecimalType(10, 2)
    val k = AttributeReference("k", IntegerType)()
    val d = AttributeReference("d", decT)()
    val stats = AttributeMap(Seq(
      k -> ColumnStat(distinctCount = Some(10), nullCount = Some(0)),
      d -> ColumnStat(distinctCount = Some(500), nullCount = Some(0))))
    val rel = StatsTestPlan(
      outputList = Seq(k, d),
      rowCount = 1000,
      attributeStats = stats,
      size = Some(1000 * 16))
    withRuleEnabled {
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(d), Complete, isDistinct = false), "sum_d")()),
        child = Project(Seq(k, d), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized),
        s"Expected rule to fire on Decimal Sum; got:\n$optimized")
    }
  }

  test("Aggregate-over-Project nested under outer Project: rule still fires") {
    // The rule's `transformUpWithPruning` should match the inner
    // Aggregate-over-Project pair regardless of an enclosing Project layer
    // above the outer Aggregate. Locks down pattern propagation through a
    // non-trivial enclosing tree.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val innerAgg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val outerPlan = Project(innerAgg.aggregateExpressions.map(_.toAttribute), innerAgg)
      val optimized = Optimize.execute(outerPlan)
      assert(hasPreAgg(optimized),
        s"Expected rule to fire on Aggregate-over-Project nested under a Project; got:\n$optimized")
    }
  }

  test("Aggregate without Project child: pattern pruning short-circuits") {
    // `containsAllPatterns(AGGREGATE, PROJECT)` requires both bits in the
    // subtree's pattern bitmask. An Aggregate directly over a relation (no
    // Project layer anywhere below) must be a strict no-op.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = rel)  // no Project layer
      val optimized = Optimize.execute(agg)
      comparePlans(optimized, agg)
    }
  }

  test("Aggregate hint default (None): preserved on FinalAggregate after rewrite") {
    // Existing `Aggregate hint preserved` test covers Some(...). Pin down the
    // default-None case too: rewrite must not invent a hint.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel),
        hint = None)
      val optimized = Optimize.execute(agg)
      assert(optimized.isInstanceOf[FinalAggregate],
        s"Expected root FinalAggregate; got ${optimized.getClass.getSimpleName}")
      assert(optimized.asInstanceOf[FinalAggregate].hint.isEmpty,
        s"Expected hint=None preserved; got ${optimized.asInstanceOf[FinalAggregate].hint}")
    }
  }

  test("Distinct partial-alias exprIds when multiple aggregates pushed together") {
    // Sum + Count produce two partial aliases. Each must have a distinct
    // exprId in `partial.output` so downstream rewires resolve unambiguously.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")(),
          Alias(AggregateExpression(Count(Seq(v)), Complete, isDistinct = false), "cnt_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      val partial = optimized.asInstanceOf[FinalAggregate].child.asInstanceOf[PartialAggregate]
      // Partial alias attrs: positions after the grouping key.
      val partialAliasExprIds = partial.output.drop(1).map(_.exprId)
      assert(partialAliasExprIds.toSet.size == partialAliasExprIds.size,
        s"Expected distinct exprIds for partial aliases; got $partialAliasExprIds")
    }
  }

  test("Mixed Complete + Partial aggregate modes: rejected") {
    // `supportedAgg` matches Complete only. A single Partial-mode aggregate in
    // a list of otherwise-Complete-mode aggregates must reject the whole
    // rewrite (same as the mixed-supported-unsupported case).
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")(),
          Alias(AggregateExpression(Count(Seq(v)), Partial, isDistinct = false), "cnt_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(!hasPreAgg(optimized),
        s"Expected rule no-op when any aggregate mode is not Complete; got:\n$optimized")
    }
  }

  test("CSE-shared AggregateExpression instance: distinctBy dedupe path") {
    // Construct the SAME `AggregateExpression` instance and reference it twice
    // in `aggregateExpressions` (e.g., `Add(sumAe, sumAe)`). This is what
    // `collectAggregateExprs` returns twice -- the `distinctBy(_.resultId)`
    // dedupe is what prevents the PartialAggregate from getting two partial
    // aliases for the same Sum.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val sharedSum = AggregateExpression(Sum(v), Complete, isDistinct = false)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(Add(sharedSum, sharedSum), "double_sum")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
      val partial = optimized.asInstanceOf[FinalAggregate].child.asInstanceOf[PartialAggregate]
      // Without dedupe, partial.aggregateExpressions would carry two identical
      // partial aliases for Sum(v). With dedupe, only one partial alias.
      assert(partial.aggregateExpressions.size == 2,
        s"Expected 2 (1 grouping + 1 deduped partial); got " +
          s"${partial.aggregateExpressions.size} in:\n$partial")
    }
  }

  test("Cost gate exactly at threshold: rule fires (ratio == benefitRatio)") {
    // `pushPartialAggHasBenefit` returns `ratio <= benefitRatio`. With
    // k_distinct = 10 and rowCount = 1000, ratio = 0.01. Setting
    // benefitRatio = 0.01 puts us exactly at the boundary; the rule must fire.
    val (rel, k, v, _, _) = relation
    withSQLConf(
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_ENABLED.key -> "true",
      SQLConf.PARTIAL_AGGREGATION_OPTIMIZATION_BENEFIT_RATIO.key -> "0.01"
    ) {
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized),
        s"Rule must fire when ratio is exactly at benefitRatio; got:\n$optimized")
    }
  }

  test("FinalAggregate as Project's child: rule is a no-op (eligibility guard)") {
    // Completes the matrix of AggregateBase subclasses (Aggregate,
    // PartialAggregate already covered): when a `FinalAggregate` sits below
    // the matched Project, the guard `!project.child.isInstanceOf[AggregateBase]`
    // rejects.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val partial = PartialAggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "p_sum")()),
        child = rel)
      val finalAgg = FinalAggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Sum(partial.output(1)), Complete, isDistinct = false),
            "f_sum")()),
        child = partial)
      val outerAgg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(Min(v), Complete, isDistinct = false), "min_v")()),
        child = Project(finalAgg.output.take(2), finalAgg))
      val optimized = Optimize.execute(outerAgg)
      // Root must remain a regular Aggregate (rewrite did not fire).
      assert(optimized.isInstanceOf[Aggregate],
        s"Expected outer Aggregate to remain unchanged; got " +
          s"${optimized.getClass.getSimpleName}")
    }
  }

  test("AggregateExpression nested inside a scalar expression: transformDown rewires") {
    // `aggregateExpressions = [k, Alias(Add(Sum(v), Literal(1)), "s_plus_one")]`.
    // The rewrite uses `transformDown` to find AggregateExpression nodes by
    // resultId anywhere inside each NamedExpression, including nested under
    // arithmetic operators. Verify the outer `Add` survives with its inner
    // Sum rewired to consume the partial alias attribute.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val sumAe = AggregateExpression(Sum(v), Complete, isDistinct = false)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(Add(sumAe, Literal(1)), "s_plus_one")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(hasPreAgg(optimized))
      // The outer Add must survive somewhere in the FinalAggregate's
      // aggregateExpressions; the inner Sum must now consume a partial alias
      // (i.e., the inner Sum's child must be an Attribute, not `v` directly).
      val outerAggExprs = optimized.asInstanceOf[FinalAggregate].aggregateExpressions
      val containsAdd = outerAggExprs.exists(_.exists {
        case _: Add => true
        case _ => false
      })
      assert(containsAdd,
        s"Outer Add must survive in rewritten aggregateExpressions; got:\n$outerAggExprs")
      // Every surviving aggregate function's *child* (input) must be an
      // Attribute (the partial alias attribute) rather than the original
      // Sum-input expression `v`. Uniform check across function types.
      val outerSumInputs = outerAggExprs.flatMap(_.collect {
        case ae: AggregateExpression => ae.aggregateFunction.children
      }.flatten)
      assert(outerSumInputs.nonEmpty,
        s"Expected at least one AggregateExpression in outer aggregateExpressions; " +
          s"got:\n$outerAggExprs")
      assert(outerSumInputs.forall(_.isInstanceOf[Attribute]),
        s"Outer aggregate function inputs must be partial alias Attributes; " +
          s"got $outerSumInputs")
    }
  }

  test("Average.evalMode propagated into partial Sum's evalMode") {
    // `decompose` constructs the partial Sum with `Sum(avg.child, avg.evalMode,
    // ...)`. The existing Avg lock-in test pins the outer Divide's
    // EvalMode.LEGACY; this test pins the *inner* partial Sum's evalMode
    // propagation -- a regression that hard-coded `EvalMode.fromSQLConf` on
    // the partial would slip past every other Avg test.
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val avgWithAnsi = Average(v, EvalMode.ANSI)
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k,
          Alias(AggregateExpression(avgWithAnsi, Complete, isDistinct = false), "avg_v")()),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      val partial = optimized.asInstanceOf[FinalAggregate].child.asInstanceOf[PartialAggregate]
      val partialSums = partial.aggregateExpressions.flatMap(_.collect {
        case ae: AggregateExpression => ae.aggregateFunction
      }).collect { case s: Sum => s }
      assert(partialSums.nonEmpty,
        s"Expected at least one partial Sum in PartialAggregate; got:\n$partial")
      assert(partialSums.forall(_.evalMode == EvalMode.ANSI),
        s"Partial Sum's evalMode must be EvalMode.ANSI; " +
          s"got ${partialSums.map(_.evalMode)}")
    }
  }

  test("comparePlans: Sum push preserves output schema (names + dtypes + exprIds)") {
    // Strengthens the schema-preservation contract: optimized.output must match
    // agg.output by name, dtype, and exprId (for non-aggregate columns).
    withRuleEnabled {
      val (rel, k, v, _, _) = relation
      val sumAlias = Alias(AggregateExpression(Sum(v), Complete, isDistinct = false), "sum_v")()
      val agg = Aggregate(
        groupingExpressions = Seq(k),
        aggregateExpressions = Seq(k, sumAlias),
        child = Project(Seq(k, v), rel))
      val optimized = Optimize.execute(agg)
      assert(optimized.output.size == agg.output.size)
      // Grouping key (k): exprId must be preserved across the rewrite.
      assert(optimized.output(0).exprId == k.exprId,
        s"Grouping key exprId must be preserved; expected ${k.exprId}, " +
          s"got ${optimized.output(0).exprId}")
      assert(optimized.output(0).name == k.name)
      assert(optimized.output(0).dataType == k.dataType)
      // Sum alias (sum_v): exprId must equal the original Alias's exprId so
      // downstream references to `sum_v` keep resolving.
      assert(optimized.output(1).exprId == sumAlias.exprId,
        s"Sum alias exprId must be preserved; expected ${sumAlias.exprId}, " +
          s"got ${optimized.output(1).exprId}")
      assert(optimized.output(1).name == "sum_v")
      assert(optimized.output(1).dataType == sumAlias.dataType)
    }
  }
}
