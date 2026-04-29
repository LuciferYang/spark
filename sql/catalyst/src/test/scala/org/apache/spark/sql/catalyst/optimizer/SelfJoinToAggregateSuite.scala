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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}

class SelfJoinToAggregateSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("SelfJoinToAggregate", FixedPoint(10),
        SelfJoinToAggregate,
        RemoveNoopOperators) :: Nil
  }

  private val tableSchema = LocalRelation($"k".int, $"c".int)
  private val tableSchema2 = LocalRelation($"k".int, $"c".int)
  private val outer = LocalRelation($"id".int)
  private val k1 = tableSchema.output(0)
  private val c1 = tableSchema.output(1)
  private val k2 = tableSchema2.output(0)
  private val c2 = tableSchema2.output(1)
  private val outerId = outer.output(0)

  /** Wrap an inner self-join Project in the canonical IN-subquery shape (LeftSemi join). */
  private def wrapInLeftSemi(inner: LogicalPlan): LogicalPlan = {
    outer.join(inner, LeftSemi, Some(outerId === k1))
  }

  /**
   * Run the rule with the flag enabled. Sets the cost-gate threshold to 0 so the gate
   * (which suppresses the rewrite below `minTableSizeInBytes`, default 1 GiB) always passes,
   * letting the tests focus on the matcher logic regardless of the test relations' tiny size.
   */
  private def runWithFlag(plan: LogicalPlan, enabled: Boolean = true): LogicalPlan = {
    withSQLConf(
      SQLConf.SELF_JOIN_TO_AGGREGATE_ENABLED.key -> enabled.toString,
      SQLConf.SELF_JOIN_TO_AGGREGATE_MIN_TABLE_SIZE.key -> "0") {
      Optimize.execute(plan)
    }
  }

  // The "rewritten" shape we expect under a LeftSemi parent: the inner Inner-Join is gone,
  // replaced by Filter(IsNotNull(c)) > Aggregate(group by k) > Filter(_cnt > 1) > Project([k]).
  private def assertRewritten(plan: LogicalPlan): Unit = {
    // Outer LeftSemi/LeftAnti join must remain.
    val outerJoins = plan.collect {
      case j: Join if j.joinType == LeftSemi || j.joinType == LeftAnti => j
    }
    assert(outerJoins.size == 1, s"Outer LeftSemi/LeftAnti join should remain, got:\n$plan")
    // The inner Inner self-join must be gone.
    val innerJoins = plan.collect { case j: Join if j.joinType == Inner => j }
    assert(innerJoins.isEmpty, s"Inner self-join should have been rewritten, got:\n$plan")
    // The merged Aggregate is present.
    val aggs = plan.collect { case a: Aggregate => a }
    assert(aggs.size == 1, s"Expected exactly one Aggregate, got:\n$plan")
    val agg = aggs.head
    // grouping is the equi-key.
    assert(agg.groupingExpressions.size == 1 &&
      agg.groupingExpressions.head.semanticEquals(k1),
      s"Expected grouping by $k1, got: ${agg.groupingExpressions}")
    // count(distinct c) is present.
    val countDistinct = agg.aggregateExpressions.collect {
      case Alias(ae: AggregateExpression, _)
          if ae.isDistinct && ae.aggregateFunction.isInstanceOf[Count] &&
            ae.aggregateFunction.children.exists {
              case a: Attribute => a.semanticEquals(c1)
              case _ => false
            } => ae
    }
    assert(countDistinct.size == 1,
      s"Expected count(DISTINCT $c1), got: ${agg.aggregateExpressions}")
    // IsNotNull(c) is present somewhere below the Aggregate (as part of a conjunctive
    // Filter that also asserts IsNotNull on every equi-key column for null-aware
    // LeftAnti soundness).
    val notNullOnC = plan.collect {
      case org.apache.spark.sql.catalyst.plans.logical.Filter(cond, _) =>
        cond.collect {
          case IsNotNull(a: Attribute) if a.semanticEquals(c1) => a
        }
    }.flatten
    assert(notNullOnC.nonEmpty,
      s"Expected an IsNotNull($c1) inside a Filter condition, got:\n$plan")
    // IsNotNull(k) is also present (NULL-key safety for null-aware LeftAnti).
    val notNullOnK = plan.collect {
      case org.apache.spark.sql.catalyst.plans.logical.Filter(cond, _) =>
        cond.collect {
          case IsNotNull(a: Attribute) if a.semanticEquals(k1) => a
        }
    }.flatten
    assert(notNullOnK.nonEmpty,
      s"Expected an IsNotNull($k1) for null-aware LeftAnti soundness, got:\n$plan")
  }

  private def assertNotRewritten(plan: LogicalPlan): Unit = {
    // The rule did NOT fire iff there is no Aggregate carrying our synthetic count alias.
    val ourAggregate = plan.collect {
      case a: Aggregate if a.aggregateExpressions.exists {
        case Alias(_, name) => name == "_self_join_to_aggregate_cnt"
        case _ => false
      } => a
    }
    assert(ourAggregate.isEmpty,
      s"Rule should not have fired, but found a SelfJoinToAggregate-produced Aggregate:\n$plan")
  }

  // ---- Positive cases ----

  test("LeftSemi parent over Inner self-join with non-equi residual: rewrites") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = wrapInLeftSemi(inner)
    assertRewritten(runWithFlag(plan))
  }

  test("LeftAnti parent over Inner self-join with non-equi residual: rewrites") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = outer.join(inner, LeftAnti, Some(outerId === k1))
    assertRewritten(runWithFlag(plan))
  }

  test("InferFilters-style IsNotNull conjuncts in the join condition: still rewrites") {
    // After InferFiltersFromConstraints runs in the second optimizer batch, the equi-key
    // attributes typically gain IsNotNull conjuncts on both sides. The rule must treat
    // these as informational and continue matching.
    val inner = tableSchema
      .join(tableSchema2, Inner,
        Some((k1 === k2) && (c1 =!= c2) && IsNotNull(k1) && IsNotNull(k2)))
      .select(k1)
    val plan = wrapInLeftSemi(inner)
    assertRewritten(runWithFlag(plan))
  }

  // ---- Negative cases: parent shape ----

  test("plain Project parent (no LeftSemi/LeftAnti wrapper): does not rewrite") {
    val plan = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    assertNotRewritten(runWithFlag(plan))
  }

  test("Inner-join parent (cardinality-sensitive): does not rewrite") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = outer.join(inner, Inner, Some(outerId === k1))
    assertNotRewritten(runWithFlag(plan))
  }

  test("LeftOuter parent: does not rewrite") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = outer.join(inner, LeftOuter, Some(outerId === k1))
    assertNotRewritten(runWithFlag(plan))
  }

  test("RightOuter parent: does not rewrite") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = outer.join(inner, RightOuter, Some(outerId === k1))
    assertNotRewritten(runWithFlag(plan))
  }

  // ---- Negative cases: inner-join shape ----

  test("flag disabled: no rewrite") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = wrapInLeftSemi(inner)
    assertNotRewritten(runWithFlag(plan, enabled = false))
  }

  test("inner is LeftSemi (not Inner): does not rewrite") {
    val inner = tableSchema
      .join(tableSchema2, LeftSemi, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = wrapInLeftSemi(inner)
    val optimized = runWithFlag(plan)
    // The inner LeftSemi is still there; outer LeftSemi is also there.
    val semiJoins = optimized.collect { case j: Join if j.joinType == LeftSemi => j }
    assert(semiJoins.size == 2,
      s"Inner LeftSemi should not be rewritten, got:\n$optimized")
  }

  test("project list references right-side column: does not rewrite") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1, c2.as("rhs_c"))
    // rhs_c is from the right side, fails the projectList-references-equi-keys check.
    val plan = wrapInLeftSemi(inner)
    assertNotRewritten(runWithFlag(plan))
  }

  test("project list references the non-equi-key column on left: does not rewrite") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1, c1)
    val plan = wrapInLeftSemi(inner)
    assertNotRewritten(runWithFlag(plan))
  }

  test("not a self-join (different schemas): does not rewrite") {
    val other = LocalRelation($"k".int, $"c".int, $"x".int)
    val inner = tableSchema
      .join(other, Inner, Some((k1 === other.output(0)) && (c1 =!= other.output(1))))
      .select(k1)
    val plan = wrapInLeftSemi(inner)
    assertNotRewritten(runWithFlag(plan))
  }

  test("no non-equi residual: does not rewrite") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some(k1 === k2))
      .select(k1)
    val plan = wrapInLeftSemi(inner)
    assertNotRewritten(runWithFlag(plan))
  }

  test("non-equi predicate on the equi-key column: does not rewrite") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (k1 =!= k2)))
      .select(k1)
    val plan = wrapInLeftSemi(inner)
    assertNotRewritten(runWithFlag(plan))
  }

  // ---- Negative cases: type guard ----

  test("non-equi column is a struct type: does not rewrite (NULL semantics divergence)") {
    val structType = StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", IntegerType, nullable = true)))
    val tStruct = LocalRelation($"k".int, AttributeReference("c", structType)())
    val tStruct2 = LocalRelation($"k".int, AttributeReference("c", structType)())
    val sk1 = tStruct.output(0)
    val sc1 = tStruct.output(1)
    val sk2 = tStruct2.output(0)
    val sc2 = tStruct2.output(1)
    val inner = tStruct
      .join(tStruct2, Inner, Some((sk1 === sk2) && (sc1 =!= sc2)))
      .select(sk1)
    val plan = outer.join(inner, LeftSemi, Some(outerId === sk1))
    assertNotRewritten(runWithFlag(plan))
  }

  test("non-equi column is an array type: does not rewrite") {
    val arrType = ArrayType(IntegerType, containsNull = true)
    val tArr = LocalRelation($"k".int, AttributeReference("c", arrType)())
    val tArr2 = LocalRelation($"k".int, AttributeReference("c", arrType)())
    val ak1 = tArr.output(0)
    val ac1 = tArr.output(1)
    val ak2 = tArr2.output(0)
    val ac2 = tArr2.output(1)
    val inner = tArr
      .join(tArr2, Inner, Some((ak1 === ak2) && (ac1 =!= ac2)))
      .select(ak1)
    val plan = outer.join(inner, LeftSemi, Some(outerId === ak1))
    assertNotRewritten(runWithFlag(plan))
  }

  // ---- Cost gate ----

  test("cost gate: relation smaller than broadcast threshold does not rewrite") {
    // Default broadcast threshold (~10 MB) is much larger than empty LocalRelation stats,
    // so the cost gate suppresses the rewrite.
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = wrapInLeftSemi(inner)

    withSQLConf(SQLConf.SELF_JOIN_TO_AGGREGATE_ENABLED.key -> "true") {
      // Note: we deliberately do NOT override AUTO_BROADCASTJOIN_THRESHOLD, leaving the
      // default in effect. This tests the cost gate, separate from runWithFlag's harness
      // override that disables broadcasting.
      assertNotRewritten(Optimize.execute(plan))
    }
  }

  // ---- Idempotence ----

  test("idempotence: applying the rule twice yields the same plan") {
    val inner = tableSchema
      .join(tableSchema2, Inner, Some((k1 === k2) && (c1 =!= c2)))
      .select(k1)
    val plan = wrapInLeftSemi(inner)
    val once = runWithFlag(plan)
    val twice = runWithFlag(once)
    comparePlans(once, twice)
  }
}
