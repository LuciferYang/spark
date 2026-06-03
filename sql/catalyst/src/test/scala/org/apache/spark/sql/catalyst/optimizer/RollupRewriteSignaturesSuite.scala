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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.RollupRewriteSignatures._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

/**
 * Unit tests for the fingerprints [[RewriteUnionAggregateAsRollup]] compares branches
 * with. Each case is a PAIR that must not collide: a collision means the rule accepts
 * two branches that compute different things and replays one for every level.
 * [[RewriteUnionAggregateAsRollupSuite]] covers the same ground through whole plans;
 * these pin the primitive, which is the part that grows as Catalyst adds classes.
 */
class RollupRewriteSignaturesSuite extends SparkFunSuite {

  private val x = AttributeReference("x", IntegerType)()
  private val y = AttributeReference("y", IntegerType)()
  private val d = AttributeReference("d", DateType)()
  private val st = AttributeReference("st",
    StructType(Seq(StructField("f1", IntegerType), StructField("f2", IntegerType))))()
  private val dbl = AttributeReference("dbl", DoubleType)()

  private val childOutput = Seq(x, y, d, st, dbl)

  private def legacySum(e: Expression): Sum = Sum(e, NumericEvalContext(EvalMode.LEGACY))

  /** Expression pairs that differ ONLY in state `toString` (and, for most of them,
   *  `canonicalized`) hides, i.e. the reason [[hiddenStateMarkers]] exists.
   */
  private val hiddenStatePairs: Seq[(String, Expression, Expression)] = Seq(
    ("Sum eval mode",
      Sum(x, NumericEvalContext(EvalMode.ANSI)), Sum(x, NumericEvalContext(EvalMode.LEGACY))),
    ("Average eval mode", Average(x, EvalMode.ANSI), Average(x, EvalMode.LEGACY)),
    ("Cast eval mode",
      Cast(x, LongType, None, EvalMode.ANSI), Cast(x, LongType, None, EvalMode.LEGACY)),
    ("Cast timezone",
      Cast(d, TimestampType, Some("UTC")), Cast(d, TimestampType, Some("Asia/Shanghai"))),
    ("StddevSamp divide-by-zero result",
      StddevSamp(x, nullOnDivideByZero = true), StddevSamp(x, nullOnDivideByZero = false)),
    ("CovPopulation divide-by-zero result",
      CovPopulation(x, y, nullOnDivideByZero = true),
      CovPopulation(x, y, nullOnDivideByZero = false)),
    ("UnaryMinus overflow behavior",
      UnaryMinus(x, failOnError = true), UnaryMinus(x, failOnError = false)),
    ("Abs overflow behavior", Abs(x, failOnError = true), Abs(x, failOnError = false)),
    ("Round ANSI flag",
      Round(x, Literal(1), ansiEnabled = true), Round(x, Literal(1), ansiEnabled = false)),
    // UnaryMathExpression renders only CEIL(child) / FLOOR(child), so the flag is
    // invisible to every other component of the signature.
    ("Ceil ANSI flag", Ceil(dbl, failOnError = true), Ceil(dbl, failOnError = false)),
    ("Floor ANSI flag", Floor(dbl, failOnError = true), Floor(dbl, failOnError = false)),
    // to_xml's options are a constructor Map, not Literal children, so they reach
    // the signature only through toString -- where stripIds turns both of these
    // into "n#".
    ("to_xml option values",
      StructsToXml(Map("nullValue" -> "n#1"), st, Some("UTC")),
      StructsToXml(Map("nullValue" -> "n#2"), st, Some("UTC"))),
    // Both render "st.f": only the ordinal says which field is read.
    ("GetStructField ordinal",
      GetStructField(st, 0, Some("f")), GetStructField(st, 1, Some("f"))))

  hiddenStatePairs.foreach { case (label, a, b) =>
    test(s"hidden state must change the signature: $label") {
      assert(hiddenStateMarkers(a) !== hiddenStateMarkers(b))
      assert(exprSignature(a, childOutput) !== exprSignature(b, childOutput))
    }
  }

  test("exprSignature ignores ExprIds so CTE copies of one measure still match") {
    val fresh = AttributeReference("x", IntegerType)()
    val freshOutput = Seq(fresh, y, d, st)
    assert(exprSignature(legacySum(x), childOutput) ===
      exprSignature(legacySum(fresh), freshOutput))
  }

  test("exprSignature separates same-named columns by ordinal") {
    // The join trap: t1.x and t2.x both render "x#" once ExprIds are stripped, so
    // only the child-output ordinal tells the two measures apart.
    val left = AttributeReference("x", IntegerType)()
    val right = AttributeReference("x", IntegerType)()
    val joined = Seq(left, right)
    assert(exprSignature(legacySum(left), joined) !== exprSignature(legacySum(right), joined))
  }

  test("exprSignature compares literal values un-stripped") {
    // stripIds would turn both of these into "v#".
    assert(exprSignature(Literal("v#1"), childOutput) !==
      exprSignature(Literal("v#2"), childOutput))
  }

  test("aggMeasureSignature separates DISTINCT and FILTER") {
    val plain = AggregateExpression(legacySum(x), Complete, isDistinct = false)
    assert(aggMeasureSignature(plain, childOutput) !==
      aggMeasureSignature(plain.copy(isDistinct = true), childOutput))
    assert(aggMeasureSignature(plain, childOutput) !==
      aggMeasureSignature(plain.copy(filter = Some(GreaterThan(y, Literal(0)))), childOutput))
  }

  test("canonicalKey compares a relation's data, which its rendered form omits") {
    val a = AttributeReference("a", IntegerType)()
    val one = LocalRelation(Seq(a), Seq(InternalRow(1)))
    val two = LocalRelation(Seq(a), Seq(InternalRow(2)))
    assert(one.toString === two.toString, "precondition: toString hides the data")
    assert(canonicalKey(one) !== canonicalKey(two))
    assert(canonicalHash(one) !== canonicalHash(two))
  }

  test("canonicalKey matches structurally identical relations with fresh ExprIds") {
    val a = AttributeReference("a", IntegerType)()
    val fresh = AttributeReference("a", IntegerType)()
    assert(canonicalKey(LocalRelation(Seq(a), Seq(InternalRow(1)))) ===
      canonicalKey(LocalRelation(Seq(fresh), Seq(InternalRow(1)))))
  }
}
