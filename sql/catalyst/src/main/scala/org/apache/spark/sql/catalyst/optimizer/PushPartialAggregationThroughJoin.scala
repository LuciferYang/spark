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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, EXTERNAL_UDF, JOIN}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.types.{DataType, DecimalType, IntegralType, LongType, NumericType}
import org.apache.spark.sql.types.DecimalType.LongDecimal

/**
 * Push down the partial aggregation through join. It supports the following cases:
 * 1. Push down partial sum, count, avg, min, max, first and last through inner join.
 * 2. Partial deduplicate the children of join if the aggregation itself is group only.
 *
 * For example:
 * CREATE TABLE t1(a int, b int, c int) using parquet;
 * CREATE TABLE t2(x int, y int, z int) using parquet;
 * SELECT b, SUM(c) FROM t1 INNER JOIN t2 ON t1.a = t2.x GROUP BY b;
 *
 * The current optimized logical plan is:
 * Aggregate [b#2], [b#2, sum((_pushed_sum_c#13L * cnt#16L)) AS sum(c)#8L]
 * +- Project [b#2, _pushed_sum_c#13L, cnt#16L]
 *    +- Join Inner, (a#1 = x#4)
 *       :- PartialAggregate [a#1, b#2], [a#1, b#2, sum(c#3) AS _pushed_sum_c#13L]
 *       :  +- Project [b#2, c#3, a#1]
 *       :     +- Filter isnotnull(a#1)
 *       :        +- Relation default.t1[a#1,b#2,c#3] parquet
 *       +- PartialAggregate [x#4], [count(1) AS cnt#16L, x#4]
 *          +- Project [x#4]
 *             +- Filter isnotnull(x#4)
 *                +- Relation default.t2[x#4,y#5,z#6] parquet
 *
 * This rule should be applied after Join Reorder.
 */
object PushPartialAggregationThroughJoin extends Rule[LogicalPlan]
  with JoinSelectionHelper
  with PredicateHelper {

  def pushPartialAggHasBenefit(
                                groupingExps: Seq[Expression],
                                plan: LogicalPlan): Boolean = {
    val originRowCount = plan.stats.rowCount
    val aggregatedRowCount = Aggregate(groupingExps, Nil, plan).stats.rowCount
    val ratio = if (aggregatedRowCount.nonEmpty && originRowCount.nonEmpty) {
      aggregatedRowCount.get.toDouble / originRowCount.get.toDouble
    } else {
      1.0
    }
    ratio <= conf.partialAggregationOptimizationBenefitRatio
  }

  /**
   * Supported join:
   * 1. Join will expansion a lot.
   * 2. It is not broadcast hash join
   */
  private def supportedJoin(join: Join): Boolean = {
    !(canPlanAsBroadcastHashJoin(join, conf) &&
      join.stats.rowCount.exists { joinCnt =>
        join.children.map(_.stats.rowCount).max.exists(maxCnt => joinCnt <= maxCnt * 2)
      })
  }

  // Returns true if `expr`'s references is non empty and can be evaluated using
  // the output of `plan`.
  private def canEvaluateOnly(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.nonEmpty && canEvaluate(expr, plan)

  // Returns true if `expr`'s references is empty or it can be evaluated using one side.
  private def canEvaluateOnOneSide(
                                    expr: Expression,
                                    leftNamedExpressions: Seq[NamedExpression],
                                    rightNamedExpressions: Seq[NamedExpression]): Boolean = {
    expr.references.subsetOf(AttributeSet(leftNamedExpressions.map(_.toAttribute))) ||
      expr.references.subsetOf(AttributeSet(rightNamedExpressions.map(_.toAttribute)))
  }

  // Splits expressions into three categories based on the attributes required to evaluate them.
  private def split(expressions: Seq[NamedExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftExprs, rest) = expressions.partition(canEvaluateOnly(_, left))
    val (rightExprs, remainingExps) = rest.partition(canEvaluateOnly(_, right))

    (leftExprs, rightExprs, remainingExps)
  }

  // Splits expressions into three categories based on the attributes required to evaluate them.
  private def splitAggregateExpressions(
    aggExps: Seq[AggregateExpression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftAggExprs, rest) = aggExps.partition(canEvaluateOnly(_, left))
    val (rightAggExprs, remainingAggExps) = rest.partition(canEvaluateOnly(_, right))

    (toExpressionMap(leftAggExprs), toExpressionMap(rightAggExprs), remainingAggExps)
  }

  // Convert aggregate expressions to a map, the key used to replace the current Aggregate
  // and the value used to push through Join. see the function of replaceAliasName.
  private def toExpressionMap(aggExps: Seq[AggregateExpression]) = {
    aggExps.map { a =>
      val name =
        s"_pushed_${a.aggregateFunction.prettyName}_${a.references.map(_.name).mkString("_")}"
      a.aggregateFunction.canonicalized -> Alias(a, name)()
    }.toMap[Expression, Alias]
  }

  // Replace the current Aggregate's aggregate expression references with pushed attribute:
  // 1. hasBenefit = true and otherSideCnt is not empty:
  //   - Replace the sum child with the current side pushed sum * the other side row count.
  //     e.g. sum(a) -> sum(_pushed_sum_a * cnt)
  //   - Rewrite the count child with the current side pushed count * the other side row count and
  //     sum it.
  //     e.g. count(a) -> sum(_pushed_count_a * cnt)
  // 2. hasBenefit = true and otherSideCnt is empty:
  //   - Replace the sum child with the current side pushed sum.
  //     e.g. sum(a) -> sum(_pushed_sum_a)
  //   - Rewrite the count child with the current side row count and sum it.
  //     e.g. count(a) -> sum(_pushed_count_a)
  // 3. hasBenefit = false and otherSideCnt is not empty:
  //   - Replace the sum child with the current side sum child * other side row count.
  //     e.g. sum(a) -> sum(a * cnt)
  //   - Rewrite the count child with if(child is null, 0, 1) * other side row count and sum it.
  //     e.g. count(a) -> sum(if(a is null, 0, 1) * cnt)
  private def replaceAliasName(
                                expr: NamedExpression,
                                aliasMap: Map[Expression, Alias],
                                hasBenefit: Boolean,
                                otherSideCnt: Option[Attribute]): NamedExpression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId.
    expr.mapChildren(_.transformUp {
      case ae @ AggregateExpression(e @ Sum(_, useAnsiAdd, dt), _, _, _, _)
          if aliasMap.contains(e.canonicalized) =>
        val resultType = e.dataType
        val countType = if (resultType.isInstanceOf[DecimalType]) LongDecimal else resultType
        val child = if (hasBenefit) aliasMap(e.canonicalized).toAttribute else e.child
        val newChild = otherSideCnt.map { c =>
          val ctx = multiplyContext(useAnsiAdd, resultType)
          val product = Multiply(child.cast(resultType), c.cast(countType), ctx)
          resultType match {
            case d: DecimalType if tracksOverflow(e) => CheckOverflow(product, d, true)
            case _ => product
          }
        }.getOrElse(child)
        val newSum = ae.copy(aggregateFunction =
          Sum(newChild, useAnsiAdd, Some(dt.getOrElse(resultType))))
        if (tracksOverflow(e)) {
          val nonEmpty = if (hasBenefit) {
            aliasMap.get(Count(e.child).canonicalized)
              .map(n => GreaterThan(n.toAttribute, Literal(0L)))
              .getOrElse(Literal.TrueLiteral)
          } else {
            IsNotNull(e.child)
          }
          nullOnOverflow(newSum, newChild, nonEmpty)
        } else {
          newSum
        }
      case ae @ AggregateExpression(e: Count, _, _, _, _) if aliasMap.contains(e.canonicalized) =>
        val child = if (hasBenefit) {
          aliasMap(e.canonicalized).toAttribute
        } else {
          If(e.children.map(IsNull).reduce(Or), Literal(0L, LongType), Literal(1L, LongType))
        }
        val newChild = otherSideCnt.map(Multiply(child, _)).getOrElse(child)
        sumOfCounts(ae, newChild)
      case e: Min if hasBenefit && aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: Max if hasBenefit && aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: First if hasBenefit && aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
      case e: Last if hasBenefit && aliasMap.contains(e.canonicalized) =>
        e.copy(child = aliasMap(e.canonicalized).toAttribute)
    }).asInstanceOf[NamedExpression]
  }

  // An external UDF is not pushed: `PlanExternalUDFs` lifts one out of a plain `Aggregate` only,
  // so in a `PartialAggregate` or `FinalAggregate` it would be evaluated below the aggregate.
  private def pushableExpression(e: Expression): Boolean =
    e.deterministic && InjectRuntimeFilter.isSimpleExpression(e) && !e.containsPattern(EXTERNAL_UDF)

  // Whether `sum` returns null on overflow, which it detects with an isEmpty flag in its buffer
  // (see `Sum.shouldTrackIsEmpty`). A pushed sum outputs its evaluated value instead, which is null
  // after an overflow, and the sum above it skips nulls.
  private def tracksOverflow(sum: Sum): Boolean = sum.evalContext.evalMode match {
    case EvalMode.ANSI => false
    case mode => sum.dataType match {
      case _: DecimalType => true
      case _: IntegralType => mode == EvalMode.TRY
      case _ => false
    }
  }

  // Returns null when an input of `sum` is null although the values it was computed from are not,
  // which only an overflow causes.
  private def nullOnOverflow(
      sum: AggregateExpression,
      input: Expression,
      inputNonEmpty: Expression): Expression = {
    val overflow = Max(And(IsNull(input), inputNonEmpty)).toAggregateExpression()
    If(overflow, Literal.create(null, sum.dataType), sum)
  }

  // Replaces `count` with the sum of `counts`. A count is 0, not null, on an empty input, even in a
  // grouped aggregate: `RewriteCorrelatedScalarSubquery` computes what a decorrelated subquery
  // returns for no rows from each aggregate's value on an empty input. This also keeps the column
  // non-nullable.
  private def sumOfCounts(count: AggregateExpression, counts: Expression): Expression =
    Coalesce(Seq(
      count.copy(aggregateFunction = Sum(counts, resultDataType = Some(LongType))), Literal(0L)))

  // The product of a pushed decimal and a row count keeps the scale of the decimal.
  private def multiplyContext(ctx: NumericEvalContext, dataType: DataType): NumericEvalContext =
    dataType match {
      case _: DecimalType => NumericEvalContext(ctx.evalMode, allowDecimalPrecisionLoss = false)
      case _ => ctx
    }

  private def pushableAggExp(ae: AggregateExpression): Boolean = ae match {
    case AggregateExpression(Sum(e, _, _), Complete, false, None, _) =>
      e.dataType.isInstanceOf[NumericType]
    case AggregateExpression(_: Min, Complete, false, None, _) => true
    case AggregateExpression(_: Max, Complete, false, None, _) => true
    case AggregateExpression(_: First, Complete, false, None, _) => true
    case AggregateExpression(_: Last, Complete, false, None, _) => true
    case AggregateExpression(Average(e, _), Complete, false, None, _) =>
      e.dataType.isInstanceOf[NumericType]
    case _ => false
  }

  // Support count(*), count(id)
  private def pushableCountExp(ae: AggregateExpression): Boolean = ae match {
    case AggregateExpression(_: Count, Complete, false, None, _) => true
    case _ => false
  }

  private def deduplicateNamedExpressions(
    aggregateExpressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    ExpressionSet(aggregateExpressions).toSeq.map(_.asInstanceOf[NamedExpression])
  }

  // Rewrite Average to Sum / Count(*). Please see AverageBase.getEvaluateExpression
  private def rewriteAverage(agg: Aggregate): Aggregate = {
    if (agg.collectAggregateExprs.exists(_.aggregateFunction.isInstanceOf[Average])) {
      val newAggAggregateExpressions = agg.aggregateExpressions.map { expr =>
        expr.mapChildren(_.transformUp {
          case ae @ AggregateExpression(af, _, _, _, _) => af match {
            case avg @ Average(e, useAnsiAdd) if e.references.nonEmpty =>
              val evalCtx = NumericEvalContext(useAnsiAdd)
              val sum = Sum(e, evalCtx, Some(avg.sumDataType))
                .toAggregateExpression()
              val count = Count(e).toAggregateExpression()
              e.dataType match {
                case _: DecimalType =>
                  // As Average.evaluateExpression.
                  If(EqualTo(count, Literal(0L)), Literal(null, avg.dataType),
                    DecimalDivideWithOverflowCheck(sum, count.cast(DecimalType.LongDecimal),
                      avg.dataType.asInstanceOf[DecimalType], avg.getContextOrNull(),
                      useAnsiAdd != EvalMode.ANSI))
                case _ =>
                  Divide(sum.cast(avg.dataType), count.cast(avg.dataType))
              }
            case _ => ae
          }
        }).asInstanceOf[NamedExpression]
      }

      agg.copy(aggregateExpressions = newAggAggregateExpressions, hint = agg.hint)
    } else {
      agg
    }
  }

  private def pullOutJoinKeys(
    joinKeys: Seq[Expression]): (Seq[Attribute], ArrayBuffer[NamedExpression]) = {
    val complexJoinKeys = new ArrayBuffer[NamedExpression]()
    val newJoinKeys = joinKeys.map {
      case a: Attribute => a
      case o =>
        val ne = Alias(o, s"_pullout_${o.prettyName}_${o.references.map(_.name).mkString("_")}")()
        complexJoinKeys += ne
        ne.toAttribute
    }
    (newJoinKeys, complexJoinKeys)
  }

  private def constructPartialAgg(
    joinKeys: Seq[Attribute],
    groupExps: Seq[NamedExpression],
    remainingExps: Seq[NamedExpression],
    aliasMap: Map[Expression, Alias],
    rowCnt: Alias,
    plan: LogicalPlan): PartialAggregate = {
    val partialGroupingExps = ExpressionSet(joinKeys ++ groupExps).toSeq
    val partialAggExps = joinKeys ++ groupExps ++ remainingExps ++ (aliasMap.values.toSeq :+ rowCnt)
    PartialAggregate(partialGroupingExps, deduplicateNamedExpressions(partialAggExps), plan)
  }

  private def pushDistinctThroughJoin(join: Join): Join = {
    var left = join.left
    var right = join.right

    // A non-deterministic condition is evaluated once per pair of rows, so removing rows changes
    // how often it is evaluated.
    val deterministic = join.condition.forall(_.deterministic)
    val pushLeftHasBenefit =
      deterministic && canDeduplicate(left) && pushPartialAggHasBenefit(left.output, left)
    val pushRightHasBenefit =
      deterministic && canDeduplicate(right) && pushPartialAggHasBenefit(right.output, right)

    if (pushLeftHasBenefit || pushRightHasBenefit) {
      left =
        if (pushLeftHasBenefit) PartialAggregate(left.output, left.output, left) else left
      right =
        if (pushRightHasBenefit) PartialAggregate(right.output, right.output, right) else right
      join.copy(left = left, right = right)
    } else {
      join
    }
  }

  // A session window is merged by `MergingSessionsExec` before the aggregate that groups by it,
  // which the pushed rewrite does not plan.
  private def isSessionWindow(e: Expression): Boolean = e match {
    case a: Attribute => a.metadata.contains(SessionWindow.marker)
    case _ => false
  }

  // Whether a parent of `plan` sees the same values when `plan` is deduplicated within each
  // partition. Without output columns the deduplication is a global aggregate, which outputs a row
  // even for an empty partition. A column that is not binary stable keeps one of its equal values.
  // Floating-point columns are fine: a PartialAggregate groups on their raw values, which is finer
  // than SQL equality.
  private[optimizer] def canDeduplicate(plan: LogicalPlan): Boolean =
    plan.output.nonEmpty && plan.output.forall(a => UnsafeRowUtils.isBinaryStable(a.dataType))

  // The entry of push down partial aggregate through join.
  // Will return the current aggregate if it can't push down.
  private def pushAggThroughJoin(
    agg: Aggregate,
    projectList: Seq[NamedExpression],
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    join: Join): LogicalPlan = {
    val rewrittenAgg = rewriteAverage(agg)
    val aggregateExpressions = rewrittenAgg.collectAggregateExprs

    val (leftProjectList, rightProjectList, remainingProjectList) =
      split(join.condition.map(_.references.toSeq).getOrElse(Nil) ++ projectList,
        join.left, join.right)

    // remainingProjectList must should be empty. We do not support this case:
    // SELECT b + y, SUM(c) FROM t1 INNER JOIN t2 ON t1.a = t2.x GROUP BY 1
    //
    // Supported cases:
    // 1. groupingExpressions is not empty and aggregateExpressions are pushableAggExp or
    //    pushableCountExp
    // 2. groupingExpressions is empty and aggregateExpressions are pushableAggExp
    if (remainingProjectList.isEmpty &&
      ((rewrittenAgg.groupingExpressions.nonEmpty &&
        aggregateExpressions.forall(e => (pushableAggExp(e) || pushableCountExp(e)) &&
          canEvaluateOnOneSide(e, leftProjectList, rightProjectList))) ||
        (rewrittenAgg.groupingExpressions.isEmpty &&
          aggregateExpressions.forall(e => pushableAggExp(e) &&
            canEvaluateOnOneSide(e, leftProjectList, rightProjectList))))) {

      val pushedLeftProject = Project(leftProjectList, join.left)
      val pushedRightProject = Project(rightProjectList, join.right)

      // All groupingExpressions are Attributes, see PullOutGroupingExpressions.
      // Splits groupingExpressions into three categories based on the attributes.
      // We will use it as aggregateExpressions in PartialAggregate
      val (leftGroupExps, rightGroupExps, _) =
      split(rewrittenAgg.groupingExpressions.map(_.asInstanceOf[Attribute]),
        pushedLeftProject, pushedRightProject)

      val pushLeftHasBenefit = pushPartialAggHasBenefit(
        AttributeSet(leftKeys ++ leftGroupExps).toSeq, pushedLeftProject)
      val pushRightHasBenefit = pushPartialAggHasBenefit(
        AttributeSet(rightKeys ++ rightGroupExps).toSeq, pushedRightProject)

      if (pushLeftHasBenefit || pushRightHasBenefit) {
        val nonEmptyCounts = aggregateExpressions.collect {
          case AggregateExpression(s: Sum, _, _, _, _)
              if tracksOverflow(s) && s.references.nonEmpty && s.child.nullable =>
            Count(s.child).toAggregateExpression()
        }
        val (leftAliasMap, rightAliasMap, _) = splitAggregateExpressions(
          aggregateExpressions ++ nonEmptyCounts, pushedLeftProject, pushedRightProject)

        val remainingAggregateExps = rewrittenAgg.aggregateExpressions
          .filterNot(_.exists(_.isInstanceOf[AggregateFunction]))
        val (leftRemainingExps, rightRemainingExps, _) =
          split(remainingAggregateExps, pushedLeftProject, pushedRightProject)

        // pull out complex join condition
        val (newLeftJoinKeys, complexLeftJoinKeys) = pullOutJoinKeys(leftKeys)
        val (newRightJoinKeys, complexRightJoinKeys) = pullOutJoinKeys(rightKeys)

        val pullOutedLeft = pushedLeftProject
          .copy(projectList = deduplicateNamedExpressions(leftProjectList ++ complexLeftJoinKeys))
        val pullOutedRight = pushedRightProject
          .copy(projectList = deduplicateNamedExpressions(rightProjectList ++ complexRightJoinKeys))
        val newCond = newLeftJoinKeys.zip(newRightJoinKeys)
          .map { case (l, r) => EqualTo(l, r) }
          .reduceLeftOption(And)

        // Construct partial aggregate and rewrite current aggregate
        val cntExp = Count(Seq(Literal(1))).toAggregateExpression()
        val leftCnt = if (pushLeftHasBenefit) Some(Alias(cntExp, "cnt")()) else None
        val rightCnt = if (pushRightHasBenefit) Some(Alias(cntExp, "cnt")()) else None
        val leftCntAttr = leftCnt.map(_.toAttribute)
        val rightCntAttr = rightCnt.map(_.toAttribute)

        val newLeft = leftCnt.map(constructPartialAgg(newLeftJoinKeys, leftGroupExps,
          leftRemainingExps, leftAliasMap, _, pullOutedLeft)).getOrElse(pullOutedLeft)
        val newRight = rightCnt.map(constructPartialAgg(newRightJoinKeys, rightGroupExps,
          rightRemainingExps, rightAliasMap, _, pullOutedRight)).getOrElse(pullOutedRight)
        val newJoin = join.copy(left = newLeft, right = newRight, condition = newCond)

        val newAggregateExps = rewrittenAgg.aggregateExpressions
          .map(replaceAliasName(_, leftAliasMap, pushLeftHasBenefit, rightCntAttr))
          .map(replaceAliasName(_, rightAliasMap, pushRightHasBenefit, leftCntAttr))
          .map { expr =>
            // At least one of leftCntAttr/rightCntAttr is defined because
            // the outer guard requires pushLeftHasBenefit || pushRightHasBenefit.
            val cntAttrs = (leftCntAttr ++ rightCntAttr).map(_.asInstanceOf[Expression]).toSeq
            assert(cntAttrs.nonEmpty)
            expr.mapChildren(_.transformUp {
              case ae @ AggregateExpression(Count(Seq(IntegerLiteral(1))), _, _, _, _) =>
                sumOfCounts(ae, cntAttrs.reduceLeft(_ * _))
              case ae @ AggregateExpression(e @ Sum(v, useAnsiAdd, dt), _, _, _, _)
                  if e.references.isEmpty =>
                val cnt = cntAttrs.reduceLeft(_ * _)
                val newChild = e.dataType match {
                  case decType: DecimalType =>
                    // Do not use DecimalPrecision because it may be change the precision and scale
                    val multiply = Multiply(v.cast(decType), cnt.cast(LongDecimal),
                      multiplyContext(useAnsiAdd, decType))
                    CheckOverflow(multiply, decType, useAnsiAdd.evalMode != EvalMode.ANSI)
                  case _ =>
                    Multiply(v.cast(e.dataType), cnt.cast(e.dataType), useAnsiAdd)
                }
                val newSum = ae.copy(aggregateFunction =
                  Sum(newChild, useAnsiAdd, Some(dt.getOrElse(e.dataType))))
                if (tracksOverflow(e)) nullOnOverflow(newSum, newChild, IsNotNull(v)) else newSum
              // These expression do not need to rewrite:
              // Min/Max(Literal(_)), First/Last(Literal(_), _) and Average(Literal(_), _)
            }).asInstanceOf[NamedExpression]
          }

        val newAgg = if (ExpressionSet(leftKeys).subsetOf(ExpressionSet(leftGroupExps)) ||
          ExpressionSet(rightKeys).subsetOf(ExpressionSet(rightGroupExps))) {
          FinalAggregate(
            rewrittenAgg.groupingExpressions, newAggregateExps, newJoin, rewrittenAgg.hint)
        } else {
          rewrittenAgg.copy(aggregateExpressions = newAggregateExps, child = newJoin)
        }

        ResolveTimeZone(SimplifyCasts(CollapseProject(ColumnPruning(newAgg))))
      } else {
        agg
      }
    } else {
      // We will not rewrite average if it can't push down through join.
      agg
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      plan.transformWithPruning(_.containsAllPatterns(AGGREGATE, JOIN), ruleId) {
        // A FinalAggregate or PartialAggregate is not planned as a stateful aggregate.
        case agg: Aggregate if agg.isStreaming =>
          agg
        case agg @ Aggregate(_, _, j: Join, _)
          if j.children.exists(_.isInstanceOf[AggregateBase]) =>
          agg
        case agg @ Aggregate(_, _, Project(_, j: Join), _)
          if j.children.exists(_.isInstanceOf[AggregateBase]) =>
          agg

        case agg @ Aggregate(_, aggExps,
        j @ Join(_, _, Inner | LeftOuter | RightOuter | FullOuter | Cross, _, _), hint)
          if aggExps.forall(_.deterministic) && agg.collectAggregateExprs.forall(_.isDistinct) &&
            supportedJoin(j) =>
          agg.copy(child = pushDistinctThroughJoin(j), hint = hint)

        case agg @ Aggregate(_, aggExps, p @ Project(projectList,
        j @ Join(_, _, Inner | LeftOuter | RightOuter | FullOuter | Cross, _, _)), hint)
          if aggExps.forall(_.deterministic) && projectList.forall(_.deterministic) &&
            agg.collectAggregateExprs.forall(_.isDistinct) && supportedJoin(j) =>
          agg.copy(child = p.copy(child = pushDistinctThroughJoin(j)), hint = hint)

        case agg @ Aggregate(groupExps, aggExps,
        j @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, _, _, _, _), _)
          if groupExps.forall(_.isInstanceOf[Attribute]) && leftKeys.nonEmpty &&
            !groupExps.exists(isSessionWindow) &&
            aggExps.forall(pushableExpression) &&
            agg.collectAggregateExprs.forall(e => pushableAggExp(e) || pushableCountExp(e)) &&
            supportedJoin(j) =>
          pushAggThroughJoin(agg, j.output, leftKeys, rightKeys, j)

        case agg @ Aggregate(groupExps, aggExps, Project(projectList,
        j @ ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, None, _, _, _, _)), hint)
          if groupExps.forall(_.isInstanceOf[Attribute]) && leftKeys.nonEmpty &&
            !groupExps.exists(isSessionWindow) &&
            aggExps.forall(pushableExpression) &&
            projectList.forall(_.deterministic) &&
            agg.collectAggregateExprs.forall(e => pushableAggExp(e) || pushableCountExp(e)) &&
            supportedJoin(j) =>
          pushAggThroughJoin(agg, projectList, leftKeys, rightKeys, j)
      }
    }
  }
}
