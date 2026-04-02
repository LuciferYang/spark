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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeMap, BinaryArithmetic, BinaryComparison, Expression, Literal, NamedExpression, Or, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.read.SupportsMerge
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Result of attempting to merge a plan via [[PlanMerger.merge]].
 *
 * @param mergedPlan The resulting plan, either:
 *                   - An existing cached plan (if identical match found)
 *                   - A newly merged plan combining the input with a cached plan
 *                   - The original input plan (if no merge was possible)
 * @param mergedPlanIndex The index of this plan in the PlanMerger's cache.
 * @param outputMap Maps attributes from the input plan to corresponding attributes in
 *                  `mergedPlan`. Used to rewrite expressions referencing the original plan
 *                  to reference the merged plan instead.
 */
case class MergeResult(
    mergedPlan: MergedPlan,
    mergedPlanIndex: Int,
    outputMap: AttributeMap[Attribute])

/**
 * Represents a plan in the PlanMerger's cache.
 *
 * @param plan The logical plan, which may have been merged from multiple original plans.
 * @param merged Whether this plan is the result of merging two or more plans (true), or
 *               is an original unmerged plan (false). Merged plans typically require special
 *               handling such as wrapping in CTEs.
 */
case class MergedPlan(plan: LogicalPlan, merged: Boolean)

/**
 * A stateful utility for merging identical or similar logical plans to enable query plan reuse.
 *
 * `PlanMerger` maintains a cache of previously seen plans and attempts to either:
 * 1. Reuse an identical plan already in the cache
 * 2. Merge a new plan with a cached plan by combining their outputs
 *
 * The merging process preserves semantic equivalence while combining outputs from multiple
 * plans into a single plan. This is primarily used by [[MergeSubplans]] to deduplicate subplan
 * execution.
 *
 * Supported plan types for merging:
 * - [[Project]]: Merges project lists
 * - [[Aggregate]]: Merges aggregate expressions with identical grouping
 * - [[Filter]]: Requires identical filter conditions
 * - [[Join]]: Requires identical join type, hints, and conditions
 *
 * @example
 * {{{
 *   val merger = PlanMerger()
 *   val result1 = merger.merge(plan1)  // Adds plan1 to cache
 *   val result2 = merger.merge(plan2)  // Merges with plan1 if compatible
 *   // result2.mergedPlan.merged == true if plans were merged
 *   // result2.outputMap maps plan2's attributes to the merged plan's attributes
 * }}}
 */
class PlanMerger extends Logging {
  private val cache = ArrayBuffer.empty[MergedPlan]

  /**
   * Attempts to merge the given plan with cached plans, or adds it to the cache.
   *
   * The method tries the following in order:
   * 1. Check if an identical plan exists in cache (using canonicalized comparison)
   * 2. Try to merge with each cached plan using [[tryMergePlans]]
   * 3. If no merge is possible, add as a new cache entry
   *
   * @param plan The logical plan to merge or cache.
   * @param subqueryPlan If the logical plan is a subquery plan.
   * @return A [[MergeResult]] containing:
   *         - The merged/cached plan to use
   *         - Its index in the cache
   *         - An attribute mapping for rewriting expressions
   */
  def merge(plan: LogicalPlan, subqueryPlan: Boolean): MergeResult = {
    cache.zipWithIndex.collectFirst(Function.unlift {
      case (mp, i) =>
        checkIdenticalPlans(plan, mp.plan).map { outputMap =>
          // Identical subquery expression plans are not marked as `merged` as the
          // `ReusedSubqueryExec` rule can handle them without extracting the plans to CTEs.
          // But, when a non-subquery subplan is identical to a cached plan we need to mark the plan
          // `merged` and so extract it to a CTE later.
          val newMergePlan = MergedPlan(mp.plan, cache(i).merged || !subqueryPlan)
          cache(i) = newMergePlan
          MergeResult(newMergePlan, i, outputMap)
        }.orElse {
          tryMergePlans(plan, mp.plan, false).collect {
            case (mergedPlan, outputMap, None, None, _) =>
              val newMergePlan = MergedPlan(mergedPlan, true)
              cache(i) = newMergePlan
              MergeResult(newMergePlan, i, outputMap)
          }
        }
      case _ => None
    }).getOrElse {
      val newMergePlan = MergedPlan(plan, false)
      cache += newMergePlan
      val outputMap = AttributeMap(plan.output.map(a => a -> a))
      MergeResult(newMergePlan, cache.length - 1, outputMap)
    }
  }

  /**
   * Returns all plans currently in the cache as an immutable indexed sequence.
   *
   * @return An indexed sequence of [[MergedPlan]]s in cache order. The index of each plan
   *         corresponds to the `mergedPlanIndex` returned by [[merge]].
   */
  def mergedPlans(): IndexedSeq[MergedPlan] = cache.toIndexedSeq

  // If 2 plans are identical return the attribute mapping from the new to the cached version.
  private def checkIdenticalPlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[AttributeMap[Attribute]] = {
    if (newPlan.canonicalized == cachedPlan.canonicalized) {
      Some(AttributeMap(newPlan.output.zip(cachedPlan.output)))
    } else {
      None
    }
  }

  /**
   * Recursively traverse down and try merging 2 plans.
   *
   * @param newPlan a new plan to merge into a cached plan
   * @param cachedPlan a cached plan (original or already merged)
   * @param filterPropagationSupported signals that an Aggregate
   *   node above can accept propagated filters
   * @return A tuple of the merged plan, attribute mapping,
   *   two optional propagated filters and optional cost.
   */
  private def tryMergePlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan,
      filterPropagationSupported: Boolean):
    Option[(LogicalPlan, AttributeMap[Attribute],
        Option[Expression], Option[Expression],
        Option[Double])] = {
    checkIdenticalPlans(newPlan, cachedPlan).map { outputMap =>
      val mergeCost =
        if (filterPropagationSupported) Some(0d) else None
      (cachedPlan, outputMap, None, None, mergeCost)
    }.orElse(
      (newPlan, cachedPlan) match {
        case (np: Project, cp: Project) =>
          tryMergePlans(
            np.child, cp.child, filterPropagationSupported
          ).map {
            case (mergedChild, outputMap,
                newChildFilter, mergedChildFilter,
                childMergeCost) =>
              val (mergedProjectList, newOutputMap,
                  newPlanFilter, mergedPlanFilter,
                  mergeCost) =
                mergeNamedExpressions(
                  np.projectList, outputMap,
                  cp.projectList, newChildFilter,
                  mergedChildFilter, childMergeCost)
              val mergedPlan =
                Project(mergedProjectList, mergedChild)
              (mergedPlan, newOutputMap,
                newPlanFilter, mergedPlanFilter, mergeCost)
          }
        case (np, cp: Project) =>
          tryMergePlans(
            np, cp.child, filterPropagationSupported
          ).map {
            case (mergedChild, outputMap,
                newChildFilter, mergedChildFilter,
                childMergeCost) =>
              val (mergedProjectList, newOutputMap,
                  newPlanFilter, mergedPlanFilter,
                  mergeCost) =
                mergeNamedExpressions(
                  np.output, outputMap,
                  cp.projectList, newChildFilter,
                  mergedChildFilter, childMergeCost)
              val mergedPlan =
                Project(mergedProjectList, mergedChild)
              (mergedPlan, newOutputMap,
                newPlanFilter, mergedPlanFilter, mergeCost)
          }
        case (np: Project, cp) =>
          tryMergePlans(
            np.child, cp, filterPropagationSupported
          ).map {
            case (mergedChild, outputMap,
                newChildFilter, mergedChildFilter,
                childMergeCost) =>
              val (mergedProjectList, newOutputMap,
                  newPlanFilter, mergedPlanFilter,
                  mergeCost) =
                mergeNamedExpressions(
                  np.projectList, outputMap,
                  cp.output, newChildFilter,
                  mergedChildFilter, childMergeCost)
              val mergedPlan =
                Project(mergedProjectList, mergedChild)
              (mergedPlan, newOutputMap,
                newPlanFilter, mergedPlanFilter, mergeCost)
          }
        case (np: Aggregate, cp: Aggregate)
            if supportedAggregateMerge(np, cp) =>
          val fpEnabled =
            SQLConf.get.getConf(
              SQLConf.PLAN_MERGE_FILTER_PROPAGATION_ENABLED
            ) && supportsFilterPropagation(np) &&
              supportsFilterPropagation(cp)
          tryMergePlans(np.child, cp.child, fpEnabled).flatMap {
            case (mergedChild, outputMap,
                None, None, _) =>
              val mappedNewGroupingExpression =
                np.groupingExpressions.map(
                  mapAttributes(_, outputMap))
              if (mappedNewGroupingExpression
                  .map(_.canonicalized) ==
                  cp.groupingExpressions
                    .map(_.canonicalized)) {
                val (mergedAggExprs, newOutputMap,
                    _, _, _) =
                  mergeNamedExpressions(
                    np.aggregateExpressions, outputMap,
                    cp.aggregateExpressions,
                    None, None, None)
                val mergedPlan = Aggregate(
                  cp.groupingExpressions,
                  mergedAggExprs, mergedChild)
                Some(mergedPlan, newOutputMap,
                  None, None, None)
              } else {
                None
              }
            case (mergedChild, outputMap,
                newChildFilter, mergedChildFilter,
                childMergeCost) =>
              assert(np.groupingExpressions.isEmpty &&
                cp.groupingExpressions.isEmpty)
              val (mergedAggExprs, newOutputMap,
                  _, _, _) =
                mergeNamedExpressions(
                  filterAggregateExpressions(
                    np.aggregateExpressions,
                    newChildFilter),
                  outputMap,
                  filterAggregateExpressions(
                    cp.aggregateExpressions,
                    mergedChildFilter),
                  None, None, None)

              val mergeFilters =
                newChildFilter.isEmpty ||
                  mergedChildFilter.isEmpty || {
                  val mergeCost = childMergeCost.map { c =>
                    val extraCost =
                      mergedChildFilter
                        .map(getCost).getOrElse(0d) +
                      newChildFilter
                        .map(getCost).getOrElse(0d)
                    c + extraCost + extraCost
                  }
                  mergeCost.forall { c =>
                    val maxCost = SQLConf.get.getConf(
                      SQLConf
                        .PLAN_MERGE_FILTER_PROPAGATION_MAX_COST
                    )
                    val ok = maxCost < 0 || c <= maxCost
                    if (!ok) {
                      logDebug(
                        s"Plan merge of\n${np}" +
                        s"and\n${cp}" +
                        s"failed as the merge cost" +
                        s" is too high: $c")
                    }
                    ok
                  }
                }
              if (mergeFilters) {
                val mergedPlan = Aggregate(
                  Seq.empty, mergedAggExprs,
                  mergedChild)
                Some(mergedPlan, newOutputMap,
                  None, None, None)
              } else {
                None
              }
            case _ => None
          }

        case (np: Filter, cp: Filter) =>
          tryMergePlans(
            np.child, cp.child, filterPropagationSupported
          ).flatMap {
            case (mergedChild, outputMap,
                newChildFilter, mergedChildFilter,
                childMergeCost) =>
              val mappedNewCondition =
                mapAttributes(np.condition, outputMap)
              if (mappedNewCondition.canonicalized ==
                  cp.condition.canonicalized) {
                val filters =
                  (mergedChildFilter.toSeq ++
                    newChildFilter.toSeq)
                    .reduceOption(Or)
                    .map(PropagatedFilter)
                val mergedCondition =
                  (filters.toSeq :+ cp.condition)
                    .reduce(And)
                val mergedPlan =
                  Filter(mergedCondition, mergedChild)
                val mergeCost = addFilterCost(
                  childMergeCost, mergedCondition,
                  getCost(np.condition),
                  getCost(cp.condition))
                Some(mergedPlan, outputMap,
                  newChildFilter, mergedChildFilter,
                  mergeCost)
              } else if (filterPropagationSupported) {
                val newPlanFilter =
                  (newChildFilter.toSeq :+
                    mappedNewCondition).reduce(And)
                val cachedPlanFilter =
                  (mergedChildFilter.toSeq :+
                    cp.condition).reduce(And)
                val mergedCondition = PropagatedFilter(
                  Or(cachedPlanFilter, newPlanFilter))
                val mergedPlan =
                  Filter(mergedCondition, mergedChild)
                val nonPropCachedFilter =
                  extractNonPropagatedFilter(
                    cp.condition)
                val mergedPlanFilter =
                  (mergedChildFilter.toSeq ++
                    nonPropCachedFilter.toSeq)
                    .reduceOption(And)
                val mergeCost = addFilterCost(
                  childMergeCost, mergedCondition,
                  getCost(np.condition),
                  getCost(cp.condition))
                Some(mergedPlan, outputMap,
                  Some(newPlanFilter),
                  mergedPlanFilter, mergeCost)
              } else {
                None
              }
          }
        case (np, cp: Filter)
            if filterPropagationSupported =>
          tryMergePlans(np, cp.child, true).map {
            case (mergedChild, outputMap,
                newChildFilter, mergedChildFilter,
                childMergeCost) =>
              val nonPropCachedFilter =
                extractNonPropagatedFilter(cp.condition)
              val mergedPlanFilter =
                (mergedChildFilter.toSeq ++
                  nonPropCachedFilter.toSeq)
                  .reduceOption(And)
              if (newChildFilter.isEmpty) {
                (mergedChild, outputMap, None,
                  mergedPlanFilter, childMergeCost)
              } else {
                val cachedPlanFilter =
                  (mergedChildFilter.toSeq :+
                    cp.condition).reduce(And)
                val mergedCondition = PropagatedFilter(
                  Or(cachedPlanFilter,
                    newChildFilter.get))
                val mergedPlan =
                  Filter(mergedCondition, mergedChild)
                val mergeCost = addFilterCost(
                  childMergeCost, mergedCondition,
                  0d, getCost(cp.condition))
                (mergedPlan, outputMap,
                  newChildFilter, mergedPlanFilter,
                  mergeCost)
              }
          }
        case (np: Filter, cp)
            if filterPropagationSupported =>
          tryMergePlans(np.child, cp, true).map {
            case (mergedChild, outputMap,
                newChildFilter, mergedChildFilter,
                childMergeCost) =>
              val mappedNewCondition =
                mapAttributes(np.condition, outputMap)
              val newPlanFilter =
                (newChildFilter.toSeq :+
                  mappedNewCondition).reduce(And)
              if (mergedChildFilter.isEmpty) {
                (mergedChild, outputMap,
                  Some(newPlanFilter), None,
                  childMergeCost)
              } else {
                val mergedCondition = PropagatedFilter(
                  Or(mergedChildFilter.get,
                    newPlanFilter))
                val mergedPlan =
                  Filter(mergedCondition, mergedChild)
                val mergeCost = addFilterCost(
                  childMergeCost, mergedCondition,
                  getCost(np.condition), 0d)
                (mergedPlan, outputMap,
                  Some(newPlanFilter),
                  mergedChildFilter, mergeCost)
              }
          }

        case (np: Join, cp: Join)
            if np.joinType == cp.joinType &&
              np.hint == cp.hint =>
          // Filter propagation is not allowed through joins
          tryMergePlans(np.left, cp.left, false).flatMap {
            case (mergedLeft, leftOutputMap, None, None, _) =>
              tryMergePlans(np.right, cp.right, false).flatMap {
                case (mergedRight, rightOutputMap, None, None, _) =>
                  val outputMap = leftOutputMap ++ rightOutputMap
                  val mappedNewCondition = np.condition.map(mapAttributes(_, outputMap))
                  if (mappedNewCondition.map(_.canonicalized) ==
                    cp.condition.map(_.canonicalized)) {
                    val mergedPlan = cp.withNewChildren(Seq(mergedLeft, mergedRight))
                    Some(mergedPlan, outputMap, None, None, None)
                  } else {
                    None
                  }
                case _ => None
              }
            case _ => None
          }

        // Merge V2 data source scans via SupportsMerge interface.
        case (
          np @ DataSourceV2ScanRelation(nr, ns: SupportsMerge, _, _, _, _),
          cp @ DataSourceV2ScanRelation(cr, cs: SupportsMerge, _, _, _, _))
            if nr.table.isInstanceOf[SupportsRead] =>
          val table = nr.table.asInstanceOf[SupportsRead]
          val merged = cs.mergeWith(ns, table)
          if (merged.isPresent) {
            val mergedScan = merged.get()
            val mergedOutput = (cp.output ++ np.output).distinct
            val mergedRelation = cp.copy(
              scan = mergedScan,
              output = mergedOutput)
            val outputMap = AttributeMap[Attribute](
              np.output.map { a =>
                val mapped: Attribute = mergedOutput.find(
                  _.semanticEquals(a)).getOrElse(a)
                a -> mapped
              })
            Some((mergedRelation, outputMap, None, None, None))
          } else {
            None
          }

        // Otherwise merging is not possible.
        case _ => None
      })
  }

  private def mapAttributes[T <: Expression](
      expr: T,
      outputMap: AttributeMap[Attribute]) = {
    expr.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T]
  }

  /**
   * Merges named expression lists and propagates filters
   * and costs through Project or Aggregate nodes.
   */
  private def mergeNamedExpressions(
      newExpressions: Seq[NamedExpression],
      outputMap: AttributeMap[Attribute],
      cachedExpressions: Seq[NamedExpression],
      newChildFilter: Option[Expression],
      mergedChildFilter: Option[Expression],
      childMergeCost: Option[Double]):
    (Seq[NamedExpression], AttributeMap[Attribute],
        Option[Attribute], Option[Attribute],
        Option[Double]) = {
    val mergedExpressions =
      ArrayBuffer[NamedExpression](cachedExpressions: _*)
    val commonCachedExpressions =
      mutable.Set.empty[NamedExpression]
    var cachedPlanExtraCost = 0d
    val newOutputMap =
      AttributeMap(newExpressions.map { ne =>
        val mapped = mapAttributes(ne, outputMap)
        val withoutAlias = mapped match {
          case Alias(child, _) => child
          case e => e
        }
        ne.toAttribute -> mergedExpressions.find {
          case Alias(child, _) =>
            child semanticEquals withoutAlias
          case e => e semanticEquals withoutAlias
        }.map { e =>
          if (childMergeCost.isDefined) {
            commonCachedExpressions += e
          }
          e
        }.getOrElse {
          mergedExpressions += mapped
          if (childMergeCost.isDefined) {
            cachedPlanExtraCost += getCost(mapped)
          }
          mapped
        }.toAttribute
      })

    def mergeFilter(filter: Option[Expression]) = {
      filter.map { f =>
        mergedExpressions.find {
          case Alias(child, _) =>
            child semanticEquals f
          case e => e semanticEquals f
        }.map { e =>
          if (childMergeCost.isDefined) {
            commonCachedExpressions += e
          }
          e
        }.getOrElse {
          val named = f match {
            case ne: NamedExpression => ne
            case o => Alias(o, "propagatedFilter")()
          }
          mergedExpressions += named
          if (childMergeCost.isDefined) {
            cachedPlanExtraCost += getCost(named)
          }
          named
        }.toAttribute
      }
    }

    val mergedPlanFilter = mergeFilter(mergedChildFilter)
    val newPlanFilter = mergeFilter(newChildFilter)

    val mergeCost = childMergeCost.map { c =>
      val newPlanExtraCost = cachedExpressions.collect {
        case e
            if !commonCachedExpressions.contains(e) =>
          getCost(e)
      }.sum
      c + newPlanExtraCost + cachedPlanExtraCost
    }

    (mergedExpressions.toSeq, newOutputMap,
      newPlanFilter, mergedPlanFilter, mergeCost)
  }

  /**
   * Adds extra cost of using mergedCondition instead of
   * original filter conditions.
   */
  private def addFilterCost(
      childMergeCost: Option[Double],
      mergedCondition: Expression,
      newPlanFilterCost: Double,
      cachedPlanFilterCost: Double) = {
    childMergeCost.map { c =>
      val mergedConditionCost = getCost(mergedCondition)
      val newPlanExtraCost =
        mergedConditionCost - newPlanFilterCost
      val cachedPlanExtraCost =
        mergedConditionCost - cachedPlanFilterCost
      c + newPlanExtraCost + cachedPlanExtraCost
    }
  }

  // Simple cost model for filter expressions.
  private def getCost(e: Expression): Double = e match {
    case _: Literal | _: Attribute => 0d
    case PropagatedFilter(child) => getCost(child)
    case Alias(child, _) => getCost(child)
    case _: UnaryExpression =>
      1d + e.children.map(getCost).sum
    case _: BinaryComparison | _: BinaryArithmetic |
        _: And | _: Or =>
      1d + e.children.map(getCost).sum
    case _ => Double.PositiveInfinity
  }

  private def extractNonPropagatedFilter(
      e: Expression): Option[Expression] = {
    e match {
      case And(_: PropagatedFilter, right) =>
        Some(right)
      case And(left, _: PropagatedFilter) =>
        Some(left)
      case _: PropagatedFilter => None
      case o => Some(o)
    }
  }

  // Filter propagation is supported for non-grouping
  // aggregates only.
  private def supportsFilterPropagation(
      a: Aggregate) = {
    a.groupingExpressions.isEmpty
  }

  private def filterAggregateExpressions(
      aggregateExpressions: Seq[NamedExpression],
      filter: Option[Expression]) = {
    if (filter.isDefined) {
      aggregateExpressions.map(_.transform {
        case ae: AggregateExpression =>
          ae.copy(
            filter =
              (filter.get +: ae.filter.toSeq)
                .reduceOption(And))
      }.asInstanceOf[NamedExpression])
    } else {
      aggregateExpressions
    }
  }

  // Only allow aggregates of the same implementation
  // because merging different implementations could cause
  // performance regression.
  private def supportedAggregateMerge(
      newPlan: Aggregate,
      cachedPlan: Aggregate) = {
    val aggregateExpressionsSeq =
      Seq(newPlan, cachedPlan).map { plan =>
        plan.aggregateExpressions.flatMap(_.collect {
          case a: AggregateExpression => a
        })
      }
    val groupByExpressionSeq =
      Seq(newPlan, cachedPlan)
        .map(_.groupingExpressions)

    val Seq(newPlanSupportsHash,
        cachedPlanSupportsHash) =
      aggregateExpressionsSeq
        .zip(groupByExpressionSeq).map {
          case (aggExprs, groupByExprs) =>
            Aggregate.supportsHashAggregate(
              aggExprs.flatMap(
                _.aggregateFunction
                  .aggBufferAttributes),
              groupByExprs)
        }

    newPlanSupportsHash &&
      cachedPlanSupportsHash ||
      newPlanSupportsHash ==
        cachedPlanSupportsHash && {
        val Seq(newPlanSupportsObjHash,
            cachedPlanSupportsObjHash) =
          aggregateExpressionsSeq
            .zip(groupByExpressionSeq).map {
              case (aggExprs, groupByExprs) =>
                Aggregate
                  .supportsObjectHashAggregate(
                    aggExprs, groupByExprs)
            }
        newPlanSupportsObjHash &&
          cachedPlanSupportsObjHash ||
          newPlanSupportsObjHash ==
            cachedPlanSupportsObjHash
      }
  }
}

/**
 * Wrapper around already propagated predicates.
 */
case class PropagatedFilter(child: Expression)
    extends UnaryExpression with Unevaluable {
  override def dataType: DataType = child.dataType
  override protected def withNewChildInternal(
      newChild: Expression): PropagatedFilter =
    copy(child = newChild)
}
