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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.internal.LogKeys.RELATION_OUTPUT
import org.apache.spark.internal.MDC
import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, Expression, NamedExpression, PredicateHelper, ProjectionOverSchema}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters, V1Scan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.util.SchemaUtils._
import org.apache.spark.util.ArrayImplicits._

object V2ScanRelationPruneColumns extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case Subquery(_: ScanBuilderHolder, true) => plan
      case _ =>
        val pushdownRules = Seq[LogicalPlan => LogicalPlan] (pruneColumns)

        pushdownRules.foldLeft(plan) { (newPlan, pushDownRule) =>
          pushDownRule(newPlan)
        }
    }
  }

  def pruneColumns(plan: LogicalPlan): LogicalPlan = plan.transform {
    case ScanOperation(project, filtersStayUp, filtersPushDown, sHolder: ScanBuilderHolder) =>
      // column pruning
      val normalizedProjects = DataSourceStrategy
        .normalizeExprs(project, sHolder.output)
        .asInstanceOf[Seq[NamedExpression]]
      val allFilters = filtersPushDown.reduceOption(And).toSeq ++ filtersStayUp
      val normalizedFilters = DataSourceStrategy.normalizeExprs(allFilters, sHolder.output)
      val (scan, output) = PushDownUtils.pruneColumns(
        sHolder.builder, sHolder.relation, normalizedProjects, normalizedFilters)

      logInfo(
        log"""
            |Output: ${MDC(RELATION_OUTPUT, output.mkString(", "))}
           """.stripMargin)

      val wrappedScan = getWrappedScan(scan, sHolder)

      val scanRelation = DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)

      val projectionOverSchema =
        ProjectionOverSchema(output.toStructType, AttributeSet(output))
      val projectionFunc = (expr: Expression) => expr transformDown {
        case projectionOverSchema(newExpr) => newExpr
      }

      val finalFilters = normalizedFilters.map(projectionFunc)
      // bottom-most filters are put in the left of the list.
      val withFilter = finalFilters.foldLeft[LogicalPlan](scanRelation)((plan, cond) => {
        Filter(cond, plan)
      })

      if (withFilter.output != project) {
        val newProjects = normalizedProjects
          .map(projectionFunc)
          .asInstanceOf[Seq[NamedExpression]]
        Project(restoreOriginalOutputNames(newProjects, project.map(_.name)), withFilter)
      } else {
        withFilter
      }
  }

  private def getWrappedScan(scan: Scan, sHolder: ScanBuilderHolder): Scan = {
    scan match {
      case v1: V1Scan =>
        val pushedFilters = sHolder.builder match {
          case f: SupportsPushDownFilters =>
            f.pushedFilters()
          case _ => Array.empty[sources.Filter]
        }
        val pushedDownOperators = PushedDownOperators(sHolder.pushedAggregate, sHolder.pushedSample,
          sHolder.pushedLimit, sHolder.pushedOffset, sHolder.sortOrders, sHolder.pushedPredicates)
        V1ScanWrapper(v1, pushedFilters.toImmutableArraySeq, pushedDownOperators)
      case _ => scan
    }
  }
}
