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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, EmptyRow}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Project, Statistics, Union}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._

/**
 * Estimate the number of output rows by doing the sum of output rows for each child of union,
 * and estimate column stats (min, max, nullCount, distinctCount) for each column from its
 * children. Min and max are computed as the overall min/max across children, nullCount is summed,
 * and distinctCount is estimated as the max across children (capped by total row count).
 */
object UnionEstimation {
  import EstimationUtils._

  private def createStatComparator(dt: DataType): (Any, Any) => Boolean = {
    PhysicalDataType.ordering(dt).lt _
  }

  private def isTypeSupported(dt: DataType): Boolean = dt match {
    case ByteType | IntegerType | ShortType | FloatType | LongType |
         DoubleType | DateType | _: DecimalType | TimestampType | TimestampNTZType |
         _: AnsiIntervalType => true
    case _ => false
  }

  def estimate(union: Union): Option[Statistics] = {
    val sizeInBytes = union.children.map(_.stats.sizeInBytes).sum
    val outputRows = if (rowCountsExist(union.children: _*)) {
      Some(union.children.map(_.stats.rowCount.get).sum)
    } else None
    Some(Statistics(
      sizeInBytes = sizeInBytes,
      rowCount = outputRows,
      attributeStats = AttributeMap(computeColumnStats(union, outputRows))))
  }

  /**
   * Returns the [[ColumnStat]] for the given child's output at position `idx`. Primary source
   * is the child's `attributeStats`. As a fallback, when the child is a [[Project]] whose
   * projection at `idx` is `Alias(foldable, _)` (e.g. `'store channel' AS channel`), we
   * synthesize the column stat directly from the foldable value. This rescues distinctCount
   * propagation for literal-tagged columns in UNION ALL pipelines even when other parts of
   * the child's stats failed to compute.
   */
  private def childColumnStat(child: LogicalPlan, idx: Int): Option[ColumnStat] = {
    val outAttr = child.output(idx)
    child.stats.attributeStats.get(outAttr).orElse {
      child match {
        case Project(projectList, _) =>
          projectList.lift(idx).flatMap {
            case Alias(expr, _) if expr.foldable && expr.deterministic =>
              val value = expr.eval(EmptyRow)
              val size = expr.dataType.defaultSize
              if (value == null) {
                // Null literal: distinctCount = 0; nullCount uses the child's rowCount when
                // known so that downstream foldLeft over children can sum it.
                Some(ColumnStat(
                  distinctCount = Some(0),
                  min = None,
                  max = None,
                  nullCount = child.stats.rowCount,
                  avgLen = Some(size),
                  maxLen = Some(size)))
              } else {
                Some(ColumnStat(
                  distinctCount = Some(1),
                  min = Some(value),
                  max = Some(value),
                  nullCount = Some(0),
                  avgLen = Some(size),
                  maxLen = Some(size)))
              }
            case _ => None
          }
        case _ => None
      }
    }
  }

  /**
   * For each output column, compute min/max, nullCount, and distinctCount in a single pass.
   * Min and max are computed as the overall min/max across children (only for supported types),
   * nullCount is summed, and distinctCount is estimated as the max across children (capped by
   * outputRows).
   *
   * For UNION ALL, true distinctCount satisfies:
   *   max(dc_i) <= true_dc <= min(sum(dc_i), rowCount)
   * We use max as a lower-bound estimate. This may underestimate when branches have
   * disjoint values, but UNION ALL branches typically share overlapping values
   * (e.g. web_sales and catalog_sales reference the same date keys),
   * so max is a reasonable approximation in the common case.
   */
  private def computeColumnStats(
      union: Union,
      outputRows: Option[BigInt]): Seq[(Attribute, ColumnStat)] = {
    // For each child, look up the ColumnStat for each of its output attributes.
    // After transposing, maybeColStats(i) holds the ColumnStat (if available) contributed
    // by the i-th child for the current output column. `childColumnStat` provides a fallback
    // for literal-aliased columns when the child's stats are otherwise missing.
    union.output.zip(
      union.children
        .map(c => c.output.indices.map(idx => childColumnStat(c, idx)))
        .transpose)
      .flatMap {
        case (outputAttr, maybeColStats) =>
          val maybeMinMax: Option[(Any, Any)] = if (isTypeSupported(outputAttr.dataType)) {
            val statComparator = createStatComparator(outputAttr.dataType)
            val initial = maybeColStats.head
              .collect { case s if s.hasMinMaxStats => (s.min.get, s.max.get) }
            maybeColStats.tail.foldLeft(initial) {
              case (Some((minVal, maxVal)), Some(s)) if s.hasMinMaxStats =>
                Some((
                  if (statComparator(s.min.get, minVal)) s.min.get else minVal,
                  if (statComparator(maxVal, s.max.get)) s.max.get else maxVal
                ))
              case _ => None
            }
          } else None

          val maybeNullCount = maybeColStats.foldLeft(Option(BigInt(0))) {
            case (Some(total), Some(s)) => s.nullCount.map(total + _)
            case _ => None
          }

          val maybeDistinctCount = maybeColStats.foldLeft(Option(BigInt(0))) {
            case (Some(currentMax), Some(s)) => s.distinctCount.map(currentMax.max)
            case _ => None
          }
          // Cap distinctCount by outputRows if available
          val cappedDistinctCount = maybeDistinctCount.map(max => outputRows.fold(max)(max.min))

          if (maybeMinMax.isDefined || maybeNullCount.isDefined || cappedDistinctCount.isDefined) {
            Some(outputAttr -> ColumnStat(
              min = maybeMinMax.map(_._1),
              max = maybeMinMax.map(_._2),
              nullCount = maybeNullCount,
              distinctCount = cappedDistinctCount
            ))
          } else None
      }
  }
}
