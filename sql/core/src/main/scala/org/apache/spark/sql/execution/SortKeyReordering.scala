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

import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Fork-only physical rule. When a `SortMergeJoinExec` has multiple equi-keys
 * and all keys carry distinct-count statistics, predict the per-pair
 * comparison cost of every key permutation under the independence /
 * uniformity model and -- if the original-vs-best ratio exceeds a
 * configurable threshold -- permute the SMJ's `leftKeys`/`rightKeys` to put
 * the highest-NDV key first. The matching direct `SortExec` children are
 * permuted in lock-step so the SMJ's child ordering requirement remains
 * satisfied.
 *
 * Runs after `EnsureRequirements` via the `queryStagePrepRules` extension
 * hook (see [[SortKeyReorderingExtension]]). The rule never adds or removes
 * operators; the only structural change is re-indexing existing key /
 * sort-order sequences. `logicalLink` and other tag state are preserved on
 * the replacement nodes via `copyTagsFrom`.
 *
 * Gated by:
 *   - [[SQLConf.SORT_KEY_REORDERING_ENABLED]] (default false)
 *   - [[SQLConf.SORT_KEY_REORDERING_THRESHOLD]] (default 10.0)
 *
 * Conservatively skipped when:
 *   - any key is missing NDV stats;
 *   - either SMJ child is not a direct `SortExec` (avoids needing to walk
 *     through arbitrary intermediate operators to find and rewrite the
 *     ordering source);
 *   - the original key order already matches the predicted-best order
 *     (no-op transform).
 *
 * The cost-formula is a documented heuristic: under independence + uniformity
 * the expected per-pair comparison cost for a permutation pi is
 * `sum_i prod_{j<=i}(1/NDV_pi(j))`. Empirically this formula overstates the
 * realised reduction (the prefix comparator + radix sort path absorb much of
 * the predicted gain before reaching TimSort), so the threshold should be
 * tuned conservatively per workload. Equi-join correctness is preserved
 * because permuting the key order does not change the equality predicate;
 * the matching `SortExec.sortOrder` permutation keeps the sort/merge
 * sequence consistent across the join's two sides.
 *
 * Plan-validity caveat: after permutation, the SMJ's
 * `requiredChildDistribution = HashClusteredDistribution(leftKeys)` reports
 * the new key order, while the upstream shuffle was performed with the
 * original key order. Execution is correct (equi-join matches co-locate
 * under either hash) but `ValidateRequirements` would reject the plan. The
 * rule is therefore intended to be off by default and enabled only in
 * benchmark / experimental contexts where the trade-off is understood.
 *
 * Subquery scope: `transformUp` does not descend into
 * `SubqueryExpression.plan`. SMJs inside scalar / EXISTS subqueries are not
 * permuted. This is intentional for the initial scope.
 */
case class SortKeyReordering(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val conf = session.sessionState.conf
    if (!conf.getConf(SQLConf.SORT_KEY_REORDERING_ENABLED)) return plan
    val threshold = conf.getConf(SQLConf.SORT_KEY_REORDERING_THRESHOLD)
    plan.transformUp {
      case smj: SortMergeJoinExec => maybePermute(smj, threshold)
    }
  }

  private def maybePermute(
      smj: SortMergeJoinExec, threshold: Double): SortMergeJoinExec = {
    if (smj.leftKeys.size < 2) return smj
    val ndvs = collectNdvs(smj.leftKeys, smj).getOrElse(return smj)
    val ratio = predictedRatio(ndvs)
    if (ratio < threshold) return smj
    val perm: Seq[Int] = ndvs.indices.sortBy(i => -ndvs(i))
    if (perm == ndvs.indices) return smj  // already in best order
    (smj.left, smj.right) match {
      case (lSort: SortExec, rSort: SortExec)
          if lSort.sortOrder.size == smj.leftKeys.size &&
             rSort.sortOrder.size == smj.rightKeys.size =>
        val newLeftKeys: Seq[Expression] = perm.map(smj.leftKeys(_))
        val newRightKeys: Seq[Expression] = perm.map(smj.rightKeys(_))
        val newLeft = lSort.copy(sortOrder = perm.map(lSort.sortOrder(_)))
        newLeft.copyTagsFrom(lSort)
        val newRight = rSort.copy(sortOrder = perm.map(rSort.sortOrder(_)))
        newRight.copyTagsFrom(rSort)
        val newSmj = smj.copy(
          leftKeys = newLeftKeys,
          rightKeys = newRightKeys,
          left = newLeft,
          right = newRight)
        newSmj.copyTagsFrom(smj)
        logInfo(log"Reordering SortMergeJoin keys: " +
          log"predictedRatio=${MDC(LogKeys.RATIO, ratio)} " +
          log"old=${MDC(LogKeys.OLD_VALUE, ndvs.mkString(","))} " +
          log"perm=${MDC(LogKeys.NEW_VALUE, perm.mkString(","))}")
        newSmj
      case _ =>
        smj
    }
  }

  /**
   * Look up the NDV of each key via the SMJ's logical link (which provides
   * `Statistics.attributeStats`). Returns None if the logical link is
   * missing, any key is not an Attribute, or any NDV is unavailable.
   */
  private def collectNdvs(keys: Seq[Expression], smj: SortMergeJoinExec): Option[Seq[Double]] = {
    val ll = smj.logicalLink.getOrElse(return None)
    val statsByExprId = ll.stats.attributeStats.map { case (attr, colStat) =>
      attr.exprId -> colStat.distinctCount
    }
    val ndvs = keys.map {
      case a: Attribute =>
        statsByExprId.getOrElse(a.exprId, None).map(_.toDouble).filter(_ > 0)
      case _ => None
    }
    if (ndvs.exists(_.isEmpty)) None else Some(ndvs.map(_.get))
  }

  /**
   * Cost-formula: ratio of the original permutation's expected per-pair
   * comparison cost to the best permutation's cost, both computed as
   * `sum_i prod_{j<=i}(1/NDV_pi(j))`.
   */
  private def predictedRatio(ndvs: Seq[Double]): Double = {
    def cost(xs: Seq[Double]): Double = {
      var sum = 0.0
      var prod = 1.0
      xs.foreach { ndv =>
        prod *= 1.0 / ndv
        sum += prod
      }
      sum
    }
    val best = cost(ndvs.sorted(Ordering.Double.TotalOrdering.reverse))
    if (best == 0.0) Double.PositiveInfinity else cost(ndvs) / best
  }
}
