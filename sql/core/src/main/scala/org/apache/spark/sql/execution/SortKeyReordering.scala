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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.LogKeys
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.LogicalQueryStage
import org.apache.spark.sql.execution.exchange.ValidateRequirements
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Fork-only physical rule. When a `SortMergeJoinExec` has multiple equi-keys
 * and all keys carry distinct-count statistics, predict the per-pair
 * comparison cost of every key permutation under the independence /
 * uniformity model and -- if the original-vs-best ratio meets a configurable
 * threshold -- permute the SMJ's `leftKeys`/`rightKeys` to put the highest-NDV
 * key first. The matching direct `SortExec` children are permuted in lock-step
 * so the SMJ's child ordering requirement remains satisfied.
 *
 * Wired into both plan-preparation pipelines (`QueryExecution.preparations` and
 * AQE's `queryStagePreparationRules`), after `EnsureRequirements` and before
 * `RemoveRedundantSorts`. The rule never adds or removes operators; the only
 * structural change is re-indexing existing key / sort-order sequences.
 * `logicalLink` and other tag state are preserved on the replacement nodes via
 * `copyTagsFrom`.
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
 * Where the gain comes from, and why it is smaller than the formula says.
 * `SortExec.createSorter` binds its prefix comparator to `sortOrder.head`
 * ALONE, so on a multi-key sort the prefix only discriminates by the leading
 * key; every pair that ties on it falls through to the full `RowOrdering`.
 * Putting the highest-NDV key first is therefore a real reduction in
 * full-row comparisons. But two things absorb much of the predicted gain
 * before it reaches TimSort, and a third caps it:
 *   - the prefix comparison itself is not free, and it is paid on every pair
 *     either way;
 *   - `canUseRadixSort` requires `sortOrder.length == 1`, so a multi-key sort
 *     never took the radix path and there is no path change to gain from;
 *   - if the sort spills, wall time is dominated by IO rather than by
 *     comparisons. Prefer a target whose `Sort` reports a spill size of zero.
 * Under independence + uniformity the expected per-pair comparison cost for a
 * permutation pi is `sum_i prod_{j<=i}(1/NDV_pi(j))`; treat the ratio as a
 * ranking signal, not a speedup prediction, and tune the threshold per
 * workload.
 *
 * The cost model is also blind to everything OUTSIDE the two join-input sorts it
 * scores, and one of those blind spots can cost more than the permutation saves:
 * `RemoveRedundantSorts` runs AFTER this rule and deletes a `SortExec` whose child
 * already advertises the required ordering, so a downstream sort that WOULD have been
 * deleted survives once the SMJ below it advertises a permuted ordering. Measured on a
 * two-table join under `sortWithinPartitions("k1", "k2")` (the same shape `V1Writes`
 * produces for a partitioned write whose ordering coincides with the join keys): the
 * plan goes from two `SortExec`s to three, and the extra one re-sorts the ENTIRE join
 * output. Both plans validate cleanly, because the surviving sort declares no child
 * ordering requirement for the gate below to object to. Since a join's output can be
 * far larger than either input, that trade is usually a loss -- one more reason the
 * ratio is a ranking signal rather than a prediction.
 *
 * Equi-join correctness is preserved because permuting the key order does not
 * change the equality predicate; the matching `SortExec.sortOrder` permutation
 * keeps the sort/merge sequence consistent across the join's two sides.
 *
 * What the permutation breaks ABOVE the join, and the whole-plan validation that
 * contains it. `SortMergeJoinExec.outputOrdering` is derived positionally from
 * `leftKeys`/`rightKeys`, so permuting the keys also changes the ordering the join
 * ADVERTISES -- and `SortOrder.orderingSatisfies` is a positional zip, so `[k2, k1]`
 * does not satisfy `[k1, k2]`. Because this rule runs AFTER `EnsureRequirements`, an
 * ancestor for which `EnsureRequirements` declined to insert a `SortExec` (its
 * requirement was satisfied pre-permutation) is left consuming a stream that is no
 * longer sorted the way it compares it, and nothing reinstates the sort:
 * `RemoveRedundantSorts` only removes. That is a WRONG RESULT, not a slow plan -- a
 * stacked two-key SMJ measured `count(*)` 2000 -> 512.
 *
 * `apply` therefore runs `ValidateRequirements.validate` on the permuted plan and
 * discards every permutation when it fails. Two things make that the right shape:
 * `validateInternal` reads each node's own `requiredChildOrdering`, so `WindowExec`,
 * `SortAggregateExec` and any future ordering-dependent operator are covered without
 * enumerating them; and the check is not vacuous, because `PlanStabilitySuite` asserts
 * it on the executed plan of every TPC-DS query, so a stock plan arriving here already
 * passes and a failure is attributable to this rule.
 *
 * Distribution, by contrast, survives the permutation on its own. The SMJ's
 * `requiredChildDistribution` (`ClusteredDistribution(leftKeys)` via `ShuffledJoin`)
 * reports the new key order while the upstream shuffle used the original one, but
 * `spark.sql.requireAllClusterKeysForDistribution` defaults to false so
 * `HashPartitioning.satisfies0` takes the order-insensitive
 * `forall(exists(semanticEquals))` branch, and the co-partitioning check goes through
 * `HashShuffleSpec.isCompatibleWith`, which compares `hashKeyPositions` derived from
 * each side's own `clustering` -- permuting BOTH sides consistently keeps the
 * positions overlapping. One order-sensitive corner remains and is a performance
 * matter only: `spark.sql.requireAllClusterKeysForCoPartition` defaults to TRUE and
 * `areAllClusterKeysMatched` zips positionally, so if `EnsureRequirements` runs again
 * after this rule (the in-tree path is `OptimizeSkewedJoin` under
 * `spark.sql.adaptive.forceOptimizeSkewedJoin`, default false) both sides fail
 * `canCreatePartitioning` and get re-shuffled. Same answer, two extra shuffles.
 *
 * Why still off by default. The validation above turns the wrong-result risk into a
 * silently-declined optimization, but the measured wall-time benefit of the
 * permutation itself is workload-specific and unproven at scale (see the cost note
 * above), so the conf stays opt-in for benchmark / experimental contexts.
 *
 * Also note the rule cannot distinguish a `SortExec` that `EnsureRequirements`
 * inserted from one the user wrote: `df.sortWithinPartitions("a", "b")` above a hash
 * shuffle survives as the SMJ's direct child and will be permuted. The join stays
 * correct; the user-visible within-partition ordering changes.
 *
 * Subquery scope. `transformUp` does not descend into `SubqueryExpression.plan`, so a
 * single `apply` call never permutes an SMJ inside a scalar / EXISTS subquery. Those
 * SMJs ARE still permuted, just by a different invocation: `PlanSubqueries` calls
 * `QueryExecution.prepareExecutedPlan`, which runs this same rule list with
 * `subquery = true` over the subquery's own plan. Each such invocation validates its own
 * tree, so the guarantee holds per subquery rather than being skipped.
 */
case class SortKeyReordering(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val conf = session.sessionState.conf
    if (!conf.getConf(SQLConf.SORT_KEY_REORDERING_ENABLED)) return plan
    val threshold = conf.getConf(SQLConf.SORT_KEY_REORDERING_THRESHOLD)
    val permuted = plan.transformUp {
      case smj: SortMergeJoinExec => maybePermute(smj, threshold)
    }
    if (permuted eq plan) {
      plan
    } else if (validatesCleanly(permuted)) {
      permuted
    } else {
      // Permuting an SMJ's keys changes the ordering it ADVERTISES
      // (`SortMergeJoinExec.outputOrdering` is derived positionally from
      // `leftKeys`/`rightKeys`), and this rule runs after `EnsureRequirements` has
      // already decided, against the pre-permutation ordering, that some ancestor
      // needed no `SortExec`. Nothing downstream reinstates one: `RemoveRedundantSorts`
      // only removes. So the permutation can leave an ancestor consuming an input that
      // is no longer sorted the way that ancestor compares it -- silently wrong
      // results, not a slow plan.
      //
      // `transformUp` makes the stacked-SMJ case the common one: the lower join is
      // permuted first, and by the time the upper join is visited its left child is an
      // SMJ rather than a `SortExec`, so `maybePermute`'s shape guard declines and the
      // upper join keeps its original key order over a reordered stream. Measured on a
      // three-table two-key join (NDV 4 / 2000): `count(*)` went 2000 -> 512 with the
      // feature on, under AQE both on and off.
      //
      // Validating the whole tree rather than special-casing each kind of ancestor is
      // what makes this complete: `validateInternal` reads each node's own
      // `requiredChildOrdering`, so `WindowExec` (partitionSpec ++ orderSpec) and
      // `SortAggregateExec` (groupingExpressions) are covered by construction, and so is
      // any future operator with an ordering requirement.
      //
      // All-or-nothing, deliberately. Retrying per join would need a whole-tree
      // validation per candidate; and the check is not vacuous in the other direction --
      // `PlanStabilitySuite` asserts `ValidateRequirements.validate` on the executed plan
      // of every TPC-DS query, so a stock plan reaching this rule already satisfies it,
      // and a false verdict here is attributable to the permutation.
      logInfo(log"Discarding SortKeyReordering permutations: the permuted plan does not " +
        log"satisfy its own distribution/ordering requirements, which means some ancestor " +
        log"was relying on an SMJ's pre-permutation outputOrdering.")
      plan
    }
  }

  /**
   * `ValidateRequirements.validate`, treating a failure to answer as "not valid".
   *
   * The catch is not defensive padding. This is the FIRST `ValidateRequirements` call on
   * the non-AQE path -- stock Spark only reaches that object from `AdaptiveSparkPlanExec`
   * and `OptimizeSkewedJoin`, both AQE-only -- so it inspects node shapes that call site
   * never saw, and it can throw rather than return false:
   *   - `validateInternal` itself asserts `requiredChildDistribution.length ==
   *     children.length` and the same for orderings, which a custom or fork-only operator
   *     can violate;
   *   - the co-partitioning branch reaches `KeyGroupedShuffleSpec.keyPositions`, which
   *     asserts `leaves.size == 1` on a V2 storage-partitioned join's key expressions;
   *   - `Partitioning.createShuffleSpec`'s default throws `IllegalStateException` for a
   *     plugin `Partitioning` that satisfies `ClusteredDistribution` without overriding
   *     it.
   * Scala `assert` is live in shipped builds, so any of those would propagate out of
   * `apply` and FAIL THE QUERY. Turning "this optimization is declined" into "the query
   * dies" would be a strict regression against having no rule at all, and this whole
   * feature is opt-in performance work.
   *
   * `NonFatal` plus `AssertionError`, NOT `Throwable`. `AssertionError` is an `Error`, so
   * `NonFatal` alone would let the most likely case through -- the two asserts above are
   * the reason this wrapper exists. But catching `Throwable` would also swallow
   * `OutOfMemoryError`, `InterruptedException` (a cancelled query's interrupt arriving
   * mid-walk) and `ControlThrowable`, none of which mean "this plan is invalid" and all of
   * which the driver has to see.
   */
  private def validatesCleanly(permuted: SparkPlan): Boolean = {
    try {
      ValidateRequirements.validate(permuted)
    } catch {
      case e @ (NonFatal(_) | _: AssertionError) =>
        logWarning(log"SortKeyReordering could not validate the permuted plan; " +
          log"discarding the permutations.", e)
        false
    }
  }

  private def maybePermute(
      smj: SortMergeJoinExec, threshold: Double): SortMergeJoinExec = {
    if (smj.leftKeys.size < 2) return smj
    val ndvs = collectNdvs(smj.leftKeys, smj).getOrElse(return smj)
    val ratio = predictedRatio(ndvs)
    if (ratio < threshold) return smj
    // Descending NDV. `sortBy` is stable, so equal NDVs keep their original
    // relative order and the `perm == indices` short-circuit below still fires
    // on an already-best plan rather than producing an equivalent shuffle of it.
    // That short-circuit is load-bearing, not cosmetic: the threshold conf accepts
    // 1.0, at which an already-optimal key order passes the ratio gate and only this
    // line stops a no-op rewrite (which would then have to be validated and would
    // churn plan ids for nothing).
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
   *
   * The `LogicalQueryStage` descent is what makes this work under AQE, and
   * without it the rule is a no-op on any query AQE re-plans -- which is to say
   * on the workload it was written for. When a shuffle stage finishes, AQE calls
   * `reOptimize` on a logical plan whose completed subtrees have been replaced by
   * `LogicalQueryStage`, then re-runs `queryStagePreparationRules` on the result.
   * `LogicalQueryStage.computeStats()` returns the stage's RUNTIME statistics --
   * `sizeInBytes` and `rowCount` only, with an empty `attributeStats`. So on the
   * replan `JoinEstimation` has no column statistics to propagate, `ll.stats
   * .attributeStats` at the Join is empty, this method returns None, and the rule
   * declines. The un-permuted plan then costs the same as the permuted one, and
   * `AdaptiveSparkPlanExec` adopts a new plan of equal cost, so the permutation is
   * silently reverted between planning and execution. Measured directly: with AQE
   * off the executed plan keeps `[b, a]`; with AQE on the initial plan is `[b, a]`
   * and the executed plan is back to `[a, b]`.
   *
   * `LogicalQueryStage.output` IS `logicalPlan.output`, so the ExprIds in the
   * wrapped plan's `attributeStats` are the same ones the SMJ's keys reference and
   * the lookup below still matches. Only `LogicalQueryStage` nodes pay an extra
   * `.stats` call (which is cached per node), so this does not walk the tree
   * computing statistics.
   *
   * Precedence is deliberate: `getOrElseUpdate` keeps the FIRST value seen and the
   * Join-level `attributeStats` are harvested before the `LogicalQueryStage` descent,
   * so a post-filter estimate wins and the wrapped stage's raw table statistics act
   * only as a fallback for ExprIds the Join-level estimate did not cover.
   *
   * One consequence of `LogicalQueryStage` being a `LeafNode`: `invalidateStatsCache`
   * recurses on `children`, so it never reaches the wrapped `logicalPlan` and that
   * subtree's `statsCache` survives every AQE re-plan. Good for cost -- the estimation
   * behind `harvest` is paid at most once per wrapped instance rather than once per
   * re-plan -- but it also means these NDVs are frozen pre-execution estimates that
   * never refresh from what the completed stage actually produced. Consistent with the
   * paragraph above: the stage's own runtime statistics carry no `attributeStats` to
   * refresh them with.
   *
   * Two limits of the numbers this returns, neither fixable here. The rule is
   * effectively CBO-only: without `spark.sql.cbo.enabled` (and
   * `spark.sql.cbo.planStats.enabled` for table column stats to reach plan stats)
   * `LogicalPlanStats.stats` takes `SizeInBytesOnlyStatsPlanVisitor` and BOTH sources
   * return an empty `attributeStats`, so the rule always declines. And on the CBO path
   * a join key's Join-level NDV is not the sort input's own NDV:
   * `JoinEstimation.computeByNdv` sets `newNdv = min(leftNdv, rightNdv)`, so the value
   * ranking the LEFT child's sort keys is the post-join intersected count. That is
   * imprecision in a ranking signal, which is all this needs.
   *
   * Only the LEFT keys are scored, and the resulting permutation is applied to both
   * sides. Applying it jointly is structurally mandatory -- `requiredChildOrdering`
   * pairs `leftKeys(i)` with `rightKeys(i)` and the merge compares them positionally,
   * so permuting one side alone would be a correctness bug. What is genuinely one-sided
   * is the threshold gate: the right sort's cost is never computed, so a large left-side
   * gain can be booked while the right sort moves off its own optimum. On the inner-join
   * CBO path that cannot diverge much, because `keyStatsAfterJoin` assigns the same
   * `distinctCount` to both sides of each pair; it is reachable for `LeftOuter` (left
   * keys keep raw `inputAttrStats`) and on the `LogicalQueryStage` path (each side reads
   * its own table stats), where the right-side regression is bounded by the same `min`
   * clamping and is normally the smaller term.
   */
  private def collectNdvs(keys: Seq[Expression], smj: SortMergeJoinExec): Option[Seq[Double]] = {
    val ll = smj.logicalLink.getOrElse(return None)
    val statsByExprId = mutable.Map.empty[ExprId, BigInt]
    def harvest(stats: AttributeMap[ColumnStat]): Unit = stats.foreach {
      case (attr, colStat) =>
        colStat.distinctCount.foreach(ndv => statsByExprId.getOrElseUpdate(attr.exprId, ndv))
    }
    harvest(ll.stats.attributeStats)
    ll.foreach {
      case lqs: LogicalQueryStage => harvest(lqs.logicalPlan.stats.attributeStats)
      case _ =>
    }
    val ndvs = keys.map {
      case a: Attribute =>
        statsByExprId.get(a.exprId).map(_.toDouble).filter(_ > 0)
      case _ => None
    }
    if (ndvs.exists(_.isEmpty)) None else Some(ndvs.map(_.get))
  }

  /**
   * Cost-formula: ratio of the original permutation's expected per-pair
   * comparison cost to the best permutation's cost, both computed as
   * `sum_i prod_{j<=i}(1/NDV_pi(j))`.
   *
   * Descending NDV is the global minimum over all permutations, not merely the better
   * of the two orders compared: for adjacent positions holding `a, b` with
   * `P = prod_{j<k}(1/NDV_j)`, every term outside those two positions is unchanged and
   * `cost(a, b) - cost(b, a) = P * (1/a - 1/b)`, which is negative exactly when
   * `a > b`. Any inversion is therefore strictly improvable, so sorting descending
   * reaches the optimum (ties are cost-invariant).
   *
   * Two numeric edges, both benign and both worth naming so nobody "fixes" them into
   * something worse. The `best == 0.0` branch is NOT dead code: `BigInt.toDouble`
   * above `Double.MaxValue` yields `+Infinity`, which passes `collectNdvs`'s
   * `filter(_ > 0)`, and then `best`'s leading term `1/max(ndvs)` is exactly zero. It
   * takes an NDV above ~1.8e308 to reach, and returning `+Infinity` (always permute) is
   * the sensible answer there. Separately, at NDVs around 1e300 the running product
   * underflows to zero after the FIRST key, so `cost` collapses to `1.0 / xs.head` and
   * the formula degenerates into ranking by the leading key's NDV alone:
   * `ratio = max(ndvs) / ndvs.head`. It still discriminates in the right direction (and
   * still cannot divide by zero, since `best` is `1/max`), it just stops accounting for
   * the tail -- e.g. `[1e300, 1e308]` scores 1e8 and fires, while `[1e308, 1e300]` scores
   * 1.0 and would anyway have been short-circuited by `perm == ndvs.indices`.
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
    // Descending, so `best` is the cheapest permutation. `TotalOrdering` differs from the
    // default `Ordering[Double]` only on NaN, which cannot reach here anyway: `collectNdvs`
    // filters to `_ > 0` and NaN fails that.
    val best = cost(ndvs.sorted(Ordering.Double.TotalOrdering.reverse))
    if (best == 0.0) Double.PositiveInfinity else cost(ndvs) / best
  }
}
