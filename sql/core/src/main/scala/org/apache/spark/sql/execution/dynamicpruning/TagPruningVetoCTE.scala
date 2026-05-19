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

package org.apache.spark.sql.execution.dynamicpruning

import java.util.concurrent.atomic.LongAdder

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE
import org.apache.spark.sql.internal.SQLConf

/**
 * Set `pruningVeto = true` on `CTERelationDef`s where caching would block
 * beneficial outer DPP/DFP pruning AND no in-body PartitionPruning opportunity
 * substitutes for the lost outer pruning.
 *
 * Decision logic (`PruningEligibility`):
 *   - shadow-optimize `cteDef.child` (clone), then:
 *   - if `hasInBodyDPPOpportunity(shadow)` -> keep cache (in-body DPP fires
 *     during cache materialization).
 *   - else if `outerDPPFeasible(shadow, output)` -> set `pruningVeto = true`.
 *   - else -> keep cache (no DPP either way; cache reuse is the only optim).
 *
 * Read sites:
 *   - `InlineCTE.shouldInline` (sql/catalyst): vetoed CTEs fall through the
 *     auto-cache carve-out and are inlined.
 *   - `ReplaceCTERefWithCache.shouldAutoCache` (sql/core): vetoed CTEs are
 *     not materialised even if they survived InlineCTE.
 *
 * Lock-step with `ReplaceCTERefWithCache.shouldAutoCache`: only deterministic,
 * non-correlated-subquery CTEs are veto candidates. Otherwise `shouldAutoCache`
 * refuses for an unrelated reason and InlineCTE may produce an unresolved plan
 * (see comment at `InlineCTE.scala:97-115`).
 *
 * Telemetry: each veto increments [[TagPruningVetoCTE.vetoCount]] and emits an
 * INFO log line. On-call greps `Auto-CTE veto` to attribute vetoes to specific
 * queries; the LongAdder is process-wide and not per-query.
 *
 * Master switch: `spark.sql.auto.cte.skipWhenPruningApplicable` (default false).
 * Add this rule to `nonExcludableRules` in `SparkOptimizer` to prevent users
 * from bypassing veto via `spark.sql.optimizer.excludedRules`.
 */
object TagPruningVetoCTE extends Rule[LogicalPlan] with Logging {

  /** Process-wide counter of veto applications. Per-query attribution via INFO log. */
  private[spark] val vetoCount: LongAdder = new LongAdder()

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = SQLConf.get
    if (!conf.getConf(SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE)) return plan
    // Coarse sub-switch gating: if BOTH DPP and DFP detection are disabled,
    // the master switch has no effect. The current detector does not separate
    // DPP-only from DFP-only opportunities (both flow through
    // `PartitionPruning.getFilterableTableScan`), so the sub-switches act as a
    // combined off-switch rather than per-mode gates. Per-mode gating is
    // future work; see SQLConf entries for documentation.
    val dppEnabled = conf.getConf(SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE_DPP)
    val dfpEnabled = conf.getConf(SQLConf.AUTO_CTE_SKIP_WHEN_PRUNING_APPLICABLE_DFP)
    if (!dppEnabled && !dfpEnabled) return plan
    if (!plan.containsPattern(CTE)) return plan

    plan.transformWithSubqueries {
      case w @ WithCTE(_, cteDefs) =>
        val updated = cteDefs.map { d =>
          if (d.pruningVeto || !isVetoCandidate(d)) d
          else if (shouldVeto(d)) {
            vetoCount.increment()
            logInfo(log"Auto-CTE veto: CTE id=${MDC(LogKeys.CTE_ID, d.id)} " +
              log"marked for inlining " +
              log"(outer DPP/DFP path detected, in-body DPP absent)")
            d.copy(pruningVeto = true)
          } else d
        }
        if (updated.zip(cteDefs).forall { case (a, b) => a eq b }) w
        else w.copy(cteDefs = updated)
    }
  }

  /**
   * Eligibility precondition matching `ReplaceCTERefWithCache.shouldAutoCache`.
   * Setting pruningVeto on a non-eligible CTE would leave InlineCTE preserving
   * the CTE while ReplaceCTERefWithCache refuses to cache it, yielding an
   * unresolved plan via ReplaceCTERefWithRepartition.
   */
  private def isVetoCandidate(d: CTERelationDef): Boolean = {
    d.deterministic && !d.correlatedSubqueryRef
  }

  private def shouldVeto(d: CTERelationDef): Boolean = {
    val shadow = PruningEligibility.shadowOptimize(d.child)
    !PruningEligibility.hasInBodyDPPOpportunity(shadow) &&
      PruningEligibility.outerDPPFeasible(shadow, d.output)
  }
}
