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

import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.LogKeys
import org.apache.spark.sql.catalyst.analysis.GroupingAnalyticsTransformer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.RollupRewriteSignatures._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, UNION}
import org.apache.spark.sql.internal.SQLConf

/**
 * Rewrite a UNION ALL of N aggregates into a single Aggregate with ROLLUP,
 * when the branches' GROUP BYs form a strict prefix-shrinking hierarchy.
 * Two patterns are recognized:
 *
 * '''Inner-aggregate fold''' (CTE pattern):
 * {{{
 *   Union(   // branches in ANY order; the full-detail branch need not be first
 *     Aggregate([c1..cK1], [..., sum(inner_agg)], Inner),
 *     ...
 *     full_detail,  // Project(passthrough, Inner) or Aggregate([c1..cN], Inner)
 *     ...
 *     Aggregate([], [..., sum(inner_agg)], Inner)
 *   )
 *   Inner = Aggregate([c1..cN], [..., sum(expr) AS inner_agg], source)
 * }}}
 * The full-detail branch (finest level, a passthrough or full re-aggregation of
 * the Inner) may appear at ANY position, not necessarily first; every branch is
 * validated for measure-position binding (see [[verifyInnerFoldMeasurePositions]],
 * which checks ALL branches, including the passthrough/raw one).
 * Foldable aggregate functions: SUM (sum-of-sum), MAX (max-of-max),
 * MIN (min-of-min), COUNT (sum-of-count). No DISTINCT in the outer fold.
 *
 * '''Source-aggregate''' (direct pattern):
 * {{{
 *   Union(
 *     Aggregate([c1..cN],   aggExprs, source),
 *     Aggregate([c1..cN-1], aggExprs, source),
 *     ...
 *     Aggregate([],         aggExprs, source)
 *   )
 * }}}
 * Each branch independently aggregates the SAME raw source. All branches
 * must use IDENTICAL aggregate expressions (modulo attribute IDs). Any
 * deterministic aggregate function works (SUM, MAX, MIN, COUNT, AVG,
 * COUNT(DISTINCT), COLLECT_SET, etc.).
 *
 * Both patterns require:
 *  - >= 3 Union branches forming a COMPLETE prefix hierarchy [N, N-1, ..., 1, 0].
 *  - Deterministic aggregate expressions.
 *  - No per-branch non-null literal outputs (level indicators).
 *  - At least one measure (non-grouping aggregate) output.
 *  - A CONSISTENT per-position column layout across branches: at each output
 *    position every branch must agree on grouping-key/NULL-filler vs measure
 *    role; grouping keys must pass through under their own name at EVERY
 *    projection layer (no cross-labeling), occupy the reference layout's
 *    positions, and (source path) sit at the same source-output ordinal; and
 *    same-named measures must be STRUCTURALLY identical -- expression shape
 *    including source-output ordinals, literal values, subquery plans, a
 *    class:arity tree encoding, the DISTINCT flag, the FILTER, and hidden
 *    eval-mode/timezone/function-identity/ANSI-flag state. Branch projections
 *    must be passthroughs: order-preserving above a source-path aggregate,
 *    alias-only name-preserving between an outer and inner aggregate, and any
 *    timezone-dependent wrapper cast must carry the session timezone (the
 *    rewrite re-creates those casts). These rebuild invariants are required
 *    because the rewrite reconstructs every rollup level from a single
 *    reference branch's expressions and binds them to the Union output by
 *    position; a branch that diverges in ANY of them is rejected rather than
 *    silently mis-bound.
 *
 * Path-specific rules:
 *  - Inner-aggregate fold: all branches must share ONE inner aggregate whose
 *    grouping columns are projected as bare passthroughs; the foldable
 *    inner/outer aggregates must be non-DISTINCT and un-FILTERED
 *    (sum-of-sum, max-of-max, min-of-min, sum-of-count). max/min(DISTINCT) and
 *    filtered foldables are conservatively rejected here (lost optimization,
 *    never a wrong result).
 *  - Source-aggregate: each branch independently aggregates the same source, so
 *    any deterministic aggregate is allowed (incl. AVG, COUNT(DISTINCT),
 *    COLLECT_SET, FILTERed) as long as every branch's per-position measure
 *    signature (see above) is identical. Order-sensitive aggregates that
 *    Spark classifies as deterministic (first/last) are admitted: their
 *    results are input-order-undefined in the original query too, and the
 *    rewrite stays within that nondeterminism envelope.
 *
 * The inner-aggregate fold is tried first; the source-aggregate path is
 * tried only if the inner-aggregate fold does not match.
 *
 * Gated by [[SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED]].
 */
object RewriteUnionAggregateAsRollup extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED)) {
      return plan
    }
    plan.transformWithPruning(_.containsAllPatterns(UNION, AGGREGATE), ruleId) {
      // !isStreaming: collapsing N+1 stateful aggregates into 2 changes the
      // checkpointed operator topology, and whether it fires depends on session
      // state (timezone/eval-mode gates), so a restart under a different session
      // could mis-bind operator state. Sibling topology-changing rules bail the
      // same way (e.g. ReplaceDeduplicateWithAggregate).
      case u: Union if u.children.size >= 3 && !u.isStreaming =>
        // The whole matching pipeline runs inside a Try, not just buildRollupPlan.
        // This rewrite is optional, so a guard that throws must cost the optimization
        // and not the query. The fingerprint arms in RollupRewriteSignatures are total
        // today, but that file's extension contract keeps adding arms, and one that
        // threw would otherwise fail every >= 3-branch union. A positive test still
        // catches such a regression: the rewrite silently stops happening.
        Try(tryInnerAggregateFold(u).orElse(trySourceAggregate(u))) match {
          case Success(rewritten) => rewritten.getOrElse(u)
          case Failure(ex) =>
            logDebug(s"Rollup rewrite skipped, a check failed unexpectedly: ${ex.getMessage}")
            u
        }
    }
  }

  // ===== Inner-aggregate fold ================================================

  private def tryInnerAggregateFold(u: Union): Option[LogicalPlan] = {
    // TODO: parse the branches lazily. Both drivers build their branch objects --
    // and with them a canonicalKey per branch -- before the name-only hierarchy
    // check runs. Canonicalization itself is cached per plan node, but the marker
    // collection inside canonicalKey walks the whole subtree plus every subquery and
    // is not, so a wide UNION ALL whose keys cannot form a hierarchy pays that walk
    // per branch, again for each active fixed-point round, and a second time when the
    // fold path falls through to trySourceAggregate. Worth doing before the conf's
    // default flips; harmless while it is off.
    val branches = u.children.map(InnerBranch.parse)
    if (branches.exists(_.isEmpty)) return None
    val parsed = branches.flatten

    val innerKeys = parsed.map(_.innerKey).distinct
    if (innerKeys.size != 1) return None

    val inner = parsed.head.inner
    // Hash equality pins the branch inners' STRUCTURE position-for-position,
    // but canonicalization ERASES alias and attribute names -- so two
    // structurally identical inline inners with PERMUTED output names hash
    // equal while every cross-branch name binding below (fold-argument names,
    // grouping-key names, the hierarchy's name sets) silently assumes the
    // names agree. Pin the NAME VECTOR too: name-by-position equality
    // composed with hash equality restores name->structure identity across
    // all branch inners. CTE copies and coercion re-aliases preserve names,
    // so the motivating shapes are unaffected.
    val innerNames = inner.aggregateExpressions.map(_.name)
    if (parsed.exists(_.inner.aggregateExpressions.map(_.name) != innerNames)) return None
    // Distinct names, checked here rather than left to alignInnerToUnionOutput
    // further down: verifyOuterFoldsToInner keys a map by these names, where a
    // duplicate would silently last-win.
    if (innerNames.distinct.size != innerNames.size) return None
    val innerGroupCols: Seq[Attribute] = inner.groupingExpressions.collect {
      case a: AttributeReference => a
      case Alias(a: AttributeReference, _) => a
    }
    if (innerGroupCols.size != inner.groupingExpressions.size) return None
    // Every grouping column must be passed through to the inner's OUTPUT as a
    // bare AttributeReference (same ExprId; an alias would carry a fresh one).
    // All outer->inner key binding below is by NAME, so an inner that does
    // NOT project a key while NAMING another output after it (`SELECT b,
    // sum(x) AS a ... GROUP BY a, b`) lets a coarser branch's GROUP BY
    // cross-bind that output and the rewrite regroups the level by the raw
    // key; a key projected only under a value-changing wrap (`CAST(k AS
    // DOUBLE) AS k`) similarly regroups by the un-wrapped key (merging
    // distinct groups that collide post-cast). The bare passthrough also
    // guarantees the rebuilt Expand references only attributes present in
    // the inner's output, instead of relying on a later CollapseProject to
    // absorb a momentarily-dangling projection.
    if (!innerGroupCols.forall(inner.outputSet.contains)) return None

    // Inner aggregate must contain only foldable aggregate functions
    // (SUM / MAX / MIN / COUNT). Sum-of-sum, max-of-max, min-of-min
    // reduce to the inner aggregate; count rolls up via outer sum.
    val innerFoldable = collectFoldableAggregations(inner)
    if (innerFoldable.isEmpty || innerFoldable.size != countAggregations(inner)) return None

    if (!inner.deterministic) return None

    val groupColSets = parsed.map(_.groupCols.map(_.name).toSet).sortBy(-_.size)
    if (!isPrefixShrinkingHierarchy(groupColSets, innerGroupCols.map(_.name))) return None

    if (!verifyOuterFoldsToInner(u, inner)) return None
    if (!verifyOutputsRollupShaped(u, inner, innerGroupCols)) return None
    if (!verifyConsistentRoleLayout(u)) return None
    if (!verifyKeyPassthroughNamesAligned(u)) return None

    // Align the inner's output to the Union output order by name before the
    // positional bind in buildRollupPlan; bail if not uniquely matchable.
    // See alignInnerToUnionOutput.
    val alignedInner = alignInnerToUnionOutput(inner, u).getOrElse(return None)
    // Key/NULL-filler positions must match the replayed layout; see
    // verifyKeyPositionsAligned.
    if (!verifyKeyPositionsAligned(
        u, alignedInner.aggregateExpressions, innerGroupCols.map(_.exprId).toSet)) {
      return None
    }
    if (!verifyWrapperCastTimezonesAreSession(u)) return None
    if (!verifyDiscardedWrapperCastsAreUpCasts(u)) return None
    // Every fold branch must fold the SAME inner measure the aligned inner
    // places at each position (else a same-typed measure permutation is
    // silently rebound); see verifyInnerFoldMeasurePositions.
    if (!verifyInnerFoldMeasurePositions(u, alignedInner)) return None
    // Apply the outer fold to the inner's measure outputs (see
    // innerFoldAggregateExpressions).
    val foldExprs = innerFoldAggregateExpressions(alignedInner, innerGroupCols)
      .getOrElse(return None)

    Try(buildRollupPlan(u, alignedInner, innerGroupCols, foldExprs)) match {
      case Success(rewritten) =>
        logInfo(log"Rewriting UNION ALL of " +
          log"${MDC(LogKeys.NUM_CHILDREN, u.children.size)} aggregates as " +
          log"ROLLUP(${MDC(LogKeys.GROUP_BY_EXPRS, innerGroupCols.map(_.name).mkString(","))})")
        Some(rewritten)
      case Failure(ex) =>
        logDebug(s"Rollup rewrite skipped due to unexpected failure: ${ex.getMessage}")
        None
    }
  }

  /** Reorder `inner`'s aggregateExpressions so their NAMES line up positionally
   *  with `u.output`. [[buildRollupPlan]] binds the rebuilt output to
   *  `u.output` by position, so the inner aggregate's expression order must
   *  match the Union output order; a branch Project that reorders columns can
   *  break that alignment. Returns None (bail) unless every Union output name
   *  maps to exactly one inner aggregate output BY NAME (type compatibility
   *  is backstopped by the remap's canUpCast bail inside Try), so the
   *  positional bind cannot silently mis-map columns.
   */
  private def alignInnerToUnionOutput(inner: Aggregate, u: Union): Option[Aggregate] = {
    val unionOut = u.output
    val innerExprs = inner.aggregateExpressions
    if (unionOut.size != innerExprs.size) return None
    // Require UNIQUE names on BOTH sides. If u.output has duplicate names (e.g. a
    // NULL-filler aliased to collide with a measure name), name-based mapping
    // would silently map two output positions to the SAME inner expression and
    // DROP another, mis-binding columns. Requiring distinct names on both sides
    // makes the name->expr mapping a true permutation.
    if (unionOut.map(_.name).distinct.size != unionOut.size) return None
    val byName: Map[String, NamedExpression] =
      innerExprs.groupBy(_.name).collect { case (n, Seq(one)) => n -> one }
    // Require a unique name match for every Union output position.
    if (byName.size != innerExprs.size) return None
    val reordered = unionOut.map(o => byName.get(o.name))
    if (reordered.exists(_.isEmpty)) return None
    val aligned = reordered.flatten
    // Already in order? avoid building an identical node.
    if (aligned == innerExprs) Some(inner)
    else Some(inner.copy(aggregateExpressions = aligned))
  }

  /** Verify that every Union branch is either:
   *   (a) the raw branch (inner aggregate at top level, possibly under Projects)
   *       whose measure positions MATCH the reference inner's position-for-position
   *       (by name + full signature, via `measurePositionSignature`); else
   *       buildRollupPlan, which emits the reference inner's measures for every
   *       level, would silently change this branch's values, or
   *   (b) an outer Aggregate whose aggregate functions are foldable folds
   *       of the branch's own inner aggregate's outputs: sum-of-sum,
   *       max-of-max, min-of-min, or sum-of-count.
   *
   *  Each Union branch has its own copy of the inner aggregate (with fresh
   *  ExprIds post-InlineCTE), so the check must use the branch's own inner.
   *  Matching is by column NAME (not ExprId) because an intermediate Project
   *  between the outer and inner Aggregate may re-alias the inner's outputs
   *  to new ExprIds while preserving the name.
   */
  private def verifyOuterFoldsToInner(u: Union, inner: Aggregate): Boolean = {
    // Measure-position signature of the reference inner (output name + which
    // columns each aggregate reads), used to validate "raw" branches whose
    // child is the source directly: such a branch must compute the SAME
    // measures as the reference inner, else buildRollupPlan (which emits the
    // reference inner's measures for every level) would silently change the
    // raw branch's values.
    val innerMeasureSig = measurePositionSignature(inner.aggregateExpressions, inner.child.output)
    u.children.forall { child =>
      stripProject(child) match {
        case agg: Aggregate =>
          stripProject(agg.child) match {
            case branchInner: Aggregate =>
              val branchInnerAggClasses: Map[String, Class[_ <: AggregateFunction]] =
                branchInner.aggregateExpressions.flatMap {
                  case a: Alias => foldableAggregateClassAfterUnwrap(a).map(c => a.name -> c)
                  case _ => None
                }.toMap
              if (branchInnerAggClasses.isEmpty) {
                false
              } else {
                // Each aggregate-bearing OUTER output must be the fold ITSELF
                // under aliases only -- a value-changing wrapper around the
                // fold (e.g. CAST(SUM(c) AS INT) inside the aggregate) is
                // DROPPED by buildRollupPlan, which rebuilds each level as the
                // bare fold at the inner's wide type, silently changing values.
                // A legitimate Union widening-coercion cast sits in a Project
                // ABOVE the aggregate (not in agg.aggregateExpressions), so it
                // is unaffected here and looked through downstream.
                val aggOutputs = agg.aggregateExpressions.filter(
                  _.find(_.isInstanceOf[AggregateExpression]).isDefined)
                val bareFolds = aggOutputs.forall(
                  unwrapAliasesOnly(_).isInstanceOf[AggregateExpression])
                val outerAggs = aggOutputs.flatMap(_.collect {
                  case ae: AggregateExpression => ae
                })
                bareFolds && outerAggs.nonEmpty && outerAggs.forall { ae =>
                  // ae.filter.isEmpty: an outer fold with a FILTER (e.g.
                  // sum(s) FILTER (WHERE s > 100)) cannot be reconstructed by
                  // the rewrite, which emits the inner's UNFILTERED measure for
                  // every level; reject it.
                  ae.filter.isEmpty && isFoldableAggregate(ae.aggregateFunction) &&
                    !ae.isDistinct &&
                    outerMatchesInner(ae.aggregateFunction, branchInnerAggClasses)
                }
              }
            case _ =>
              // Raw branch (aggregates the source directly). Its measures must
              // match the reference inner's measures position-for-position.
              measurePositionSignature(agg.aggregateExpressions, agg.child.output) ==
                innerMeasureSig
          }
        case _ => false
      }
    }
  }


  /** Signature of only the MEASURE (aggregate) output positions: their output
   *  name, function class, read columns, distinctness, and FILTER. Grouping-key
   *  passthroughs and NULL fillers are excluded (they vary by rollup level).
   *  Used to require that raw branches compute the same measures as the
   *  reference inner. `childOutput` is the owning aggregate's child output
   *  (the ordinal space -- see [[exprSignature]]).
   */
  private def measurePositionSignature(
      exprs: Seq[NamedExpression],
      childOutput: Seq[Attribute]): Seq[String] = {
    exprs.flatMap { expr =>
      val outName = expr.name
      unwrapAliasesAndCasts(expr) match {
        case ae: AggregateExpression =>
          Some(s"$outName=${aggMeasureSignature(ae, childOutput)}")
        case _ => None
      }
    }
  }

  /** True if the outer aggregate function's single argument is a direct
   *  AttributeReference whose name maps (in `innerAggClasses`) to an
   *  aggregate function class compatible with the outer's class:
   *    - sum-of-sum, max-of-max, min-of-min (same-class idempotent fold)
   *    - sum-of-count (sum of partial counts = count). Note that
   *      count-of-count is NOT a valid fold.
   */
  private def outerMatchesInner(
      outerFn: AggregateFunction,
      innerAggClasses: Map[String, Class[_ <: AggregateFunction]]): Boolean = {
    val argOpt: Option[AttributeReference] = outerFn match {
      case Sum(a: AttributeReference, _) => Some(a)
      case Max(a: AttributeReference) => Some(a)
      case Min(a: AttributeReference) => Some(a)
      case _ => None
    }
    argOpt.flatMap(attr => innerAggClasses.get(attr.name)).exists { innerClass =>
      innerClass == outerFn.getClass ||
        (innerClass == classOf[Count] && outerFn.isInstanceOf[Sum])
    }
  }

  private def isFoldableAggregate(fn: AggregateFunction): Boolean = fn match {
    // A Sum is foldable only when its captured eval mode EQUALS the current
    // session eval mode. The inner-fold path reconstructs the fold via Sum(attr)
    // which re-derives its evalContext from the session conf, while Sum's
    // toString hides the eval mode so the cross-branch signatures cannot detect
    // a mismatch. So a Sum whose captured mode differs from the session mode
    // would be silently rebuilt at the wrong mode, changing overflow behavior:
    //   - TRY: NULL on overflow (never equals the conf-derived ANSI/LEGACY).
    //   - ANSI: throws on overflow; LEGACY: wraps. A view analyzed under one and
    //     read under the other captures a mode != session mode.
    // Rejecting the mismatch is conservative (only forgoes the optimization).
    case s: Sum => s.evalContext.evalMode == EvalMode.fromSQLConf(conf)
    case _: Max | _: Min | _: Count => true
    case _ => false
  }

  /** Verify each Union branch's output positions are exactly one of:
   *    (a) an inner grouping column (resolvable by name in innerGroupCols),
   *    (b) a NULL literal (dropped-from-GROUP-BY marker at a higher rollup level),
   *    (c) an aggregate-function expression that folds to the inner aggregate.
   *  Reject branches with per-branch level indicators (literal non-null
   *  constants) or arbitrary expressions; those queries use grouping_id()
   *  semantics that pure ROLLUP can't reconstruct.
   */
  private def verifyOutputsRollupShaped(
      u: Union,
      inner: Aggregate,
      innerGroupCols: Seq[Attribute]): Boolean = {
    val innerGroupNames = innerGroupCols.map(_.name).toSet
    val innerAggNames: Set[String] = inner.aggregateExpressions.collect {
      case a: Alias if foldableAggregateClassAfterUnwrap(a).isDefined => a.name
    }.toSet
    val outerAggNames: Set[String] = u.children.flatMap { ch =>
      stripProject(ch) match {
        case agg: Aggregate =>
          agg.aggregateExpressions.collect {
            case a: Alias if foldableAggregateClassAfterUnwrap(a).isDefined => a.name
          }
        case _ => Seq.empty
      }
    }.toSet
    val acceptableNames = innerGroupNames ++ innerAggNames ++ outerAggNames
    u.children.forall { child =>
      branchOutputExpressions(child).exists(
        _.forall(isRollupShapedOutput(_, acceptableNames)))
    }
  }

  /** Reject any branch that routes a grouping column's value to an output whose
   *  name differs from that grouping column (e.g. `SELECT c1 AS c2 ... GROUP BY
   *  c1`). [[buildRollupPlan]] reconstructs every rollup level from the
   *  reference branch's expressions and binds them to the Union output
   *  POSITIONALLY, assuming each grouping key lands at the output position
   *  carrying its name (the inner path aligns positions by name first). A
   *  branch that cross-labels a key (or nulls a key's natural position while
   *  passing the key through under a different name) would otherwise be
   *  rewritten to a standard ROLLUP that places keys differently, silently
   *  changing results. Consistent renames (every branch aliases a key the same
   *  way) are conservatively rejected too; that only forgoes an optimization.
   */
  private def verifyKeyPassthroughNamesAligned(u: Union): Boolean = {
    u.children.forall { child =>
      stripProject(child) match {
        case agg: Aggregate =>
          val groupAttrIds: Set[ExprId] = agg.groupingExpressions.flatMap {
            case a: AttributeReference => Some(a.exprId)
            case Alias(a: AttributeReference, _) => Some(a.exprId)
            case _ => None
          }.toSet
          // Check EVERY layer down to and including the Aggregate itself, not
          // just the branch's outermost output list: a cross-labeling alias can
          // live in the Aggregate's own output (SELECT b AS a ... GROUP BY a, b
          // inside a subquery) or in any intermediate Project, with a
          // passthrough Project above it hiding the offending alias behind a
          // fresh ExprId. buildRollupPlan replays the AGGREGATE's expressions,
          // so the alignment must hold at the layer it replays. A grouping
          // attribute keeps its ExprId through bare-attribute passthroughs, so
          // checking each layer's aliases against groupAttrIds catches a rename
          // at whichever layer the key is still visible by ExprId; once a layer
          // renames it (and is rejected here), upper layers only see the fresh
          // ExprId of the already-rejected alias.
          def layerAligned(outs: Seq[NamedExpression]): Boolean = outs.forall { ne =>
            unwrapAliasesAndCasts(ne) match {
              case a: AttributeReference if groupAttrIds.contains(a.exprId) =>
                ne.name == a.name
              case _ => true
            }
          }
          def allLayersAligned(p: LogicalPlan): Boolean = p match {
            case Project(projList, pchild) => layerAligned(projList) && allLayersAligned(pchild)
            case a: Aggregate => layerAligned(a.aggregateExpressions)
            case _ => false
          }
          allLayersAligned(child)
        case _ => false
      }
    }
  }

  /** Pin grouping-key / NULL-filler POSITIONS to the reference layout. The
   *  per-position role signatures treat every key passthrough and NULL filler
   *  as an interchangeable "KEY" (their output names legitimately differ per
   *  branch), so WITHOUT this check a branch may place a real key at the
   *  position where the reference keeps a DIFFERENT key (and a NULL filler at
   *  the key's own position) and still compare equal -- e.g. the level-1 branch
   *  `SELECT NULL, a, sum(x) ... GROUP BY a` against a reference whose layout
   *  is [a, b, sum]. [[buildRollupPlan]] replays the reference layout for every
   *  level, silently moving such a branch's key values into a different output
   *  column. Here, for every output position where the REFERENCE places
   *  grouping key K: a branch whose level retains K must place exactly K
   *  (matched by name) at that position, and a branch whose level drops K must
   *  place a NULL literal there. A position where the reference places a NULL
   *  literal is constrained too -- see `refNullFillerAtPos` below. Measure
   *  positions are validated separately (per-position measure signatures /
   *  fold-measure bindings).
   */
  private def verifyKeyPositionsAligned(
      u: Union,
      referenceExprs: Seq[NamedExpression],
      refGroupColIds: Set[ExprId]): Boolean = {
    val refKeyNameAtPos: Seq[Option[String]] = referenceExprs.map { e =>
      unwrapAliasesAndCasts(e) match {
        case a: AttributeReference if refGroupColIds.contains(a.exprId) => Some(a.name)
        case _ => None
      }
    }
    // Positions where the REFERENCE places a NULL literal. The reference is the
    // finest level, so such a position is NULL at every level and every branch
    // must place a NULL literal there too. Nothing else catches a branch that
    // passes one of its own grouping keys through instead: the per-position
    // signatures and the role layout both render a key passthrough and a NULL
    // filler as "KEY", and buildRollupPlan then replays the reference's NULL.
    val refNullFillerAtPos: Seq[Boolean] = referenceExprs.map { e =>
      unwrapAliasesAndCasts(e) match {
        case l: Literal => l.value == null
        case _ => false
      }
    }
    u.children.forall { child =>
      stripProject(child) match {
        case agg: Aggregate =>
          val levelNames: Set[String] = agg.groupingExpressions.flatMap {
            case a: AttributeReference => Some(a.name)
            case Alias(a: AttributeReference, _) => Some(a.name)
            case _ => None
          }.toSet
          effectiveOutputs(child).exists { outs =>
            outs.size == refKeyNameAtPos.size &&
              outs.indices.forall { i =>
                refKeyNameAtPos(i) match {
                  case Some(keyName) =>
                    outs(i) match {
                      case a: AttributeReference =>
                        a.name == keyName && levelNames.contains(keyName)
                      case l: Literal =>
                        l.value == null && !levelNames.contains(keyName)
                      case _ => false
                    }
                  case None =>
                    !refNullFillerAtPos(i) || (outs(i) match {
                      case l: Literal => l.value == null
                      case _ => false
                    })
                }
              }
          }
        case _ => false
      }
    }
  }

  /** Reject any branch whose output WRAPPER chains (the alias/cast nests
   *  around each position's resolved value -- across the branch-top Project
   *  layers, the first Aggregate's own outputs, and any mid Projects above an
   *  inner aggregate) contain a timezone-DEPENDENT Cast whose timeZoneId is
   *  not the current session's. [[buildRollupPlan]] DISCARDS those wrappers
   *  and re-creates the Union-output binding cast with the SESSION timezone,
   *  so a wrapper cast frozen under a different timezone (a temp view stores
   *  its ANALYZED plan, freezing zoneIds at CREATE time) would have its
   *  values silently recomputed under the session timezone. Timezone-
   *  independent wrapper casts (decimal/long widening coercion etc.) are
   *  ignored, so ordinary type-coercion Projects never over-reject. Casts
   *  INSIDE aggregate-function arguments are not wrappers (they are replayed
   *  verbatim and cross-branch compared by the measure signatures), so the
   *  walk stops at the resolved leaf. Conservative over-reach: a union whose
   *  branches CONSISTENTLY carry the same non-session frozen timezone is also
   *  rejected (lost optimization, never wrong results).
   */
  private def verifyWrapperCastTimezonesAreSession(u: Union): Boolean = {
    val sessionTz = Some(conf.sessionLocalTimeZone)
    def wrapperCastsOk(e: Expression): Boolean = e match {
      case a: Alias => wrapperCastsOk(a.child)
      case c: Cast =>
        (!Cast.needsTimeZone(c.child.dataType, c.dataType) || c.timeZoneId == sessionTz) &&
          wrapperCastsOk(c.child)
      case _ => true
    }
    def midProjectsOk(plan: LogicalPlan): Boolean = plan match {
      case Project(projList, child) => projList.forall(wrapperCastsOk) && midProjectsOk(child)
      case _ => true
    }
    def layersOk(plan: LogicalPlan): Boolean = plan match {
      case Project(projList, child) => projList.forall(wrapperCastsOk) && layersOk(child)
      case agg: Aggregate =>
        agg.aggregateExpressions.forall(wrapperCastsOk) &&
          // A fold branch's mid Projects (between the outer and inner
          // aggregate) are discarded by the rewrite too; the inner aggregate
          // itself is preserved verbatim, so the walk stops there.
          (stripProject(agg.child) match {
            case _: Aggregate => midProjectsOk(agg.child)
            case _ => true
          })
      case _ => false
    }
    u.children.forall(layersOk)
  }

  /** The branch's per-position output expressions RESOLVED through every
   *  Project layer down to the branch Aggregate's own expressions, with
   *  aliases/casts unwrapped at each layer. Under the Union's type-coercion
   *  Project a NULL filler surfaces as an AttributeReference to the
   *  Aggregate's null-literal alias; single-layer unwrapping cannot resolve
   *  that reference, so a check on the surface expression would misclassify
   *  the filler. None if the branch is not Aggregate-rooted.
   */
  private def effectiveOutputs(plan: LogicalPlan): Option[Seq[Expression]] = plan match {
    case Project(projList, child) =>
      effectiveOutputs(child).map { childEff =>
        val byId: Map[ExprId, Expression] =
          child.output.map(_.exprId).zip(childEff).toMap
        projList.map { ne =>
          unwrapAliasesAndCasts(ne) match {
            case a: AttributeReference => byId.getOrElse(a.exprId, a)
            case other => other
          }
        }
      }
    case agg: Aggregate =>
      Some(agg.aggregateExpressions.map(unwrapAliasesAndCasts))
    case _ => None
  }

  /** Verify every branch has the SAME per-position role layout: at each output
   *  position all branches must agree on whether the column is a grouping
   *  key / NULL filler ("KEY") or an aggregate ("AGG"). [[buildRollupPlan]]
   *  emits the reference branch's layout for every rollup level, so a branch
   *  that placed a measure where another placed a grouping column (possible
   *  when they share a type) would be silently bound to the wrong output
   *  column. Name-set checks in [[verifyOutputsRollupShaped]] are
   *  order-insensitive and do not catch this.
   */
  private def verifyConsistentRoleLayout(u: Union): Boolean = {
    val layouts = u.children.map(branchRoleLayout)
    if (layouts.exists(_.isEmpty)) return false
    val roleLayouts = layouts.flatten
    roleLayouts.forall(_ == roleLayouts.head)
  }

  /** Per-position role layout of a branch's output. The branch's rows are
   *  produced by an [[Aggregate]] (possibly under coercion Projects); in such
   *  a Project the aggregate outputs appear as bare [[AttributeReference]]s, so
   *  we resolve them back to the aggregate's aggregate-valued outputs by ExprId
   *  rather than by expression type.
   */
  private def branchRoleLayout(branch: LogicalPlan): Option[Seq[String]] = {
    stripProject(branch) match {
      case agg: Aggregate =>
        val aggExprIds: Set[ExprId] = agg.aggregateExpressions.collect {
          case ne: NamedExpression
            if ne.find(_.isInstanceOf[AggregateExpression]).isDefined => ne.exprId
        }.toSet
        branchOutputExpressions(branch).map(_.map(outputRole(_, aggExprIds)))
      case _ => None
    }
  }

  /** Positional role of a branch output: "AGG" for an aggregate (either a
   *  direct [[AggregateExpression]] or an [[AttributeReference]] to one),
   *  "KEY" for a grouping-column passthrough or a NULL filler (interchangeable
   *  across rollup levels), "OTHER" for anything else. (Measure IDENTITY per
   *  position -- which inner measure each fold reads -- is checked separately by
   *  [[verifyInnerFoldMeasurePositions]], because output names legitimately vary
   *  per branch, e.g. a passthrough `sumx` vs an auto-named `sum(sumx)`.)
   */
  private def outputRole(expr: NamedExpression, aggExprIds: Set[ExprId]): String =
    unwrapAliasesAndCasts(expr) match {
      case _: AggregateExpression => "AGG"
      case a: AttributeReference => if (aggExprIds.contains(a.exprId)) "AGG" else "KEY"
      case l: Literal if l.value == null => "KEY"
      case _ => "OTHER"
    }

  /** Reject inner-fold unions where any branch binds a measure position to a
   *  DIFFERENT inner measure than the aligned inner does at that position.
   *  [[buildRollupPlan]] reconstructs every rollup level from `alignedInner` and
   *  binds positionally to `u.output`, ignoring the branches' own measure order;
   *  so two same-typed measures swapped in any branch would be silently rebound to
   *  the reference order, changing that level's values.
   *
   *  Reference: at each output position, the inner-measure NAME the rewrite emits
   *  (None at grouping-key / NULL-filler positions). EVERY branch must, at every
   *  position, represent the inner measure of that same name (see
   *  [[branchMeasureBinding]]). Matching is by inner-measure name (preserved across
   *  CTE copies and coercion re-alias Projects) -- unlike ExprIds (fresh per copy)
   *  or auto-generated output names (`sum(s)` vs the inner's `s`).
   *
   *  All branch shapes are checked, including the passthrough / raw branch: it can
   *  be a NON-head branch (e.g. the finest level placed after a coarser fold branch
   *  that defines `u.output`'s name order), so it is NOT safe to exempt -- a name
   *  reorder in `alignInnerToUnionOutput` would rebind its values to the wrong
   *  columns. ([[verifyOuterFoldsToInner]]'s `measurePositionSignature` pins a raw
   *  branch to the inner only in the inner's PRE-alignment order, which is the head
   *  order only when the head is a faithful passthrough.)
   */
  private def verifyInnerFoldMeasurePositions(u: Union, alignedInner: Aggregate): Boolean = {
    val reference: Seq[Option[String]] = alignedInner.aggregateExpressions.map { e =>
      foldableMeasureAttr(e).map(_ => e.name)
    }
    u.children.forall { child =>
      branchMeasureBinding(child) match {
        case Some(binding) => binding == reference
        case None => false // an inner-fold branch we cannot analyze: bail safely
      }
    }
  }

  /** Per-output-position name of the inner measure each measure position of
   *  `branch` represents, or None at grouping-key / NULL-filler positions; None for
   *  the whole branch only if it is not an [[Aggregate]]-rooted branch.
   *   - FOLD branch (Aggregate over an inner Aggregate): the fold ARGUMENT's name
   *     (= the inner output it reads). A coercion Project on top exposes folds as
   *     bare [[AttributeReference]]s, so resolve those back to the fold by ExprId.
   *   - PASSTHROUGH / RAW branch (Aggregate directly over the source -- this branch
   *     IS, canonically, the inner): a measure is either a passthrough reference to
   *     an inner measure output -> that output's name, or a re-aggregation alias
   *     over a source column -> its own output name (which equals the inner measure
   *     name, since the branch matches the inner position-for-position).
   */
  private def branchMeasureBinding(branch: LogicalPlan): Option[Seq[Option[String]]] =
    stripProject(branch) match {
      case agg: Aggregate if stripProject(agg.child).isInstanceOf[Aggregate] =>
        val argNameByOut: Map[ExprId, String] = agg.aggregateExpressions.flatMap {
          case ne: NamedExpression =>
            unwrapAliasesAndCasts(ne) match {
              case ae: AggregateExpression => foldArgName(ae).map(ne.exprId -> _)
              case _ => None
            }
        }.toMap
        branchOutputExpressions(branch).map(_.map { out =>
          unwrapAliasesAndCasts(out) match {
            case ae: AggregateExpression => foldArgName(ae)
            case a: AttributeReference => argNameByOut.get(a.exprId)
            case _ => None
          }
        })
      case agg: Aggregate =>
        // Passthrough / raw branch: measures reference an inner measure output (by
        // its preserved name) or re-aggregate a source column (output name = inner
        // measure name). Keys / NULL fillers -> None.
        val measureOutIds: Set[ExprId] = agg.aggregateExpressions.collect {
          case ne: NamedExpression if foldableMeasureAttr(ne).isDefined => ne.exprId
        }.toSet
        branchOutputExpressions(branch).map(_.map { out =>
          unwrapAliasesAndCasts(out) match {
            case _: AggregateExpression => Some(out.name)
            case a: AttributeReference if measureOutIds.contains(a.exprId) => Some(a.name)
            case _ => None
          }
        })
      case _ => None
    }

  /** The name of a foldable outer aggregate's single attribute argument
   *  (sum-of-X, max-of-X, min-of-X over an inner output attribute), else None.
   */
  private def foldArgName(ae: AggregateExpression): Option[String] =
    ae.aggregateFunction match {
      case Sum(a: AttributeReference, _) => Some(a.name)
      case Max(a: AttributeReference) => Some(a.name)
      case Min(a: AttributeReference) => Some(a.name)
      case _ => None
    }

  private def branchOutputExpressions(branch: LogicalPlan): Option[Seq[NamedExpression]] =
    branch match {
      case Project(projList, _) => Some(projList)
      case agg: Aggregate => Some(agg.aggregateExpressions)
      case _ => None
    }

  private def isRollupShapedOutput(
      expr: NamedExpression,
      acceptableNames: Set[String]): Boolean = {
    val unwrapped = unwrapAliasesAndCasts(expr)
    unwrapped match {
      case a: AttributeReference => acceptableNames.contains(a.name)
      case l: Literal => l.value == null
      case _: AggregateExpression => true
      case _ => false
    }
  }

  private def unwrapAliasesAndCasts(expr: Expression): Expression = expr match {
    case Alias(child, _) => unwrapAliasesAndCasts(child)
    case Cast(child, _, _, _) => unwrapAliasesAndCasts(child)
    case other => other
  }

  /** Unwrap ONLY aliases (NOT casts). Used where a Cast is value-changing
   *  state that must NOT be looked through -- e.g. distinguishing a bare fold
   *  `Alias(Sum(c))` from a narrowing `Alias(Cast(Sum(c), Int))`. */
  private def unwrapAliasesOnly(expr: Expression): Expression = expr match {
    case Alias(child, _) => unwrapAliasesOnly(child)
    case other => other
  }

  /** True unless some Project layered ABOVE the branch's Aggregate computes a NEW
   *  value -- i.e. an output that does not unwrap (via [[unwrapAliasesAndCasts]])
   *  to a passthrough [[AttributeReference]] or a NULL filler, or whose wrapper
   *  chain narrows a type (see [[castChainIsUpCastOnly]]). Such a Project is
   *  discarded by [[parseSourceBranch]]/[[buildRollupPlan]] (which roll up the
   *  aggregate's own expressions), so a value-changing one would be silently
   *  dropped. Recursion STOPS at the first non-Project node: source-side Projects
   *  BELOW the Aggregate are part of what the aggregate reads (preserved via
   *  `canonicalHash(agg.child)`) and may legitimately compute values.
   */
  private def aboveAggregateProjectsArePassthrough(plan: LogicalPlan): Boolean = plan match {
    case Project(projList, child) =>
      projList.forall { ne =>
        castChainIsUpCastOnly(ne) && (unwrapAliasesAndCasts(ne) match {
          case _: AttributeReference => true
          case l: Literal if l.value == null => true
          case _ => false
        })
      } && aboveAggregateProjectsArePassthrough(child)
    case _ => true
  }

  /** True if every Cast in an output's alias/cast wrapper chain is an UP-cast.
   *
   *  [[buildRollupPlan]] discards these wrappers -- it replays the branch
   *  aggregate's own expressions and re-applies only the widening to `u.output`'s
   *  type -- so a cast that can change the value must not be dropped. An up-cast is
   *  value-preserving, which makes dropping it and re-widening safe; a narrowing one
   *  (`CAST(sum AS DECIMAL(10,0))`, `CAST(sum AS INT)`) truncates, or throws under
   *  ANSI, and neither survives the rewrite.
   *
   *  The remap's `canUpCast` bail does NOT cover this. It compares the Expand side
   *  against `u.output`, and when the narrowing cast sits on only SOME branches the
   *  union type widens back to the un-cast type, leaving the remap a type-level
   *  no-op. Conservative over-reach: a widening the analyzer admits but `canUpCast`
   *  does not (decimal to DOUBLE) is rejected here -- a lost rewrite, never a wrong
   *  result.
   */
  private def castChainIsUpCastOnly(e: Expression): Boolean = e match {
    case Alias(child, _) => castChainIsUpCastOnly(child)
    case Cast(child, dt, _, _) =>
      Cast.canUpCast(child.dataType, dt) && castChainIsUpCastOnly(child)
    case _ => true
  }

  /** The inner-fold path's counterpart to the [[castChainIsUpCastOnly]] check that
   *  [[aboveAggregateProjectsArePassthrough]] performs for the source path: reject a
   *  branch whose DISCARDED wrapper layers hold a Cast that is not an up-cast.
   *
   *  The layers walked are the ones [[buildRollupPlan]] drops: every branch's top
   *  Projects, plus -- only for a FOLD branch, where the first Aggregate is the outer
   *  one -- that aggregate's own outputs and the mid Projects beneath it. A
   *  raw/full-detail branch's first Aggregate IS the shared inner, preserved verbatim
   *  as the Expand child, so a narrowing cast among ITS outputs is legitimate (both
   *  the original and the rewrite truncate there) and must not be rejected.
   *
   *  Without this, a value-changing but TYPE-PRESERVING chain slips through
   *  everything: `CAST(CAST(m AS INT) AS BIGINT)` leaves the branch types in
   *  agreement, so the analyzer adds no coercion Project and the remap's `canUpCast`
   *  bail sees matching types, while every other fold-path guard resolves the
   *  position through `unwrapAliasesAndCasts` and never sees the cast.
   *
   *  The mid-Project layer is redundant today -- `midProjectsAreNamePreserving`
   *  rejects any cast there already -- and is walked anyway so relaxing that check
   *  later cannot silently reopen this hole. Deliberately NOT merged with
   *  [[verifyWrapperCastTimezonesAreSession]]: that walk also covers a raw branch's
   *  inner outputs, and narrowing it to match this one would change an existing
   *  guard's behavior beyond this fix.
   */
  private def verifyDiscardedWrapperCastsAreUpCasts(u: Union): Boolean = {
    def midProjectsOk(plan: LogicalPlan): Boolean = plan match {
      case Project(projList, child) =>
        projList.forall(castChainIsUpCastOnly) && midProjectsOk(child)
      case _ => true
    }
    def layersOk(plan: LogicalPlan): Boolean = plan match {
      case Project(projList, child) =>
        projList.forall(castChainIsUpCastOnly) && layersOk(child)
      case agg: Aggregate =>
        stripProject(agg.child) match {
          case _: Aggregate =>
            agg.aggregateExpressions.forall(castChainIsUpCastOnly) &&
              midProjectsOk(agg.child)
          case _ => true
        }
      case _ => false
    }
    u.children.forall(layersOk)
  }

  private def foldableAggregateClassAfterUnwrap(
      expr: Expression): Option[Class[_ <: AggregateFunction]] =
    unwrapAliasesAndCasts(expr) match {
      // Require non-distinct + unfiltered here too, so this classifier is safe
      // independent of the earlier collectFoldableAggregations / verifyOuterFolds
      // gates (it no longer relies on call ordering to exclude distinct/filtered
      // foldables).
      case AggregateExpression(fn, _, isDistinct, filter, _)
          if isFoldableAggregate(fn) && !isDistinct && filter.isEmpty =>
        Some(fn.getClass)
      case _ => None
    }

  private def countAggregations(a: Aggregate): Int = {
    a.aggregateExpressions.iterator.flatMap(_.collect {
      case ae: AggregateExpression => ae
    }).size
  }

  private def collectFoldableAggregations(a: Aggregate): Seq[AggregateFunction] = {
    a.aggregateExpressions.flatMap(_.collect {
      // Only NON-DISTINCT, UNFILTERED foldables fold through ROLLUP.
      // sum-of-count(distinct) is NOT decomposable: the sum of per-subgroup
      // distinct counts differs from the distinct count over their union
      // whenever a value appears in more than one finer subgroup. A FILTERed
      // inner foldable is rejected too: the rewrite reuses the inner measure
      // verbatim for every level and does not validate that the filter is a
      // level-invariant per-row source predicate. Excluding these here makes
      // the size != countAggregations(a) check below bail.
      case AggregateExpression(fn, _, isDistinct, filter, _)
          if isFoldableAggregate(fn) && !isDistinct && filter.isEmpty => fn
    })
  }

  private case class InnerBranch(
      inner: Aggregate,
      innerKey: (LogicalPlan, String),
      groupCols: Seq[Attribute])

  private object InnerBranch {
    def parse(child: LogicalPlan): Option[InnerBranch] = stripProject(child) match {
      case agg: Aggregate =>
        stripProject(agg.child) match {
          case inner: Aggregate =>
            // Hash the ENTIRE inner aggregate (grouping + aggregate
            // expressions + source), not just its child: branches whose inner
            // aggregates compute different expressions over the same source
            // must NOT be treated as sharing an inner, or buildRollupPlan would
            // emit the reference inner's measures for every level and silently
            // change the other branches' rolled-up values.
            //
            // The Projects stripped between the outer and inner Aggregate must
            // be NAME-PRESERVING passthroughs. Every fold-measure and fold-key
            // check binds the outer aggregate to the inner BY NAME (re-alias
            // Projects legitimately refresh ExprIds while keeping names), so a
            // mid Project that re-aliases one inner output to ANOTHER inner
            // output's name (e.g. `SELECT a, t AS s, s AS t FROM ia`) re-routes
            // which column the outer fold actually reads while every name-based
            // check still sees the reference binding -- the rewrite would
            // silently swap the folded measures (or group by a different key).
            if (!midProjectsAreNamePreserving(agg.child)) return None
            extractGroupCols(agg).map(InnerBranch(inner, canonicalKey(inner), _))
          case _ =>
            extractGroupCols(agg).map(InnerBranch(agg, canonicalKey(agg), _))
        }
      case _ => None
    }

    /** True if every Project layer between the outer Aggregate and the first
     *  non-Project node only passes attributes through UNDER THEIR OWN NAME
     *  (aliases only -- NO casts, NO literals). A mid-Project CAST is
     *  value-changing state the fold rebuild discards: the rewrite folds over
     *  the inner's UN-cast output and replays the inner's exact key, so e.g.
     *  `CAST(k AS DOUBLE) AS k` over a BIGINT key (which merges >2^53 keys in
     *  the original) would silently change that level. A mid-Project NULL
     *  filler is rejected too: aliased to an inner output's name (`SELECT a,
     *  b, NULL AS s FROM ia`) it SHADOWS that output, re-routing the outer
     *  fold to a constant while every name-based check still sees the
     *  reference binding. Legitimate fillers live in the outer Aggregate or
     *  the branch-top Projects, where the role/binding checks see them.
     */
    private def midProjectsAreNamePreserving(plan: LogicalPlan): Boolean = plan match {
      case Project(projList, child) =>
        projList.forall { ne =>
          unwrapAliasesOnly(ne) match {
            case a: AttributeReference => ne.name == a.name
            case _ => false
          }
        } && midProjectsAreNamePreserving(child)
      case _ => true
    }

    private def extractGroupCols(agg: Aggregate): Option[Seq[Attribute]] = {
      val cols = agg.groupingExpressions.collect {
        case a: AttributeReference => a
        case Alias(a: AttributeReference, _) => a
      }
      if (cols.size != agg.groupingExpressions.size) None else Some(cols)
    }
  }

  // ===== Source-aggregate ====================================================

  private def trySourceAggregate(u: Union): Option[LogicalPlan] = {
    val parsed = u.children.map(parseSourceBranch)
    if (parsed.exists(_.isEmpty)) return None
    val branches = parsed.flatten

    val sourceKeys = branches.map(_.sourceKey).distinct
    if (sourceKeys.size != 1) return None

    val byLen = branches.sortBy(-_.groupCols.size)
    val refBranch = byLen.head
    val refGroupCols = refBranch.groupCols

    if (refGroupCols.isEmpty) return None

    val groupColSets = byLen.map(_.groupCols.map(_.name).toSet)
    if (!isPrefixShrinkingHierarchy(groupColSets, refGroupCols.map(_.name))) return None

    if (!refBranch.agg.deterministic) return None

    val refAggSig = aggregateExpressionSignatures(refBranch.agg)
    // Require at least one non-grouping (measure) output; a pure grouping-key
    // union is out of scope.
    if (refAggSig.forall(_ == "KEY")) return None
    // Position-sensitive equality: every branch must place grouping/NULL-filler
    // and measure columns in the SAME positions as the reference branch.
    // buildRollupPlan emits the reference branch's layout for all rollup levels,
    // so a branch that swapped a measure and a (same-typed) grouping column
    // would otherwise be silently rewritten to the wrong layout. An opaque
    // ("UNSUPPORTED") measure expression is allowed only when it is identical
    // across ALL branches including the GROUP BY () grand-total branch -- which
    // can reference only aggregates and constants -- so it folds correctly.
    val allMatch = byLen.tail.forall { b =>
      aggregateExpressionSignatures(b.agg) == refAggSig
    }
    if (!allMatch) return None

    if (byLen.exists(b => hasNonNullLiteralOutput(b.agg))) return None
    if (!verifyKeyPassthroughNamesAligned(u)) return None
    // Key/NULL-filler positions must match the reference layout; see
    // verifyKeyPositionsAligned.
    if (!verifyKeyPositionsAligned(
        u, refBranch.agg.aggregateExpressions, refGroupCols.map(_.exprId).toSet)) {
      return None
    }
    // Keys are matched across branches by NAME everywhere above, which
    // conflates same-named columns from different relations of a join
    // (GROUP BY t2.a against a reference grouping t1.a) -- the same trap the
    // measure signatures close with source ORDINALS. Require every branch's
    // grouping key to sit at the SAME ordinal of its (hash-matched, hence
    // order-identical) source output as the same-named reference key.
    if (!verifySourceKeyOrdinalsAligned(byLen, refBranch, refGroupCols)) return None
    if (!verifyWrapperCastTimezonesAreSession(u)) return None

    Try(buildRollupPlan(
        u, refBranch.agg.child, refGroupCols, refBranch.agg.aggregateExpressions)) match {
      case Success(rewritten) =>
        logInfo(log"Rewriting UNION ALL of " +
          log"${MDC(LogKeys.NUM_CHILDREN, u.children.size)} source-aggregates as " +
          log"ROLLUP(${MDC(LogKeys.GROUP_BY_EXPRS, refGroupCols.map(_.name).mkString(","))})")
        Some(rewritten)
      case Failure(ex) =>
        logDebug(s"Source-aggregate rollup rewrite skipped: ${ex.getMessage}")
        None
    }
  }

  /** Require every branch's grouping keys to occupy the SAME source-output
   *  ordinals as the same-named reference keys. The per-position signatures
   *  mark keys anonymously ("KEY") and all other key checks compare NAMES, so
   *  without this a branch grouping by a DIFFERENT relation's same-named
   *  column (`GROUP BY t2.a` where the reference groups `t1.a` over a join)
   *  passes every guard and the rewrite silently regroups that level by the
   *  reference's column. Ordinals are cross-branch comparable because the
   *  sources are canonicalHash-matched and canonicalization is order-faithful.
   */
  private def verifySourceKeyOrdinalsAligned(
      branches: Seq[SourceBranch],
      refBranch: SourceBranch,
      refGroupCols: Seq[Attribute]): Boolean = {
    val refChildOrd: Map[ExprId, Int] =
      refBranch.agg.child.output.map(_.exprId).zipWithIndex.toMap
    val refOrdByName: Map[String, Int] = refGroupCols.flatMap { k =>
      refChildOrd.get(k.exprId).map(k.name -> _)
    }.toMap
    if (refOrdByName.size != refGroupCols.size) return false
    branches.forall { b =>
      val childOrd: Map[ExprId, Int] =
        b.agg.child.output.map(_.exprId).zipWithIndex.toMap
      b.groupCols.forall { k =>
        childOrd.get(k.exprId).exists(o => refOrdByName.get(k.name).contains(o))
      }
    }
  }

  private case class SourceBranch(
      agg: Aggregate,
      sourceKey: (LogicalPlan, String),
      groupCols: Seq[Attribute])

  private def parseSourceBranch(child: LogicalPlan): Option[SourceBranch] = {
    // A value-changing Project ABOVE the aggregate (e.g. SELECT sum(x)+1 over a
    // sub-aggregate, or a narrowing cast) would be stripped below and DROPPED by
    // buildRollupPlan, which rolls up the aggregate's own expressions and
    // re-applies only u.output's widening cast -- silently changing results. The
    // optimizer's operator-optimization batches run AFTER this rule (it is in
    // the earlier "Replace Operators" batch), so this guard cannot rely on
    // upstream folding; it makes the source path correct BY CONSTRUCTION. (The
    // inner-fold path handles its own value-changing wrappers separately: a
    // narrowing cast wrapping a fold INSIDE the outer aggregate is rejected by
    // verifyOuterFoldsToInner's bare-fold check, and an above-aggregate
    // value-changing Project there is rejected by verifyOutputsRollupShaped.)
    if (!aboveAggregateProjectsArePassthrough(child)) return None
    // Branch-top Projects must also preserve ORDER: every per-position guard
    // compares the AGGREGATE's own layout across branches, and buildRollupPlan
    // binds the reference aggregate's order positionally to u.output (the
    // head branch's POST-Project order). A passthrough Project that merely
    // REORDERS columns (`SELECT a, b, sx, sy FROM (SELECT ..., sum(y) AS sy,
    // sum(x) AS sx ...)`) therefore silently permutes that branch's columns
    // in the rewrite. Coercion Projects cast strictly in place and pass.
    if (!branchTopProjectsAreOrderPreserving(child)) return None
    stripProject(child) match {
      case agg: Aggregate =>
        stripProject(agg.child) match {
          case _: Aggregate => None  // inner-aggregate fold path handles this
          case _ =>
            val cols = agg.groupingExpressions.collect {
              case a: AttributeReference => a
              case Alias(a: AttributeReference, _) => a
            }
            // Hash the UNSTRIPPED agg.child (Projects included), because
            // buildRollupPlan rolls up over `refBranch.agg.child` verbatim. A
            // cast (or other expression) inside a Project between the aggregate
            // and the leaf source must participate in cross-branch matching;
            // stripping to the leaf would lose e.g. a cast's eval mode and let
            // two branches with different-mode source casts collide.
            if (cols.size != agg.groupingExpressions.size) None
            else Some(SourceBranch(agg, canonicalKey(agg.child), cols))
        }
      case _ => None
    }
  }

  /** Per-POSITION signature of a branch's output, compared across branches so
   *  the full positional layout must agree (not just the aggregate
   *  subsequence): a branch that swapped a measure and a same-typed grouping
   *  column, or that aggregated a DIFFERENT same-typed column under the same
   *  output name, must NOT compare equal to the reference, because
   *  [[buildRollupPlan]] emits the reference branch's expressions for every
   *  rollup level bound to the Union output by position. Markers:
   *   - grouping-column passthrough OR NULL filler -> "KEY". Bare (no output
   *     name): a key present at a low level becomes a NULL filler at a higher
   *     level, and these legitimately carry DIFFERENT output names per branch
   *     (e.g. `ca_county` vs `NULL AS county`); the Union takes its name from
   *     the first branch and rolled-up levels are NULL regardless. Key
   *     cross-labeling (a key routed to a differently-named output) is caught
   *     separately by [[verifyKeyPassthroughNamesAligned]].
   *   - aggregate expression -> "<outName>=AGG:<aggMeasureSignature>" (see
   *     [[aggMeasureSignature]]: the full [[exprSignature]] encoding +
   *     DISTINCT + FILTER). The output name is included so a measure bound to
   *     the wrong output never matches; the full function encoding distinguishes
   *     measures that differ only inside a complex argument (e.g. sum(CASE WHEN
   *     x>0 ..) vs sum(CASE WHEN x>5 ..)) which a references-only signature would
   *     miss.
   *   - non-null literal (per-branch level indicator) -> "BAD_LITERAL:<value>"
   *   - anything else -> "UNSUPPORTED:..."
   */
  private def aggregateExpressionSignatures(agg: Aggregate): Seq[String] = {
    val childOutput = agg.child.output
    agg.aggregateExpressions.map { expr =>
      val outName = expr match {
        case ne: NamedExpression => ne.name
        case _ => "?"
      }
      val unwrapped = expr match {
        case a: Alias => a.child
        case other => other
      }
      unwrapped match {
        case _: AttributeReference => "KEY"
        case l: Literal if l.value == null => "KEY"
        case _: Literal =>
          s"BAD_LITERAL:${unwrapped.toString}"
        case ae: AggregateExpression =>
          s"$outName=AGG:${aggMeasureSignature(ae, childOutput)}"
        case other =>
          // Route through exprSignature so this opaque branch ALSO encodes
          // hidden eval-mode/timezone state (e.g. a top-level `sum(a) + sum(b)`
          // whose Add differs in eval mode across branches: try_add -> NULL on
          // overflow vs + -> wrap). exprSignature's stripIds(toString) normalizes
          // ExprIds so structurally-identical opaque measures still match.
          s"UNSUPPORTED:${exprSignature(other, childOutput)}"
      }
    }
  }

  private def hasNonNullLiteralOutput(agg: Aggregate): Boolean = {
    agg.aggregateExpressions.exists { expr =>
      val unwrapped = expr match {
        case a: Alias => a.child
        case other => other
      }
      unwrapped match {
        case l: Literal => l.value != null
        case _ => false
      }
    }
  }

  // ===== Shared helpers ======================================================

  /** Branches' GROUP BY column sets must form a COMPLETE ROLLUP hierarchy
   *  `[N, N-1, ..., 1, 0]`. The sets must be pre-sorted by descending size.
   *
   *  Each level must be EXACTLY the ROLLUP prefix of its size: the level with
   *  `k` columns must equal `{refColNames(0), ..., refColNames(k-1)}`. A subset
   *  that drops a non-trailing key (e.g. `{a, c}` for ref `[a, b, c]`) is a
   *  valid GROUPING SETS member but NOT a ROLLUP level; it must be rejected
   *  because [[buildRollupPlan]] always emits true prefixes, so accepting it
   *  would silently compute the wrong grouping (different rows / values).
   */
  private def isPrefixShrinkingHierarchy(
      sortedGroupColSets: Seq[Set[String]],
      refColNames: Seq[String]): Boolean = {
    if (refColNames.distinct.size != refColNames.size) return false
    val n = refColNames.size
    if (sortedGroupColSets.size != n + 1) return false
    sortedGroupColSets.zipWithIndex.forall { case (levelSet, idx) =>
      levelSet == refColNames.take(n - idx).toSet
    }
  }

  /** Build the rewritten plan: an Aggregate-with-Rollup-via-Expand over `child`
   *  producing `aggregationExpressions` for levels [N..1], remapped to the
   *  Union's output exprIds and UNION-ed back with the original grand-total
   *  branch.
   *
   *  The two paths supply different `(child, aggregationExpressions)`:
   *   - Source-aggregate: `child` = the shared source, `aggregationExpressions`
   *     = the reference branch's own aggregates (recomputed from the source at
   *     each level).
   *   - Inner-aggregate fold: `child` = the shared INNER aggregate, and
   *     `aggregationExpressions` apply the OUTER fold (sum-of-sum, max-of-max,
   *     min-of-min, sum-of-count) to the inner's measure outputs (see
   *     [[innerFoldAggregateExpressions]]). Rolling up over the inner (not the
   *     source) preserves the original TWO-level accumulation exactly,
   *     including the inner level's own decimal overflow / NULL semantics --
   *     so no decimal-width adjustment is needed.
   */
  private def buildRollupPlan(
      u: Union,
      child: LogicalPlan,
      groupCols: Seq[Attribute],
      aggregationExpressions: Seq[NamedExpression]): LogicalPlan = {
    // Selected grouping sets for levels [N, N-1, ..., 1] ONLY. The grand-total
    // (empty grouping set) is intentionally EXCLUDED from the Expand and is
    // instead supplied by unioning the original GROUP BY () branch back below.
    // Rationale: a grouped ROLLUP aggregate emits ZERO rows on empty input,
    // whereas the original global aggregate emits exactly ONE (e.g. sum=NULL,
    // count=0); folding the empty level into Expand would drop that grand-total
    // row on empty input and double-count it on non-empty input.
    // Defense-in-depth: every grouping column must be an attribute of the
    // rollup child's output -- a dangling reference would otherwise be
    // silently absorbed (and semantically re-bound) by CollapseProject later.
    // Throwing here lands in the caller's Try -> conservative bail.
    if (!groupCols.forall(child.outputSet.contains)) {
      throw new IllegalStateException(
        "Rollup-rewrite: grouping column not present in the rollup child output")
    }
    val selectedGroupings: Seq[Seq[Expression]] =
      groupCols.indices.reverse.map(i =>
        groupCols.take(i + 1).map(e => e: Expression))

    val expanded = GroupingAnalyticsTransformer(
      newAlias = (c, name, qualifier) =>
        Alias(c, name.getOrElse("rollup_alias"))(qualifier = qualifier),
      childOutput = child.output,
      groupByExpressions = groupCols.map(e => e: Expression),
      selectedGroupByExpressions = selectedGroupings,
      child = child,
      aggregationExpressions = aggregationExpressions
    )

    // Remap the Expand levels [N..1] to the Union's output exprIds/types, then
    // UNION ALL the original grand-total (GROUP BY ()) branch back. The
    // grand-total branch is an original Union child, so its output types already
    // equal u.output exactly. In practice the Expand side's types ALSO equal
    // u.output (the source path rolls up a real Union branch; the inner-fold path's
    // innerFoldAggregateExpressions builds the fold at the widened result type
    // directly -- e.g. Sum over a Decimal(p+10) inner already yields Decimal(p+20)).
    // The up-cast below is LOAD-BEARING when the Expand side is genuinely
    // narrower than the union type (e.g. a DATE key under per-branch
    // CAST(d AS TIMESTAMP) wrappers: the Expand keeps the DATE and the remap
    // casts it up); on the common paths it is a type-level no-op. Its other
    // essential job is the Alias that REBINDS each Expand output to u.output's
    // exprId/qualifier. The resulting Union's output = the remapped Expand side =
    // u.output exprIds, which downstream references.
    val unionOut = u.output
    val expandedOut = expanded.output
    if (unionOut.size != expandedOut.size) {
      throw new IllegalStateException(
        s"Rollup-rewrite schema mismatch: union has ${unionOut.size} columns " +
          s"but rewritten plan has ${expandedOut.size}")
    }
    val remap = unionOut.zip(expandedOut).map { case (uAttr, eAttr) =>
      // Equal exprId and dataType is the fast path; nullability is deliberately not
      // compared. A grouping-key position never reaches it (GroupingAnalyticsTransformer
      // re-aliases keys, so their exprIds differ), and a measure position reuses the
      // reference's own Attribute, so the two agree there by construction.
      //
      // One consequence to keep in mind: when the grand-total branch is the FIRST
      // Union child, u.output's exprIds are its own, so both children of the emitted
      // Union carry the same exprIds. The analyzer keeps Union branches exprId-disjoint
      // (DeduplicateRelations), but nothing maintains that on optimizer output, so this
      // rests on later rules not keying on between-branch exprId uniqueness.
      if (eAttr.dataType == uAttr.dataType && eAttr.exprId == uAttr.exprId) {
        eAttr
      } else {
        val casted: Expression = if (eAttr.dataType == uAttr.dataType) {
          eAttr
        } else if (Cast.canUpCast(eAttr.dataType, uAttr.dataType)) {
          // Supply the session timezone explicitly: a timezone-needing up-cast
          // (e.g. DATE -> TIMESTAMP) built bare would leave the Cast -- and the
          // whole rewritten plan -- UNRESOLVED, and nothing re-runs
          // ResolveTimeZone after the optimizer.
          Cast(eAttr, uAttr.dataType, Some(conf.sessionLocalTimeZone))
        } else {
          throw new IllegalStateException(
            s"Rollup-rewrite would require unsafe Cast from ${eAttr.dataType.sql} " +
              s"to ${uAttr.dataType.sql} for column ${uAttr.name}")
        }
        Alias(casted, uAttr.name)(exprId = uAttr.exprId, qualifier = uAttr.qualifier)
      }
    }
    val remappedRollup = Project(remap, expanded)

    val grandTotal = findGrandTotalBranch(u).getOrElse(
      throw new IllegalStateException(
        "Rollup-rewrite: no grand-total (GROUP BY ()) branch found"))
    Union(Seq(remappedRollup, grandTotal))
  }

  /** The original GROUP BY () (grand-total) branch of the Union, returned with
   *  any Project wrappers intact. The completeness hierarchy guarantees exactly
   *  one such branch.
   */
  private def findGrandTotalBranch(u: Union): Option[LogicalPlan] =
    u.children.find { ch =>
      stripProject(ch) match {
        case agg: Aggregate => agg.groupingExpressions.isEmpty
        case _ => false
      }
    }

  /** Aggregate expressions for the inner-fold ROLLUP, applying the OUTER fold to
   *  the INNER aggregate's outputs (the Expand child is the inner aggregate).
   *  For each position of the inner aggregate's `aggregateExpressions`:
   *   - a grouping-key passthrough -> the inner key attribute, passed through
   *     (it becomes an Expand grouping attribute, nulled at higher levels);
   *   - a foldable measure alias `m` (sum/max/min/count) -> the OUTER fold over
   *     `m`'s attribute: sum-of-sum, max-of-max, min-of-min, sum-of-count, each
   *     aliased back to `m`'s name/exprId.
   *
   *  Rolling up the inner's already-materialized measure (rather than the raw
   *  source) reproduces the original two-level accumulation exactly, including
   *  the inner level's decimal width and overflow (NULL/error) behavior. Returns
   *  None if any position is neither a key passthrough nor a foldable measure
   *  (the caller then bails).
   */
  private def innerFoldAggregateExpressions(
      inner: Aggregate,
      innerGroupCols: Seq[Attribute]): Option[Seq[NamedExpression]] = {
    val groupColIds = innerGroupCols.map(_.exprId).toSet
    val built = inner.aggregateExpressions.map { expr =>
      unwrapAliasesAndCasts(expr) match {
        case a: AttributeReference if groupColIds.contains(a.exprId) =>
          // grouping-key passthrough: keep the inner key attribute as-is
          Some(a: NamedExpression)
        case _ =>
          // expr is a NamedExpression (Aggregate.aggregateExpressions), so its
          // name is directly available -- no non-local return needed.
          val outName = expr.name
          foldableMeasureAttr(expr).map { case (attr, fnClass) =>
            val foldFn: AggregateFunction =
              if (fnClass == classOf[Sum]) Sum(attr)
              else if (fnClass == classOf[Max]) Max(attr)
              else if (fnClass == classOf[Min]) Min(attr)
              else Sum(attr) // count rolls up via sum-of-count
            // FRESH exprId: the fold produces a NEW value at a possibly WIDER
            // type than the inner measure (e.g. sum-of-sum widens Decimal(p+10)
            // to Decimal(p+20)), so it must NOT reuse the inner measure's
            // exprId. buildRollupPlan's remap re-binds this to the Union output
            // exprId by position.
            Alias(
              AggregateExpression(foldFn, Complete, isDistinct = false),
              outName)(): NamedExpression
          }
      }
    }
    if (built.exists(_.isEmpty)) None else Some(built.flatten)
  }

  /** If `expr` (an inner aggregate output) is a foldable, non-distinct,
   *  unfiltered aggregate alias, return its output attribute paired with the
   *  aggregate function class; else None.
   */
  private def foldableMeasureAttr(
      expr: NamedExpression): Option[(Attribute, Class[_ <: AggregateFunction])] =
    expr match {
      case a: Alias =>
        foldableAggregateClassAfterUnwrap(a).map(c => (a.toAttribute, c))
      case _ => None
    }

  private def stripProject(p: LogicalPlan): LogicalPlan = p match {
    case Project(_, child) => stripProject(child)
    case other => other
  }

  /** True if every Project layer above the first non-Project node is a SIZE-
   *  and ORDER-preserving passthrough: position p unwraps (aliases/casts) to
   *  the child's position-p output attribute. See the call site in
   *  [[parseSourceBranch]].
   */
  private def branchTopProjectsAreOrderPreserving(plan: LogicalPlan): Boolean = plan match {
    case Project(projList, child) =>
      projList.size == child.output.size &&
        projList.zip(child.output).forall { case (ne, attr) =>
          unwrapAliasesAndCasts(ne) match {
            case a: AttributeReference => a.exprId == attr.exprId
            case _ => false
          }
        } && branchTopProjectsAreOrderPreserving(child)
    case _ => true
  }
}
