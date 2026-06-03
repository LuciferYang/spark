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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Cross-branch equivalence fingerprints for [[RewriteUnionAggregateAsRollup]].
 *
 * That rule rebuilds every rollup level from ONE reference branch, so it must first
 * prove the other branches compute the same thing. [[exprSignature]] and
 * [[aggMeasureSignature]] fingerprint an expression, [[canonicalKey]] and
 * [[canonicalHash]] a plan. None of them carries rollup knowledge, which is why they
 * sit here rather than in the rule: each can be tested on a pair of expressions.
 *
 * All are total. The rule's guards run outside the `buildRollupPlan` Try, so a
 * fingerprint that threw would fail the query instead of skipping the rewrite.
 */
private[optimizer] object RollupRewriteSignatures {

  /** Per-aggregate cross-branch signature: `exprSignature` of the function plus
   *  DISTINCT and FILTER. See [[exprSignature]] for the structure + hidden-state
   *  encoding. `childOutput` is the branch aggregate's child output, used to
   *  encode attribute ORDINALS (see [[exprSignature]]).
   */
  def aggMeasureSignature(ae: AggregateExpression, childOutput: Seq[Attribute]): String = {
    s"${exprSignature(ae.aggregateFunction, childOutput)}:distinct=${ae.isDistinct}:" +
      s"filter=${ae.filter.map(exprSignature(_, childOutput)).getOrElse("none")}"
  }

  /** Structural signature of an expression for cross-branch equivalence:
   *  `stripIds(toString)` (real attribute NAMES + full argument STRUCTURE, with
   *  only `#<id>` ExprIds stripped) PLUS attribute ORDINALS in `childOutput`,
   *  LITERAL values, SUBQUERY plan hashes, pre-order CLASS:ARITY tree-shape
   *  sequences, and [[hiddenStateMarkers]].
   *
   *  Rationale for the parts:
   *   - `stripIds(toString)` distinguishes measures reading DIFFERENT columns
   *     (`sum(a#)` vs `sum(b#)`) and differing only inside a complex argument
   *     (`sum(CASE WHEN x>0 ..)` vs `sum(CASE WHEN x>5 ..)`, `sum(x*2)` vs
   *     `sum(x*3)`). We deliberately do NOT use `canonicalized`, which rewrites
   *     attribute names to "none" and would make `sum(a)` and `sum(b)` collide.
   *     Stripping only the numeric ExprIds lets structurally-identical measures
   *     with fresh ExprIds across branches still match.
   *   - `ords`: each referenced attribute's ORDINAL position in the branch
   *     aggregate's child output. NAMES alone conflate same-named attributes
   *     from different relations of a join (`sum(l.x)` and `sum(r.x)` both
   *     render `sum(x#)` after ID-stripping -- toString carries no qualifier),
   *     and the rewrite would replay one branch's column for all levels.
   *     Ordinals are comparable across branches because the branches' children
   *     are required to be canonically identical (hash-matched), so the
   *     same-position output is the same column in every branch.
   *   - `lits`: every literal's value, UN-stripped. `stripIds` munges `#<n>`
   *     inside literal VALUES too (`'v#1'` and `'v#2'` both become `v#`), so
   *     two branches differing only in such a literal (e.g. inside a FILTER)
   *     would otherwise collide.
   *   - `subq`: [[canonicalHash]] of every subquery plan. A subquery
   *     expression's `toString` hides its plan entirely (`scalar-subquery#
   *     [...]`), so two branches computing DIFFERENT subqueries would
   *     otherwise collide and the rewrite would replay one branch's subquery
   *     for every level.
   *   - `hiddenStateMarkers` adds the result-affecting fields `toString` hides --
   *     eval modes, timezones, function identities, analysis-frozen conf flags;
   *     that method carries the authoritative per-class list. Without them, e.g.
   *     sum(x)[ANSI] vs sum(x)[LEGACY] collide and the rewrite reuses one mode
   *     for all levels, silently changing overflow behavior (ANSI throws /
   *     LEGACY wraps / TRY -> NULL).
   */
  def exprSignature(e: Expression, childOutput: Seq[Attribute]): String = {
    val ordinalById: Map[ExprId, Int] =
      childOutput.map(_.exprId).zipWithIndex.toMap
    val ords = e.collect { case a: AttributeReference =>
      // External attributes (not in the branch child's output -- e.g. outer
      // references when OptimizeSubqueries re-runs this batch on a correlated
      // subquery) are encoded with their ExprId: a genuinely shared outer
      // reference resolves ONCE in the enclosing scope, so all branches carry
      // the same id, while two DIFFERENT same-named outer attributes (l.q vs
      // r.q from a self-joined outer scope) must not collide.
      ordinalById.get(a.exprId).map(_.toString)
        .getOrElse(s"ext:${a.exprId.id}:${a.name}")
    }
    // Literal.toString renders VALUES deterministically for the common cases
    // (top-level binary -> hex, arrays -> elements); raw `value` for an
    // Array[Byte] is an identity-hash string that differs across
    // separately-parsed but textually-identical literals, which would
    // nondeterministically over-reject. Residual: binary NESTED inside
    // array/struct literal values still renders identity-based and fails
    // CLOSED (always-mismatch, conservative). Must never throw (guards run
    // outside the buildRollupPlan Try); Literal.toString is total.
    val lits = e.collect { case l: Literal =>
      s"${l.dataType.simpleString}:${l.toString}"
    }
    val subqs = e.collect { case s: SubqueryExpression => canonicalHash(s.plan) }
    // Alpha-invariant lambda BINDING structure: each NamedLambdaVariable
    // reference is encoded as the pre-order first-occurrence index of its
    // ExprId. Lambda variables are LeafExpressions (not AttributeReferences,
    // so ords is blind to them) and their rendered names can contain
    // #<digits> that stripIds munges -- two alpha-INequivalent nested lambdas
    // (inner body referencing the OUTER vs the INNER parameter) would
    // otherwise collide when the spellable names differ only in stripped
    // digits. Index sequences capture exactly which binder each reference
    // resolves to, independent of names.
    val lambdaIds = e.collect { case v: NamedLambdaVariable => v.exprId }
    val lambdaFirstIdx = lambdaIds.distinct.zipWithIndex.toMap
    val lambdas = lambdaIds.map(lambdaFirstIdx)
    // Pre-order class-name:arity sequence over the FULL tree.
    // Expression.toString truncates flat argument lists at
    // spark.sql.debug.maxToStringFields, so two expressions agreeing on the
    // leading arguments but differing STRUCTURALLY in the truncated tail
    // (e.g. AND vs OR inside the 26th argument) would otherwise collide; the
    // class sequence is truncation-immune. The ARITY makes the encoding a
    // bijective tree shape: without it, same-class nestings that merely move
    // children between levels (f(f(a,b),c,d) vs f(f(a,b,c),d)) share the
    // pre-order class sequence.
    val classes = e.collect { case node =>
      s"${node.getClass.getSimpleName}:${node.children.size}"
    }
    stripIds(e.toString) +
      ords.mkString("|ords=[", ",", "]") +
      lits.mkString("|lits=[", ",", "]") +
      subqs.mkString("|subq=[", ",", "]") +
      classes.mkString("|cls=[", ",", "]") +
      lambdas.mkString("|lam=[", ",", "]") +
      hiddenStateMarkers(e).mkString("|hidden=[", ",", "]")
  }

  /** Markers for result-affecting state that expressions hide from
   *  `toString`/`flatArguments` but keep as constructor fields or captured
   *  objects: eval modes / NumericEvalContext (ANSI throws vs LEGACY wraps vs
   *  TRY -> NULL on overflow), EVERY timezone-aware expression's timeZoneId,
   *  function-object identity (ScalaUDF / V2 functions / from_csv / from_xml),
   *  and analysis-frozen conf flags (statistical divide-by-zero results,
   *  exists() three-valued logic, the ANSI failOnError family). Two branches
   *  differing only in such hidden state otherwise collide in the cross-branch
   *  comparators, and the rewrite reuses one branch's version for every rollup
   *  level -- silently changing results. Each carrier is matched explicitly;
   *  collected in pre-order so the sequence reflects tree position.
   *
   *  NOTE: a fully generic "emit every node's non-child constructor args"
   *  approach over-rejects -- benign per-node fields (e.g. `Literal` value
   *  rendering, metadata, ExprIds) vary cosmetically and would block
   *  legitimate matches. The carriers below were enumerated by auditing which
   *  expression classes override toString/stringArgs/flatArguments to omit
   *  result-affecting constructor state; default-rendered classes expose all
   *  fields via productIterator and need no marker.
   *
   *  EXTENSION CONTRACT: this is a per-class enumeration over an OPEN set, so it
   *  cannot fail closed. A Catalyst expression class that keeps result-affecting
   *  state OUT of toString/stringArgs/flatArguments needs an arm HERE; without
   *  one, two branches differing only in that state compare equal and the
   *  rewrite replays one branch's version for every rollup level. The trailing
   *  UserDefinedExpression arm fails closed for that one family only.
   */
  def hiddenStateMarkers(e: Expression): Seq[String] = e.collect {
    case s: Sum => s"sumctx=${s.evalContext}"
    case avg: Average => s"avgmode=${avg.evalMode}"
    case a: BinaryArithmetic => s"arithctx=${a.evalContext}"
    // castto: the TARGET TYPE's field names are value-semantic (struct field
    // names appear in to_json output and drive variant/json key lookups) and
    // stripIds munges #<digits> inside dataType.simpleString in toString;
    // encode the type un-stripped.
    case c: Cast =>
      s"castmode=${c.evalMode}:casttz=${c.timeZoneId}:castto=${c.dataType.catalogString}"
    // A UDF's behavior lives in its function object, which toString hides
    // (only the udf NAME is rendered) and no comparator sees -- a temp view
    // freezes the resolved function, so a same-named re-registered udf in
    // another branch would collide. Mark by OBJECT IDENTITY: branches of one
    // query resolve to the same registry instance (plan copies share the
    // reference), while a view-frozen instance differs. identityHashCode is
    // stable within a rule invocation, which is the comparison scope.
    case u: ScalaUDF =>
      s"udf=${u.udfName.getOrElse("?")}:${System.identityHashCode(u.function)}"
    case af: ApplyFunctionExpression =>
      s"v2fn=${af.function.name}:${System.identityHashCode(af.function)}"
    // from_csv/from_xml capture the corrupt-record column NAME as a private
    // constructor-time val (invisible even to productIterator), so their
    // hidden state cannot be value-compared. Mark by NODE identity: this
    // INTENTIONALLY never matches across separately-analyzed branch copies
    // (InlineCTE's multi-reference rebuild and per-branch parsing both create
    // distinct instances), i.e. the rewrite conservatively never fires when
    // from_csv/from_xml appears ANYWHERE in a compared tree -- measures or
    // the shared source subtree alike (the markers flow into canonicalKey);
    // only provably-same-instance plans (shared DataFrame subtrees) still
    // match. Placed BEFORE the TimeZoneAware arm (both are tz-aware;
    // identity subsumes the tz marker).
    case c: CsvToStructs => s"csv=${System.identityHashCode(c)}"
    case x: XmlToStructs => s"xml=${System.identityHashCode(x)}"
    // from_json (parser) / to_csv (writer) / variant_get carry SCHEMA and
    // OPTIONS as constructor fields rendered (and #<digit>-munged) only via
    // toString: schema field names drive key lookups and appear in output
    // text; option VALUES (nullValue, datetime patterns) are copied into
    // output. Encode them un-stripped. These classes are TimeZoneAware, so
    // the arms must precede the tzaware arm and include the tz themselves.
    // (from_json's corrupt-record name is a lazy conf read at execution --
    // session-uniform -- unlike from_csv/from_xml's frozen ctor val, so value
    // marking works.) NOTE: to_json (StructsToJson) is RuntimeReplaceable and
    // is already replaced (by ReplaceExpressions in the Finish Analysis batch)
    // before this rule runs; its options/schema/tz then surface through the
    // replacement's Literal(StructsToJsonEvaluator(...)) -- caught by the
    // `lits` component and by canonicalKey plan equality -- so it needs no arm.
    case j: JsonToStructs =>
      s"jsonschema=${j.schema.catalogString}:" +
        s"jsonopts=${j.options.toSeq.sorted.mkString(",")}:tz=${j.timeZoneId}"
    case s: StructsToCsv =>
      s"tocsvopts=${s.options.toSeq.sorted.mkString(",")}:tz=${s.timeZoneId}"
    // to_xml's options are a constructor Map, so they reach the signature only
    // through the default toString, where stripIds munges #<digits> inside option
    // values -- the same exposure the StructsToCsv arm closes. Must stay above the
    // tzaware arm (StructsToXml is TimeZoneAware, so that arm would capture it
    // first), hence the tz here too.
    case x: StructsToXml =>
      s"toxmlopts=${x.options.toSeq.sorted.mkString(",")}:tz=${x.timeZoneId}"
    case v: VariantGet =>
      s"varianttype=${v.targetType.catalogString}:variantfail=${v.failOnError}:" +
        s"tz=${v.timeZoneId}"
    // EVERY timezone-aware expression, not just Cast: several override
    // toString to hide their timeZoneId entirely (e.g. TimeAdd renders as
    // "$left + $right"), and canonicalized does not strip the field either --
    // so a branch whose plan was analyzed under a DIFFERENT session timezone
    // (a temp view stores its ANALYZED plan, freezing zoneIds at CREATE time)
    // collides with same-text branches analyzed under the current session.
    // The class name is included so different tz-hiding expressions at the
    // same tree position cannot cross-match. NOTE: this arm precedes the
    // UserDefinedExpression catch-all below -- a future UDF carrier that
    // also mixes in TimeZoneAwareExpression would be captured HERE (tz-only
    // marker, no identity); add a specific arm above this one in that case.
    case tz: TimeZoneAwareExpression =>
      s"tzaware=${tz.getClass.getSimpleName}:${tz.timeZoneId}"
    // Constructor-captured conf flags that custom toString/stringArgs/
    // flatArguments overrides HIDE (verified per class): the statistical
    // aggregates' divide-by-zero result (NaN vs NULL), exists()'s
    // three-valued-logic flag (NULL vs false), and the ANSI failOnError
    // family (throw vs wrap/NULL). All are frozen at analysis time and can
    // diverge across branches via temp views.
    case a: StddevPop => s"momentdiv0=${a.nullOnDivideByZero}"
    case a: StddevSamp => s"momentdiv0=${a.nullOnDivideByZero}"
    case a: VariancePop => s"momentdiv0=${a.nullOnDivideByZero}"
    case a: VarianceSamp => s"momentdiv0=${a.nullOnDivideByZero}"
    case a: Skewness => s"momentdiv0=${a.nullOnDivideByZero}"
    case a: Kurtosis => s"momentdiv0=${a.nullOnDivideByZero}"
    case c: Corr => s"corrdiv0=${c.nullOnDivideByZero}"
    case c: CovPopulation => s"covdiv0=${c.nullOnDivideByZero}"
    case c: CovSample => s"covdiv0=${c.nullOnDivideByZero}"
    case ae: ArrayExists => s"exists3vl=${ae.followThreeValuedLogic}"
    case um: UnaryMinus => s"negfail=${um.failOnError}"
    case ab: Abs => s"absfail=${ab.failOnError}"
    case r: Round => s"roundansi=${r.ansiEnabled}"
    case r: BRound => s"roundansi=${r.ansiEnabled}"
    // ceil/floor: UnaryMathExpression renders only prettyName(child), so the flag
    // is invisible to toString and no arm above matches these classes. Their
    // Double path branches on it -- ANSI throws on overflow where LEGACY wraps.
    case c: Ceil => s"ceilfail=${c.failOnError}"
    case f: Floor => s"floorfail=${f.failOnError}"
    // Decimal wrappers whose toString omits nullOnOverflow (overflow throws vs
    // yields NULL): CheckOverflow renders only child and dataType, MakeDecimal only
    // child, precision and scale. Both take the flag from the session's ANSI setting
    // at construction. CheckOverflowInSum needs no arm -- its toString renders it.
    case c: CheckOverflow => s"ovfnull=${c.nullOnOverflow}"
    case m: MakeDecimal => s"mkdecnull=${m.nullOnOverflow}"
    case g: GetArrayItem => s"getitemfail=${g.failOnError}"
    // Struct-field selectors render only `child.fieldName` in toString -- the
    // authoritative ORDINAL is hidden, and stripIds munges `#<digits>` inside
    // field NAMES, so two measures reading different fields whose names
    // differ only in such a suffix (s.`m#1` vs s.`m#2`) would collide. Encode
    // the ordinal/name (markers are appended UN-stripped).
    case g: GetStructField => s"structord=${g.ordinal}"
    case g: GetArrayStructFields => s"arrstructord=${g.ordinal}"
    case w: WithField => s"withfield=${w.name}"
    case d: DropField => s"dropfield=${d.name}"
    // A session variable renders its value inside toString, where stripIds
    // can munge `#<digits>` content; encode the value un-stripped.
    case v: VariableReference => s"varval=${v.varDef.currentValue.sql}"
    // Python UDF/UDAF behavior lives in the hidden `func` object -- same
    // family as the ScalaUDF arm above (identity: plan copies share the
    // instance; view-frozen re-registrations differ).
    case p: PythonUDF => s"pyudf=${p.name}:${System.identityHashCode(p.func)}"
    case p: PythonUDAF => s"pyudaf=${p.name}:${System.identityHashCode(p.func)}"
    // Fail-closed catch-all for every OTHER user-defined expression carrier
    // (ScalaUDAF, ScalaAggregator, ...): node identity -- separately-analyzed
    // copies never match (conservative non-fire, the accepted UDF-measure
    // limitation); the specific arms above match first for their classes.
    case u: UserDefinedExpression => s"udx=${System.identityHashCode(u)}"
  }

  /** Stable identity of a plan for cross-branch equivalence: the CANONICALIZED
   *  PLAN ITSELF (compared with case-class equality -- the basis of
   *  `sameResult`) paired with a hidden-state/shape supplement string.
   *
   *  The canonicalized plan normalizes attribute ExprIds to positional
   *  ordinals -- structurally-identical subtrees with fresh ExprIds compare
   *  equal, while subtrees reading DIFFERENT columns do not. Comparing the
   *  PLAN (not its toString) is essential: leaf toStrings omit
   *  identity-bearing state (LocalRelation renders only its schema, never its
   *  DATA; DSv1 file relations render only the format name, never the PATH),
   *  so a string-derived identity collides across same-schema sources with
   *  different contents and the rewrite would silently replay one branch's
   *  source for another's level. Case-class equality compares LocalRelation
   *  data and relation/FileIndex identity exactly.
   *
   *  The supplement adds what `canonicalized` equality under-distinguishes:
   *  [[hiddenStateMarkers]] over every expression in the subtree (eval modes,
   *  timezones, function identities, frozen conf flags -- `canonicalized`
   *  normalizes some of these fields), collected in tree order.
   */
  def canonicalKey(p: LogicalPlan): (LogicalPlan, String) = {
    val c = p.canonicalized
    // Collect markers from the plan AND every nested subquery plan: case-class
    // equality recurses into subquery plans already, but markers cover the
    // non-constructor conf-frozen state equality cannot see (e.g. a csv/xml
    // corrupt-record name inside a source subquery).
    val markers = (c +: c.subqueriesAll).flatMap { plan =>
      plan.collect { case node => node.expressions }.flatten
        .flatMap(hiddenStateMarkers)
    }
    (c, markers.mkString("|", ",", "|"))
  }

  /** String form of plan identity for embedding in [[exprSignature]] (the
   *  subquery component). Uses the canonicalized plan's case-class hashCode
   *  (data- and path-aware, unlike toString) plus the same supplement;
   *  residual 32-bit collision risk is the accepted hash-width class.
   */
  def canonicalHash(p: LogicalPlan): String = {
    val (c, markers) = canonicalKey(p)
    "%08x".format((c.hashCode.toString + markers).hashCode)
  }

  def stripIds(s: String): String = s.replaceAll("#\\d+", "#")
}
