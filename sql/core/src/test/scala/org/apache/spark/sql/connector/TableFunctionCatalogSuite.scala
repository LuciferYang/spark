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

package org.apache.spark.sql.connector

import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, NamedExpression, TableFunctionGenerator}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, Project, RepartitionByExpression, Sort}
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier, InMemoryCatalog, TableFunctionCatalog}
import org.apache.spark.sql.connector.catalog.functions.{BoundTableFunction, SupportsScalarInvocation, SupportsTableArgument, TableFunctionEvaluator, TableFunctionEvaluatorFactory, TableFunctionParameter, UnboundTableFunction}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, Expressions, NamedReference, SortDirection, SortOrder => V2SortOrder}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Tests for scalar-argument table-valued functions contributed by a V2
 * [[org.apache.spark.sql.connector.catalog.TableFunctionCatalog]] (PR1).
 */
class TableFunctionCatalogSuite extends SharedSparkSession with BeforeAndAfter {

  before {
    spark.conf.set("spark.sql.catalog.tvf", classOf[InMemoryCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf("spark.sql.catalog.tvf")
  }

  private def catalog: InMemoryCatalog =
    spark.sessionState.catalogManager.catalog("tvf").asInstanceOf[InMemoryCatalog]

  test("scalar-arg TVF resolves and returns rows through the read path") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    checkAnswer(sql("SELECT * FROM tvf.ns.gen(3)"), Row(0L) :: Row(1L) :: Row(2L) :: Nil)
  }

  test("distributed batch-scan TVF executes via BatchScanExec across partitions") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "drange"), UnboundDistributedRange)
    val df = sql("SELECT id, id * 10 AS v FROM tvf.ns.drange(6)")
    checkAnswer(df, (0 until 6).map(i => Row(i.toLong, i.toLong * 10)))
    val scans = df.queryExecution.executedPlan.collect { case b: BatchScanExec => b }
    assert(scans.nonEmpty, "expected a BatchScanExec for a distributed TVF scan")
    assert(scans.head.inputRDD.getNumPartitions == 3,
      s"expected 3 input partitions, got ${scans.head.inputRDD.getNumPartitions}")
  }

  test("self-join of a TVF resolves without duplicate-exprId conflicts") {
    // Exercises TableValuedFunctionRelation.newInstance (MultiInstanceRelation). The join must
    // reuse a SINGLE relation instance (df.join(df)) so both sides share the same output exprIds
    // -- only then does DeduplicateRelations detect the collision and call newInstance() to renew
    // one side. (Two separate `FROM tvf(...)` calls would mint distinct exprIds and never trigger
    // it, giving false coverage.) Mirrors DataSourceV2Suite's "data source v2 self join".
    catalog.createTableFunction(Identifier.of(Array("ns"), "drange"), UnboundDistributedRange)
    val df = sql("SELECT id, v FROM tvf.ns.drange(3)")
    checkAnswer(
      df.join(df, "id"),
      (0 until 3).map(i => Row(i.toLong, i.toLong * 10, i.toLong * 10)))
  }

  test("column pruning is pushed into the TVF scan") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "drange"), UnboundDistributedRange)
    val df = sql("SELECT id FROM tvf.ns.drange(4)")
    checkAnswer(df, (0 until 4).map(i => Row(i.toLong)))
    val scan = df.queryExecution.executedPlan.collect {
      case b: BatchScanExec => b.scan.asInstanceOf[DistributedRangeScan]
    }.head
    assert(scan.prunedSchema.fieldNames.toSeq == Seq("id"),
      s"expected pruned schema [id], got ${scan.prunedSchema.fieldNames.mkString(",")}")
  }

  test("named arguments are rearranged to parameter order") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    // gen has one parameter `n`; exercise the by-name path.
    checkAnswer(sql("SELECT * FROM tvf.ns.gen(n => 2)"), Row(0L) :: Row(1L) :: Nil)
  }

  test("TVF inside a subquery is lowered by OptimizeSubqueries") {
    // EvalTableValuedFunctions uses transformDown, which does not descend into subquery
    // expressions; the node inside the IN-subquery is lowered because OptimizeSubqueries runs the
    // full optimizer (including earlyScanPushDownRules) on each subquery body. This asserts that
    // path works end-to-end -- a TableValuedFunctionRelation surviving unlowered would fail with an
    // internal "No plan" error.
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    sql("CREATE TABLE tvf.ns.t (id BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.t VALUES (0), (1), (5)")
    checkAnswer(
      sql("SELECT id FROM tvf.ns.t WHERE id IN (SELECT id FROM tvf.ns.gen(3))"),
      Row(0L) :: Row(1L) :: Nil)
  }

  test("no such TVF gives a clear error") {
    val e = intercept[AnalysisException](sql("SELECT * FROM tvf.ns.missing(1)").collect())
    assert(e.getCondition == "UNRESOLVABLE_TABLE_VALUED_FUNCTION")
  }

  test("non-foldable scalar arg is rejected with a foldable-args error") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.gen(CAST(rand() * 3 AS INT))").collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_REQUIRES_FOLDABLE_ARGUMENTS")
  }

  test("current_date-derived arg is accepted and evaluates (not rejected as pre-refactor)") {
    // Pre-refactor, a current_date()-derived arg was REJECTED (analysis-time eager eval could not
    // be pinned consistently). After deferring eval to EvalTableValuedFunctions it is accepted and
    // agrees with the query's own current_date(). NOTE: this asserts acceptance + consistency, not
    // the deferral mechanism itself -- eager eval would (outside a midnight boundary) yield the
    // same YEAR; the load-bearing deferral guard is the 'epoch' test below, whose value differs
    // between eager (NULL) and deferred (1970) eval.
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    val df = sql("SELECT * FROM tvf.ns.gen(YEAR(current_date()) - 1999)")
    val expected = sql("SELECT YEAR(current_date()) - 1999").collect().head.getInt(0)
    checkAnswer(df, (0 until expected).map(i => Row(i.toLong)))
  }

  test("CAST of TIME to TIMESTAMP arg is evaluated after ComputeCurrentTime pins it") {
    // A CAST(<time> AS TIMESTAMP) derives its date from CURRENT_DATE; deferring evaluation to the
    // optimizer lets ComputeCurrentTime pin it consistently, so the arg evaluates rather than being
    // rejected. Here the sub-second/hour math is irrelevant; we just assert it resolves to a stable
    // small row count without error.
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    val df = sql("SELECT * FROM tvf.ns.gen(HOUR(CAST(TIME'02:00:00' AS TIMESTAMP)))")
    checkAnswer(df, (0 until 2).map(i => Row(i.toLong)))
  }

  test("CAST of a special-string to TIMESTAMP arg is resolved, not eager-evaluated to NULL") {
    // The load-bearing deferral guard for the TIMESTAMP path (analogous to the DATE 'epoch' test
    // below): a foldable CAST('epoch' AS TIMESTAMP) is resolved by the optimizer's
    // SpecialDatetimeValues. Pre-refactor eager eval (Cast.eval -> stringToTimestamp, no
    // special-value handling) would return NULL -> gen(NULL)=0 rows; deferred eval yields a
    // resolved timestamp -> a positive row count. The expected count is computed by an equivalent
    // reference query (not hardcoded) so it is timezone-independent and cannot rot -- YEAR of the
    // epoch instant depends on the session time zone, but both queries resolve it identically,
    // whereas a broken eager TVF path would still yield 0.
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    val n = sql("SELECT YEAR(CAST('epoch' AS TIMESTAMP)) - 1968").collect().head.getInt(0)
    assert(n > 0, s"reference query should resolve 'epoch' to a positive offset, got $n")
    checkAnswer(
      sql("SELECT * FROM tvf.ns.gen(YEAR(CAST('epoch' AS TIMESTAMP)) - 1968)"),
      (0 until n).map(i => Row(i.toLong)))
  }

  test("CAST of special string to DATE arg is resolved by SpecialDatetimeValues, not NULL") {
    // 'epoch' -> 1970-01-01 is resolved by the optimizer's SpecialDatetimeValues rule. Because arg
    // evaluation is deferred until after that rule, the TVF receives the resolved date (YEAR 1970)
    // instead of the silent NULL the pre-refactor eager eval produced. gen(1970-1970)=gen(0)=no
    // rows; use a positive offset to get a deterministic non-empty count.
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    val df = sql("SELECT * FROM tvf.ns.gen(YEAR(CAST('epoch' AS DATE)) - 1968)")
    checkAnswer(df, (0 until 2).map(i => Row(i.toLong)))  // 1970 - 1968 = 2
  }

  test("TABLE argument is rejected until SupportsTableArgument lands") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    sql("CREATE TABLE tvf.ns.t (id INT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.gen(TABLE(tvf.ns.t))").collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_WITH_TABLE_ARGUMENT_UNSUPPORTED")
  }

  test("by-name TABLE argument is also rejected with the table-argument error") {
    // A TABLE argument passed by name is wrapped in a NamedArgumentExpression; the rejection must
    // see through the wrapper and report the specific unsupported-table-argument error rather than
    // letting the raw table subquery slip into type coercion (which would report a misleading
    // UNEXPECTED_INPUT_TYPE / foldable-args error).
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    sql("CREATE TABLE tvf.ns.t (id INT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.gen(n => TABLE(tvf.ns.t))").collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_WITH_TABLE_ARGUMENT_UNSUPPORTED")
  }

  test("TABLE argument on a missing TVF still reports no such function") {
    // The existence check must precede the TABLE-argument rejection, so a genuine miss is not
    // masked by the unsupported-table-argument error (it surfaces as the no-such-TVF error, not
    // TABLE_VALUED_FUNCTION_WITH_TABLE_ARGUMENT_UNSUPPORTED).
    sql("CREATE TABLE tvf.ns.t (id INT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.missing(TABLE(tvf.ns.t))").collect())
    assert(e.getCondition == "UNRESOLVABLE_TABLE_VALUED_FUNCTION")
  }

  test("bound function with no invocation mixin raises an internal error") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "nomixin"), UnboundNoMixin)
    val e = intercept[SparkException](sql("SELECT * FROM tvf.ns.nomixin(1)").collect())
    assert(e.getMessage.contains("implements no supported invocation interface"))
  }

  test("catalog TVF used in scalar position reports NOT_A_SCALAR_FUNCTION") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    val e = intercept[AnalysisException](sql("SELECT tvf.ns.gen(3)").collect())
    assert(e.getCondition == "NOT_A_SCALAR_FUNCTION")
    assert(e.getMessageParameters.get("functionName") == "`tvf`.`ns`.`gen`")
  }

  test("TVF against a non-TableFunctionCatalog reports MISSING_CATALOG_ABILITY") {
    withSQLConf("spark.sql.catalog.plaincat" -> classOf[BasicInMemoryTableCatalog].getName) {
      val e = intercept[AnalysisException](sql("SELECT * FROM plaincat.ns.foo(1)").collect())
      assert(e.getCondition == "MISSING_CATALOG_ABILITY.TABLE_VALUED_FUNCTIONS")
      assert(e.getMessageParameters.get("plugin") == "plaincat")
    }
  }

  test("scalar TVF parameter default is applied when the argument is omitted") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gendef"), UnboundGenWithDefault)
    // gendef(n INT DEFAULT 2): calling with no argument uses the default.
    checkAnswer(sql("SELECT * FROM tvf.ns.gendef()"), Row(0L) :: Row(1L) :: Nil)
    checkAnswer(sql("SELECT * FROM tvf.ns.gendef(3)"), Row(0L) :: Row(1L) :: Row(2L) :: Nil)
  }

  test("argument whose type differs from the parameter is implicitly coerced") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    // gen(n INT) called with a SMALLINT literal: coerced to INT at analysis time, not a crash.
    checkAnswer(sql("SELECT * FROM tvf.ns.gen(CAST(2 AS SMALLINT))"), Row(0L) :: Row(1L) :: Nil)
  }

  test("argument whose type cannot be coerced reports UNEXPECTED_INPUT_TYPE") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    // gen(n INT) called with an ARRAY that is not implicitly castable to INT.
    val e = intercept[AnalysisException](sql("SELECT * FROM tvf.ns.gen(ARRAY(1, 2))").collect())
    assert(e.getCondition == "UNEXPECTED_INPUT_TYPE")
    assert(e.getMessageParameters.get("functionName") == "`tvf`.`ns`.`gen`")
    assert(e.getMessageParameters.get("requiredType") == "\"INT\"")
  }

  test("wrong argument count for a scalar TVF reports WRONG_NUM_ARGS") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    // gen has exactly one required parameter; too many arguments must fail analysis.
    val e = intercept[AnalysisException](sql("SELECT * FROM tvf.ns.gen(1, 2)").collect())
    assert(e.getCondition.startsWith("WRONG_NUM_ARGS"))
  }

  test("unrecognized named argument for a scalar TVF reports UNRECOGNIZED_PARAMETER_NAME") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    val e = intercept[AnalysisException](sql("SELECT * FROM tvf.ns.gen(bad => 2)").collect())
    assert(e.getCondition == "UNRECOGNIZED_PARAMETER_NAME")
    assert(e.getMessageParameters.get("argumentName") == "`bad`")
  }

  test("bind() throwing UnsupportedOperationException surfaces as a clean analysis error") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "badbind"), UnboundBadBind)
    // The raw UnsupportedOperationException must NOT escape as an internal error; it is wrapped
    // into an AnalysisException carrying the connector's message.
    val e = intercept[AnalysisException](sql("SELECT * FROM tvf.ns.badbind(1)").collect())
    assert(e.getMessage.contains("cannot bind these types"))
  }

  test("CHAR/VARCHAR result columns are replaced with STRING on the relation output") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "genchar"), UnboundGenChar)
    val df = sql("SELECT * FROM tvf.ns.genchar(2)")
    // The engine does not support CHAR/VARCHAR on output; the relation must expose plain STRING
    // (mirrors DataSourceV2Relation.create's char/varchar replacement), not CharType.
    assert(df.schema("c").dataType == DataTypes.StringType)
    checkAnswer(df, Row("r0") :: Row("r1") :: Nil)
  }

  test("argument coercion works with ANSI disabled (non-ANSI TypeCoercion branch)") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      // Exercises the `else TypeCoercion` arm of coerceAndValidateTableFunctionArgs.
      checkAnswer(sql("SELECT * FROM tvf.ns.gen(CAST(2 AS SMALLINT))"), Row(0L) :: Row(1L) :: Nil)
      val e = intercept[AnalysisException](sql("SELECT * FROM tvf.ns.gen(ARRAY(1, 2))").collect())
      assert(e.getCondition == "UNEXPECTED_INPUT_TYPE")
    }
  }

  test("TVF from a catalog that is NOT also a TableCatalog executes end-to-end") {
    // A TableFunctionCatalog is not required to implement TableCatalog (it parallels
    // ProcedureCatalog). The synthetic relation must therefore carry no catalog/identifier, so the
    // table-metadata refresh / cache-by-identity phase (ExtractV2CatalogAndIdentifier ->
    // asTableCatalog) does not fire MISSING_CATALOG_ABILITY.TABLES on it.
    withSQLConf("spark.sql.catalog.tvfonly" -> classOf[TableFunctionOnlyCatalog].getName) {
      val fnOnly = spark.sessionState.catalogManager.catalog("tvfonly")
        .asInstanceOf[TableFunctionOnlyCatalog]
      fnOnly.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
      checkAnswer(
        sql("SELECT * FROM tvfonly.ns.gen(3)"), Row(0L) :: Row(1L) :: Row(2L) :: Nil)
    }
  }

  test("TABLE-arg TVF lowers to Generate(TableFunctionGenerator) with repartition+sort") {
    // Step 3 checkpoint: a SupportsTableArgument function with a call-site PARTITION BY k ORDER BY
    // ts must lower to a Generate over the TABLE argument, and the analyzer's generic
    // TABLE-argument expansion must insert the RepartitionByExpression + Sort (the FTSAE.evaluable
    // rewrite) so the exec node (Step 4) sees hash-partitioned, ordered input.
    catalog.createTableFunction(Identifier.of(Array("ns"), "collectgroups"), UnboundCollectGroups)
    sql("CREATE TABLE tvf.ns.src (k INT, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.src VALUES (1, 11), (2, 20), (1, 10)")
    val analyzed = sql(
      "SELECT * FROM tvf.ns.collectgroups(TABLE(tvf.ns.src) PARTITION BY k ORDER BY ts)")
      .queryExecution.analyzed
    // The Generate sits inside a LateralSubquery (the generic TABLE-argument expansion), so descend
    // into subquery expressions to find it.
    val gens = analyzed.collectWithSubqueries { case g: Generate => g.generator }.collect {
      case t: TableFunctionGenerator => t
    }
    assert(gens.nonEmpty, s"expected a TableFunctionGenerator, plan:\n$analyzed")
    // The PARTITION BY column ordinal(s) within the struct input column were propagated to the
    // generator (one per partition key). The exact ordinal (which points at the projected
    // partitioning column within the struct) is validated end-to-end by the Step 4 grouping test.
    assert(gens.head.partitionColumnIndexes.length == 1,
      s"expected one partition-key ordinal, got ${gens.head.partitionColumnIndexes}")
    // The TABLE-argument expansion inserted the repartition + sort.
    assert(analyzed.collectWithSubqueries {
      case r: RepartitionByExpression => r }.nonEmpty,
      s"expected a RepartitionByExpression, plan:\n$analyzed")
    assert(analyzed.collectWithSubqueries { case s: Sort => s }.nonEmpty,
      s"expected a Sort, plan:\n$analyzed")
  }

  test("TABLE-arg TVF whose bound form lacks SupportsTableArgument is rejected") {
    // A connector that accepts a TABLE argument at bind() but returns a bound function without the
    // SupportsTableArgument mixin cannot be executed; reject with the specific error.
    catalog.createTableFunction(Identifier.of(Array("ns"), "gen"), UnboundGen)
    sql("CREATE TABLE tvf.ns.t2 (id INT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.gen(TABLE(tvf.ns.t2))").collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_WITH_TABLE_ARGUMENT_UNSUPPORTED")
  }

  test("TABLE-arg TVF executes: PARTITION BY k ORDER BY ts (each group once, in order)") {
    // Step 4 end-to-end checkpoint. collectgroups emits one row per PARTITION BY group with the
    // group's ts values comma-joined in ORDER BY order. Correct output proves: (a) rows are
    // hash-partitioned so each k lands in one group, (b) TableFunctionExec segments adjacent rows
    // on the partition-key ordinal, (c) intra-group ORDER BY ts is honored, (d) the connector's
    // evaluator sees only the TABLE argument's columns (k, ts), not the internal partition_by
    // marker column.
    catalog.createTableFunction(Identifier.of(Array("ns"), "collectgroups"), UnboundCollectGroups)
    sql("CREATE TABLE tvf.ns.events (k INT, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.events VALUES (1, 11), (2, 20), (1, 10), (3, 30), (2, 21), (1, 12)")
    checkAnswer(
      sql("SELECT k, tss FROM tvf.ns.collectgroups(" +
        "TABLE(tvf.ns.events) PARTITION BY k ORDER BY ts)"),
      Row(1, "10,11,12") :: Row(2, "20,21") :: Row(3, "30") :: Nil)
  }

  test("TABLE-arg TVF executes: WITH SINGLE PARTITION (whole input as one group, ordered)") {
    catalog.createTableFunction(Identifier.of(Array("ns"), "collectall"), UnboundCollectAll)
    sql("CREATE TABLE tvf.ns.events2 (k INT, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.events2 VALUES (1, 30), (2, 10), (1, 20)")
    // No PARTITION BY, WITH SINGLE PARTITION + ORDER BY ts: one group, all rows, ts-ordered.
    checkAnswer(
      sql("SELECT tss FROM tvf.ns.collectall(" +
        "TABLE(tvf.ns.events2) WITH SINGLE PARTITION ORDER BY ts)"),
      Row("10,20,30") :: Nil)
  }

  test("TABLE-arg TVF executes: no PARTITION BY (row count conserved)") {
    // Without PARTITION BY, groups are per Spark-partition; a count-conserving function proves the
    // no-partition-key path (whole task-partition is one group) works and rows are not dropped.
    catalog.createTableFunction(Identifier.of(Array("ns"), "countrows"), UnboundCountRows)
    sql("CREATE TABLE tvf.ns.events3 (k INT, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.events3 VALUES (1, 1), (2, 2), (3, 3)")
    val total = sql("SELECT cnt FROM tvf.ns.countrows(TABLE(tvf.ns.events3))")
      .collect().map(_.getLong(0)).sum
    assert(total == 3, s"expected 3 rows conserved across per-partition groups, got $total")
  }

  test("TABLE-arg TVF executes: function-declared requiredDistribution + requiredOrdering") {
    // The function declares its OWN partitioning (clustered by k) and ordering (by ts) via
    // requiredDistribution/requiredOrdering, with NO call-site PARTITION BY / ORDER BY. This drives
    // applyRequiredMetadata's ClusteredDistribution -> PARTITION BY and requiredOrdering -> ORDER
    // BY threading plus the V2ExpressionUtils.toCatalyst(Ordering) conversions, and must produce
    // the same per-group ordered result as the call-site-clause path.
    catalog.createTableFunction(
      Identifier.of(Array("ns"), "groupbyrequired"), UnboundGroupByRequired)
    sql("CREATE TABLE tvf.ns.events4 (k INT, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.events4 VALUES (1, 11), (2, 20), (1, 10), (3, 30), (2, 21), (1, 12)")
    checkAnswer(
      sql("SELECT k, tss FROM tvf.ns.groupbyrequired(TABLE(tvf.ns.events4))"),
      Row(1, "10,11,12") :: Row(2, "20,21") :: Row(3, "30") :: Nil)
  }

  test("TABLE-arg TVF rejects a call-site PARTITION BY that conflicts with required partitioning") {
    // groupbyrequired declares its own required partitioning; a call site that ALSO specifies
    // PARTITION BY is a conflict and must fail analysis with the required-metadata error (mirrors
    // the Python UDTF applyToTableArgument conflict validation).
    catalog.createTableFunction(
      Identifier.of(Array("ns"), "groupbyrequired"), UnboundGroupByRequired)
    sql("CREATE TABLE tvf.ns.events5 (k INT, ts BIGINT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT k, tss FROM tvf.ns.groupbyrequired(TABLE(tvf.ns.events5) PARTITION BY k)")
        .collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_REQUIRED_METADATA_INCOMPATIBLE_WITH_CALL")
  }

  test("TABLE-arg TVF over an empty input produces no rows") {
    // Empty input: each (possibly empty) partition yields an empty group, and the evaluator returns
    // an empty iterator, so the query returns no rows rather than erroring.
    catalog.createTableFunction(Identifier.of(Array("ns"), "collectgroups"), UnboundCollectGroups)
    sql("CREATE TABLE tvf.ns.events6 (k INT, ts BIGINT) USING foo")
    checkAnswer(
      sql("SELECT k, tss FROM tvf.ns.collectgroups(" +
        "TABLE(tvf.ns.events6) PARTITION BY k ORDER BY ts)"),
      Nil)
  }

  test("TABLE-arg TVF PARTITION BY a BINARY key groups by value, not array identity") {
    // A BINARY partition key returns a fresh byte[] per row (UnsafeRow.getBinary), whose Scala `==`
    // is reference equality. Grouping must compare keys by VALUE (byte-wise), so two rows with the
    // same binary key land in one group -- not one singleton group per row.
    catalog.createTableFunction(Identifier.of(Array("ns"), "collectbin"), UnboundCollectBin)
    sql("CREATE TABLE tvf.ns.binevents (k BINARY, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.binevents VALUES " +
      "(X'01', 11), (X'02', 30), (X'01', 10), (X'02', 21), (X'01', 12)")
    // Expect two groups (k=X'01' with 3 rows, k=X'02' with 2 rows), each collapsed to one output
    // row -- NOT five singleton groups.
    checkAnswer(
      sql("SELECT cnt FROM tvf.ns.collectbin(TABLE(tvf.ns.binevents) PARTITION BY k ORDER BY ts)"),
      Row(3L) :: Row(2L) :: Nil)
  }

  test("TABLE-arg TVF result CHAR/VARCHAR columns are replaced with STRING on generator output") {
    // The engine does not support CHAR/VARCHAR on plan output; the TABLE-arg path must strip them
    // to STRING like the scalar path (DataSourceV2Relation.create / TableValuedFunctionRelation).
    catalog.createTableFunction(Identifier.of(Array("ns"), "charout"), UnboundCharOut)
    sql("CREATE TABLE tvf.ns.charsrc (k INT, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.charsrc VALUES (1, 1), (1, 2)")
    val df = sql("SELECT * FROM tvf.ns.charout(TABLE(tvf.ns.charsrc) PARTITION BY k)")
    assert(df.schema("label").dataType == DataTypes.StringType,
      s"expected STRING, got ${df.schema("label").dataType}")
    checkAnswer(df, Row("g") :: Nil)
  }

  test("TABLE-arg TVF that declares a non-deterministic transform is marked non-deterministic") {
    // The generator must propagate bound.isDeterministic()=false so the optimizer does not reorder
    // or de-duplicate the transform. Assert the analyzed generator carries deterministic = false.
    catalog.createTableFunction(Identifier.of(Array("ns"), "nondet"), UnboundNonDet)
    sql("CREATE TABLE tvf.ns.ndsrc (k INT, ts BIGINT) USING foo")
    val analyzed = sql("SELECT * FROM tvf.ns.nondet(TABLE(tvf.ns.ndsrc) PARTITION BY k)")
      .queryExecution.analyzed
    val gens = analyzed.collectWithSubqueries { case g: Generate => g.generator }.collect {
      case t: TableFunctionGenerator => t
    }
    assert(gens.nonEmpty, s"expected a TableFunctionGenerator, plan:\n$analyzed")
    assert(!gens.head.deterministic,
      s"expected the generator to be non-deterministic, got deterministic=true")
  }

  test("TABLE-arg TVF requiredOrdering without a required distribution is rejected") {
    // A function that declares requiredOrdering but leaves requiredDistribution unspecified cannot
    // have its ordering enforced (evaluable only sorts under a partition/single-partition), so it
    // must fail analysis rather than silently drop the ordering.
    catalog.createTableFunction(Identifier.of(Array("ns"), "orderonly"), UnboundOrderOnly)
    sql("CREATE TABLE tvf.ns.oosrc (k INT, ts BIGINT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.orderonly(TABLE(tvf.ns.oosrc))").collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_REQUIRED_METADATA_INVALID")
  }

  test("TABLE-arg TVF with an unsupported required distribution is rejected") {
    // A connector may declare only a clustered or unspecified distribution. Any other Distribution
    // (e.g. Distributions.ordered) cannot be honored by the TABLE-argument path and must fail
    // analysis loudly rather than being silently dropped and run on wrongly distributed data.
    catalog.createTableFunction(Identifier.of(Array("ns"), "ordereddist"), UnboundOrderedDist)
    sql("CREATE TABLE tvf.ns.odsrc (k INT, ts BIGINT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.ordereddist(TABLE(tvf.ns.odsrc))").collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_REQUIRED_METADATA_INVALID")
  }

  test("TABLE-arg TVF requiredOrdering is honored when the call site supplies PARTITION BY") {
    // orderonly declares requiredOrdering (ts) with an unspecified distribution. On its own that is
    // rejected, but a call-site PARTITION BY satisfies the "ordering needs a partition"
    // requirement, so the merged plan must sort each group by ts (not over-reject).
    catalog.createTableFunction(Identifier.of(Array("ns"), "collectorder"), UnboundCollectOrder)
    sql("CREATE TABLE tvf.ns.cosrc (k INT, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.cosrc VALUES (1, 11), (1, 10), (2, 21), (1, 12), (2, 20)")
    checkAnswer(
      sql("SELECT k, tss FROM tvf.ns.collectorder(TABLE(tvf.ns.cosrc) PARTITION BY k)"),
      Row(1, "10,11,12") :: Row(2, "20,21") :: Nil)
  }

  test("TABLE-arg TVF mixing a scalar argument with the TABLE argument is rejected") {
    // A table-argument function consumes only its TABLE argument; its evaluator has no channel for
    // a scalar argument. A call that mixes a scalar with the TABLE must fail analysis (rather than
    // silently drop the scalar or crash treating a leading scalar as the input struct).
    catalog.createTableFunction(
      Identifier.of(Array("ns"), "scalarplustable"), UnboundScalarPlusTable)
    sql("CREATE TABLE tvf.ns.sptsrc (k INT, ts BIGINT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.scalarplustable(5, TABLE(tvf.ns.sptsrc))").collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_TABLE_ARGUMENT_WITH_SCALAR_UNSUPPORTED")
  }

  test("TABLE-arg TVF select pushdown inserts a Project of exactly the declared columns") {
    // selecttwo declares selectedInputColumns() = [b, a] over a (a, b, c) input. The analyzed
    // TABLE-argument subtree must contain a Project computing exactly [b, a] (subset + reorder)
    // feeding the struct input column, so the connector's evaluator sees only those two columns.
    catalog.createTableFunction(Identifier.of(Array("ns"), "selecttwo"), UnboundSelectTwo)
    sql("CREATE TABLE tvf.ns.stsrc (a INT, b BIGINT, c STRING) USING foo")
    val analyzed =
      sql("SELECT * FROM tvf.ns.selecttwo(TABLE(tvf.ns.stsrc))").queryExecution.analyzed
    // The struct input column `c` = CreateStruct over the selected columns, so the inserted
    // Project's output names are exactly the selected columns in the declared order.
    val structProjects = analyzed.collectWithSubqueries {
      case p: Project => p
    }.filter(_.projectList.exists {
      case Alias(_: CreateNamedStruct, "c") => true
      case _ => false
    })
    assert(structProjects.nonEmpty, s"expected the struct-input Project, plan:\n$analyzed")
    val struct = structProjects.head.projectList.collectFirst {
      case Alias(s: CreateNamedStruct, "c") => s
    }.get
    // CreateNamedStruct children alternate name-literal, value; the value names must be [b, a].
    val selectedNames = struct.children.grouped(2).map {
      case Seq(_, ref: NamedExpression) => ref.name
      case other => fail(s"unexpected struct child shape: $other")
    }.toSeq
    assert(selectedNames == Seq("b", "a"),
      s"expected struct over [b, a], got $selectedNames, plan:\n$analyzed")
  }

  test("TABLE-arg TVF select pushdown: evaluator receives only the selected columns, in order") {
    // echocols echoes the field count and the concatenated string form of each received row, so a
    // correct pushdown makes it see exactly [b, a] (two columns) rather than all three.
    catalog.createTableFunction(Identifier.of(Array("ns"), "echocols"), UnboundEchoCols)
    sql("CREATE TABLE tvf.ns.ecsrc (a INT, b BIGINT, c STRING) USING foo")
    sql("INSERT INTO tvf.ns.ecsrc VALUES (1, 10, 'x'), (2, 20, 'y')")
    checkAnswer(
      sql("SELECT ncols, rowstr FROM tvf.ns.echocols(TABLE(tvf.ns.ecsrc))"),
      Row(2, "10|1") :: Row(2, "20|2") :: Nil)
  }

  test("TABLE-arg TVF select pushdown combines with a call-site PARTITION BY") {
    // selectpart selects [ts, k] and the call site PARTITION BY k. The partition-key ordinal must
    // track the selected layout (selected ++ partition_by marker), so each k lands in one group
    // and the group's ts values are collected -- proving the ordinals survived the select Project.
    catalog.createTableFunction(Identifier.of(Array("ns"), "selectpart"), UnboundSelectPart)
    sql("CREATE TABLE tvf.ns.spsrc (k INT, ts BIGINT, junk STRING) USING foo")
    sql("INSERT INTO tvf.ns.spsrc VALUES " +
      "(1, 11, 'a'), (2, 20, 'b'), (1, 10, 'c'), (2, 21, 'd'), (1, 12, 'e')")
    checkAnswer(
      sql("SELECT k, tss FROM tvf.ns.selectpart(TABLE(tvf.ns.spsrc) PARTITION BY k ORDER BY ts)"),
      Row(1, "10,11,12") :: Row(2, "20,21") :: Nil)
  }

  test("TABLE-arg TVF selecting a non-existent input column is rejected") {
    // selectmissing declares selectedInputColumns() = [nope]; the TABLE argument has no such
    // column, so analysis must fail with a clean error rather than an internal resolution failure.
    catalog.createTableFunction(Identifier.of(Array("ns"), "selectmissing"), UnboundSelectMissing)
    sql("CREATE TABLE tvf.ns.smsrc (a INT, b BIGINT) USING foo")
    val e = intercept[AnalysisException](
      sql("SELECT * FROM tvf.ns.selectmissing(TABLE(tvf.ns.smsrc))").collect())
    assert(e.getCondition == "TABLE_VALUED_FUNCTION_SELECTED_COLUMN_NOT_FOUND")
  }

  test("TABLE-arg TVF with no selectedInputColumns passes every input column (default)") {
    // echocols' sibling with an empty selection must still see all three columns -- the default
    // (empty selectedInputColumns) is a no-op pushdown, preserving pre-PR3 behavior.
    catalog.createTableFunction(Identifier.of(Array("ns"), "echoall"), UnboundEchoAll)
    sql("CREATE TABLE tvf.ns.easrc (a INT, b BIGINT, c STRING) USING foo")
    sql("INSERT INTO tvf.ns.easrc VALUES (1, 10, 'x')")
    checkAnswer(
      sql("SELECT ncols, rowstr FROM tvf.ns.echoall(TABLE(tvf.ns.easrc))"),
      Row(3, "1|10|x") :: Nil)
  }

  test("TABLE-arg TVF select pushdown matches column names case-insensitively") {
    // selectcase declares selectedInputColumns() = [B, A] but the table columns are lower-case
    // (a, b, c). Under the default case-insensitive analysis these must resolve to [b, a], the same
    // as echocols -- mirroring how PARTITION BY / ORDER BY references resolve in this path.
    catalog.createTableFunction(Identifier.of(Array("ns"), "selectcase"), UnboundSelectCase)
    sql("CREATE TABLE tvf.ns.scsrc (a INT, b BIGINT, c STRING) USING foo")
    sql("INSERT INTO tvf.ns.scsrc VALUES (1, 10, 'x'), (2, 20, 'y')")
    checkAnswer(
      sql("SELECT ncols, rowstr FROM tvf.ns.selectcase(TABLE(tvf.ns.scsrc))"),
      Row(2, "10|1") :: Row(2, "20|2") :: Nil)
  }

  test("TABLE-arg TVF select excluding the partition keys slices markers and groups by all keys") {
    // selectmulti selects ONLY [ts], excluding the two PARTITION BY keys k1, k2. This pins two
    // things the selectpart test cannot: (a) the appended partition_by markers are sliced off
    // before the evaluator (ncols must be 1, not 3); (b) the partition ordinals track the selected
    // layout across MULTIPLE keys (offset [1, 2], not [0, 1]) -- a missing offset would segment on
    // ts and split every row into its own group.
    catalog.createTableFunction(Identifier.of(Array("ns"), "selectmulti"), UnboundSelectMulti)
    sql("CREATE TABLE tvf.ns.smtsrc (k1 INT, k2 INT, ts BIGINT) USING foo")
    sql("INSERT INTO tvf.ns.smtsrc VALUES (1, 1, 10), (1, 1, 11), (1, 2, 20), (2, 1, 30)")
    checkAnswer(
      sql("SELECT ncols, tss FROM tvf.ns.selectmulti(" +
        "TABLE(tvf.ns.smtsrc) PARTITION BY (k1, k2) ORDER BY ts)"),
      Row(1, "10,11") :: Row(1, "20") :: Row(1, "30") :: Nil)
  }
}

/** gen(n) -> rows (0..n-1) with a single LONG column `id`, via a LocalScan. */
object UnboundGen extends UnboundTableFunction {
  override def name(): String = "gen"
  override def bind(inputType: StructType): BoundTableFunction = BoundGen
}

object BoundGen extends BoundTableFunction with SupportsScalarInvocation {
  override def name(): String = "gen"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("n", DataTypes.IntegerType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("id", "long")
  override def newScanBuilder(input: InternalRow): ScanBuilder = new ScanBuilder {
    override def build(): Scan = new LocalScan {
      override def readSchema(): StructType = resultSchema()
      override def rows(): Array[InternalRow] =
        Array.tabulate(input.getInt(0))(i => InternalRow(i.toLong))
    }
  }
}

/** gendef(n INT DEFAULT 2) -> rows (0..n-1); exercises the scalar-parameter default path. */
object UnboundGenWithDefault extends UnboundTableFunction {
  override def name(): String = "gendef"
  override def bind(inputType: StructType): BoundTableFunction = BoundGenWithDefault
}

object BoundGenWithDefault extends BoundTableFunction with SupportsScalarInvocation {
  override def name(): String = "gendef"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("n", DataTypes.IntegerType).defaultValue("2").build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("id", "long")
  override def newScanBuilder(input: InternalRow): ScanBuilder = new ScanBuilder {
    override def build(): Scan = new LocalScan {
      override def readSchema(): StructType = resultSchema()
      override def rows(): Array[InternalRow] =
        Array.tabulate(input.getInt(0))(i => InternalRow(i.toLong))
    }
  }
}

/** A bound function that implements neither invocation mixin -- a connector bug fixture. */
object UnboundNoMixin extends UnboundTableFunction {
  override def name(): String = "nomixin"
  override def bind(inputType: StructType): BoundTableFunction = BoundNoMixin
}

object BoundNoMixin extends BoundTableFunction {
  override def name(): String = "nomixin"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("n", DataTypes.IntegerType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("id", "long")
}

/** A function whose bind() rejects its input by throwing, to exercise the clean-error wrap. */
object UnboundBadBind extends UnboundTableFunction {
  override def name(): String = "badbind"
  override def bind(inputType: StructType): BoundTableFunction =
    throw new UnsupportedOperationException("cannot bind these types")
}

/** genchar(n) -> rows ("r0".."r(n-1)") with a single CHAR(2) column `c`; exercises char strip. */
object UnboundGenChar extends UnboundTableFunction {
  override def name(): String = "genchar"
  override def bind(inputType: StructType): BoundTableFunction = BoundGenChar
}

object BoundGenChar extends BoundTableFunction with SupportsScalarInvocation {
  override def name(): String = "genchar"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("n", DataTypes.IntegerType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("c", "char(2)")
  override def newScanBuilder(input: InternalRow): ScanBuilder = new ScanBuilder {
    override def build(): Scan = new LocalScan {
      // The scan reads the annotated string form, matching the relation's stripped output schema.
      override def readSchema(): StructType = new StructType().add("c", "string")
      override def rows(): Array[InternalRow] =
        Array.tabulate(input.getInt(0))(i =>
          InternalRow(org.apache.spark.unsafe.types.UTF8String.fromString(s"r$i")))
    }
  }
}

/** drange(n) -> rows (0..n-1) with columns (id LONG, v LONG=id*10), distributed + prunable. */
object UnboundDistributedRange extends UnboundTableFunction {
  override def name(): String = "drange"
  override def bind(inputType: StructType): BoundTableFunction = BoundDistributedRange
}

object BoundDistributedRange extends BoundTableFunction with SupportsScalarInvocation {
  override def name(): String = "drange"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("n", DataTypes.IntegerType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("id", "long").add("v", "long")
  override def newScanBuilder(input: InternalRow): ScanBuilder =
    new DistributedRangeScanBuilder(input.getInt(0), resultSchema())
}

class DistributedRangeScanBuilder(n: Int, fullSchema: StructType)
  extends ScanBuilder with SupportsPushDownRequiredColumns {
  private var schema: StructType = fullSchema
  override def pruneColumns(requiredSchema: StructType): Unit = { schema = requiredSchema }
  override def build(): Scan = DistributedRangeScan(n, schema)
}

case class DistributedRangeScan(n: Int, prunedSchema: StructType) extends Scan with Batch {
  override def readSchema(): StructType = prunedSchema
  override def toBatch: Batch = this
  override def planInputPartitions(): Array[InputPartition] =
    (0 until n).grouped(2).map(g => RangePartition(g.head, g.last + 1)).toArray
  override def createReaderFactory(): PartitionReaderFactory =
    RangeReaderFactory(prunedSchema.fieldNames.toSeq)
}

case class RangePartition(start: Int, end: Int) extends InputPartition

case class RangeReaderFactory(cols: Seq[String]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[RangePartition]
    new PartitionReader[InternalRow] {
      private var cur = p.start - 1
      override def next(): Boolean = { cur += 1; cur < p.end }
      override def get(): InternalRow = {
        val values = cols.map {
          case "id" => cur.toLong
          case "v" => cur.toLong * 10
        }
        InternalRow(values: _*)
      }
      override def close(): Unit = ()
    }
  }
}

/**
 * A catalog that implements ONLY [[TableFunctionCatalog]] -- not [[org.apache.spark.sql.connector
 * .catalog.TableCatalog]] -- to exercise the standalone-marker shape (paralleling
 * [[org.apache.spark.sql.connector.catalog.procedures.ProcedureCatalog]]). A scalar-arg TVF from
 * such a catalog must execute without the table-refresh phase demanding a `TableCatalog`.
 */
class TableFunctionOnlyCatalog extends TableFunctionCatalog {
  private var catalogName: String = _
  private val tableFunctions =
    new java.util.concurrent.ConcurrentHashMap[Identifier, UnboundTableFunction]()

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName

  override def listTableFunctions(namespace: Array[String]): Array[Identifier] =
    tableFunctions.keySet.asScala.filter(_.namespace.sameElements(namespace)).toArray

  override def loadTableFunction(ident: Identifier): UnboundTableFunction =
    Option(tableFunctions.get(ident)).getOrElse(throw new NoSuchFunctionException(ident))

  def createTableFunction(ident: Identifier, fn: UnboundTableFunction): Unit = {
    tableFunctions.put(ident, fn)
  }
}

/**
 * collectgroups(TABLE t PARTITION BY k ORDER BY ts) -> one row per group: (k INT, tss STRING),
 * where `tss` is the comma-joined `ts` values in received order. A `SupportsTableArgument` fixture
 * used to exercise TABLE-argument lowering (Step 3) and per-group ordered execution (Step 4). It
 * declares no required distribution/ordering of its own, so the call-site PARTITION BY / ORDER BY
 * governs.
 */
object UnboundCollectGroups extends UnboundTableFunction {
  override def name(): String = "collectgroups"
  override def bind(inputType: StructType): BoundTableFunction = BoundCollectGroups
}

object BoundCollectGroups extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "collectgroups"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("k", "int").add("tss", "string")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] = Array.empty
  override def evaluatorFactory(): TableFunctionEvaluatorFactory =
    CollectGroupsEvaluatorFactory
}

object CollectGroupsEvaluatorFactory extends TableFunctionEvaluatorFactory {
  override def create(): TableFunctionEvaluator = new TableFunctionEvaluator {
    override def eval(
        partition: java.util.Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
      val rows = partition.asScala.toSeq
      if (rows.isEmpty) {
        java.util.Collections.emptyIterator()
      } else {
        val k = rows.head.getInt(0)
        val tss = rows.map(_.getLong(1)).mkString(",")
        java.util.Collections.singletonList(
          InternalRow(k, org.apache.spark.unsafe.types.UTF8String.fromString(tss))
            .asInstanceOf[InternalRow]).iterator()
      }
    }
  }
}

/**
 * collectall(TABLE t WITH SINGLE PARTITION ORDER BY ts) -> one row: (tss STRING), the comma-joined
 * ts values in order across the whole (single-partition) input. A `SupportsTableArgument` fixture
 * for the WITH SINGLE PARTITION path.
 */
object UnboundCollectAll extends UnboundTableFunction {
  override def name(): String = "collectall"
  override def bind(inputType: StructType): BoundTableFunction = BoundCollectAll
}

object BoundCollectAll extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "collectall"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("tss", "string")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] = Array.empty
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CollectAllEvaluatorFactory
}

object CollectAllEvaluatorFactory extends TableFunctionEvaluatorFactory {
  override def create(): TableFunctionEvaluator = new TableFunctionEvaluator {
    override def eval(
        partition: java.util.Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
      val rows = partition.asScala.toSeq
      if (rows.isEmpty) {
        java.util.Collections.emptyIterator()
      } else {
        val tss = rows.map(_.getLong(1)).mkString(",")
        java.util.Collections.singletonList(
          InternalRow(org.apache.spark.unsafe.types.UTF8String.fromString(tss))
            .asInstanceOf[InternalRow]).iterator()
      }
    }
  }
}

/**
 * countrows(TABLE t) -> per input group, one row (cnt LONG) = the group's row count. With no
 * PARTITION BY, each Spark task-partition is one group, so summing the outputs equals the total
 * input row count. A `SupportsTableArgument` fixture for the no-PARTITION-BY path.
 */
object UnboundCountRows extends UnboundTableFunction {
  override def name(): String = "countrows"
  override def bind(inputType: StructType): BoundTableFunction = BoundCountRows
}

object BoundCountRows extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "countrows"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("cnt", "long")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] = Array.empty
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CountRowsEvaluatorFactory
}

object CountRowsEvaluatorFactory extends TableFunctionEvaluatorFactory {
  override def create(): TableFunctionEvaluator = new TableFunctionEvaluator {
    override def eval(
        partition: java.util.Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
      val n = partition.asScala.size.toLong
      if (n == 0L) {
        java.util.Collections.emptyIterator()
      } else {
        java.util.Collections.singletonList(
          InternalRow(n).asInstanceOf[InternalRow]).iterator()
      }
    }
  }
}

/**
 * groupbyrequired(TABLE t) -> one row per group: (k INT, tss STRING). Unlike collectgroups, this
 * fixture declares its OWN required distribution (clustered by `k`) and ordering (by `ts` ASC)
 * rather than relying on a call-site PARTITION BY / ORDER BY. It exercises applyRequiredMetadata's
 * ClusteredDistribution -> PARTITION BY and requiredOrdering -> ORDER BY threading (and the
 * V2ExpressionUtils conversions), plus the call-site-vs-required conflict validation.
 */
object UnboundGroupByRequired extends UnboundTableFunction {
  override def name(): String = "groupbyrequired"
  override def bind(inputType: StructType): BoundTableFunction = BoundGroupByRequired
}

object BoundGroupByRequired extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "groupbyrequired"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("k", "int").add("tss", "string")
  override def requiredDistribution(): Distribution =
    Distributions.clustered(Array[V2Expression](Expressions.column("k")))
  override def requiredOrdering(): Array[V2SortOrder] =
    Array(Expressions.sort(Expressions.column("ts"), SortDirection.ASCENDING))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CollectGroupsEvaluatorFactory
}

/**
 * collectbin(TABLE t PARTITION BY k) -> one row per group: (cnt LONG) = the group's row count.
 * The partition key `k` is BINARY, exercising value-based (byte-wise) group segmentation.
 */
object UnboundCollectBin extends UnboundTableFunction {
  override def name(): String = "collectbin"
  override def bind(inputType: StructType): BoundTableFunction = BoundCollectBin
}

object BoundCollectBin extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "collectbin"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("cnt", "long")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] = Array.empty
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CountRowsEvaluatorFactory
}

/**
 * charout(TABLE t PARTITION BY k) -> one row per group: (label CHAR(1)) = the constant "g". Its
 * result schema declares a CHAR column so the test can assert it is replaced with STRING on output.
 */
object UnboundCharOut extends UnboundTableFunction {
  override def name(): String = "charout"
  override def bind(inputType: StructType): BoundTableFunction = BoundCharOut
}

object BoundCharOut extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "charout"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("label", "char(1)")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] = Array.empty
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CharOutEvaluatorFactory
}

object CharOutEvaluatorFactory extends TableFunctionEvaluatorFactory {
  override def create(): TableFunctionEvaluator = new TableFunctionEvaluator {
    override def eval(
        partition: java.util.Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
      if (!partition.hasNext) {
        java.util.Collections.emptyIterator()
      } else {
        partition.asScala.size // drain
        java.util.Collections.singletonList(
          InternalRow(org.apache.spark.unsafe.types.UTF8String.fromString("g"))
            .asInstanceOf[InternalRow]).iterator()
      }
    }
  }
}

/**
 * nondet(TABLE t PARTITION BY k) -> a non-deterministic transform (isDeterministic()=false). Used
 * to assert the generator propagates determinism; the evaluator body is irrelevant to the test.
 */
object UnboundNonDet extends UnboundTableFunction {
  override def name(): String = "nondet"
  override def bind(inputType: StructType): BoundTableFunction = BoundNonDet
}

object BoundNonDet extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "nondet"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = false
  override def resultSchema(): StructType = new StructType().add("cnt", "long")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] = Array.empty
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CountRowsEvaluatorFactory
}

/**
 * orderonly declares a requiredOrdering but leaves requiredDistribution unspecified -- an invalid
 * combination Spark rejects at analysis (the ordering could never be enforced).
 */
object UnboundOrderOnly extends UnboundTableFunction {
  override def name(): String = "orderonly"
  override def bind(inputType: StructType): BoundTableFunction = BoundOrderOnly
}

object BoundOrderOnly extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "orderonly"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("cnt", "long")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] =
    Array(Expressions.sort(Expressions.column("ts"), SortDirection.ASCENDING))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CountRowsEvaluatorFactory
}

/**
 * ordereddist declares an ORDERED distribution, which the TABLE-argument path does not support
 * (only clustered / unspecified). Spark must reject it at analysis rather than silently drop it.
 */
object UnboundOrderedDist extends UnboundTableFunction {
  override def name(): String = "ordereddist"
  override def bind(inputType: StructType): BoundTableFunction = BoundOrderedDist
}

object BoundOrderedDist extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "ordereddist"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("cnt", "long")
  override def requiredDistribution(): Distribution =
    Distributions.ordered(Array[V2SortOrder](
      Expressions.sort(Expressions.column("k"), SortDirection.ASCENDING)))
  override def requiredOrdering(): Array[V2SortOrder] = Array.empty
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CountRowsEvaluatorFactory
}

/**
 * collectorder declares requiredOrdering (ts ASC) with an UNSPECIFIED distribution, and emits
 * (k, tss) per group. On its own the ordering-without-partition combination is rejected, but a
 * call-site PARTITION BY satisfies it, so the merged plan sorts each group by ts.
 */
object UnboundCollectOrder extends UnboundTableFunction {
  override def name(): String = "collectorder"
  override def bind(inputType: StructType): BoundTableFunction = BoundCollectOrder
}

object BoundCollectOrder extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "collectorder"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("k", "int").add("tss", "string")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] =
    Array(Expressions.sort(Expressions.column("ts"), SortDirection.ASCENDING))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CollectGroupsEvaluatorFactory
}

/**
 * scalarplustable declares two parameters (a scalar `n` before the TABLE `input`), so a call like
 * `scalarplustable(5, TABLE(t))` produces a leading scalar argument alongside the TABLE argument.
 * A table-argument function cannot consume a scalar, so this must be rejected at analysis.
 */
object UnboundScalarPlusTable extends UnboundTableFunction {
  override def name(): String = "scalarplustable"
  override def bind(inputType: StructType): BoundTableFunction = BoundScalarPlusTable
}

object BoundScalarPlusTable extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "scalarplustable"
  override def parameters(): Array[TableFunctionParameter] =
    Array(
      TableFunctionParameter.scalar("n", DataTypes.IntegerType).build(),
      TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("cnt", "long")
  override def requiredDistribution(): Distribution = Distributions.unspecified()
  override def requiredOrdering(): Array[V2SortOrder] = Array.empty
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CountRowsEvaluatorFactory
}

/**
 * selecttwo(TABLE t) over input (a, b, c) declares selectedInputColumns() = [b, a] -- a subset and
 * reorder. Used only for the plan-level assertion that the inserted struct-input Project selects
 * exactly [b, a], so its evaluator is irrelevant.
 */
object UnboundSelectTwo extends UnboundTableFunction {
  override def name(): String = "selecttwo"
  override def bind(inputType: StructType): BoundTableFunction = BoundSelectTwo
}

object BoundSelectTwo extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "selecttwo"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("cnt", "long")
  override def selectedInputColumns(): Array[NamedReference] =
    Array(Expressions.column("b"), Expressions.column("a"))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CountRowsEvaluatorFactory
}

/**
 * echocols(TABLE t) declares selectedInputColumns() = [b, a] over input (a INT, b BIGINT, c STRING)
 * and emits one row per input row: (ncols INT, rowstr STRING), where ncols is the number of fields
 * its evaluator received and rowstr is the pipe-joined field values. It proves the evaluator sees
 * exactly the selected columns (2), in order [b, a] (BIGINT then INT).
 */
object UnboundEchoCols extends UnboundTableFunction {
  override def name(): String = "echocols"
  override def bind(inputType: StructType): BoundTableFunction = BoundEchoCols
}

object BoundEchoCols extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "echocols"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("ncols", "int").add("rowstr", "string")
  override def selectedInputColumns(): Array[NamedReference] =
    Array(Expressions.column("b"), Expressions.column("a"))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = EchoColsEvaluatorFactory
}

/**
 * Emits (ncols, rowstr) per input row for the selected [b BIGINT, a INT] layout: ncols = number of
 * fields received (must be 2), rowstr = "<b>|<a>".
 */
object EchoColsEvaluatorFactory extends TableFunctionEvaluatorFactory {
  override def create(): TableFunctionEvaluator = new TableFunctionEvaluator {
    override def eval(
        partition: java.util.Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
      partition.asScala.map { row =>
        val rowstr = s"${row.getLong(0)}|${row.getInt(1)}"
        InternalRow(row.numFields, org.apache.spark.unsafe.types.UTF8String.fromString(rowstr))
          .asInstanceOf[InternalRow]
      }.asJava
    }
  }
}

/**
 * echoall(TABLE t) is echocols with no selection (default = all columns). Over (a INT, b BIGINT,
 * c STRING) its evaluator must see all three columns, in schema order.
 */
object UnboundEchoAll extends UnboundTableFunction {
  override def name(): String = "echoall"
  override def bind(inputType: StructType): BoundTableFunction = BoundEchoAll
}

object BoundEchoAll extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "echoall"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("ncols", "int").add("rowstr", "string")
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = EchoAllEvaluatorFactory
}

/**
 * Emits (ncols, rowstr) per input row for the full (a INT, b BIGINT, c STRING) layout: ncols must
 * be 3, rowstr = "<a>|<b>|<c>".
 */
object EchoAllEvaluatorFactory extends TableFunctionEvaluatorFactory {
  override def create(): TableFunctionEvaluator = new TableFunctionEvaluator {
    override def eval(
        partition: java.util.Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
      partition.asScala.map { row =>
        val rowstr = s"${row.getInt(0)}|${row.getLong(1)}|${row.getUTF8String(2)}"
        InternalRow(row.numFields, org.apache.spark.unsafe.types.UTF8String.fromString(rowstr))
          .asInstanceOf[InternalRow]
      }.asJava
    }
  }
}

/**
 * selectpart(TABLE t PARTITION BY k) over (k INT, ts BIGINT, junk STRING) declares
 * selectedInputColumns() = [ts, k]. It emits one row per group: (k INT, tss STRING), the group's
 * ts values comma-joined in order. Because the selected layout is [ts, k] (+ the appended
 * partition_by marker), correct output proves the PARTITION BY ordinal tracks the selected layout.
 */
object UnboundSelectPart extends UnboundTableFunction {
  override def name(): String = "selectpart"
  override def bind(inputType: StructType): BoundTableFunction = BoundSelectPart
}

object BoundSelectPart extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "selectpart"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("k", "int").add("tss", "string")
  override def selectedInputColumns(): Array[NamedReference] =
    Array(Expressions.column("ts"), Expressions.column("k"))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = SelectPartEvaluatorFactory
}

/**
 * Per group over the selected [ts, k] layout: emits (k, tss) where k is column 1 and tss is the
 * comma-joined ts values (column 0) in received order.
 */
object SelectPartEvaluatorFactory extends TableFunctionEvaluatorFactory {
  override def create(): TableFunctionEvaluator = new TableFunctionEvaluator {
    override def eval(
        partition: java.util.Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
      val rows = partition.asScala.toSeq
      if (rows.isEmpty) {
        java.util.Collections.emptyIterator()
      } else {
        val k = rows.head.getInt(1)
        val tss = rows.map(_.getLong(0)).mkString(",")
        java.util.Collections.singletonList(
          InternalRow(k, org.apache.spark.unsafe.types.UTF8String.fromString(tss))
            .asInstanceOf[InternalRow]).iterator()
      }
    }
  }
}

/**
 * selectmissing(TABLE t) declares selectedInputColumns() = [nope], a column absent from the input,
 * so analysis must fail with a clean TABLE_VALUED_FUNCTION_SELECTED_COLUMN_NOT_FOUND error.
 */
object UnboundSelectMissing extends UnboundTableFunction {
  override def name(): String = "selectmissing"
  override def bind(inputType: StructType): BoundTableFunction = BoundSelectMissing
}

object BoundSelectMissing extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "selectmissing"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType = new StructType().add("cnt", "long")
  override def selectedInputColumns(): Array[NamedReference] =
    Array(Expressions.column("nope"))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = CountRowsEvaluatorFactory
}

/**
 * selectcase(TABLE t) is echocols with UPPER-cased selectedInputColumns() = [B, A] over lower-cased
 * input (a, b, c). Under the default case-insensitive analysis these resolve to [b, a], so its
 * evaluator (reused from echocols) must see exactly two columns in [b, a] order.
 */
object UnboundSelectCase extends UnboundTableFunction {
  override def name(): String = "selectcase"
  override def bind(inputType: StructType): BoundTableFunction = BoundSelectCase
}

object BoundSelectCase extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "selectcase"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("ncols", "int").add("rowstr", "string")
  override def selectedInputColumns(): Array[NamedReference] =
    Array(Expressions.column("B"), Expressions.column("A"))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = EchoColsEvaluatorFactory
}

/**
 * selectmulti(TABLE t PARTITION BY k1, k2) over (k1 INT, k2 INT, ts BIGINT) selects ONLY [ts],
 * excluding both partition keys. It emits one row per group: (ncols INT, tss STRING) where ncols is
 * the number of fields its evaluator received (must be 1 -- the two appended partition_by markers
 * are sliced off) and tss is the group's ts values comma-joined. Correct output proves the marker
 * slicing and the multi-key partition ordinals both track the selected layout.
 */
object UnboundSelectMulti extends UnboundTableFunction {
  override def name(): String = "selectmulti"
  override def bind(inputType: StructType): BoundTableFunction = BoundSelectMulti
}

object BoundSelectMulti extends BoundTableFunction with SupportsTableArgument {
  override def name(): String = "selectmulti"
  override def parameters(): Array[TableFunctionParameter] =
    Array(TableFunctionParameter.scalar("input", DataTypes.StringType).build())
  override def isDeterministic(): Boolean = true
  override def resultSchema(): StructType =
    new StructType().add("ncols", "int").add("tss", "string")
  override def selectedInputColumns(): Array[NamedReference] =
    Array(Expressions.column("ts"))
  override def evaluatorFactory(): TableFunctionEvaluatorFactory = SelectMultiEvaluatorFactory
}

/**
 * Per group over the selected [ts] layout: emits (ncols, tss) where ncols is the received field
 * count (must be 1, markers sliced) and tss is the comma-joined ts values (column 0) in order.
 */
object SelectMultiEvaluatorFactory extends TableFunctionEvaluatorFactory {
  override def create(): TableFunctionEvaluator = new TableFunctionEvaluator {
    override def eval(
        partition: java.util.Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
      val rows = partition.asScala.toSeq
      if (rows.isEmpty) {
        java.util.Collections.emptyIterator()
      } else {
        val ncols = rows.head.numFields
        val tss = rows.map(_.getLong(0)).mkString(",")
        java.util.Collections.singletonList(
          InternalRow(ncols, org.apache.spark.unsafe.types.UTF8String.fromString(tss))
            .asInstanceOf[InternalRow]).iterator()
      }
    }
  }
}
