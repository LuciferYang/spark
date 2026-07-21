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
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, Identifier, InMemoryCatalog, TableFunctionCatalog}
import org.apache.spark.sql.connector.catalog.functions.{BoundTableFunction, SupportsScalarInvocation, TableFunctionParameter, UnboundTableFunction}
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
