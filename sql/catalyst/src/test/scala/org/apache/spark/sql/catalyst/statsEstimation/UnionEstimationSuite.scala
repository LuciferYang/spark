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

package org.apache.spark.sql.catalyst.statsEstimation

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LeafNode, LogicalPlan, Project, Statistics, Union}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A test-only leaf plan whose [[Statistics]] carry only `sizeInBytes`, no `rowCount` and no
 * per-attribute stats. Used to model the "deep plan whose stats failed to propagate" scenario
 * - when this is the child of a [[Project]], `ProjectEstimation` returns `None` and falls
 * through to `SizeInBytesOnlyStatsPlanVisitor`, which does NOT populate `attributeStats`.
 */
private case class NoStatsLeaf(override val output: Seq[Attribute])
    extends LeafNode with MultiInstanceRelation {
  override def computeStats(): Statistics = Statistics(sizeInBytes = 100)
  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))
}

class UnionEstimationSuite extends StatsEstimationTestBase {

  test("test row size estimation") {
    val attrInt = AttributeReference("cint", IntegerType)()

    val sz = Some(BigInt(1024))
    val child1 = StatsTestPlan(
      outputList = Seq(attrInt),
      rowCount = 2,
      attributeStats = AttributeMap(Nil),
      size = sz)

    val child2 = StatsTestPlan(
      outputList = Seq(attrInt),
      rowCount = 2,
      attributeStats = AttributeMap(Nil),
      size = sz)

    val union = Union(Seq(child1, child2))
    val expectedStats = logical.Statistics(sizeInBytes = 2 * 1024, rowCount = Some(4))
    assert(union.stats === expectedStats)
  }

  test("col stats estimation") {
    val sz = Some(BigInt(1024))

    val attrInt = AttributeReference("cint", IntegerType)()
    val attrDouble = AttributeReference("cdouble", DoubleType)()
    val attrShort = AttributeReference("cshort", ShortType)()
    val attrLong = AttributeReference("clong", LongType)()
    val attrByte = AttributeReference("cbyte", ByteType)()
    val attrFloat = AttributeReference("cfloat", FloatType)()
    val attrDecimal = AttributeReference("cdecimal", DecimalType(5, 4))()
    val attrDate = AttributeReference("cdate", DateType)()
    val attrTimestamp = AttributeReference("ctimestamp", TimestampType)()
    val attrTimestampNTZ = AttributeReference("ctimestamp_ntz", TimestampNTZType)()
    val attrYMInterval = AttributeReference("cyminterval", YearMonthIntervalType())()
    val attrDTInterval = AttributeReference("cdtinterval", DayTimeIntervalType())()

    val s1 = 1.toShort
    val s2 = 4.toShort
    val b1 = 1.toByte
    val b2 = 4.toByte
    val columnInfo = AttributeMap(
      Seq(
        attrInt -> ColumnStat(
          distinctCount = Some(2),
          min = Some(1),
          max = Some(4),
          nullCount = Some(1),
          avgLen = Some(4),
          maxLen = Some(4)),
        attrDouble -> ColumnStat(
          distinctCount = Some(2),
          min = Some(5.0),
          max = Some(4.0),
          nullCount = Some(2),
          avgLen = Some(4),
          maxLen = Some(4)),
        attrShort -> ColumnStat(min = Some(s1), max = Some(s2)),
        attrLong -> ColumnStat(min = Some(1L), max = Some(4L)),
        attrByte -> ColumnStat(min = Some(b1), max = Some(b2)),
        attrFloat -> ColumnStat(min = Some(1.1f), max = Some(4.1f)),
        attrDecimal -> ColumnStat(min = Some(Decimal(13.5)), max = Some(Decimal(19.5))),
        attrDate -> ColumnStat(min = Some(1), max = Some(4)),
        attrTimestamp -> ColumnStat(min = Some(1L), max = Some(4L)),
        attrTimestampNTZ -> ColumnStat(min = Some(1L), max = Some(4L)),
        attrYMInterval -> ColumnStat(min = Some(2), max = Some(5)),
        attrDTInterval -> ColumnStat(min = Some(2L), max = Some(5L))))

    val s3 = 2.toShort
    val s4 = 6.toShort
    val b3 = 2.toByte
    val b4 = 6.toByte
    val columnInfo1: AttributeMap[ColumnStat] = AttributeMap(
      Seq(
        AttributeReference("cint1", IntegerType)() -> ColumnStat(
          distinctCount = Some(2),
          min = Some(3),
          max = Some(6),
          nullCount = Some(1),
          avgLen = Some(8),
          maxLen = Some(8)),
        AttributeReference("cdouble1", DoubleType)() -> ColumnStat(
          distinctCount = Some(2),
          min = Some(2.0),
          max = Some(7.0),
          nullCount = Some(2),
          avgLen = Some(8),
          maxLen = Some(8)),
        AttributeReference("cshort1", ShortType)() -> ColumnStat(min = Some(s3), max = Some(s4)),
        AttributeReference("clong1", LongType)() -> ColumnStat(min = Some(2L), max = Some(6L)),
        AttributeReference("cbyte1", ByteType)() -> ColumnStat(min = Some(b3), max = Some(b4)),
        AttributeReference("cfloat1", FloatType)() -> ColumnStat(
          min = Some(2.2f),
          max = Some(6.1f)),
        AttributeReference("cdecimal1", DecimalType(5, 4))() -> ColumnStat(
          min = Some(Decimal(14.5)),
          max = Some(Decimal(19.9))),
        AttributeReference("cdate1", DateType)() -> ColumnStat(min = Some(3), max = Some(6)),
        AttributeReference("ctimestamp1", TimestampType)() -> ColumnStat(
          min = Some(3L),
          max = Some(6L)),
        AttributeReference("ctimestamp_ntz1", TimestampNTZType)() -> ColumnStat(
          min = Some(3L),
          max = Some(6L)),
        AttributeReference("cymtimestamp1", YearMonthIntervalType())() -> ColumnStat(
          min = Some(4),
          max = Some(8)),
        AttributeReference("cdttimestamp1", DayTimeIntervalType())() -> ColumnStat(
          min = Some(4L),
          max = Some(8L))))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq.sortWith(_.exprId.id < _.exprId.id),
      rowCount = 2,
      attributeStats = columnInfo,
      size = sz)

    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq.sortWith(_.exprId.id < _.exprId.id),
      rowCount = 2,
      attributeStats = columnInfo1,
      size = sz)

    val union = Union(Seq(child1, child2))

    val expectedStats = logical.Statistics(
      sizeInBytes = 2 * 1024,
      rowCount = Some(4),
      attributeStats = AttributeMap(
        Seq(
          attrInt -> ColumnStat(
            distinctCount = Some(2), min = Some(1), max = Some(6), nullCount = Some(2)),
          attrDouble -> ColumnStat(
            distinctCount = Some(2), min = Some(2.0), max = Some(7.0), nullCount = Some(4)),
          attrShort -> ColumnStat(min = Some(s1), max = Some(s4)),
          attrLong -> ColumnStat(min = Some(1L), max = Some(6L)),
          attrByte -> ColumnStat(min = Some(b1), max = Some(b4)),
          attrFloat -> ColumnStat(min = Some(1.1f), max = Some(6.1f)),
          attrDecimal -> ColumnStat(min = Some(Decimal(13.5)), max = Some(Decimal(19.9))),
          attrDate -> ColumnStat(min = Some(1), max = Some(6)),
          attrTimestamp -> ColumnStat(min = Some(1L), max = Some(6L)),
          attrTimestampNTZ -> ColumnStat(min = Some(1L), max = Some(6L)),
          attrYMInterval -> ColumnStat(min = Some(2), max = Some(8)),
          attrDTInterval -> ColumnStat(min = Some(2L), max = Some(8L)))))
    assert(union.stats === expectedStats)
  }

  test("col stats estimation when min max stats not present for one child") {
    val sz = Some(BigInt(1024))

    val attrInt = AttributeReference("cint", IntegerType)()

    val columnInfo = AttributeMap(
      Seq(
        attrInt -> ColumnStat(
          distinctCount = Some(2),
          min = Some(2),
          max = Some(2),
          nullCount = Some(0),
          avgLen = Some(4),
          maxLen = Some(4))))

    val columnInfo1 = AttributeMap(
      Seq(
        AttributeReference("cint1", IntegerType)() -> ColumnStat(
          distinctCount = Some(2),
          nullCount = Some(0),
          avgLen = Some(8),
          maxLen = Some(8))))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo,
      size = sz)

    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo1,
      size = sz)

    val union = Union(Seq(child1, child2))

    // Only nullCount and distinctCount are present (no min/max since child2 lacks them)
    val expectedStats = logical.Statistics(
      sizeInBytes = 2 * 1024,
      rowCount = Some(4),
      attributeStats = AttributeMap(
        Seq(attrInt -> ColumnStat(distinctCount = Some(2), nullCount = Some(0)))))
    assert(union.stats === expectedStats)
  }

  test("col stats estimation when null count stats are not present for one child") {
    val sz = Some(BigInt(1024))
    val attrInt = AttributeReference("cint", IntegerType)()
    val columnInfo = AttributeMap(
      Seq(
        attrInt -> ColumnStat(
          distinctCount = Some(2),
          min = Some(1),
          max = Some(2),
          nullCount = Some(2),
          avgLen = Some(4),
          maxLen = Some(4))))

    // No null count
    val columnInfo1 = AttributeMap(
      Seq(
        AttributeReference("cint1", IntegerType)() -> ColumnStat(
          distinctCount = Some(2),
          min = Some(3),
          max = Some(4),
          avgLen = Some(8),
          maxLen = Some(8))))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo,
      size = sz)

    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo1,
      size = sz)

    val union = Union(Seq(child1, child2))

    // nullCount should not be present (child2 lacks it), but distinctCount and min/max should.
    val expectedStats = logical.Statistics(
      sizeInBytes = 2 * 1024,
      rowCount = Some(4),
      attributeStats = AttributeMap(
        Seq(attrInt -> ColumnStat(
          distinctCount = Some(2), min = Some(1), max = Some(4), nullCount = None))))
    assert(union.stats === expectedStats)
  }

  test("SPARK-56047: distinctCount propagated as max across children") {
    val sz = Some(BigInt(1024))
    val attrInt = AttributeReference("cint", IntegerType)()
    val columnInfo = AttributeMap(Seq(
      attrInt -> ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(200),
        nullCount = Some(0))))
    val columnInfo1 = AttributeMap(Seq(
      AttributeReference("cint1", IntegerType)() -> ColumnStat(
        distinctCount = Some(200), min = Some(1), max = Some(300), nullCount = Some(0))))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq, rowCount = 500,
      attributeStats = columnInfo, size = sz)
    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq, rowCount = 500,
      attributeStats = columnInfo1, size = sz)

    val union = Union(Seq(child1, child2))
    val unionStats = union.stats
    val keyStat = unionStats.attributeStats(union.output.head)
    assert(keyStat.distinctCount === Some(200),
      "distinctCount should be max(100, 200) = 200")
  }

  test("SPARK-56047: distinctCount omitted when one child lacks it") {
    val sz = Some(BigInt(1024))
    val attrInt = AttributeReference("cint", IntegerType)()
    val columnInfo = AttributeMap(Seq(
      attrInt -> ColumnStat(distinctCount = Some(100), min = Some(1), max = Some(200),
        nullCount = Some(0))))
    // No distinctCount
    val columnInfo1 = AttributeMap(Seq(
      AttributeReference("cint1", IntegerType)() -> ColumnStat(
        min = Some(1), max = Some(300), nullCount = Some(0))))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq, rowCount = 500,
      attributeStats = columnInfo, size = sz)
    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq, rowCount = 500,
      attributeStats = columnInfo1, size = sz)

    val union = Union(Seq(child1, child2))
    val unionStats = union.stats
    val keyStat = unionStats.attributeStats(union.output.head)
    assert(keyStat.distinctCount.isEmpty,
      "distinctCount should be None when one child lacks it")
  }

  test("SPARK-56047: distinctCount capped by rowCount") {
    val sz = Some(BigInt(1024))
    val attrInt = AttributeReference("cint", IntegerType)()
    // distinctCount (500) > rowCount of union (6)
    val columnInfo = AttributeMap(Seq(
      attrInt -> ColumnStat(distinctCount = Some(500), min = Some(1), max = Some(1000),
        nullCount = Some(0))))
    val columnInfo1 = AttributeMap(Seq(
      AttributeReference("cint1", IntegerType)() -> ColumnStat(
        distinctCount = Some(300), min = Some(1), max = Some(1000), nullCount = Some(0))))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq, rowCount = 3,
      attributeStats = columnInfo, size = sz)
    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq, rowCount = 3,
      attributeStats = columnInfo1, size = sz)

    val union = Union(Seq(child1, child2))
    val unionStats = union.stats
    assert(unionStats.rowCount === Some(6))
    val keyStat = unionStats.attributeStats(union.output.head)
    assert(keyStat.distinctCount === Some(6),
      "distinctCount should be capped at rowCount=6, not max(500,300)=500")
  }

  test("SPARK-XXXXX: synthesize distinctCount from foldable Project alias in Union child") {
    // Mirrors the q5-style pattern: each Union branch tags its rows with a literal channel
    // via `Project(Literal('x') AS channel, child)`. The Project's child here has no
    // rowCount and no attributeStats (mimicking a deeply-nested plan where upstream stats
    // failed to propagate). ProjectEstimation returns None in that situation, so the literal
    // alias's stat would normally be lost - the new Union-level fallback rescues
    // distinctCount = 1 for the channel column.
    val storeAlias = Alias(Literal(UTF8String.fromString("store channel"), StringType), "channel")()
    val catalogAlias =
      Alias(Literal(UTF8String.fromString("catalog channel"), StringType), "channel")()
    val webAlias = Alias(Literal(UTF8String.fromString("web channel"), StringType), "channel")()

    val p1 = Project(Seq(storeAlias), NoStatsLeaf(Nil))
    val p2 = Project(Seq(catalogAlias), NoStatsLeaf(Nil))
    val p3 = Project(Seq(webAlias), NoStatsLeaf(Nil))

    // Sanity check: each branch's stats lack attributeStats for the literal alias.
    Seq(p1, p2, p3).foreach { p =>
      assert(p.stats.attributeStats.get(p.output.head).isEmpty,
        "ProjectEstimation should return no per-attribute stats when child has no rowCount")
    }

    val union = Union(Seq(p1, p2, p3))
    val stats = union.stats
    val channelStat = stats.attributeStats(union.output.head)
    // Each branch contributes distinctCount = 1 for the literal; max = 1.
    assert(channelStat.distinctCount === Some(1),
      "literal-aliased Union output should propagate distinctCount = 1")
    assert(channelStat.nullCount === Some(0),
      "non-null literal aliases should contribute nullCount = 0 per branch")
  }

  test("SPARK-XXXXX: literal alias fallback omits nullCount when child rowCount missing") {
    // Null literals need rowCount to derive nullCount per branch. When the child has no
    // rowCount, the fallback returns distinctCount = 0 but leaves nullCount unset so the
    // foldLeft in `computeColumnStats` correctly degrades to None.
    val nullAlias1 = Alias(Literal(null, StringType), "c")()
    val nullAlias2 = Alias(Literal(null, StringType), "c")()
    val p1 = Project(Seq(nullAlias1), NoStatsLeaf(Nil))
    val p2 = Project(Seq(nullAlias2), NoStatsLeaf(Nil))

    val union = Union(Seq(p1, p2))
    val cStat = union.stats.attributeStats(union.output.head)
    assert(cStat.distinctCount === Some(0))
    assert(cStat.nullCount.isEmpty,
      "nullCount should be None when child rowCount is unavailable")
  }

  test("SPARK-XXXXX: non-Project child without stats still produces no stats") {
    // Regression: the fallback should only trigger for Project. A child that's not a Project
    // and has no attributeStats must still drop distinctCount, matching prior behavior.
    val sz = Some(BigInt(1024))
    val attrInt = AttributeReference("cint", IntegerType)()

    val child1 = StatsTestPlan(
      outputList = Seq(attrInt),
      rowCount = 5,
      attributeStats = AttributeMap(Nil),
      size = sz)
    val child2 = StatsTestPlan(
      outputList = Seq(AttributeReference("cint1", IntegerType)()),
      rowCount = 5,
      attributeStats = AttributeMap(Nil),
      size = sz)

    val union = Union(Seq(child1, child2))
    assert(union.stats.attributeStats.get(union.output.head).isEmpty,
      "no fallback should fire when children are not Projects with foldable aliases")
  }

  test("SPARK-56047: hasCountStats is true when both distinctCount and nullCount propagated") {
    val sz = Some(BigInt(1024))
    val attrInt = AttributeReference("cint", IntegerType)()
    val columnInfo = AttributeMap(Seq(
      attrInt -> ColumnStat(distinctCount = Some(10), min = Some(1), max = Some(20),
        nullCount = Some(1))))
    val columnInfo1 = AttributeMap(Seq(
      AttributeReference("cint1", IntegerType)() -> ColumnStat(
        distinctCount = Some(15), min = Some(1), max = Some(30), nullCount = Some(2))))

    val child1 = StatsTestPlan(
      outputList = columnInfo.keys.toSeq, rowCount = 100,
      attributeStats = columnInfo, size = sz)
    val child2 = StatsTestPlan(
      outputList = columnInfo1.keys.toSeq, rowCount = 100,
      attributeStats = columnInfo1, size = sz)

    val union = Union(Seq(child1, child2))
    val unionStats = union.stats
    val keyStat = unionStats.attributeStats(union.output.head)
    assert(keyStat.hasCountStats,
      "hasCountStats should be true when both distinctCount and nullCount are defined")
    assert(keyStat.distinctCount === Some(15))
    assert(keyStat.nullCount === Some(3))
  }
}
