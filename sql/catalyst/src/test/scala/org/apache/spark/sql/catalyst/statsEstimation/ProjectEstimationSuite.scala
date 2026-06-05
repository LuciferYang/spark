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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Concat, Literal, Upper}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{CharVarcharCodegenUtils, DateTimeUtils}
import org.apache.spark.sql.types._


class ProjectEstimationSuite extends StatsEstimationTestBase {

  test("project with alias") {
    val (ar1, colStat1) = (attr("key1"), ColumnStat(distinctCount = Some(2), min = Some(1),
      max = Some(2), nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))
    val (ar2, colStat2) = (attr("key2"), ColumnStat(distinctCount = Some(1), min = Some(10),
      max = Some(10), nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))

    val child = StatsTestPlan(
      outputList = Seq(ar1, ar2),
      rowCount = 2,
      attributeStats = AttributeMap(Seq(ar1 -> colStat1, ar2 -> colStat2)))

    val proj = Project(Seq(ar1, Alias(ar2, "abc")()), child)
    val expectedColStats = Seq("key1" -> colStat1, "abc" -> colStat2)
    val expectedAttrStats = toAttributeMap(expectedColStats, proj)
    val expectedStats = Statistics(
      sizeInBytes = 2 * (8 + 4 + 4),
      rowCount = Some(2),
      attributeStats = expectedAttrStats)
    assert(proj.stats == expectedStats)
  }

  test("project on empty table") {
    val (ar1, colStat1) = (attr("key1"), ColumnStat(distinctCount = Some(0), min = None, max = None,
      nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))
    val child = StatsTestPlan(
      outputList = Seq(ar1),
      rowCount = 0,
      attributeStats = AttributeMap(Seq(ar1 -> colStat1)))
    checkProjectStats(
      child = child,
      projectAttrMap = child.attributeStats,
      expectedSize = 1,
      expectedRowCount = 0)
  }

  test("test row size estimation") {
    val dec1 = Decimal("1.000000000000000000")
    val dec2 = Decimal("8.000000000000000000")
    val d1 = DateTimeUtils.fromJavaDate(Date.valueOf("2016-05-08"))
    val d2 = DateTimeUtils.fromJavaDate(Date.valueOf("2016-05-09"))
    val t1 = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-05-08 00:00:01"))
    val t2 = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2016-05-09 00:00:02"))

    val columnInfo: AttributeMap[ColumnStat] = AttributeMap(Seq(
      AttributeReference("cbool", BooleanType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(false), max = Some(true),
        nullCount = Some(0), avgLen = Some(1), maxLen = Some(1)),
      AttributeReference("cbyte", ByteType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(1), max = Some(2),
        nullCount = Some(0), avgLen = Some(1), maxLen = Some(1)),
      AttributeReference("cshort", ShortType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(1), max = Some(3),
        nullCount = Some(0), avgLen = Some(2), maxLen = Some(2)),
      AttributeReference("cint", IntegerType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(1), max = Some(4),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
      AttributeReference("clong", LongType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(1), max = Some(5),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
      AttributeReference("cdouble", DoubleType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(1.0), max = Some(6.0),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8)),
      AttributeReference("cfloat", FloatType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(1.0), max = Some(7.0),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
      AttributeReference("cdecimal", DecimalType.SYSTEM_DEFAULT)() -> ColumnStat(
        distinctCount = Some(2), min = Some(dec1), max = Some(dec2),
        nullCount = Some(0), avgLen = Some(16), maxLen = Some(16)),
      AttributeReference("cstring", StringType)() -> ColumnStat(distinctCount = Some(2),
        min = None, max = None, nullCount = Some(0), avgLen = Some(3), maxLen = Some(3)),
      AttributeReference("cbinary", BinaryType)() -> ColumnStat(distinctCount = Some(2),
        min = None, max = None, nullCount = Some(0), avgLen = Some(3), maxLen = Some(3)),
      AttributeReference("cdate", DateType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(d1), max = Some(d2),
        nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)),
      AttributeReference("ctimestamp", TimestampType)() -> ColumnStat(distinctCount = Some(2),
        min = Some(t1), max = Some(t2),
        nullCount = Some(0), avgLen = Some(8), maxLen = Some(8))
    ))
    val columnSizes: Map[Attribute, Long] = columnInfo.map(kv => (kv._1, getColSize(kv._1, kv._2)))
    val child = StatsTestPlan(
      outputList = columnInfo.keys.toSeq,
      rowCount = 2,
      attributeStats = columnInfo)

    // Row with single column
    columnInfo.keys.foreach { attr =>
      withClue(s"For data type ${attr.dataType}") {
        checkProjectStats(
          child = child,
          projectAttrMap = AttributeMap(attr -> columnInfo(attr) :: Nil),
          expectedSize = 2 * (8 + columnSizes(attr)),
          expectedRowCount = 2)
      }
    }

    // Row with multiple columns
    checkProjectStats(
      child = child,
      projectAttrMap = columnInfo,
      expectedSize = 2 * (8 + columnSizes.values.sum),
      expectedRowCount = 2)
  }

  test("SPARK-39989: Support estimate column statistics if it is foldable expression") {
    val (ar1, colStat1) = (attr("key1"), ColumnStat(distinctCount = Some(2), min = Some(1),
      max = Some(2), nullCount = Some(0), avgLen = Some(4), maxLen = Some(4)))

    val child = StatsTestPlan(
      outputList = Seq(ar1),
      rowCount = 2,
      attributeStats = AttributeMap(Seq(ar1 -> colStat1)))

    // nullable expression
    val proj1 = Project(Seq(ar1, Alias(Literal(null, IntegerType), "v")()), child)
    val expectedColStats1 = Seq(
      "key1" -> colStat1,
      "v" -> ColumnStat(Some(0), None, None, Some(2), Some(4), Some(4), None, 2))
    val expectedStats1 = Statistics(
      sizeInBytes = 2 * (8 + 4 + 4),
      rowCount = Some(2),
      attributeStats = toAttributeMap(expectedColStats1, proj1))
    assert(proj1.stats == expectedStats1)

    // non-nullable expression
    val proj2 = Project(Seq(ar1, Alias(Literal(10L, LongType), "v")()), child)
    val expectedColStats2 = Seq(
      "key1" -> colStat1,
      "v" -> ColumnStat(Some(1), Some(10L), Some(10L), Some(0), Some(8), Some(8), None, 2))
    val expectedStats2 = Statistics(
      sizeInBytes = 2 * (8 + 4 + 8),
      rowCount = Some(2),
      attributeStats = toAttributeMap(expectedColStats2, proj2))
    assert(proj2.stats == expectedStats2)
  }

  test("SPARK-XXXXX: propagate count stats through CharVarcharCodegenUtils.readSidePadding") {
    // For CHAR(N) columns Spark wraps each scan with
    //   StaticInvoke(CharVarcharCodegenUtils, "readSidePadding", attr, Literal(N))
    // under an Alias. Right-padding is injective on row values, so distinctCount and
    // nullCount must be preserved through this alias; min/max change with padding so they
    // are dropped; avgLen/maxLen become the declared length.
    val src = AttributeReference("c_raw", StringType)()
    val srcStat = ColumnStat(
      distinctCount = Some(7), min = Some("a"), max = Some("z"),
      nullCount = Some(2), avgLen = Some(3), maxLen = Some(5))

    val child = StatsTestPlan(
      outputList = Seq(src),
      rowCount = 100,
      attributeStats = AttributeMap(Seq(src -> srcStat)))

    val padded = StaticInvoke(
      classOf[CharVarcharCodegenUtils],
      StringType,
      "readSidePadding",
      src :: Literal(10) :: Nil,
      returnNullable = false)
    val alias = Alias(padded, "c")()
    val proj = Project(Seq(alias), child)

    val outStat = proj.stats.attributeStats(alias.toAttribute)
    assert(outStat.distinctCount === Some(7), "distinctCount preserved through padding")
    assert(outStat.nullCount === Some(2), "nullCount preserved through padding")
    assert(outStat.min.isEmpty, "min dropped because padding changes string ordering")
    assert(outStat.max.isEmpty, "max dropped because padding changes string ordering")
    assert(outStat.avgLen === Some(10), "avgLen set to padded length")
    assert(outStat.maxLen === Some(10), "maxLen set to padded length")
  }

  test("SPARK-XXXXX: readSidePadding fallback when src attr has no stats") {
    // When the underlying attribute is missing from attributeStats, the readSidePadding
    // case must NOT fire: alias-of-Attribute and foldable cases already don't, and the
    // new case shouldn't either (no source to inherit counts from).
    val src = AttributeReference("c_raw", StringType)()

    val child = StatsTestPlan(
      outputList = Seq(src),
      rowCount = 100,
      attributeStats = AttributeMap(Nil))

    val padded = StaticInvoke(
      classOf[CharVarcharCodegenUtils],
      StringType,
      "readSidePadding",
      src :: Literal(10) :: Nil,
      returnNullable = false)
    val alias = Alias(padded, "c")()
    val proj = Project(Seq(alias), child)

    // Project should still produce a Statistics with no per-attr stat for the alias.
    assert(proj.stats.attributeStats.get(alias.toAttribute).isEmpty,
      "no stat synthesized when source attribute has no stat")
  }

  test("SPARK-XXXXX: propagate count stats through Concat with a single attribute child") {
    // Tag-with-prefix pattern used in UNION ALL TPC-DS queries:
    //   `Alias(Concat(Literal("store_"), s_store_id), "id")`.
    // Cardinality is preserved on the underlying attribute (literal prefixes/suffixes don't
    // collapse distinct inputs and don't create extra distincts); nulls propagate.
    val src = AttributeReference("k", StringType)()
    val srcStat = ColumnStat(
      distinctCount = Some(5), min = Some("a"), max = Some("z"),
      nullCount = Some(1), avgLen = Some(3), maxLen = Some(5))

    val child = StatsTestPlan(
      outputList = Seq(src),
      rowCount = 100,
      attributeStats = AttributeMap(Seq(src -> srcStat)))

    // Pattern 1: literal prefix, attr suffix.
    val alias1 = Alias(Concat(Seq(Literal("store_"), src)), "id")()
    val proj1 = Project(Seq(alias1), child)
    val stat1 = proj1.stats.attributeStats(alias1.toAttribute)
    assert(stat1.distinctCount === Some(5))
    assert(stat1.nullCount === Some(1))
    assert(stat1.min.isEmpty && stat1.max.isEmpty)

    // Pattern 2: attr prefix, literal suffix.
    val alias2 = Alias(Concat(Seq(src, Literal("_v"))), "id")()
    val proj2 = Project(Seq(alias2), child)
    val stat2 = proj2.stats.attributeStats(alias2.toAttribute)
    assert(stat2.distinctCount === Some(5))
    assert(stat2.nullCount === Some(1))

    // Pattern 3: literals on both sides.
    val alias3 = Alias(Concat(Seq(Literal("a_"), src, Literal("_z"))), "id")()
    val proj3 = Project(Seq(alias3), child)
    val stat3 = proj3.stats.attributeStats(alias3.toAttribute)
    assert(stat3.distinctCount === Some(5))
    assert(stat3.nullCount === Some(1))
  }

  test("SPARK-XXXXX: Concat with multiple attributes does NOT propagate") {
    // Two attributes: cardinality could be up to dc1 * dc2, which we can't safely
    // estimate here. The case must skip rather than guess.
    val a = AttributeReference("a", StringType)()
    val b = AttributeReference("b", StringType)()
    val statA = ColumnStat(distinctCount = Some(3), nullCount = Some(0))
    val statB = ColumnStat(distinctCount = Some(4), nullCount = Some(0))

    val child = StatsTestPlan(
      outputList = Seq(a, b),
      rowCount = 100,
      attributeStats = AttributeMap(Seq(a -> statA, b -> statB)))

    val alias = Alias(Concat(Seq(a, b)), "k")()
    val proj = Project(Seq(alias), child)
    assert(proj.stats.attributeStats.get(alias.toAttribute).isEmpty,
      "Concat with multiple non-foldable children should not get synthesized stats")
  }

  test("SPARK-XXXXX: Concat with a null foldable part does NOT propagate src counts") {
    // If any foldable part evaluates to null, `Concat` returns null for every row, so
    // propagating `nullCount = src.nullCount` would be wrong (the output is all null).
    // We conservatively skip and rely on size-only fallback.
    val a = AttributeReference("a", StringType)()
    val statA = ColumnStat(distinctCount = Some(5), nullCount = Some(0))

    val child = StatsTestPlan(
      outputList = Seq(a),
      rowCount = 100,
      attributeStats = AttributeMap(Seq(a -> statA)))

    val alias = Alias(Concat(Seq(Literal(null, StringType), a)), "k")()
    val proj = Project(Seq(alias), child)
    assert(proj.stats.attributeStats.get(alias.toAttribute).isEmpty,
      "Concat with a null foldable part must not propagate src counts")
  }

  test("SPARK-XXXXX: Concat with non-Attribute non-foldable child does NOT propagate") {
    // The non-foldable child is `Upper(a)`, a function that can collapse distincts, so
    // we have no safe propagation rule. The case must skip.
    val a = AttributeReference("a", StringType)()
    val statA = ColumnStat(distinctCount = Some(5), nullCount = Some(0))

    val child = StatsTestPlan(
      outputList = Seq(a),
      rowCount = 100,
      attributeStats = AttributeMap(Seq(a -> statA)))

    val alias = Alias(Concat(Seq(Literal("x_"), Upper(a))), "k")()
    val proj = Project(Seq(alias), child)
    assert(proj.stats.attributeStats.get(alias.toAttribute).isEmpty,
      "non-foldable non-Attribute child must not match the Concat-injective fast path")
  }

  private def checkProjectStats(
      child: LogicalPlan,
      projectAttrMap: AttributeMap[ColumnStat],
      expectedSize: BigInt,
      expectedRowCount: BigInt): Unit = {
    val proj = Project(projectAttrMap.keys.toSeq, child)
    val expectedStats = Statistics(
      sizeInBytes = expectedSize,
      rowCount = Some(expectedRowCount),
      attributeStats = projectAttrMap)
    assert(proj.stats == expectedStats)
  }
}
