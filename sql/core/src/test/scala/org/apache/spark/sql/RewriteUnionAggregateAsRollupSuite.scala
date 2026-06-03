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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for [[RewriteUnionAggregateAsRollup]]. Unlike the
 * synthetic catalyst-level tests, these exercise the rule through the
 * full SQL pipeline (parser -> analyzer -> optimizer -> physical) on
 * real DataFrames. They guard against regressions caused by changes in
 * upstream analyzer/optimizer phases that may add Cast wrappers,
 * intermediate Projects, or other plan shapes the rule's detection must
 * tolerate.
 *
 * Each test verifies two things:
 *   1. With the rule enabled, the optimized plan contains an Expand
 *      operator (proving the rule fired).
 *   2. Result rows are identical with the rule on vs off (semantic
 *      equivalence).
 */
class RewriteUnionAggregateAsRollupSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private def planContainsExpand(sqlText: String): Boolean = {
    spark.sql(sqlText).queryExecution.optimizedPlan.collectFirst {
      case _: Expand => true
    }.isDefined
  }

  /** Run `sqlText` twice (rule on / off) and assert (a) rule fires, and
   *  (b) result rows are identical.
   */
  private def checkRewriteAndEquivalence(sqlText: String): Unit = {
    // Compare as sorted row sequences (multiset), NOT as Sets. UNION ALL has
    // multiset semantics, so a rewrite that changed the COUNT of an otherwise
    // identical row would be invisible to Set comparison.
    val (withRuleSchema, withRule) = withSQLConf(
        SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED.key -> "true") {
      assert(planContainsExpand(sqlText),
        s"Rule did not fire on:\n$sqlText")
      val df = spark.sql(sqlText)
      (df.schema, df.collect().map(_.toString()).sorted.toSeq)
    }
    val (withoutRuleSchema, withoutRule) = withSQLConf(
        SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED.key -> "false") {
      val df = spark.sql(sqlText)
      (df.schema, df.collect().map(_.toString()).sorted.toSeq)
    }
    // Schema as well as values. The remap pins every output to u.output's type by
    // construction; if that pinning regressed and a widened decimal leaked through,
    // small magnitudes would still print identically and the row comparison below
    // would pass while the user-visible schema had changed.
    assert(withRuleSchema == withoutRuleSchema,
      s"Schema mismatch on:\n$sqlText\n" +
      s"  withRule=${withRuleSchema.catalogString}\n" +
      s"  withoutRule=${withoutRuleSchema.catalogString}")
    assert(withRule == withoutRule,
      s"Result mismatch on:\n$sqlText\n" +
      s"  withRule=${withRule.size} rows, withoutRule=${withoutRule.size} rows\n" +
      s"  only-with-rule=${withRule.diff(withoutRule).take(10)}\n" +
      s"  only-without-rule=${withoutRule.diff(withRule).take(10)}")
  }

  /** Run `sqlText` with rule on and assert the rule does NOT fire. The reliable
   *  signal is the ABSENCE of an Expand (the rewrite always introduces exactly
   *  one ROLLUP Expand). We deliberately do NOT also require a Union to remain:
   *  the rewrite itself now produces a 2-child union-back, and an unrelated
   *  optimizer rule could legitimately remove a Union while this rule still
   *  correctly does not fire -- so "no Expand" is the only robust check.
   */
  private def checkNotRewritten(sqlText: String): Unit = {
    withSQLConf(SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED.key -> "true") {
      val plan = spark.sql(sqlText).queryExecution.optimizedPlan
      val hasExpand = plan.collectFirst { case _: Expand => true }.isDefined
      assert(!hasExpand,
        s"Expected rule NOT to fire (no Expand), got plan:\n${plan.treeString}")
    }
  }

  // Test fixture: small in-memory table with (channel, region, product, amt).
  private def withSalesTable(body: => Unit): Unit = {
    val data = Seq(
      ("store", "east", "a", 10),
      ("store", "east", "b", 20),
      ("store", "west", "a", 30),
      ("store", "west", "b", 40),
      ("web", "east", "a", 50),
      ("web", "east", "b", 60),
      ("web", "west", "a", 70),
      ("web", "west", "b", 80))
    withTempView("sales") {
      data.toDF("channel", "region", "product", "amt").createOrReplaceTempView("sales")
      body
    }
  }

  // Fixture with genuine NULLs in grouping columns (channel, region), so the
  // rewrite's NULL-safety (gid distinguishes data-NULL from subtotal-NULL) is
  // actually exercised by checkRewriteAndEquivalence.
  private def withNullableSalesTable(body: => Unit): Unit = {
    val data = Seq(
      (Some("store"), Some("east"), 10),
      (Some("store"), Option.empty[String], 20),
      (Option.empty[String], Some("west"), 30),
      (Option.empty[String], Option.empty[String], 40),
      (Some("web"), Some("east"), 50),
      (Some("web"), Option.empty[String], 60))
    withTempView("nsales") {
      data.toDF("channel", "region", "amt").createOrReplaceTempView("nsales")
      body
    }
  }

  test("source-aggregate with runtime-empty input stays equivalent " +
      "(grand-total row preserved)") {
    // A WHERE that filters out every row makes the source empty at RUNTIME but
    // not statically (so the rule still fires; PropagateEmptyRelation does not
    // fold it). The original UNION's GROUP BY () branch yields one row
    // (sum=NULL) even on empty input; a grouped ROLLUP aggregate yields zero.
    // checkRewriteAndEquivalence (rule-on vs rule-off) catches a dropped
    // grand-total row.
    withSalesTable {
      val sql =
        """
          |SELECT channel, region, sum(amt) AS tot
          |FROM sales WHERE amt > 100000 GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, sum(amt) AS tot
          |FROM sales WHERE amt > 100000 GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(amt) AS tot
          |FROM sales WHERE amt > 100000
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("SUM-of-SUM: 3-branch ROLLUP via UNION ALL") {
    // A genuine 3-branch (N=2) hierarchy: [channel,region] -> [channel] -> ().
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, sum(amt) AS total
          |  FROM sales GROUP BY channel, region)
          |SELECT channel, region, total FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, sum(total) AS total
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(total) AS total
          |FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("inner-fold with runtime-empty input stays equivalent " +
      "(grand-total row preserved)") {
    // CTE counterpart of the source-aggregate runtime-empty test. The inner-fold
    // grand-total branch is an aggregate-OVER-aggregate (SELECT sum(tot) FROM s);
    // its single global-aggregate row (sum=NULL) must survive the union-back, and
    // the rewrite's grouped ROLLUP (zero rows on empty input) must not drop it nor
    // double-count it. WHERE empties the input at runtime only, so the rule fires.
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, sum(amt) AS tot
          |  FROM sales WHERE amt > 100000 GROUP BY channel, region)
          |SELECT channel, region, tot FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, sum(tot) AS tot FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(tot) AS tot FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("inner-fold with genuine NULLs in a grouping column stays equivalent") {
    // CTE counterpart of the source-aggregate genuine-NULL test. The inner-fold
    // ROLLUP also carries spark_grouping_id as a grouping key, so a real-NULL data
    // row (gid bit 0) must NOT merge with a subtotal-NULL row (gid bit 1). A naive
    // rewrite that conflated them would change the rolled-up totals.
    withNullableSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, sum(amt) AS tot
          |  FROM nsales GROUP BY channel, region)
          |SELECT channel, region, tot FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, sum(tot) AS tot FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(tot) AS tot FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("inner-fold decimal sum-of-sum stays equivalent even when the inner " +
      "level genuinely overflows its decimal width") {
    // amt is Decimal(38,2) (max precision; sum stays Decimal(38,2), no widening).
    // Group (a,e) has 1000 rows of ~10^34, so their inner sum (~10^37) exceeds
    // the Decimal(38,2) range and overflows -> the original inner `tot` is NULL
    // for (a,e), which propagates through the outer sum-of-sum (sum over NULL).
    // The rewrite must reproduce this NULL EXACTLY by folding OVER the inner
    // aggregate. (With the old over-source widening it would have recomputed a
    // non-NULL value, diverging.) Only a few rows are needed -- the overflow is
    // genuine, not replication-dependent.
    withTempView("big") {
      // amt carried as a STRING and CAST to Decimal(38,2) in SQL (a 34-digit
      // value does not fit the Decimal(38,18) the encoder would infer from a
      // BigDecimal column). 1000 rows of ~10^34 in group (a,e) sum to ~10^37,
      // overflowing Decimal(38,2) (max ~10^36) -> inner tot NULL for (a,e).
      val huge = "9" * 34 + ".99"  // 34 integer digits
      val data = Seq(
        ("a", "e", huge),
        ("a", "w", "10.00"),
        ("b", "e", "20.00"))
      val many = (1 to 1000).map(_ => ("a", "e", huge)) ++ data.drop(1)
      many.toDF("channel", "region", "amt_str")
        .selectExpr("channel", "region", "CAST(amt_str AS DECIMAL(38,2)) AS amt")
        .createOrReplaceTempView("big")
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, sum(amt) AS tot
          |  FROM big GROUP BY channel, region)
          |SELECT channel, region, tot FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, sum(tot) AS tot FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(tot) AS tot FROM s
          |""".stripMargin
      // Non-ANSI so the genuine inner overflow yields NULL (not a thrown error),
      // letting checkRewriteAndEquivalence compare rule-on vs rule-off results.
      // Both must produce the SAME NULL for the (a,e) group's rolled-up totals.
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkRewriteAndEquivalence(sql)
      }
    }
  }

  test("MAX-of-MAX: 4-branch complete ROLLUP via UNION ALL") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, max(amt) AS mx
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, mx FROM s
          |UNION ALL
          |SELECT channel, region, NULL AS product, max(mx) AS mx
          |FROM s GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, max(mx) AS mx
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, max(mx) AS mx
          |FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("MIN-of-MIN: 4-branch complete ROLLUP via UNION ALL") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, min(amt) AS mn
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, mn FROM s
          |UNION ALL
          |SELECT channel, region, NULL AS product, min(mn) AS mn
          |FROM s GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, min(mn) AS mn
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, min(mn) AS mn
          |FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("mixed SUM + MAX: column-class match works for both") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, sum(amt) AS tot, max(amt) AS mx
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, tot, mx FROM s
          |UNION ALL
          |SELECT channel, region, NULL AS product, sum(tot) AS tot, max(mx) AS mx
          |FROM s GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, sum(tot) AS tot, max(mx) AS mx
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product,
          |       sum(tot) AS tot, max(mx) AS mx
          |FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("COUNT-rollup: outer SUM-of-COUNT fold") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, count(amt) AS cnt
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, cnt FROM s
          |UNION ALL
          |SELECT channel, region, NULL AS product, sum(cnt) AS cnt
          |FROM s GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, sum(cnt) AS cnt
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, sum(cnt) AS cnt
          |FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("COUNT(*)-rollup: outer SUM-of-COUNT(*) fold") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, count(*) AS cnt
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, cnt FROM s
          |UNION ALL
          |SELECT channel, region, NULL AS product, sum(cnt) AS cnt
          |FROM s GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, sum(cnt) AS cnt
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, sum(cnt) AS cnt
          |FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("count-of-count is NOT a valid fold and is correctly rejected") {
    withSalesTable {
      // Inner COUNT, outer COUNT (not SUM) -- count-of-count counts the
      // grouping cardinality, not the raw row count. Reject.
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, count(amt) AS cnt
          |  FROM sales GROUP BY channel, region)
          |SELECT channel, region, cnt FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, count(cnt) AS cnt
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, count(cnt) AS cnt
          |FROM s
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("mixed SUM + COUNT: per-column fold matching") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, sum(amt) AS tot, count(amt) AS cnt
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, tot, cnt FROM s
          |UNION ALL
          |SELECT channel, region, NULL AS product, sum(tot) AS tot, sum(cnt) AS cnt
          |FROM s GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, sum(tot) AS tot, sum(cnt) AS cnt
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product,
          |       sum(tot) AS tot, sum(cnt) AS cnt
          |FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("rejects 3-of-4 branches (missing GROUP BY with 1 column)") {
    withSalesTable {
      // The original union has branches at GROUP BY [3 cols], [2 cols], [0 cols].
      // ROLLUP would generate ALL levels including [1 col], so we must reject.
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, sum(amt) AS tot
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, tot FROM s
          |UNION ALL
          |SELECT channel, region, NULL AS product, sum(tot) AS tot
          |FROM s GROUP BY channel, region
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, sum(tot) AS tot
          |FROM s
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("function mismatch (SUM-of-MAX) is correctly rejected") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, max(amt) AS mx
          |  FROM sales GROUP BY channel, region)
          |SELECT channel, region, mx FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, sum(mx) AS mx
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(mx) AS mx
          |FROM s
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("AVG (non-idempotent) is correctly rejected") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, avg(amt) AS av
          |  FROM sales GROUP BY channel, region)
          |SELECT channel, region, av FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, avg(av) AS av
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, avg(av) AS av
          |FROM s
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("non-prefix-shrinking GROUP BY hierarchy is correctly rejected") {
    withSalesTable {
      // [region], [channel] -- not a strict prefix hierarchy
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, sum(amt) AS tot
          |  FROM sales GROUP BY channel, region)
          |SELECT channel, region, tot FROM s
          |UNION ALL
          |SELECT NULL AS channel, region, sum(tot) AS tot
          |FROM s GROUP BY region
          |UNION ALL
          |SELECT channel, NULL AS region, sum(tot) AS tot
          |FROM s GROUP BY channel
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("subset-but-not-ROLLUP-prefix level is correctly rejected") {
    withSalesTable {
      // Levels [c,r,p], [c,p], [c], [] -- the size-2 level drops the MIDDLE
      // key (region), so it is a valid GROUPING SETS member but NOT a ROLLUP
      // level. ROLLUP(c,r,p) would compute [c,r] at size 2, not [c,p], so
      // rewriting this would silently change results. Must stay a Union.
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, sum(amt) AS tot
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, tot FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, product, sum(tot) AS tot
          |FROM s GROUP BY channel, product
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, sum(tot) AS tot
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, sum(tot) AS tot
          |FROM s
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("source-aggregate with genuine NULLs in a grouping column stays equivalent") {
    // gid (spark_grouping_id) is part of the rewritten Aggregate's grouping
    // keys, so a genuine NULL data value in a grouping column (gid bit 0) is
    // never merged with a ROLLUP subtotal NULL (gid bit 1). This locks the
    // NULL-safety of the rewrite and refutes the "NULL conflation" concern.
    withNullableSalesTable {
      val sql =
        """
          |SELECT channel, region, sum(amt) AS tot
          |FROM nsales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, sum(amt) AS tot
          |FROM nsales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(amt) AS tot
          |FROM nsales
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  // -----------------------------------------------------------
  // Tests for the source-aggregate variant: each branch independently
  // aggregates the same raw source (no shared inner aggregate).
  // -----------------------------------------------------------

  test("source-aggregate AVG ROLLUP via UNION ALL") {
    withSalesTable {
      // Each branch independently computes avg(amt) from `sales` at a
      // different GROUP BY prefix level. No shared inner aggregate.
      val sql =
        """
          |SELECT channel, region, product, avg(amt) AS av
          |FROM sales GROUP BY channel, region, product
          |UNION ALL
          |SELECT channel, region, NULL AS product, avg(amt) AS av
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, avg(amt) AS av
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, avg(amt) AS av
          |FROM sales
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("source-aggregate SUM ROLLUP via UNION ALL") {
    withSalesTable {
      val sql =
        """
          |SELECT channel, region, product, sum(amt) AS tot
          |FROM sales GROUP BY channel, region, product
          |UNION ALL
          |SELECT channel, region, NULL AS product, sum(amt) AS tot
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, sum(amt) AS tot
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, sum(amt) AS tot
          |FROM sales
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("source-aggregate decimal SUM ROLLUP stays equivalent") {
    // Source path with a DECIMAL measure: each branch independently re-aggregates
    // the source at its own level, so sum(amt: Decimal(12,2)) widens to
    // Decimal(22,2) identically at every level. (Inner-fold has a fold-of-fold
    // decimal test; the source path previously had none.)
    withTempView("dsales") {
      val data = Seq(
        ("store", "east", BigDecimal("10.50")),
        ("store", "west", BigDecimal("20.25")),
        ("web", "east", BigDecimal("30.75")),
        ("web", "west", BigDecimal("40.00")))
      data.toDF("channel", "region", "amt")
        .selectExpr("channel", "region", "CAST(amt AS DECIMAL(12,2)) AS amt")
        .createOrReplaceTempView("dsales")
      val sql =
        """
          |SELECT channel, region, sum(amt) AS tot FROM dsales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, sum(amt) AS tot FROM dsales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(amt) AS tot FROM dsales
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("source-aggregate: rejects branches with differing aggregate expressions") {
    withSalesTable {
      // Branch 1 uses avg, branch 2 uses sum -- signatures differ -> reject.
      val sql =
        """
          |SELECT channel, region, avg(amt) AS m
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, sum(amt) AS m
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(amt) AS m
          |FROM sales
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("source-aggregate: rejects branches whose aggregate FILTERs differ") {
    withSalesTable {
      // Same fn/inputs but the MIDDLE level filters and the others do not. The
      // filter references only the aggregated column (amt), so the signature
      // must encode the filter or these collide and the rewrite would reuse one
      // branch's (un)filtered expression for all levels -> wrong results.
      val sql =
        """
          |SELECT channel, region, sum(amt) AS m
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, sum(amt) FILTER (WHERE amt > 25) AS m
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(amt) AS m
          |FROM sales
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("source-aggregate: rejects branches whose aggregate ARGUMENT structure " +
      "differs even when references are equal") {
    withSalesTable {
      // The MIDDLE level sums CASE WHEN amt > 25 while the others sum CASE WHEN
      // amt > 0. Both reference only {amt}, so a references-based signature
      // collides; the rewrite would reuse one branch's CASE for all levels and
      // change results. The signature must encode the full argument structure.
      val sql =
        """
          |SELECT channel, region,
          |       sum(CASE WHEN amt > 0 THEN amt ELSE 0 END) AS m
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region,
          |       sum(CASE WHEN amt > 25 THEN amt ELSE 0 END) AS m
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region,
          |       sum(CASE WHEN amt > 0 THEN amt ELSE 0 END) AS m
          |FROM sales
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("source-aggregate with a COMPLEX but CONSISTENT aggregate argument " +
      "still rewrites and stays equivalent") {
    // All branches sum the SAME CASE expression -> signatures match -> rewrite
    // fires, and results must be equivalent. Guards against the argument-encoding
    // fix being over-strict (rejecting a legitimate shared complex measure).
    withSalesTable {
      val sql =
        """
          |SELECT channel, region,
          |       sum(CASE WHEN amt > 25 THEN amt ELSE 0 END) AS m
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region,
          |       sum(CASE WHEN amt > 25 THEN amt ELSE 0 END) AS m
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region,
          |       sum(CASE WHEN amt > 25 THEN amt ELSE 0 END) AS m
          |FROM sales
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("source-aggregate: rejects branches with per-branch non-null literal columns") {
    withSalesTable {
      // Branches have differing non-null literals at the same position.
      val sql =
        """
          |SELECT channel, region, 0 AS g, avg(amt) AS av
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, 1 AS g, avg(amt) AS av
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, 1 AS g, avg(amt) AS av
          |FROM sales
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  test("inner-aggregate fold handles CTE pattern (source-aggregate path skips it)") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, product, sum(amt) AS total
          |  FROM sales GROUP BY channel, region, product)
          |SELECT channel, region, product, total FROM s
          |UNION ALL
          |SELECT channel, region, NULL AS product, sum(total) AS total
          |FROM s GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, NULL AS product, sum(total) AS total
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, NULL AS product, sum(total) AS total
          |FROM s
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("source-aggregate COUNT(DISTINCT) via UNION ALL") {
    withSalesTable {
      val sql =
        """
          |SELECT channel, region, count(distinct product) AS dcnt
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, count(distinct product) AS dcnt
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, count(distinct product) AS dcnt
          |FROM sales
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("source-aggregate SUM with FILTER via UNION ALL") {
    withSalesTable {
      val sql =
        """
          |SELECT channel, region,
          |       sum(amt) FILTER (WHERE product = 'a') AS filtered_sum
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region,
          |       sum(amt) FILTER (WHERE product = 'a') AS filtered_sum
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region,
          |       sum(amt) FILTER (WHERE product = 'a') AS filtered_sum
          |FROM sales
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("source-aggregate COLLECT_SET via UNION ALL") {
    withSalesTable {
      val sql =
        """
          |SELECT channel, region,
          |       sort_array(collect_set(product)) AS prods
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region,
          |       sort_array(collect_set(product)) AS prods
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region,
          |       sort_array(collect_set(product)) AS prods
          |FROM sales
          |""".stripMargin
      checkRewriteAndEquivalence(sql)
    }
  }

  test("source-aggregate: rejects a 2-branch union (below the 3-branch minimum)") {
    // Killed by the `u.children.size >= 3` gate (a 2-branch union can never
    // form a complete multi-level hierarchy worth rewriting). Hierarchy
    // COMPLETENESS with >= 3 branches is pinned separately by the inner-fold
    // 3-of-4 test above.
    withSalesTable {
      val sql =
        """
          |SELECT channel, region, sum(amt) AS tot
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(amt) AS tot
          |FROM sales
          |""".stripMargin
      checkNotRewritten(sql)
    }
  }

  // ===== Regressions from the post-rebase adversarial review (R1) ===========
  // Every shape below was CONFIRMED to fire and produce wrong results (or an
  // unresolved plan) before its guard existed, by running rule-on vs rule-off.

  test("source-aggregate: rejects a branch that swaps a key and a NULL filler " +
      "across positions") {
    // Killed by verifyKeyPositionsAligned. The level-1 branch outputs
    // [NULL, a, sum] where the reference layout is [a, b, sum]: the rewrite
    // would replay the reference layout (a, NULL, sum) for that level, moving
    // the key's values into a different output column.
    withTempView("kp") {
      Seq((1, 10, 100), (2, 20, 200), (2, 30, 300))
        .toDF("a", "b", "x").createOrReplaceTempView("kp")
      checkNotRewritten("""
        SELECT a, b, sum(x) AS s FROM kp GROUP BY a, b
        UNION ALL
        SELECT NULL, a, sum(x) AS s FROM kp GROUP BY a
        UNION ALL
        SELECT NULL, NULL, sum(x) AS s FROM kp
      """)
    }
  }

  test("inner-fold: rejects a branch that swaps a key and a NULL filler " +
      "across positions") {
    // Inner-path counterpart of the previous test, killed by the inner-path
    // verifyKeyPositionsAligned call. The filler is deliberately aliased to
    // the ACCEPTABLE name "b": an arbitrary filler name (e.g. "na") would be
    // rejected earlier by verifyOutputsRollupShaped's acceptable-name check,
    // shadowing the key-position guard and leaving it unpinned.
    withTempView("kpi") {
      Seq((1, 10, 100), (2, 20, 200), (2, 30, 300))
        .toDF("a", "b", "x").createOrReplaceTempView("kpi")
      checkNotRewritten("""
        SELECT a, b, s FROM (SELECT a, b, sum(x) AS s FROM kpi GROUP BY a, b)
        UNION ALL
        SELECT NULL AS b, a, sum(s) AS s
        FROM (SELECT a, b, sum(x) AS s FROM kpi GROUP BY a, b) GROUP BY a
        UNION ALL
        SELECT NULL AS a, NULL AS b, sum(s) AS s
        FROM (SELECT a, b, sum(x) AS s FROM kpi GROUP BY a, b)
      """)
    }
  }

  test("inner-fold: rejects a mid-Project that re-aliases inner outputs crosswise") {
    // Killed by InnerBranch.parse's midProjectsAreNamePreserving. The
    // mid-Project `SELECT a, t AS s, s AS t` re-routes which inner measure the
    // outer folds actually read while every NAME-based fold check still sees
    // the reference binding -- the rewrite silently swapped columns s and t at
    // the folded levels before this guard existed.
    withTempView("mp") {
      Seq((1, 10, 100, 1), (2, 20, 200, 2), (2, 30, 300, 3))
        .toDF("a", "b", "x", "y").createOrReplaceTempView("mp")
      checkNotRewritten("""
        SELECT a, b, s, t FROM (SELECT a, b, sum(x) AS s, sum(y) AS t FROM mp GROUP BY a, b)
        UNION ALL
        SELECT a, NULL AS b, sum(s) AS s, sum(t) AS t
        FROM (SELECT a, t AS s, s AS t
              FROM (SELECT a, b, sum(x) AS s, sum(y) AS t FROM mp GROUP BY a, b))
        GROUP BY a
        UNION ALL
        SELECT NULL AS a, NULL AS b, sum(s) AS s, sum(t) AS t
        FROM (SELECT a, b, sum(x) AS s, sum(y) AS t FROM mp GROUP BY a, b)
      """)
    }
  }

  test("source-aggregate: rejects same-named measure columns from different " +
      "sides of a self-join") {
    // Killed by the attribute-ORDINAL component of exprSignature. sum(l.x) and
    // sum(r.x) render identically after ID-stripping (toString carries no
    // qualifier), so before ordinals the rewrite replayed sum(l.x) for the
    // sum(r.x) level.
    withTempView("jt") {
      Seq((1, 1, 10, 100), (1, 2, 20, 999), (2, 3, 30, 300))
        .toDF("k", "a", "b", "x").createOrReplaceTempView("jt")
      checkNotRewritten("""
        SELECT l.a, l.b, sum(l.x) AS s FROM jt l JOIN jt r ON l.k = r.k GROUP BY l.a, l.b
        UNION ALL
        SELECT l.a, NULL, sum(r.x) AS s FROM jt l JOIN jt r ON l.k = r.k GROUP BY l.a
        UNION ALL
        SELECT NULL, NULL, sum(l.x) AS s FROM jt l JOIN jt r ON l.k = r.k
      """)
    }
  }

  test("source-aggregate: rejects branches whose scalar subqueries differ") {
    // Killed by the subquery-plan-hash component of exprSignature. A subquery
    // expression's toString hides its plan, so two branches computing
    // DIFFERENT subqueries collided and the rewrite replayed one branch's
    // subquery for every level.
    withTempView("sq", "sq1", "sq2") {
      Seq((1, 10, 100), (2, 20, 200)).toDF("a", "b", "x").createOrReplaceTempView("sq")
      Seq(5, 7).toDF("y").createOrReplaceTempView("sq1")
      Seq(1000, 2000).toDF("z").createOrReplaceTempView("sq2")
      checkNotRewritten("""
        SELECT a, b, sum(x) + (SELECT max(y) FROM sq1) AS s FROM sq GROUP BY a, b
        UNION ALL
        SELECT a, NULL, sum(x) + (SELECT min(z) FROM sq2) AS s FROM sq GROUP BY a
        UNION ALL
        SELECT NULL, NULL, sum(x) + (SELECT max(y) FROM sq1) AS s FROM sq
      """)
    }
  }

  test("source-aggregate: rejects FILTERs differing only in a literal that " +
      "contains '#<digits>'") {
    // Killed by the literal-values component of exprSignature. stripIds munges
    // `#<n>` inside literal VALUES too ('v#1' and 'v#2' both became 'v#'), so
    // branches differing only in such a FILTER literal collided.
    withTempView("ft") {
      Seq((1, 10, 100, "v#1"), (1, 10, 999, "v#2"), (2, 20, 200, "v#1"))
        .toDF("a", "b", "x", "tag").createOrReplaceTempView("ft")
      checkNotRewritten("""
        SELECT a, b, sum(x) FILTER (WHERE tag = 'v#1') AS s FROM ft GROUP BY a, b
        UNION ALL
        SELECT a, NULL, sum(x) FILTER (WHERE tag = 'v#2') AS s FROM ft GROUP BY a
        UNION ALL
        SELECT NULL, NULL, sum(x) FILTER (WHERE tag = 'v#1') AS s FROM ft
      """)
    }
  }

  test("source-aggregate: rejects cross-labeled keys hidden behind a " +
      "passthrough Project") {
    // Killed by the multi-layer verifyKeyPassthroughNamesAligned. The
    // cross-labeling aliases (b AS a, a AS b) live in the Aggregate's own
    // output; the passthrough wrapper Project hid them from the previous
    // top-layer-only check and the rewrite re-derived the reference layout,
    // un-swapping columns the query had swapped.
    withTempView("xl") {
      Seq((1, 10, 100), (2, 20, 200), (2, 30, 300))
        .toDF("a", "b", "x").createOrReplaceTempView("xl")
      checkNotRewritten("""
        SELECT a AS a, b, s FROM
          (SELECT b AS a, a AS b, sum(x) AS s FROM xl GROUP BY a, b)
        UNION ALL
        SELECT a, NULL, sum(x) AS s FROM xl GROUP BY a
        UNION ALL
        SELECT NULL, NULL, sum(x) AS s FROM xl
      """)
    }
  }

  test("source-aggregate: rejects mixed count vs count(DISTINCT) across branches") {
    // Killed by the `:distinct=` component of aggMeasureSignature: DISTINCT
    // lives on AggregateExpression, not the function, so without that
    // component count(product) and count(DISTINCT product) collide and the
    // rewrite would replay the reference's non-distinct count for every level.
    withSalesTable {
      checkNotRewritten("""
        SELECT channel, region, count(product) AS c FROM sales GROUP BY channel, region
        UNION ALL
        SELECT channel, NULL AS region, count(DISTINCT product) AS c FROM sales GROUP BY channel
        UNION ALL
        SELECT NULL AS channel, NULL AS region, count(product) AS c FROM sales
      """)
    }
  }

  test("inner-fold: key under a widening cast fires and stays equivalent " +
      "(remap supplies the session timezone)") {
    // POSITIVE control for buildRollupPlan's up-cast remap: the DATE key is
    // CAST to TIMESTAMP in every branch, so the Union output is TIMESTAMP
    // while the Expand key stays DATE and the remap must cast DATE ->
    // TIMESTAMP. That cast needs a timezone: built bare it left the whole
    // rewritten plan UNRESOLVED (PLAN_VALIDATION_FAILED_RULE_IN_BATCH under
    // plan-change validation) before the fix.
    withTempView("dt") {
      Seq(("2024-01-01", 1, 100L), ("2024-01-02", 2, 200L))
        .toDF("ds", "k", "v")
        .selectExpr("CAST(ds AS DATE) AS d", "k", "v")
        .createOrReplaceTempView("dt")
      checkRewriteAndEquivalence("""
        WITH ia AS (SELECT d, k, sum(v) AS s FROM dt GROUP BY d, k)
        SELECT CAST(d AS TIMESTAMP) AS d, k, s FROM ia
        UNION ALL
        SELECT CAST(d AS TIMESTAMP) AS d, NULL AS k, sum(s) AS s FROM ia GROUP BY d
        UNION ALL
        SELECT NULL AS d, NULL AS k, sum(s) AS s FROM ia
      """)
    }
  }

  test("source-aggregate: rejects a temp-view branch whose tz-hiding measure " +
      "froze a different session timezone") {
    // Killed by the TimeZoneAwareExpression arm of hiddenStateMarkers. A temp
    // view stores its ANALYZED plan, freezing zoneIds at CREATE time; TimeAdd
    // (ts + INTERVAL '1' MONTH) hides its timeZoneId from toString, so before
    // the marker the view branch collided with the inline branches and the
    // rewrite replayed the session timezone over the view's frozen one.
    withTempView("tzbase", "tzv") {
      Seq(("a", "x", "2024-03-30 23:30:00"), ("a", "y", "2024-01-15 12:00:00"),
        ("b", "x", "2024-05-31 22:00:00"))
        .toDF("k1", "k2", "tss")
        .selectExpr("k1", "k2", "CAST(tss AS TIMESTAMP) AS ts")
        .createOrReplaceTempView("tzbase")
      val originalTz = spark.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)
      try {
        spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "Asia/Shanghai")
        spark.sql(
          """CREATE OR REPLACE TEMPORARY VIEW tzv AS
            |SELECT k1, k2, max(ts + INTERVAL '1' MONTH) AS m
            |FROM tzbase GROUP BY k1, k2""".stripMargin)
        spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")
        checkNotRewritten("""
          SELECT k1, k2, m FROM tzv
          UNION ALL
          SELECT k1, NULL AS k2, max(ts + INTERVAL '1' MONTH) AS m
          FROM tzbase GROUP BY k1
          UNION ALL
          SELECT NULL AS k1, NULL AS k2, max(ts + INTERVAL '1' MONTH) AS m
          FROM tzbase
        """)
      } finally {
        spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, originalTz)
      }
    }
  }

  test("inner-fold: rejects a temp-view branch whose key CAST froze a " +
      "different session timezone") {
    // Killed by verifyWrapperCastTimezonesAreSession. buildRollupPlan discards
    // the branch-top wrappers and re-creates the Union-output binding cast
    // with the SESSION timezone, so a view-frozen CAST(date AS timestamp)
    // under another timezone had its per-row values silently recomputed under
    // the session timezone before the guard.
    withTempView("dtz", "dtzv") {
      Seq(("2024-01-01", 1, 100L), ("2024-01-02", 2, 200L))
        .toDF("ds", "k", "v")
        .selectExpr("CAST(ds AS DATE) AS d", "k", "v")
        .createOrReplaceTempView("dtz")
      val originalTz = spark.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)
      try {
        spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "Asia/Shanghai")
        spark.sql(
          """CREATE OR REPLACE TEMPORARY VIEW dtzv AS
            |SELECT CAST(d AS TIMESTAMP) AS d, k, s
            |FROM (SELECT d, k, sum(v) AS s FROM dtz GROUP BY d, k)""".stripMargin)
        spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")
        checkNotRewritten("""
          SELECT d, k, s FROM dtzv
          UNION ALL
          SELECT CAST(d AS TIMESTAMP) AS d, NULL AS k, sum(s) AS s
          FROM (SELECT d, k, sum(v) AS s FROM dtz GROUP BY d, k) GROUP BY d
          UNION ALL
          SELECT NULL AS d, NULL AS k, sum(s) AS s
          FROM (SELECT d, k, sum(v) AS s FROM dtz GROUP BY d, k)
        """)
      } finally {
        spark.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, originalTz)
      }
    }
  }

  test("source-aggregate: rejects measures differing only beyond the " +
      "toString truncation limit") {
    // Killed by the class-sequence component of exprSignature.
    // Expression.toString truncates flat arguments at
    // spark.sql.debug.maxToStringFields (25), so branches agreeing on the
    // first 25 coalesce arguments but differing structurally (AND vs OR) in
    // the 26th collided before the class sequence.
    withTempView("trunc") {
      Seq((1, 10, 5, -3, 100, 999), (2, 20, -1, 7, 200, 888))
        .toDF("a", "b", "p", "q", "x", "y").createOrReplaceTempView("trunc")
      val nulls = List.fill(25)("CAST(NULL AS INT)").mkString(", ")
      val andTail = s"coalesce($nulls, CASE WHEN p > 0 AND q > 0 THEN x ELSE y END)"
      val orTail = s"coalesce($nulls, CASE WHEN p > 0 OR q > 0 THEN x ELSE y END)"
      checkNotRewritten(s"""
        SELECT a, b, sum($andTail) AS s FROM trunc GROUP BY a, b
        UNION ALL
        SELECT a, NULL AS b, sum($orTail) AS s FROM trunc GROUP BY a
        UNION ALL
        SELECT NULL AS a, NULL AS b, sum($andTail) AS s FROM trunc
      """)
    }
  }

  test("source-aggregate: identical binary FILTER literals fire and stay " +
      "equivalent (deterministic literal rendering)") {
    // POSITIVE control for the literal-values component: each branch parses
    // its own Literal(Array[Byte]); an identity-based rendering
    // (String.valueOf) made textually identical binary literals compare
    // unequal, nondeterministically suppressing the rewrite. Literal.toString
    // renders binary as hex, so these must match and fire.
    withTempView("binl") {
      Seq((1, 10, 100, Array[Byte](0x0a)), (1, 20, 999, Array[Byte](0x0b)),
        (2, 30, 200, Array[Byte](0x0a)))
        .toDF("a", "b", "x", "bin").createOrReplaceTempView("binl")
      checkRewriteAndEquivalence("""
        SELECT a, b, sum(x) FILTER (WHERE bin = X'0A') AS s FROM binl GROUP BY a, b
        UNION ALL
        SELECT a, NULL AS b, sum(x) FILTER (WHERE bin = X'0A') AS s FROM binl GROUP BY a
        UNION ALL
        SELECT NULL AS a, NULL AS b, sum(x) FILTER (WHERE bin = X'0A') AS s FROM binl
      """)
    }
  }

  test("source-aggregate: rejects a branch grouping by the OTHER join side's " +
      "same-named column") {
    // Killed by verifySourceKeyOrdinalsAligned. Keys are otherwise matched by
    // NAME only, so GROUP BY t2.a against a reference grouping t1.a (same
    // name, different join side, different source ordinal) fired and silently
    // regrouped that level by the reference's column.
    withTempView("jk") {
      Seq((1, 1, 10, 100), (1, 2, 20, 200), (2, 3, 30, 300))
        .toDF("k", "a", "b", "x").createOrReplaceTempView("jk")
      checkNotRewritten("""
        SELECT t1.a, t1.b, sum(t1.x) AS s
        FROM jk t1 JOIN jk t2 ON t1.k = t2.k GROUP BY t1.a, t1.b
        UNION ALL
        SELECT t2.a, NULL, sum(t1.x) AS s
        FROM jk t1 JOIN jk t2 ON t1.k = t2.k GROUP BY t2.a
        UNION ALL
        SELECT NULL, NULL, sum(t1.x) AS s
        FROM jk t1 JOIN jk t2 ON t1.k = t2.k
      """)
    }
  }

  test("source-aggregate: rejects a branch-top Project that REORDERS measures") {
    // Killed by branchTopProjectsAreOrderPreserving. Per-position guards
    // compare the AGGREGATE layer across branches, while buildRollupPlan binds
    // the reference aggregate's order positionally to the union output (the
    // head branch's POST-Project order) -- a reordering passthrough Project on
    // the head silently swapped the level-2 measure columns.
    withTempView("ro") {
      Seq((1, 10, 100, 7), (1, 20, 200, 8), (2, 30, 300, 9))
        .toDF("a", "b", "x", "y").createOrReplaceTempView("ro")
      checkNotRewritten("""
        SELECT a, b, sx, sy FROM
          (SELECT a, b, sum(y) AS sy, sum(x) AS sx FROM ro GROUP BY a, b)
        UNION ALL
        SELECT a, NULL, sum(y) AS sy, sum(x) AS sx FROM ro GROUP BY a
        UNION ALL
        SELECT NULL, NULL, sum(y) AS sy, sum(x) AS sx FROM ro
      """)
    }
  }

  test("inner-fold: rejects a name-preserving mid-Project CAST on a key") {
    // Killed by the aliases-only unwrap in midProjectsAreNamePreserving. A
    // mid-Project CAST is value-changing state the fold rebuild discards:
    // CAST(k AS DOUBLE) AS k merges BIGINT keys 2^53 and 2^53+1 in the
    // original (double collision) while the rewrite replayed the inner's
    // exact BIGINT key and kept them distinct.
    withTempView("ck") {
      Seq((9007199254740992L, 1, 10), (9007199254740993L, 1, 20), (5L, 2, 30))
        .toDF("k", "b", "x").createOrReplaceTempView("ck")
      checkNotRewritten("""
        SELECT k, b, s FROM (SELECT k, b, sum(x) AS s FROM ck GROUP BY k, b)
        UNION ALL
        SELECT k, NULL AS b, sum(s) AS s
        FROM (SELECT CAST(k AS DOUBLE) AS k, s
              FROM (SELECT k, b, sum(x) AS s FROM ck GROUP BY k, b))
        GROUP BY k
        UNION ALL
        SELECT NULL AS k, NULL AS b, sum(s) AS s
        FROM (SELECT k, b, sum(x) AS s FROM ck GROUP BY k, b)
      """)
    }
  }

  test("inner-fold: rejects an inner whose measure is NAMED AFTER an " +
      "unprojected grouping key") {
    // Killed by the bare-passthrough key guard. The inner groups by (a, b)
    // but projects only b, NAMING its sum "a" -- a coarser branch's GROUP BY
    // then cross-binds the sum-valued column by name, and the rewrite
    // regrouped that level by the raw key (different groups, different sums).
    withTempView("xk") {
      Seq((1, 1, 10), (1, 2, 20), (2, 1, 10))
        .toDF("a", "b", "x").createOrReplaceTempView("xk")
      checkNotRewritten("""
        WITH ia AS (SELECT b, sum(x) AS a FROM xk GROUP BY a, b)
        SELECT b, a FROM ia
        UNION ALL
        SELECT NULL AS b, sum(a) AS a FROM ia GROUP BY a
        UNION ALL
        SELECT NULL AS b, sum(a) AS a FROM ia
      """)
    }
  }

  test("inner-fold: rejects an inner that projects a key only under a " +
      "value-changing CAST") {
    // Killed by the bare-passthrough key guard. The inner outputs
    // CAST(k AS DOUBLE) AS k while grouping by the BIGINT k: the original
    // level-1 branch groups by the DOUBLE (merging keys that collide above
    // 2^53), but the rewrite regrouped by the exact BIGINT.
    withTempView("xc") {
      Seq((9007199254740992L, 1, 10), (9007199254740993L, 1, 20), (5L, 2, 30))
        .toDF("k", "b", "x").createOrReplaceTempView("xc")
      checkNotRewritten("""
        WITH ia AS (SELECT CAST(k AS DOUBLE) AS k, b, sum(x) AS s FROM xc GROUP BY k, b)
        SELECT k, b, s FROM ia
        UNION ALL
        SELECT k, NULL AS b, sum(s) AS s FROM ia GROUP BY k
        UNION ALL
        SELECT NULL AS k, NULL AS b, sum(s) AS s FROM ia
      """)
    }
  }

  test("inner-fold: rejects a mid-Project NULL filler that shadows an inner " +
      "measure name") {
    // Killed by the no-literals rule in midProjectsAreNamePreserving. The mid
    // Project's `NULL AS s` shadows the inner measure s, so the branch's
    // max(s) aggregates a constant NULL -- while every name-based check saw
    // the reference binding and the rewrite replayed the REAL max for that
    // level.
    withTempView("xs") {
      Seq((1, 1, 10), (1, 2, 20), (2, 1, 30))
        .toDF("a", "b", "x").createOrReplaceTempView("xs")
      checkNotRewritten("""
        WITH ia AS (SELECT a, b, max(x) AS s FROM xs GROUP BY a, b)
        SELECT a, b, s FROM ia
        UNION ALL
        SELECT a, CAST(NULL AS INT) AS b, max(s) AS s
        FROM (SELECT a, b, NULL AS s FROM ia) GROUP BY a
        UNION ALL
        SELECT NULL AS a, NULL AS b, max(s) AS s FROM ia
      """)
    }
  }

  test("inner-fold: rejects hash-equal inline inners with SWAPPED measure names") {
    // Killed by the inner output-name-vector pin. Canonicalization ERASES
    // alias names, so branch 2's structurally identical inner (sum(x) AS t,
    // sum(y) AS s -- names swapped) hashes equal to the reference; its
    // sum(s) actually folds sum(y) while every name binding sees the
    // reference's s=sum(x). The rewrite swapped the measure columns at the
    // fold levels before the pin.
    withTempView("nv1") {
      Seq((1, 10, 100, 7), (1, 20, 200, 8), (2, 30, 300, 9))
        .toDF("a", "b", "x", "y").createOrReplaceTempView("nv1")
      checkNotRewritten("""
        SELECT a, b, s, t
        FROM (SELECT a, b, sum(x) AS s, sum(y) AS t FROM nv1 GROUP BY a, b)
        UNION ALL
        SELECT a, CAST(NULL AS INT) AS b, sum(s) AS s, sum(t) AS t
        FROM (SELECT a, b, sum(x) AS t, sum(y) AS s FROM nv1 GROUP BY a, b)
        GROUP BY a
        UNION ALL
        SELECT CAST(NULL AS INT) AS a, CAST(NULL AS INT) AS b, sum(s) AS s, sum(t) AS t
        FROM (SELECT a, b, sum(x) AS s, sum(y) AS t FROM nv1 GROUP BY a, b)
      """)
    }
  }

  test("inner-fold: rejects hash-equal inline inners with SWAPPED key names") {
    // Killed by the inner output-name-vector pin. Branch 2's inner reads the
    // same source through a name-swapped projection (x AS k2, y AS k1) with
    // positionally identical structure -- hash-equal, but its GROUP BY k1
    // denotes y where the reference's k1 denotes x. The rewrite regrouped
    // that level by the wrong column before the pin.
    withTempView("nv2") {
      Seq((1, 10, 100L), (1, 20, 200L), (2, 10, 300L))
        .toDF("x", "y", "v").createOrReplaceTempView("nv2")
      checkNotRewritten("""
        SELECT k1, k2, s
        FROM (SELECT k1, k2, sum(v) AS s
              FROM (SELECT x AS k1, y AS k2, v FROM nv2) GROUP BY k1, k2)
        UNION ALL
        SELECT k1, CAST(NULL AS INT) AS k2, sum(s) AS s
        FROM (SELECT k2, k1, sum(v) AS s
              FROM (SELECT x AS k2, y AS k1, v FROM nv2) GROUP BY k2, k1)
        GROUP BY k1
        UNION ALL
        SELECT CAST(NULL AS INT) AS k1, CAST(NULL AS INT) AS k2, sum(s) AS s
        FROM (SELECT k1, k2, sum(v) AS s
              FROM (SELECT x AS k1, y AS k2, v FROM nv2) GROUP BY k1, k2)
      """)
    }
  }

  test("source-aggregate: rejects same-schema sources with DIFFERENT data") {
    // Killed by the canonical-plan-equality source key. LocalRelation's
    // toString renders only its schema (never its DATA) and DSv1 file
    // relations render only the format (never the PATH), so a string-derived
    // identity collided across same-schema sources with different contents
    // and the rewrite replayed one branch's source for another's level.
    withTempView("dv1", "dv2") {
      spark.sql("""CREATE OR REPLACE TEMP VIEW dv1 AS
        SELECT * FROM VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30) AS t(a, b, x)""")
      spark.sql("""CREATE OR REPLACE TEMP VIEW dv2 AS
        SELECT * FROM VALUES (1, 1, 999) AS t(a, b, x)""")
      checkNotRewritten("""
        SELECT a, b, sum(x) AS s FROM dv1 GROUP BY a, b
        UNION ALL
        SELECT a, NULL, sum(x) AS s FROM dv2 GROUP BY a
        UNION ALL
        SELECT NULL, NULL, sum(x) AS s FROM dv1
      """)
    }
  }

  test("inner-fold: rejects same-schema inline inners over DIFFERENT-data views") {
    // Inner-path counterpart of the previous test (canonical-plan-equality
    // inner key).
    withTempView("dv3", "dv4") {
      spark.sql("""CREATE OR REPLACE TEMP VIEW dv3 AS
        SELECT * FROM VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30) AS t(a, b, x)""")
      spark.sql("""CREATE OR REPLACE TEMP VIEW dv4 AS
        SELECT * FROM VALUES (1, 1, 999) AS t(a, b, x)""")
      checkNotRewritten("""
        SELECT a, b, s FROM (SELECT a, b, sum(x) AS s FROM dv3 GROUP BY a, b)
        UNION ALL
        SELECT a, CAST(NULL AS INT) AS b, sum(s) AS s
        FROM (SELECT a, b, sum(x) AS s FROM dv4 GROUP BY a, b) GROUP BY a
        UNION ALL
        SELECT CAST(NULL AS INT) AS a, CAST(NULL AS INT) AS b, sum(s) AS s
        FROM (SELECT a, b, sum(x) AS s FROM dv3 GROUP BY a, b)
      """)
    }
  }

  test("source-aggregate: rejects struct fields differing only in a #digit " +
      "name suffix") {
    // Killed by the structord marker. GetStructField's toString renders only
    // child.fieldName (the ordinal is hidden) and stripIds munges #<digits>
    // inside field NAMES, so sum(st.`m#1`) and sum(st.`m#2`) collided and the
    // rewrite replayed the reference's field for the other level.
    withTempView("sfx") {
      spark.sql("""CREATE OR REPLACE TEMP VIEW sfx AS
        SELECT a, b, named_struct('m#1', m1, 'm#2', m2) AS st
        FROM VALUES (1, 1, 10L, 777L), (1, 2, 20L, 888L), (2, 1, 30L, 999L)
          AS t(a, b, m1, m2)""")
      checkNotRewritten("""
        SELECT a, b, sum(st.`m#1`) AS v FROM sfx GROUP BY a, b
        UNION ALL
        SELECT a, NULL, sum(st.`m#2`) AS v FROM sfx GROUP BY a
        UNION ALL
        SELECT NULL, NULL, sum(st.`m#1`) AS v FROM sfx
      """)
    }
  }

  test("POSITIVE control: parquet-backed source fires and stays equivalent") {
    // Pins that the canonical-plan-equality branch identity (which compares
    // relation/FileIndex objects) still matches same-table references within
    // one query for FILE-BASED sources -- the committed suites otherwise
    // exercise only LocalRelation-backed views, so a comparator change that
    // silently stopped file-based firing would not be caught.
    withTempDir { dir =>
      withTempView("pqv") {
        spark.range(0, 20)
          .selectExpr("CAST(id % 3 AS INT) AS a", "CAST(id % 4 AS INT) AS b", "id AS x")
          .write.mode("overwrite").parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("pqv")
        checkRewriteAndEquivalence("""
          SELECT a, b, sum(x) AS s FROM pqv GROUP BY a, b
          UNION ALL
          SELECT a, NULL AS b, sum(x) AS s FROM pqv GROUP BY a
          UNION ALL
          SELECT NULL AS a, NULL AS b, sum(x) AS s FROM pqv
        """)
      }
    }
  }

  test("source-aggregate: rejects from_json schemas with #digit field names") {
    // Killed by the jsonschema marker. The schema is a constructor field
    // rendered (and #digit-munged) only via toString; key lookup is BY NAME,
    // so branches parsing different `f#1`/`f#2` fields collided and the
    // rewrite replayed the reference's schema for the other level.
    withTempView("fjx") {
      spark.sql("""CREATE OR REPLACE TEMP VIEW fjx AS
        SELECT a, b, '{"f#1": 1, "f#2": 100}' AS j
        FROM VALUES (1, 1), (1, 2), (2, 1) AS t(a, b)""")
      checkNotRewritten("""
        SELECT a, b, sum(from_json(j, 'STRUCT<`f#1`: BIGINT>').`f#1`) AS v
        FROM fjx GROUP BY a, b
        UNION ALL
        SELECT a, NULL, sum(from_json(j, 'STRUCT<`f#2`: BIGINT>').`f#2`) AS v
        FROM fjx GROUP BY a
        UNION ALL
        SELECT NULL, NULL, sum(from_json(j, 'STRUCT<`f#1`: BIGINT>').`f#1`) AS v
        FROM fjx
      """)
    }
  }

  test("source-aggregate: rejects to_csv options differing in a #digit nullValue") {
    // Killed by the tocsvopts marker. Option VALUES are copied verbatim into
    // the output text and rendered only via toString where stripIds munges
    // #<digits> -- 'v#1' and 'v#2' collided.
    withTempView("tcx") {
      spark.sql("""CREATE OR REPLACE TEMP VIEW tcx AS
        SELECT a, b, named_struct('p', CAST(NULL AS INT), 'q', x) AS s
        FROM VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30) AS t(a, b, x)""")
      checkNotRewritten("""
        SELECT a, b, max(to_csv(s, map('nullValue', 'v#1'))) AS v FROM tcx GROUP BY a, b
        UNION ALL
        SELECT a, NULL, max(to_csv(s, map('nullValue', 'v#2'))) AS v FROM tcx GROUP BY a
        UNION ALL
        SELECT NULL, NULL, max(to_csv(s, map('nullValue', 'v#1'))) AS v FROM tcx
      """)
    }
  }

  test("source-aggregate: rejects alpha-INequivalent nested lambdas with " +
      "#digit parameter names") {
    // Killed by the lam binding-structure component. Lambda variables are
    // LeafExpressions (invisible to ords) whose rendered names can contain
    // #<digits>; an inner body referencing the OUTER vs INNER parameter
    // collided when the names differ only in stripped digits. The index
    // encoding captures which binder each reference resolves to.
    withTempView("ltx") {
      spark.sql("""CREATE OR REPLACE TEMP VIEW ltx AS
        SELECT a, b, array(1, 2) AS arr, array(9) AS arr2
        FROM VALUES (1, 1), (1, 2), (2, 1) AS t(a, b)""")
      checkNotRewritten("""
        SELECT a, b,
          max(transform(arr, `x#1` -> transform(arr2, `x#2` -> `x#1`))) AS v
        FROM ltx GROUP BY a, b
        UNION ALL
        SELECT a, NULL,
          max(transform(arr, `x#1` -> transform(arr2, `x#2` -> `x#2`))) AS v
        FROM ltx GROUP BY a
        UNION ALL
        SELECT NULL, NULL,
          max(transform(arr, `x#1` -> transform(arr2, `x#2` -> `x#1`))) AS v
        FROM ltx
      """)
    }
  }

  test("inner-fold: rejects a narrowing CAST wrapping the fold inside a " +
      "fold branch's outer aggregate") {
    // Killed by verifyOuterFoldsToInner's bare-fold check. A fold branch
    // measure CAST(SUM(c) AS INT) carries a value-changing narrowing cast
    // INSIDE the outer aggregate; buildRollupPlan rebuilds the level as a bare
    // Sum(c) at the inner's wide type, dropping the narrowing -- the k1
    // subtotal (sum past INT range) came out un-wrapped before the guard. A
    // legitimate Union widening coercion cast sits in a Project ABOVE the
    // aggregate and is unaffected (the no-cast baseline still fires -- pinned
    // by the SUM-of-SUM positive test above).
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withTempView("nc") {
        Seq((1, 1, 1500000000L), (1, 2, 1500000000L), (2, 1, 10L))
          .toDF("k1", "k2", "x").createOrReplaceTempView("nc")
        checkNotRewritten("""
          WITH ia AS (SELECT k1, k2, SUM(x) AS c FROM nc GROUP BY k1, k2)
          SELECT k1, k2, c FROM ia
          UNION ALL
          SELECT k1, NULL AS k2, CAST(sum(c) AS INT) AS c FROM ia GROUP BY k1
          UNION ALL
          SELECT NULL AS k1, NULL AS k2, sum(c) AS c FROM ia
        """)
      }
    }
  }

  test("rule is config-gated: inner-aggregate disabled => Union remains") {
    withSalesTable {
      val sql =
        """
          |WITH s AS (
          |  SELECT channel, region, sum(amt) AS tot
          |  FROM sales GROUP BY channel, region)
          |SELECT channel, region, tot FROM s
          |UNION ALL
          |SELECT channel, NULL AS region, sum(tot) AS tot
          |FROM s GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, sum(tot) AS tot
          |FROM s
          |""".stripMargin
      withSQLConf(SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED.key -> "false") {
        assert(!planContainsExpand(sql),
          "Rule should not fire when conf is disabled")
      }
    }
  }

  test("rule is config-gated: source-aggregate disabled => Union remains") {
    withSalesTable {
      val sql =
        """
          |SELECT channel, region, avg(amt) AS av
          |FROM sales GROUP BY channel, region
          |UNION ALL
          |SELECT channel, NULL AS region, avg(amt) AS av
          |FROM sales GROUP BY channel
          |UNION ALL
          |SELECT NULL AS channel, NULL AS region, avg(amt) AS av
          |FROM sales
          |""".stripMargin
      withSQLConf(SQLConf.UNION_AGGREGATE_TO_ROLLUP_ENABLED.key -> "false") {
        assert(!planContainsExpand(sql),
          "Source-aggregate rule should not fire when conf is disabled")
      }
    }
  }

}
