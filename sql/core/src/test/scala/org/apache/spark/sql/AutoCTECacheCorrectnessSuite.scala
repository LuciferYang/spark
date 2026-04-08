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

import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Correctness tests for auto-CTE caching that cover two scenarios not exercised
 * by [[AutoCTECacheSuite]]:
 *
 *   1. `hasDivergentPredicates` heuristic - making sure we correctly cache when
 *      per-reference predicates are semantically equal (possibly with different
 *      AST orderings) and correctly skip when they're genuinely divergent.
 *
 *   2. Interaction with SPARK-40193 subquery plan merging - making sure the two
 *      optimizations coexist, produce correct results, and auto-CTE still fires
 *      when a CTE body contains a mergeable subquery.
 */
class AutoCTECacheCorrectnessSuite extends QueryTest with SharedSparkSession {

  // Disable the size-based gate for the existing tests so they only exercise
  // the structural gate. The stats gate is exercised explicitly by the tests
  // in the "stats gate" section below.
  override protected def sparkConf: org.apache.spark.SparkConf =
    super.sparkConf.set(SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key, "0")

  override protected def afterEach(): Unit = {
    try {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    } finally {
      super.afterEach()
    }
  }

  private def prepareData(): Unit = {
    spark.range(10000)
      .selectExpr(
        "id",
        "id % 100 as key",
        "cast(id % 50 as int) as col1",
        "cast(id % 30 as int) as col2",
        "cast(id as double) as value")
      .write.mode("overwrite").saveAsTable("auto_cte_corr_test")
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS auto_cte_corr_test")
      spark.sql("DROP TABLE IF EXISTS auto_cte_tiny_test")
    } finally {
      super.afterAll()
    }
  }

  private def prepareTinyData(): Unit = {
    spark.range(10)
      .selectExpr("id", "id as key", "cast(id as double) as value")
      .write.mode("overwrite").saveAsTable("auto_cte_tiny_test")
  }

  private def cachedEntries: Int =
    spark.sharedState.autoCTECacheManager.numEntries

  private def countInMemoryRelations(sql: String): Int = {
    spark.sql(sql).queryExecution.optimizedPlan.collect {
      case _: InMemoryRelation => 1
    }.sum
  }

  // ---------------------------------------------------------------------------
  // hasDivergentPredicates tests (blocker #2)
  // ---------------------------------------------------------------------------

  test("hasDivergentPredicates: caches when refs have identical predicates") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key, a.total + b.total
          |FROM cte a JOIN cte b ON a.key = b.key
          |WHERE a.key > 10 AND b.key > 10""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Should cache when all references have identical pushable predicates")
    }
  }

  test("hasDivergentPredicates: caches when predicates are semantically equal " +
       "but syntactically reordered") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // This is the reordering bug case: both references have the same conjunction
      // but in different orders. Without canonicalization, semanticHash() of the
      // two predicates differs and hasDivergentPredicates returns true (bug -> skip).
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.total, b.total FROM
          |  (SELECT key, total FROM cte WHERE key > 10 AND total > 100) a
          |  JOIN
          |  (SELECT key, total FROM cte WHERE total > 100 AND key > 10) b
          |  ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Should cache when reordered but semantically equal predicates are used on both refs; " +
        "hasDivergentPredicates must normalize before hashing")
    }
  }

  test("hasDivergentPredicates: caches when refs have non-correlated divergent predicates") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Each reference has a different non-correlated filter. The previous
      // heuristic skipped caching here, but `PushdownPredicatesAndPruneColumnsForCTEDef`
      // already ORs the per-reference filters and pushes the combined predicate
      // into the CTE body, so caching does NOT block pushdown - the per-reference
      // filters sit above the cache. The EMR reference cluster caches q24a/q64
      // which have exactly this shape, and we should match.
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.total, b.total FROM
          |  (SELECT key, total FROM cte WHERE key < 10) a,
          |  (SELECT key, total FROM cte WHERE key > 90) b
          |  WHERE a.key + 100 = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Should cache when references have non-correlated divergent filters; " +
        "the OR-combined predicate is already pushed into the body, so the " +
        "per-reference filters above the cache preserve pushdown semantics.")
    }
  }

  // ---------------------------------------------------------------------------
  // SPARK-40193 interaction tests (blocker #6)
  // ---------------------------------------------------------------------------

  test("SPARK-40193: auto-CTE + subquery merging produce identical results") {
    prepareData()
    // CTE body contains a scalar subquery (>(SELECT AVG...)) that SPARK-40193
    // might want to merge with the other scalar subquery in the outer select.
    val sql =
      """WITH cte AS (
        |  SELECT key, sum(value) as total, rand() as r
        |  FROM auto_cte_corr_test
        |  WHERE value > (SELECT AVG(value) FROM auto_cte_corr_test)
        |  GROUP BY key
        |)
        |SELECT a.key, a.total + b.total,
        |       (SELECT MAX(value) FROM auto_cte_corr_test)
        |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

    val baseline = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      spark.sql(sql).collect().map(_.toString).sorted.toSeq
    }
    val optimized = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(sql).collect().map(_.toString).sorted.toSeq
    }
    assert(baseline == optimized,
      s"Results must be identical with and without auto-CTE caching. " +
      s"baseline.size=${baseline.size} optimized.size=${optimized.size}")
  }

  test("SPARK-40193: auto-CTE still fires when CTE body has a mergeable subquery") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test
          |  WHERE value > (SELECT AVG(value) FROM auto_cte_corr_test)
          |  GROUP BY key
          |)
          |SELECT a.key, a.total + b.total
          |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Auto-CTE should still fire when the CTE body contains a subquery that " +
        "SPARK-40193 could merge; rule ordering in SparkOptimizer must allow it")
    }
  }

  // ---------------------------------------------------------------------------
  // Deterministic CTE caching tests (TPC-DS shape)
  // ---------------------------------------------------------------------------
  // The existing AutoCTECacheSuite uses rand() in every test query to make CTEs
  // non-deterministic, which is the only condition that bypasses InlineCTE's
  // unconditional deterministic-CTE inlining. Real TPC-DS queries are
  // deterministic, so without an InlineCTE carve-out the entire feature is dead
  // code on real workloads. These tests use deterministic CTEs to catch that
  // regression and verify the fix.

  test("deterministic multi-ref CTE with identical filters is cached") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total, count(*) as cnt
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key, a.total, b.cnt
          |FROM agg a JOIN agg b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Deterministic multi-ref CTE with expensive body must be cached, " +
        "not inlined by InlineCTE")
    }
  }

  test("deterministic multi-ref CTE with different filters is cached (q64-shape)") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Mimics q64's cs1/cs2 self-join: same CTE referenced twice with
      // different non-correlated filters on a group-by key.
      val sql =
        """WITH agg AS (
          |  SELECT key, col1, sum(value) as total, count(*) as cnt
          |  FROM auto_cte_corr_test GROUP BY key, col1
          |)
          |SELECT a.key, a.total, b.total FROM
          |  (SELECT key, total FROM agg WHERE col1 = 1) a
          |  JOIN
          |  (SELECT key, total FROM agg WHERE col1 = 2) b
          |  ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Deterministic multi-ref CTE with non-correlated divergent filters " +
        "must be cached. PushdownPredicatesAndPruneColumnsForCTEDef already " +
        "ORs the per-reference filters into the body, so caching does not " +
        "block pushdown.")
    }
  }

  test("flag off: deterministic multi-ref CTE is inlined and not cached") {
    prepareData()
    // Default behavior: AUTO_REUSED_CTE_ENABLED=false. The InlineCTE carve-out
    // must NOT fire, so the deterministic CTE is inlined as before and no
    // cache entries are created. This guards against a future change to the
    // carve-out condition that might activate it unconditionally.
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key, a.total + b.total
          |FROM agg a JOIN agg b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries == 0,
        "With AUTO_REUSED_CTE_ENABLED=false, deterministic multi-ref CTE " +
        "must be inlined by InlineCTE (default behavior); the carve-out " +
        "must not fire and no cache entries should be created.")
    }
  }

  test("scan-only deterministic multi-ref CTE is still inlined when flag is on") {
    prepareData()
    // Counterpart to the cache-eligible tests: a scan-only CTE (no Join,
    // Aggregate, Sort, or Window in its body) is below the
    // isAutoCacheEligible / isExpensiveEnough threshold, so InlineCTE's
    // carve-out must NOT keep it. The CTE should be inlined as before and
    // no cache entry should be created. Guards against the carve-out being
    // accidentally relaxed to fire on cheap CTEs.
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH simple AS (
          |  SELECT key, value FROM auto_cte_corr_test WHERE key < 50
          |)
          |SELECT a.key, b.value
          |FROM simple a JOIN simple b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries == 0,
        "Scan-only CTE (no Join/Aggregate/Sort/Window in body) must be " +
        "inlined by InlineCTE even when AUTO_REUSED_CTE_ENABLED=true. " +
        "InlineCTE.isAutoCacheEligible and ReplaceCTERefWithCache.isExpensiveEnough " +
        "must agree on this exclusion.")
    }
  }

  test("InlineCTE-skipped CTE that ReplaceCTERefWithCache also rejects falls through cleanly") {
    prepareData()
    // This is the contract test for the fall-through path. If
    // InlineCTE.isAutoCacheEligible and ReplaceCTERefWithCache's gates ever
    // diverge, a CTE that the carve-out keeps may fail
    // shouldAutoCache. The CTE then falls through to
    // ReplaceCTERefWithRepartition, which must produce a valid plan. This
    // test does not assert specific cache state - it asserts that the
    // query runs to completion and returns rows without a plan validation
    // failure.
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total, count(*) as cnt
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key, a.total + b.cnt FROM agg a JOIN agg b ON a.key = b.key
          |WHERE a.total > 0 OR b.cnt > 0""".stripMargin

      val result = spark.sql(sql).collect()
      assert(result.nonEmpty,
        "Query with cache-eligible CTE must execute successfully regardless " +
        "of which downstream rule (cache vs repartition) handles it.")
    }
  }

  test("nested deterministic CTE (q64-shape: outer CTE references inner CTE)") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Mimics q64's cs_ui (single-ref, inlinable) inside cross_sales
      // (multi-ref, cache-eligible). The inner CTE should be inlined into
      // the outer CTE's body, then the outer CTE should be cached.
      val sql =
        """WITH inner_cte AS (
          |  SELECT key, col1, sum(value) as inner_total
          |  FROM auto_cte_corr_test
          |  GROUP BY key, col1
          |  HAVING sum(value) > 0
          |),
          |outer_cte AS (
          |  SELECT key, col1, inner_total, count(*) as outer_cnt
          |  FROM inner_cte
          |  GROUP BY key, col1, inner_total
          |)
          |SELECT a.key, a.outer_cnt, b.outer_cnt FROM
          |  (SELECT key, outer_cnt FROM outer_cte WHERE col1 = 1) a
          |  JOIN
          |  (SELECT key, outer_cnt FROM outer_cte WHERE col1 = 2) b
          |  ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Nested CTE: inner_cte should be inlined into outer_cte, then " +
        "outer_cte (multi-ref) should be cached. q64 has this exact shape " +
        "with cs_ui inside cross_sales.")
    }
  }

  // ---------------------------------------------------------------------------
  // Correlated-subquery detection tests (TagCorrelatedCTERefs)
  // ---------------------------------------------------------------------------

  test("correlated-ref: q1-shape correlated scalar subquery is NOT cached") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Mimics TPC-DS q1: a CTE referenced once at top level and once
      // inside a correlated scalar subquery. The correlation is the
      // `a.key = b.key` join condition that references the outer `a.key`
      // from inside the subquery.
      //
      // TagCorrelatedCTERefs must observe the SubqueryExpression with
      // outerAttrs.nonEmpty before RewriteCorrelatedScalarSubquery
      // decorrelates it, and tag the CTE so AutoCTECache skips caching.
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key FROM agg a
          |WHERE a.total > (SELECT avg(b.total) * 1.2 FROM agg b WHERE a.key = b.key)
          |""".stripMargin

      val result = spark.sql(sql).collect()
      assert(cachedEntries == 0,
        "q1-shape correlated scalar subquery must NOT be cached. " +
        "TagCorrelatedCTERefs should mark the CTE as correlatedSubqueryRef=true " +
        "before RewriteCorrelatedScalarSubquery decorrelates the subquery.")
      // Sanity: query still executes correctly
      assert(result != null)
    }
  }

  test("correlated-ref: q23a-shape uncorrelated IN subquery IS cached") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Counter-example to the q1-shape test: an IN subquery is NOT
      // correlated (no outerAttrs). RewritePredicateSubquery converts it
      // into a LeftSemi join. TagCorrelatedCTERefs must NOT tag this case
      // (outerAttrs is empty), so the CTE remains cache-eligible. q23a/q24a
      // depend on this distinction.
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key, a.total FROM agg a
          |WHERE a.key IN (SELECT key FROM agg WHERE total > 0)
          |""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Uncorrelated IN subquery must NOT be tagged as correlated; the CTE " +
        "should remain cache-eligible. q23a/q24a-shape queries depend on this.")
    }
  }

  test("correlated-ref: q1-shape produces same results with flag on vs off") {
    prepareData()
    val sql =
      """WITH agg AS (
        |  SELECT key, sum(value) as total FROM auto_cte_corr_test GROUP BY key
        |)
        |SELECT a.key, a.total FROM agg a
        |WHERE a.total > (SELECT avg(b.total) * 1.2 FROM agg b WHERE a.key = b.key)
        |ORDER BY a.key
        |""".stripMargin

    val baseline = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      spark.sql(sql).collect().map(_.toString).toSeq
    }
    val withFlag = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(sql).collect().map(_.toString).toSeq
    }
    assert(baseline == withFlag,
      s"q1-shape correlated subquery must produce identical results " +
      s"regardless of AUTO_REUSED_CTE_ENABLED. " +
      s"baseline.size=${baseline.size} withFlag.size=${withFlag.size}")
  }

  test("correlated-ref: CTE inside a non-correlated subquery whose body has " +
       "a correlated reference to the CTE is tagged") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // The outer query has a non-correlated IN subquery. Inside that subquery,
      // a correlated scalar subquery references the SAME CTE. TagCorrelatedCTERefs
      // must recursively walk into the non-correlated outer subquery to find
      // the correlated inner one and tag the CTE.
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key FROM agg a
          |WHERE a.key IN (
          |  SELECT b.key FROM agg b
          |  WHERE b.total > (SELECT avg(c.total) FROM agg c WHERE c.key = b.key)
          |)
          |""".stripMargin

      val result = spark.sql(sql).collect()
      assert(cachedEntries == 0,
        "Nested correlated subquery must tag the inner CTE; " +
        "TagCorrelatedCTERefs must walk through SubqueryExpressions recursively " +
        "even when the outer SubqueryExpression is itself non-correlated.")
      assert(result != null)
    }
  }

  test("SPARK-40193: cross-query reuse survives subquery merging") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val cteBody =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_corr_test
          |  WHERE value > (SELECT AVG(value) FROM auto_cte_corr_test)
          |  GROUP BY key
          |)""".stripMargin

      val q1 = cteBody +
        "\nSELECT a.total + b.total FROM cte a JOIN cte b ON a.key = b.key"
      val q2 = cteBody +
        "\nSELECT a.total * b.total FROM cte a JOIN cte b ON a.key = b.key"

      spark.sql(q1).collect()
      val afterQ1 = cachedEntries
      assert(afterQ1 >= 1, "q1 should populate the auto-CTE cache")

      spark.sql(q2).collect()
      val afterQ2 = cachedEntries
      assert(afterQ2 == afterQ1,
        s"q2 should reuse q1's cache via sameResult() match, not create a new entry. " +
        s"before=$afterQ1 after=$afterQ2 - sameResult() match is broken, " +
        s"possibly due to asymmetric rewriting by SPARK-40193 or another optimizer rule")
    }
  }

  // ---------------------------------------------------------------------------
  // Stats-based gate tests (AUTO_CTE_CACHE_MIN_SIZE_BYTES)
  // ---------------------------------------------------------------------------
  // The suite-level sparkConf above sets minSizeBytes=0, so the existing tests
  // only exercise the structural gate. The tests in this section explicitly
  // set the threshold to verify the size-based gate.

  test("stats gate: tiny CTE with Sort-only is NOT cached when threshold is non-zero") {
    prepareTinyData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      // Threshold set high enough that the 10-row CTE cannot clear it.
      SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "1048576") {
      val sql =
        """WITH tiny AS (
          |  SELECT key, value FROM auto_cte_tiny_test ORDER BY key
          |)
          |SELECT a.key, b.value FROM tiny a JOIN tiny b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries == 0,
        "Tiny CTE that passes the structural gate (Sort) but fails the stats " +
        "gate (sizeInBytes < threshold) must NOT be cached. Otherwise we waste " +
        "memory materialising small CTEs that are cheaper to recompute inline.")
    }
  }

  test("stats gate: large CTE IS cached when threshold is small") {
    prepareData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      // Threshold low enough that the 10000-row CTE clears it easily.
      SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "1024") {
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total
          |  FROM auto_cte_corr_test GROUP BY key
          |)
          |SELECT a.key, a.total + b.total FROM agg a JOIN agg b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "Large CTE with expensive body must be cached when stats clear the " +
        "configured threshold. Both gates pass.")
    }
  }

  test("stats gate: tiny CTE IS cached when threshold is zero (suite default)") {
    // Sanity test confirming the suite-level minSizeBytes=0 override works:
    // the structural gate alone is enough.
    prepareTinyData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH agg AS (
          |  SELECT key, sum(value) as total FROM auto_cte_tiny_test GROUP BY key
          |)
          |SELECT a.key, a.total + b.total FROM agg a JOIN agg b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries >= 1,
        "With minSizeBytes=0 the stats gate is effectively disabled; tiny " +
        "structurally-expensive CTEs should still cache.")
    }
  }

  test("stats gate: scan-only CTE is NOT cached even when stats are huge") {
    prepareData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0") {
      // Scan-only: structural gate fails regardless of size. Confirms that the
      // structural gate is checked BEFORE the stats gate, not replaced by it.
      val sql =
        """WITH scan_only AS (
          |  SELECT key, value FROM auto_cte_corr_test WHERE key < 50
          |)
          |SELECT a.key, b.value FROM scan_only a JOIN scan_only b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(cachedEntries == 0,
        "Scan-only CTE must remain non-cacheable; the structural gate is " +
        "checked first and short-circuits before the stats gate.")
    }
  }

  // ---------------------------------------------------------------------------
  // O(1) plan-index tests (recordAccessByPlan)
  // ---------------------------------------------------------------------------
  // The previous implementation iterated the entire Guava cache on every
  // recordAccessByPlan call, doing sameResult comparisons one entry at a time.
  // The new implementation maintains a secondary ConcurrentHashMap keyed on
  // canonicalized plans for O(1) average-case lookup. These tests verify the
  // index is built, used, and cleaned up correctly.

  test("plan index: trackEntry populates the index") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val mgr = spark.sharedState.autoCTECacheManager
      val before = mgr.planIndexSize
      // Run a query that will cache one CTE
      spark.sql(
        """WITH agg AS (SELECT key, sum(value) as t FROM auto_cte_corr_test GROUP BY key)
          |SELECT a.key FROM agg a JOIN agg b ON a.key = b.key""".stripMargin).collect()
      val after = mgr.planIndexSize
      assert(after > before,
        s"Plan index should grow when an entry is tracked: before=$before after=$after")
      assert(after == mgr.numEntries,
        s"Plan index size should match cache size for distinct plans: " +
        s"index=$after cache=${mgr.numEntries}")
    }
  }

  test("plan index: cross-query reuse hits the index without re-tracking") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val mgr = spark.sharedState.autoCTECacheManager
      val cteBody =
        """WITH agg AS (SELECT key, sum(value) as t FROM auto_cte_corr_test GROUP BY key)"""
      // Both queries must reference the SAME set of CTE columns. Column
      // pruning can otherwise produce different cached plans for what looks
      // like the same CTE body, defeating cross-query reuse.
      val q1 = s"$cteBody\nSELECT a.t + b.t FROM agg a JOIN agg b ON a.key = b.key"
      val q2 = s"$cteBody\nSELECT a.t * b.t FROM agg a JOIN agg b ON a.key = b.key"

      spark.sql(q1).collect()
      val cacheAfter1 = mgr.numEntries
      val indexAfter1 = mgr.planIndexSize
      assert(cacheAfter1 > 0, "First query should populate the cache")

      spark.sql(q2).collect()
      val cacheAfter2 = mgr.numEntries
      val indexAfter2 = mgr.planIndexSize
      assert(cacheAfter2 == cacheAfter1,
        s"Cross-query reuse should not grow the cache: before=$cacheAfter1 after=$cacheAfter2")
      assert(indexAfter2 == indexAfter1,
        s"Cross-query reuse should not grow the plan index: " +
        s"before=$indexAfter1 after=$indexAfter2. The index key (built from " +
        s"`QueryExecution.normalize(plan).canonicalized`) must match what " +
        s"`CacheManager.lookupCachedData` uses, otherwise the second query " +
        s"will silently re-track an already-cached entry.")
    }
  }

  test("plan index: clearAll empties both the cache and the index") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val mgr = spark.sharedState.autoCTECacheManager
      spark.sql(
        """WITH agg AS (SELECT key, sum(value) as t FROM auto_cte_corr_test GROUP BY key)
          |SELECT a.key FROM agg a JOIN agg b ON a.key = b.key""".stripMargin).collect()
      assert(mgr.numEntries > 0)
      assert(mgr.planIndexSize > 0)
      mgr.clearAll(spark)
      assert(mgr.numEntries == 0,
        s"clearAll should empty the cache; left ${mgr.numEntries}")
      assert(mgr.planIndexSize == 0,
        s"clearAll should empty the plan index; left ${mgr.planIndexSize}")
    }
  }

  test("plan index: TTL eviction also cleans the index via removalListener") {
    import org.apache.spark.sql.execution.AutoCTECacheManager
    // Manager with a 1 ms TTL so we can deterministically trigger eviction.
    val shortMgr = new AutoCTECacheManager(ttlMs = 1, maxSizeBytes = -1)
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val plan = spark.sql(
        "SELECT key, sum(value) FROM auto_cte_corr_test GROUP BY key")
        .queryExecution.optimizedPlan
      shortMgr.trackEntry(99L, plan)
      assert(shortMgr.numEntries == 1)
      assert(shortMgr.planIndexSize == 1)
      Thread.sleep(20)
      shortMgr.evictStaleEntries(spark)
      assert(shortMgr.numEntries == 0,
        s"TTL eviction should drain the cache; left ${shortMgr.numEntries}")
      assert(shortMgr.planIndexSize == 0,
        "TTL eviction's removalListener should also clean up the plan index; " +
        s"left ${shortMgr.planIndexSize} buckets")
    }
  }
}
