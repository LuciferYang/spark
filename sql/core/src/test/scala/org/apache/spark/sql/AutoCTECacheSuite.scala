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

import org.apache.spark.sql.catalyst.plans.logical.{CTERelationRef, RepartitionByExpression}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.StorageLevel

class AutoCTECacheSuite extends QueryTest with SharedSparkSession {

  // Disable the size-based gate so the existing structural-gate tests with
  // small range() data still cache. Stats-gate behavior is exercised
  // separately by AutoCTECacheCorrectnessSuite.
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
      .write.mode("overwrite").saveAsTable("auto_cte_test")
  }

  // A non-deterministic CTE with Aggregate -- won't be inlined by InlineCTE
  // (non-deterministic + refCount >= 2) and passes isExpensiveEnough (has Aggregate).
  //
  // It then passes `shouldAutoCache`'s FIRST gate, `cteDef.deterministic`, only because the
  // outer query does not select `r`: column pruning drops the `rand()` before the cache rule
  // runs, so the body that reaches it is deterministic. The fixture therefore leans on two
  // different determinism verdicts -- `InlineCTE`'s, on the body WITH `rand()`, and the cache
  // rule's, on the pruned body. If column pruning ever stops firing here, tests built on this
  // fixture fail for a reason unrelated to what they assert.
  private val cachableCteSQL =
    """WITH cte AS (
      |  SELECT key, sum(value) as total, rand() as r
      |  FROM auto_cte_test GROUP BY key
      |)
      |SELECT a.key, a.total, b.total
      |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

  // `cachableCteSQL` is non-deterministic (`rand()`), so it can never be inlined and always
  // keeps a CTE. The body-size tests below need a deterministic body, which only survives
  // `InlineCTE` via the auto-cache carve-out.
  private val deterministicCteSQL =
    """WITH cte AS (
      |  SELECT key, sum(value) as total
      |  FROM auto_cte_test GROUP BY key
      |)
      |SELECT a.key, a.total, b.total
      |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

  private def optimizedPlanOf(sqlText: String) =
    spark.sql(sqlText).queryExecution.optimizedPlan

  test("row-expanding body: a join with no collapsing operator is never cached") {
    prepareData()
    // TPC-DS q95's `ws_wh` shape: a self-join with nothing above it that bounds the output by a
    // key space, so materializing it can only store more rows than it reads. Measured 15.3x
    // SLOWER than the inlined baseline at 100TB.
    val sql =
      """WITH cte AS (
        |  SELECT a.key AS k
        |  FROM auto_cte_test a JOIN auto_cte_test b
        |    ON a.key = b.key AND a.col1 <> b.col1
        |)
        |SELECT x.k FROM cte x JOIN cte y ON x.k = y.k""".stripMargin
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(sql).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "A row-expanding body must not be cached")
      val plan = optimizedPlanOf(sql)
      // Both `InlineCTE` and `ReplaceCTERefWithCache` apply the same structural predicate, so
      // the def is inlined rather than left for `ReplaceCTERefWithRepartition` -- which on this
      // shape measured 22 min against 1.4 min for inlining.
      assert(plan.collectWithSubqueries { case r: CTERelationRef => r }.isEmpty,
        s"row-expanding body should be inlined:\n$plan")
      assert(plan.collectWithSubqueries { case r: RepartitionByExpression => r }.isEmpty,
        s"row-expanding body must not get the extra shuffle:\n$plan")
    }
  }

  test("row-expanding body: an aggregate above the join makes it cacheable") {
    prepareData()
    val sql =
      """WITH cte AS (
        |  SELECT a.key AS k, count(*) AS c
        |  FROM auto_cte_test a JOIN auto_cte_test b
        |    ON a.key = b.key AND a.col1 <> b.col1
        |  GROUP BY a.key
        |)
        |SELECT x.k, x.c, y.c FROM cte x JOIN cte y ON x.k = y.k""".stripMargin
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(sql).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "An aggregate above the join bounds the output, so the body is cacheable")
    }
  }

  test("body size ceiling: skip caching a CTE body over maxBodySizeBytes") {
    prepareData()
    withSQLConf(
        SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
        SQLConf.AUTO_CTE_CACHE_MAX_BODY_SIZE_BYTES.key -> "1") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "Should not cache a body whose estimated size exceeds maxBodySizeBytes")
    }
  }

  test("body size ceiling: an oversized body is inlined, not repartitioned") {
    prepareData()
    // An oversized body must not merely be declined by the cache rule: it must also avoid the
    // round-robin shuffle `ReplaceCTERefWithRepartition` inserts for a non-inlined CTE. On
    // TPC-DS q95 that shuffle (plus the local sort `sortBeforeRepartition` adds before a
    // round-robin, plus the dedup aggregate it strands above a now key-agnostic partitioning)
    // ran 22 min against 8 min for the useless cache and 1.4 min for plain inlining.
    withSQLConf(
        SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
        SQLConf.AUTO_CTE_CACHE_MAX_BODY_SIZE_BYTES.key -> "1") {
      val plan = optimizedPlanOf(deterministicCteSQL)
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0)
      assert(plan.collectWithSubqueries { case r: RepartitionByExpression => r }.isEmpty,
        s"Oversized body must not be wrapped in a repartition shuffle:\n$plan")
      assert(plan.collectWithSubqueries { case r: CTERelationRef => r }.isEmpty,
        s"Oversized body should have been inlined:\n$plan")
    }
  }

  test("body size ceiling: a declined-for-other-reasons body still repartitions") {
    prepareData()
    // Under the ceiling, so the SKIP_EXTRA_REPARTITION tag must NOT be set, and declined for a
    // different reason (`storageLevel=NONE`). That case must keep the pre-existing
    // shuffle-sharing fallback -- inlining a non-deterministic body per reference would let each
    // reference see different `rand()` values.
    withSQLConf(
        SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
        SQLConf.AUTO_CTE_CACHE_MAX_BODY_SIZE_BYTES.key -> "100g",
        SQLConf.AUTO_CTE_CACHE_STORAGE_LEVEL.key -> "NONE") {
      val plan = optimizedPlanOf(cachableCteSQL)
      assert(plan.collectWithSubqueries { case r: RepartitionByExpression => r }.nonEmpty,
        s"A non-oversized declined body must still get the repartition:\n$plan")
    }
  }

  test("body size ceiling: -1 disables the ceiling") {
    prepareData()
    withSQLConf(
        SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
        SQLConf.AUTO_CTE_CACHE_MAX_BODY_SIZE_BYTES.key -> "-1") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "Ceiling disabled, so the body should still be cached")
    }
  }

  test("body size ceiling: a body under the ceiling is still cached") {
    prepareData()
    withSQLConf(
        SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
        SQLConf.AUTO_CTE_CACHE_MAX_BODY_SIZE_BYTES.key -> "100g") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "A small body must not be rejected by a generous ceiling")
    }
  }

  test("auto-cache CTE when enabled") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "Should have auto-cached the CTE")
    }
  }

  test("no auto-cache when disabled") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0)
    }
  }

  test("correctness: auto-cached CTE produces same results") {
    prepareData()
    val sql =
      """WITH cte AS (
        |  SELECT key, count(*) as cnt, rand() as r
        |  FROM auto_cte_test GROUP BY key
        |)
        |SELECT a.key, a.cnt + b.cnt as total_cnt
        |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

    val baseline = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
      spark.sql(sql).selectExpr("key", "total_cnt").collect()
    }
    val optimized = withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(sql).selectExpr("key", "total_cnt").collect()
    }
    assert(baseline.length == optimized.length,
      s"Row count mismatch: ${baseline.length} vs ${optimized.length}")
  }

  test("within-query reuse: multiple references share one cache entry") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "Two references to the same CTE should share one cache entry")
    }
  }

  test("cross-query CTE reuse via plan normalization") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {

      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1)

      // Same CTE body, different outer query. QueryExecution.normalize
      // normalizes rand() seeds so the plans match via sameResult().
      val sql2 =
        """WITH cte AS (
          |  SELECT key, sum(value) as total, rand() as r
          |  FROM auto_cte_test GROUP BY key
          |)
          |SELECT a.total + b.total as combined
          |FROM cte a JOIN cte b ON a.key = b.key""".stripMargin

      spark.sql(sql2).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "Should reuse cached CTE across queries")
    }
  }

  /**
   * The `WithCTE` wrapper around a body about to be cached must carry only the skipped
   * siblings that body can reach. The wrapped plan IS the cache key, so an unrelated sibling
   * inside it makes two queries that share the body miss each other's entry and materialise
   * the same data twice -- the opposite of what `prePushdownBody` exists for.
   *
   * `noise` and `shared` are scan-only, so `isExpensiveEnough` declines them and they land in
   * `skippedDefs`; they are also non-deterministic, which is what keeps `InlineCTE` from
   * inlining them away before the cache rule ever sees them. `heavy` has a join and an
   * aggregate, so it is kept and cached, and it references `shared`, which is what triggers
   * the wrap. `noise` is declared FIRST because `skippedDefs` accumulates in declaration
   * order -- declared after `heavy` it would not be in the buffer yet when `heavy` is
   * processed.
   *
   * The two queries differ only in `noise`'s filter constant, which `heavy` never references.
   * The constant rather than the `rand` seed is what varies, because `QueryExecution.normalize`
   * normalizes seeds and the difference would disappear. `noise` sits in a separate `UNION ALL`
   * branch rather than joined to `heavy`: joined, `InferFiltersFromConstraints` carries the
   * constant across the join key into `heavy`'s own body, and the two queries then differ for a
   * reason that has nothing to do with the wrapper.
   *
   * Asserted on `cacheBuilder` identity, NOT on `numEntries`. The manager's Guava cache is
   * keyed by `cteDef.id` and those ids restart per query, so a second, differently-keyed
   * `CachedData` overwrites the tracking row for the first and `numEntries` still reads 1.
   * Measured: with the wrapper carrying every skipped def, the two normalized cache keys were
   * 1584311006 and 595407320 -- two distinct materializations -- while `numEntries` stayed 1.
   */
  test("cache key: an unrelated skipped sibling must not split the entry") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      def query(noiseFilter: Int): String =
        s"""WITH noise AS (
           |  SELECT key, rand(1) AS r FROM auto_cte_test WHERE key < $noiseFilter
           |),
           |shared AS (
           |  SELECT key, value, rand(7) AS r FROM auto_cte_test
           |),
           |heavy AS (
           |  SELECT s1.key, sum(s1.value) AS total
           |  FROM shared s1 JOIN shared s2 ON s1.key = s2.key
           |  GROUP BY s1.key
           |)
           |SELECT h1.key, h1.total FROM heavy h1 JOIN heavy h2 ON h1.key = h2.key
           |UNION ALL
           |SELECT n1.key, n1.r FROM noise n1 JOIN noise n2 ON n1.key = n2.key""".stripMargin

      def builderIds(sqlText: String): Set[Int] = {
        // `collect()`, not just `optimizedPlan`: publication now happens at execution
        // (`SQLExecution.withNewExecutionId` -> `AutoCTECacheManager.publishPending`), so a
        // plan-only run registers nothing for the next query to reuse -- which is the point of
        // review item 1. The plan is read from the same DataFrame after the action.
        val df = spark.sql(sqlText)
        df.collect()
        df.queryExecution.optimizedPlan.collect {
          case r: InMemoryRelation => System.identityHashCode(r.cacheBuilder)
        }.toSet
      }

      val first = builderIds(query(11))
      assert(first.size == 1, s"expected `heavy` to be the one cached body, got $first")
      val second = builderIds(query(23))
      assert(second == first,
        s"the second query changed only `noise`, which `heavy` does not reference, so it must " +
        s"reuse the cached body instead of materialising a second copy of the same data " +
        s"(first=$first second=$second)")
    }
  }

  /**
   * Two different bodies materialised by two queries must each keep a tracking row. The
   * manager used to key its Guava cache by `cteDef.id`, and those ids collide across queries,
   * so the second `put` replaced the first row. A replacement is not an eviction, so the
   * `removalListener` never ran: the first body stayed registered in `CacheManager` but
   * untracked here, which means its TTL never fires and `clearAll` never unpersists it.
   */
  test("tracking: two different cached bodies must not share one tracking row") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(cachableCteSQL).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "the first query must cache its body")
      spark.sql(cachableCteSQL.replace("sum(value)", "avg(value)")).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 2,
        "the second query materialised a DIFFERENT body, so both must stay tracked -- " +
        "otherwise the first one never expires and clearAll never unpersists it")
    }
  }

  /**
   * Review item 1: planning must not publish. `cacheQuery` registers into the `CacheManager` that
   * every session sharing this `SharedState` consults, so calling it from an optimizer rule meant
   * one `EXPLAIN` -- or one read of `optimizedPlan` -- handed an entry to unrelated queries, which
   * would then `sameResult`-hit it and materialise it for real. The rule now only prepares the
   * relation; `SQLExecution.withNewExecutionId` publishes it, so the registry moves only when a
   * query actually runs.
   */
  test("publication waits for execution: EXPLAIN and optimizedPlan register nothing") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(s"EXPLAIN $cachableCteSQL").collect()
      assert(spark.sharedState.cacheManager.isEmpty,
        "EXPLAIN must not put an entry into the shared CacheManager")
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "EXPLAIN must not start a TTL clock either")

      optimizedPlanOf(cachableCteSQL)
      assert(spark.sharedState.cacheManager.isEmpty,
        "reading optimizedPlan must not register anything: a lineage tool or df.explain() " +
        "would otherwise publish a cache entry other sessions can hit")

      spark.sql(cachableCteSQL).collect()
      assert(!spark.sharedState.cacheManager.isEmpty,
        "an executed query must publish, otherwise cross-query reuse never happens")
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1)
    }
  }

  test("skip caching for scan-only CTE") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      val sql =
        """WITH simple_cte AS (
          |  SELECT key, value, rand() as r
          |  FROM auto_cte_test WHERE key < 50
          |)
          |SELECT a.key, b.value
          |FROM simple_cte a JOIN simple_cte b ON a.key = b.key""".stripMargin

      spark.sql(sql).collect()
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "Should not cache scan-only CTE (not expensive enough)")
    }
  }

  private def autoCTEStorageLevels(sqlText: String): Seq[StorageLevel] =
    spark.sql(sqlText).queryExecution.optimizedPlan.collect {
      case r: InMemoryRelation
          if r.cacheBuilder.tableName.exists(_.startsWith("auto_cte_")) =>
        r.cacheBuilder.storageLevel
    }.distinct

  test("storage level: default is MEMORY_ONLY") {
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      spark.sql(cachableCteSQL).collect()
      val levels = autoCTEStorageLevels(cachableCteSQL)
      assert(levels.length == 1, s"expected one auto-CTE level, got $levels")
      assert(levels.head == StorageLevel.MEMORY_ONLY)
    }
  }

  test("storage level: custom value is honored") {
    prepareData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.AUTO_CTE_CACHE_STORAGE_LEVEL.key -> "MEMORY_AND_DISK") {
      spark.sql(cachableCteSQL).collect()
      val levels = autoCTEStorageLevels(cachableCteSQL)
      assert(levels.length == 1)
      assert(levels.head == StorageLevel.MEMORY_AND_DISK)
    }
  }

  test("storage level: invalid value rejected at conf-set time") {
    intercept[IllegalArgumentException] {
      withSQLConf(SQLConf.AUTO_CTE_CACHE_STORAGE_LEVEL.key -> "NOT_A_LEVEL") {}
    }
  }

  test("storage level: lowercase value is normalized") {
    prepareData()
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.AUTO_CTE_CACHE_STORAGE_LEVEL.key -> "memory_and_disk") {
      spark.sql(cachableCteSQL).collect()
      val levels = autoCTEStorageLevels(cachableCteSQL)
      assert(levels.length == 1)
      assert(levels.head == StorageLevel.MEMORY_AND_DISK)
    }
  }

  test("TTL-based eviction") {
    import org.apache.spark.sql.execution.AutoCTECacheManager
    // Create a manager with 1ms TTL directly (these configs are not session-bindable)
    val shortTtlManager = new AutoCTECacheManager(ttlMs = 1, maxSizeBytes = -1)
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true") {
      // Manually track an entry to simulate caching
      val plan = spark.sql(
        "SELECT key, sum(value) as total FROM auto_cte_test GROUP BY key")
        .queryExecution.optimizedPlan
      shortTtlManager.trackEntry(1L, plan)
      assert(shortTtlManager.numEntries == 1)

      Thread.sleep(10)
      shortTtlManager.evictStaleEntries(spark)

      assert(shortTtlManager.numEntries == 0,
        "Should have evicted expired CTE cache entries")
    }
  }

  test("excludedRules cannot switch off ReplaceCTERefWithCache or TagCorrelatedCTERefs") {
    // The four auto-CTE rules are one unit, and `InlineCTE`'s carve-out keys off
    // `AUTO_REUSED_CTE_ENABLED` rather than off whether the cache rule is present. So if
    // `excludedRules` could drop `ReplaceCTERefWithCache`, a multi-reference CTE would be
    // neither inlined nor cached and would fall through to `ReplaceCTERefWithRepartition`,
    // which by its own comment produces an unresolved plan for a CTE `InlineCTE` did not
    // dedup. `nonExcludableRules` is what prevents that; dropping either name from it
    // reddens this test.
    //
    // Note `Rule.ruleName` strips the trailing `$` of an object, so the conf value has to be
    // the class name without it -- a value with `$` excludes nothing and the test would pass
    // for the wrong reason. Building the value from `ruleName` keeps the two in step.
    prepareData()
    val excluded = Seq(
      org.apache.spark.sql.execution.ReplaceCTERefWithCache.ruleName,
      org.apache.spark.sql.catalyst.optimizer.TagCorrelatedCTERefs.ruleName)
    assert(excluded.forall(!_.endsWith("$")), s"ruleName should not carry a '$$': $excluded")
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excluded.mkString(",")) {
      val df = spark.sql(deterministicCteSQL)
      // `collect()` first: materialisation is deferred to execution, so a plan-only run
      // registers nothing.
      val rows = df.collect()
      assert(rows.nonEmpty)
      assert(spark.sharedState.autoCTECacheManager.numEntries == 1,
        "the cache rule must still run even though excludedRules names it")
      assert(df.queryExecution.optimizedPlan.exists(_.isInstanceOf[InMemoryRelation]),
        "the CTE body must still be replaced by an InMemoryRelation")
      assert(!df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionByExpression]),
        "falling through to ReplaceCTERefWithRepartition is the failure this guards against")
    }
  }

  test("a recursive CTE under LIMIT ALL is inlined, not carved out for caching") {
    // `LIMIT ALL` over a reference to a recursive CTE means unlimited recursion, and
    // `ApplyLimitAll` records that on the ref as `isUnlimitedRecursion`. The semantics are
    // applied by `InlineCTE.setUnlimitedRecursion`, whose result is consumed only on the
    // inlining path -- so if the auto-cache carve-out keeps the def, `UnionLoop.limit` stays
    // `None`, execution falls back to `spark.sql.cteRecursionRowLimit`, and a query that
    // returns complete results with the feature off fails with RECURSION_ROW_LIMIT_EXCEEDED.
    //
    // The body needs an `Aggregate` so it passes the carve-out's structural gate (a Join
    // instead would be rejected by `isRowExpanding`), and both references have to sit under
    // `LIMIT ALL`. Recursion runs to 60 rows against a limit of 50, so the row limit is what
    // fails if the carve-out wrongly fires. Dropping `!hasUnlimitedRecursionRef` from
    // `InlineCTE.shouldInline` reddens this test.
    val sql =
      """WITH RECURSIVE t MAX RECURSION LEVEL 100 AS (
        |  SELECT max(c) AS n FROM VALUES (1) AS v(c)
        |  UNION ALL
        |  SELECT n + 1 FROM t WHERE n < 60
        |)
        |(SELECT n FROM t LIMIT ALL) UNION ALL (SELECT n FROM t LIMIT ALL)""".stripMargin
    withSQLConf(
      SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
      SQLConf.CTE_RECURSION_ROW_LIMIT.key -> "50") {
      val rows = spark.sql(sql).collect()
      assert(rows.length == 120, "each of the two LIMIT ALL references must return 60 rows")
      assert(spark.sharedState.autoCTECacheManager.numEntries == 0,
        "the def must be inlined, so nothing is cached")
    }
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS auto_cte_test")
    } finally {
      super.afterAll()
    }
  }
}
