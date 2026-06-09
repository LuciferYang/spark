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

import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, IsNotNull}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * `injectImpliedPredicate`: cache the pre-pushdown body plus ONLY the implied,
 * fully-pushable part of the merged predicate, rather than the body carrying the whole
 * merged predicate.
 *
 * WHY THIS EXISTS. `keepMergedPredicate` alone fires on TPC-DS q4 and moves nothing.
 * Its merged predicate is `(dyear = 2001 AND year_total > 0) OR (dyear = 2002)`, a single
 * top-level conjunct whose reference set includes an aggregate output, and
 * `PushPredicateThroughNonJoin` (`Optimizer.scala:1837-1840`) splits by top-level conjunct
 * and judges each whole -- so it pushes nothing and the body's date_dim scan still reads all
 * 73049 rows. Measured on 100TB non-partitioned ORC (app_1785399804759_0081 execution 4):
 * the gate fired, the cached body carried the predicate, and the scan showed only
 * `IsNotNull`. The gate had reasoned from `extractPredicatesWithinOutputSet`, which reaches
 * INSIDE a conjunct, about a rule that does not.
 *
 * `(A AND B) OR C` implies `A OR C`, and that implied part reads only grouping columns, so
 * as its OWN top-level conjunct it does push. Same helper, used as a rewrite instead of as a
 * verdict.
 *
 * The two properties these tests pin, in order of what a bug would cost:
 *
 *   1. IMPLICATION. The injected predicate must not drop a row the merged predicate would
 *      have kept, or the cached body is missing rows some reference needs and the query
 *      returns a wrong answer. "answers are identical across all three body shapes" pins it,
 *      over data `createSales` builds so that a non-implied extraction changes the count.
 *   2. IT ACTUALLY PUSHES. The point is reaching the scan. "the injected part reaches the scan,
 *      and the merged predicate does not" asserts the year filter appears in the scan's
 *      `dataFilters` with injection on and NOT with it off -- the local, miniature form of the
 *      cluster signal above.
 *
 * The fixtures are synthetic rather than TPC-DS so each shape can be written directly;
 * `AutoCteInjectionProbe` holds the sf100 plan sweep that established q4/q11 are the
 * queries this targets.
 */
class AutoCteImpliedInjectionSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper with PredicateHelper {

  /** `inject` only means anything when `keep` is true; both are exercised. */
  private def conf(keep: Boolean, inject: Boolean): Seq[(String, String)] = Seq(
    SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
    SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1",
    SQLConf.AUTO_CTE_CACHE_KEEP_MERGED_PREDICATE.key -> keep.toString,
    SQLConf.AUTO_CTE_CACHE_INJECT_IMPLIED_PREDICATE.key -> inject.toString,
    SQLConf.CBO_ENABLED.key -> "true",
    SQLConf.PLAN_STATS_ENABLED.key -> "true")

  private def cachedBodies(sqlText: String): Seq[org.apache.spark.sql.execution.SparkPlan] = {
    spark.sharedState.autoCTECacheManager.clearAll(spark)
    val bodies = spark.sql(sqlText).queryExecution.optimizedPlan.collectWithSubqueries {
      case imr: InMemoryRelation => imr.cacheBuilder.cachedPlan
    // `.distinct` is load-bearing for any COUNTING assertion. `ReplaceCTERefWithCache` puts
    // one `InMemoryRelation` at every CTE reference, and the deduplicated copies share one
    // `cacheBuilder`, so `cachedPlan` is the SAME instance N times over. Without this, a
    // union test asserting "the predicate reached both branches" is satisfied by one
    // branch's filter counted twice.
    }.distinct
    assert(bodies.nonEmpty, s"nothing was auto-cached for:\n$sqlText")
    bodies
  }

  /**
   * What the cached body's SCANS filter on. This is the reading that matters: a predicate
   * sitting in a `FilterExec` above the aggregate costs the entry its reuse and saves no
   * work, which is exactly the defect this conf addresses.
   *
   * `dropIsNotNull` defaults to true because the analyzer adds those everywhere, but it has
   * to be switchable: `narrowsRows` rejects an `IsNotNull`-only residual, and with the
   * conjuncts dropped a body carrying an injected `isnotnull(dyear)` is indistinguishable
   * from one carrying nothing.
   */
  private def scanFilters(sqlText: String, dropIsNotNull: Boolean = true): Seq[String] =
    cachedBodies(sqlText).flatMap { body =>
      collect(body) { case f: FileSourceScanExec => f.dataFilters }.flatten.filterNot {
        case _: IsNotNull => dropIsNotNull
        case _ => false
      }.map(_.sql)
    }

  /** Every predicate in the cached body, scan-level or not. */
  private def allFilters(sqlText: String, dropIsNotNull: Boolean = true): Seq[String] =
    cachedBodies(sqlText).flatMap { body =>
      collect(body) {
        case f: FilterExec => splitConjunctivePredicates(f.condition)
        case f: FileSourceScanExec => f.dataFilters
      }.flatten.filterNot {
        case _: IsNotNull => dropIsNotNull
        case _ => false
      }.map(_.sql)
    }

  /** Runtime Bloom filters (`might_contain`) in one materialised body. */
  private def bloomCount(body: org.apache.spark.sql.execution.SparkPlan): Int =
    collect(body) { case p: org.apache.spark.sql.execution.SparkPlan =>
      p.expressions.map(_.collect { case _: BloomFilterMightContain => 1 }.sum).sum
    }.sum

  /**
   * The 6000-row sales fixture every test here shares: 200 customers x 6 years x 5 rows.
   *
   * Customers 0-19 lose money IN 2002 ONLY. That year-specificity is the whole point and it
   * took a second pass to get right. With the loss applying to every year (the first
   * version) those customers also failed `t1.year_total > 0` in 2001, so the `t1` side
   * already excluded them and dropping their 2002 groups from the cached body left the count
   * at 180 either way -- the test that claimed to pin the implication property could not see
   * a violation of it. Restricted to 2002, all 200 customers pass on the `t1` side (correct
   * count 200) while injecting the un-implied `year_total > 0` erases 0-19's 2002 groups,
   * which `t2` needs, and the count drops to 180.
   */
  private def createSales(table: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.range(0, 6000)
      .selectExpr(
        "id % 200 AS cust",
        "2000 + CAST(id / 1000 AS INT) AS yr",
        "CASE WHEN id % 200 < 20 AND CAST(id / 1000 AS INT) = 2 THEN -1000 ELSE id END AS amt")
      .write.saveAsTable(table)
    spark.sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR ALL COLUMNS")
  }

  /**
   * The q4 shape that matters: each reference filters on a grouping column AND on an
   * aggregate output, so the merged predicate is `(dyear = 2001 AND year_total > 0) OR
   * (dyear = 2002)` -- pushable in part, unpushable as a whole.
   */
  private def withMixedShape(f: String => Unit): Unit = {
    withTable("sales") {
      createSales("sales")
      f(
        """WITH yt AS (
          |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total
          |  FROM sales GROUP BY cust, yr)
          |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
          |WHERE t1.dyear = 2001 AND t1.year_total > 0 AND t2.dyear = 2002
          |""".stripMargin)
    }
  }

  test("the injected part reaches the scan, and the merged predicate does not") {
    withMixedShape { q =>
      var withoutInjection: Seq[String] = Nil
      var withInjection: Seq[String] = Nil
      withSQLConf(conf(keep = true, inject = false): _*) {
        withoutInjection = scanFilters(q)
      }
      withSQLConf(conf(keep = true, inject = true): _*) {
        withInjection = scanFilters(q)
      }
      // The defect, reproduced locally: keeping the whole merged predicate puts NO year
      // filter on the scan, because the single `Or` conjunct references `year_total`.
      assert(!withoutInjection.exists(p => p.contains("2001") || p.contains("2002")),
        s"keepMergedPredicate alone must NOT get a year filter onto the scan -- that is the " +
        s"q4 defect this conf fixes. Found $withoutInjection")
      // And the fix: the implied part is its own conjunct, so it pushes all the way down.
      assert(withInjection.exists(p => p.contains("2001") || p.contains("2002")),
        s"the injected `dyear = 2001 OR dyear = 2002` reads only grouping columns and is a " +
        s"top-level conjunct, so PushPredicateThroughNonJoin must move it below the " +
        s"aggregate and onto the scan. Found $withInjection")
    }
  }

  /**
   * The safety property, on data built so that violating it changes the answer.
   *
   * The injected predicate must be IMPLIED by the merged one: every row the merged predicate
   * keeps, the injected one keeps too. `(dyear = 2001 AND year_total > 0) OR (dyear = 2002)`
   * implies `dyear = 2001 OR dyear = 2002`, so the cached body is a SUPERSET of what any
   * reference asks for, and each reference re-applies its own predicate above the cache.
   *
   * The pinned number is 200, and it is pinned rather than merely compared so a fixture
   * change cannot quietly restore the vacuity `createSales` documents. All 200 customers
   * pass the `t1` side (2001 is profitable for everyone); injecting the un-implied
   * `year_total > 0` erases customers 0-19's 2002 groups, which `t2` needs, and the answer
   * becomes 180.
   */
  test("answers are identical across all three body shapes") {
    withMixedShape { q =>
      var preP: Seq[Row] = Nil
      var whole: Seq[Row] = Nil
      var injected: Seq[Row] = Nil
      var off: Seq[Row] = Nil
      withSQLConf(conf(keep = false, inject = false): _*) {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        preP = spark.sql(q).collect().toSeq
      }
      withSQLConf(conf(keep = true, inject = false): _*) {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        whole = spark.sql(q).collect().toSeq
      }
      withSQLConf(conf(keep = true, inject = true): _*) {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        injected = spark.sql(q).collect().toSeq
      }
      withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        off = spark.sql(q).collect().toSeq
      }
      // 200 customers, all of them profitable in 2001 and present in 2002. Asserted as a
      // literal because this exact number is what distinguishes an implied injection (200)
      // from the un-implied `year_total > 0` (180) the docstring names.
      assert(off.head.getLong(0) === 200,
        s"the fixture must return 200, or the comparisons below cannot see an un-implied " +
        s"injection. Got $off -- check `createSales`")
      assert(off === preP, "auto-CTE must not change the answer")
      assert(off === whole, "keepMergedPredicate must not change the answer")
      assert(off === injected,
        "the injected predicate is implied by the merged one, so it cannot drop a row any " +
        "reference needs")
    }
  }

  /**
   * The body must carry ONLY the implied part, not the whole merged predicate. This is the
   * difference between the two shapes, and it is what makes the injected entry usable by
   * more queries than the whole-predicate one: a second query filtering
   * `dyear = 2001 AND year_total > 500` gets the same injected body, since the injected
   * conjunct depends only on the set of years the references mention.
   */
  test("the cached body carries the implied part only, not the aggregate-output conjunct") {
    withMixedShape { q =>
      withSQLConf(conf(keep = true, inject = true): _*) {
        val filters = allFilters(q)
        assert(filters.exists(p => p.contains("2001") || p.contains("2002")),
          s"the year part must be in the body; found $filters")
        assert(!filters.exists(_.contains("year_total")),
          s"`year_total > 0` is NOT implied by the merged predicate's 2002 branch, so " +
          s"injecting it would drop rows the `dyear = 2002` reference needs. It must not " +
          s"appear in the injected body. Found $filters")
      }
    }
  }

  test("the conf is a real switch") {
    withMixedShape { q =>
      var whole: Seq[String] = Nil
      var injected: Seq[String] = Nil
      withSQLConf(conf(keep = true, inject = false): _*) { whole = allFilters(q) }
      withSQLConf(conf(keep = true, inject = true): _*) { injected = allFilters(q) }
      assert(whole.exists(_.contains("year_total")),
        s"with injection off the WHOLE merged predicate is cached, `year_total` included -- " +
        s"that is keepMergedPredicate's shipped behaviour. Found $whole")
      assert(!injected.exists(_.contains("year_total")),
        s"with injection on only the implied part is cached. Found $injected")
    }
  }

  /**
   * Injection is SUBORDINATE to `keepMergedPredicate`: with that off, the body must be the
   * plain pre-pushdown one whatever `injectImpliedPredicate` says, because a deployment that
   * set it off did so to keep cross-query reuse.
   *
   * Note this test does NOT reach the injection branch --
   * `pushablePartOfMergedPredicate` returns at its first line when the keep gate is off, so
   * `prePushdownBody` takes `case None`. What it pins is the guard ORDERING: a refactor that
   * consulted `injectImpliedPredicate` without going through the keep gate would break it.
   * Hence the assertion is entry SHARING with the both-off configuration, not merely the
   * absence of a year literal -- sharing one entry means the two bodies canonicalize
   * identically, which "no year literal appears" does not establish.
   */
  test("the inject conf is subordinate to the keep gate") {
    withMixedShape { q =>
      withSQLConf(conf(keep = false, inject = true): _*) {
        val filters = allFilters(q)
        assert(!filters.exists(p => p.contains("2001") || p.contains("2002")),
          s"with keepMergedPredicate off the body must stay query-independent; found $filters")
      }
      // Same body as both-off, proven by the second configuration finding the first's entry.
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      withSQLConf(conf(keep = false, inject = false): _*) { spark.sql(q).collect() }
      withSQLConf(conf(keep = false, inject = true): _*) { spark.sql(q).collect() }
      assert(spark.sharedState.autoCTECacheManager.numEntries === 1,
        "with the keep gate off, both settings of the inject conf must cache the SAME body, " +
        "so the second execution reuses the first's entry")
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }

  /**
   * Injection can RESTORE cross-query sharing that `keepMergedPredicate` gives up.
   *
   * Two queries whose reference predicates differ only in an aggregate-output THRESHOLD have
   * different MERGED predicates but the same IMPLIED part, so the whole-predicate bodies
   * differ while the injected bodies are identical. This is the real q39a/q39b shape:
   * `(d_moy=1) OR (d_moy=2)` against `(d_moy=1 AND cov>1.5) OR (d_moy=2)`, whose shared
   * entry `keepMergedPredicate` splits -- measured at 34.4s on the 100TB cluster.
   *
   * Both queries must reference the SAME COLUMNS. `PushdownPredicatesAndPruneColumnsForCTEDef`
   * prunes the body to the union of what the references read, so a query that never mentions
   * `year_total` gets a narrower body and the entries split on pruning alone, predicate
   * aside. The first version of this test had exactly that confound and reported 2 entries
   * under injection -- correctly, for the wrong reason.
   *
   * `keep=false` is the control: it establishes that these two queries CAN share, so the
   * `keep=true` split is the gate's doing and the injected join is a genuine recovery.
   */
  test("two queries differing only in an aggregate-output threshold share the injected body") {
    withTable("sales") {
      createSales("sales")
      // Identical column sets, identical years; only the threshold on the aggregate output
      // differs, and that is exactly the part extraction drops.
      def query(threshold: Int): String =
        s"""WITH yt AS (
           |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total
           |  FROM sales GROUP BY cust, yr)
           |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
           |WHERE t1.dyear = 2001 AND t1.year_total > $threshold AND t2.dyear = 2002
           |""".stripMargin
      val qa = query(0)
      val qb = query(500)

      // `numEntries` only grows on a cache MISS: the caching path calls `trackEntry` in the
      // `getOrElse` branch of `lookupCachedData` and merely refreshes the TTL on a hit. So
      // 1 after both queries means the second one read the first's entry.
      def entriesAfterBoth(keep: Boolean, inject: Boolean): Int = {
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        withSQLConf(conf(keep, inject): _*) {
          spark.sql(qa).collect()
          spark.sql(qb).collect()
        }
        spark.sharedState.autoCTECacheManager.numEntries
      }

      val prePushdown = entriesAfterBoth(keep = false, inject = false)
      val whole = entriesAfterBoth(keep = true, inject = false)
      val injected = entriesAfterBoth(keep = true, inject = true)
      assert(prePushdown === 1,
        s"CONTROL: the pre-pushdown body is query-independent, so these two queries share " +
        s"ONE entry. If this is not 1 the fixture differs in something other than the " +
        s"predicate and the rest of this test means nothing. Got $prePushdown")
      assert(whole === 2,
        s"the two merged predicates differ, so keepMergedPredicate must materialise TWO " +
        s"entries -- this is the reuse it gives up. Got $whole")
      assert(injected === 1,
        s"both queries imply the same `dyear = 2001 OR dyear = 2002`, so the injected " +
        s"bodies are identical and ONE entry serves both: injection buys the first-round " +
        s"saving WITHOUT this part of the reuse cost. Got $injected")
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }

  /**
   * NOTHING PUSHABLE means all three shapes collapse to the same body -- the "fails closed"
   * property. A predicate purely over an aggregate output cannot cross the aggregate, so
   * `extractPredicatesWithinOutputSet` leaves only the analyzer's `IsNotNull` conjuncts,
   * `narrowsRows` rejects those, and `pushablePartOfMergedPredicate` returns `None`.
   *
   * WHAT THIS TEST DOES NOT PIN, stated because an earlier docstring claimed the opposite:
   * it is NOT specific to `injectImpliedPredicate`. Control never reaches the injection arm,
   * so deleting that arm leaves `case None => pruned(originalPlan)` and every assertion below
   * holds unchanged. It is a regression test for the `None` path, which the two confs share
   * with the behaviour that shipped before either existed -- worth having, and worth not
   * mistaking for coverage of the new branch.
   *
   * The assertion is three-way body equality, checked through the cache: three configurations
   * materialising ONE entry between them means all three produced the same canonical body.
   * That is stronger than "no filters appear" and is what "fails closed" actually means.
   */
  test("nothing pushable means all three shapes cache the same body") {
    withTable("sales") {
      spark.range(0, 5000)
        .selectExpr("id % 200 AS cust", "id AS amt")
        .write.saveAsTable("sales")
      spark.sql("ANALYZE TABLE sales COMPUTE STATISTICS FOR ALL COLUMNS")
      def query(threshold: Int): String =
        s"""WITH yt AS (
           |  SELECT cust AS customer_id, sum(amt) AS year_total
           |  FROM sales GROUP BY cust)
           |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
           |WHERE t1.year_total > $threshold AND t2.year_total > 2000000
           |""".stripMargin
      withSQLConf(conf(keep = true, inject = true): _*) {
        val filters = allFilters(query(1000000))
        assert(filters.isEmpty,
          s"no part of this predicate can cross the aggregate, so the pre-pushdown body " +
          s"must be cached unchanged. Found $filters")
      }
      // All three configurations, same query, one shared entry: the bodies are identical.
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      withSQLConf(conf(keep = false, inject = false): _*) {
        spark.sql(query(1000000)).collect()
      }
      withSQLConf(conf(keep = true, inject = false): _*) {
        spark.sql(query(1000000)).collect()
      }
      withSQLConf(conf(keep = true, inject = true): _*) {
        spark.sql(query(1000000)).collect()
      }
      assert(spark.sharedState.autoCTECacheManager.numEntries === 1,
        "with nothing pushable all three configurations must cache the SAME body, so the " +
        "second and third executions reuse the first's entry")
      // And the consequence for reuse: two DIFFERENT queries also share it, because the body
      // stayed query-independent.
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      withSQLConf(conf(keep = true, inject = true): _*) {
        spark.sql(query(1000000)).collect()
        spark.sql(query(1500000)).collect()
      }
      assert(spark.sharedState.autoCTECacheManager.numEntries === 1,
        "nothing was injected, so the body stayed query-independent and the second query " +
        "must reuse the first's entry")
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }

  /**
   * A union body -- q4's real shape is three channels, q11's two. The injected conjunct has
   * to be expressed in the union's own attributes so `PushPredicateThroughNonJoin`'s Union
   * case (`Optimizer.scala:1881`) can rewrite it per branch; that case asserts
   * `newCond.references.subsetOf(grandchild.outputSet)`, so getting the attributes wrong
   * fails the query rather than merely missing the optimization.
   *
   * The two branches read DIFFERENT tables so "reached both branches" can be checked by
   * table rather than by counting. Counting was wrong: `cachedBodies` sees one
   * `InMemoryRelation` per CTE reference, so with two references every filter string appears
   * twice and a `count >= 2` assertion was satisfied by ONE branch counted twice. That is why
   * `cachedBodies` now dedupes.
   */
  test("a union-shaped body gets the injected part into every branch's scan") {
    withTable("s_sales", "c_sales") {
      Seq("s_sales", "c_sales").foreach(createSales)
      val q =
        """WITH yt AS (
          |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total
          |  FROM s_sales GROUP BY cust, yr
          |  UNION ALL
          |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total
          |  FROM c_sales GROUP BY cust, yr)
          |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
          |WHERE t1.dyear = 2001 AND t1.year_total > 0 AND t2.dyear = 2002
          |""".stripMargin
      var off: Seq[Row] = Nil
      withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
        off = spark.sql(q).collect().toSeq
      }
      assert(off.head.getLong(0) > 0, s"the fixture must return rows, got $off")
      withSQLConf(conf(keep = true, inject = true): _*) {
        // Per table, not per occurrence: which tables' scans carry a year filter.
        val tablesWithYearFilter = cachedBodies(q).flatMap { body =>
          collect(body) { case f: FileSourceScanExec =>
            val name = f.tableIdentifier.map(_.table).getOrElse("?")
            val hasYear = f.dataFilters.map(_.sql)
              .exists(p => p.contains("2001") || p.contains("2002"))
            (name, hasYear)
          }
        }.collect { case (name, true) => name }.toSet
        assert(tablesWithYearFilter === Set("s_sales", "c_sales"),
          s"the injected conjunct must reach BOTH branches' scans, one per table; " +
          s"got $tablesWithYearFilter")
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        assert(spark.sql(q).collect().toSeq === off,
          "pushing the injected conjunct through the union must not change the answer")
      }
    }
  }

  /**
   * A body column no reference reads, so pruning is real: `originalPlan.output` is wider than
   * `cteDef.output` and `prePushdownBody` takes its `Project(cteDef.output, ...)` arm. Every
   * other fixture here references all three body columns and only exercises the identity arm.
   *
   * WHAT THIS COVERS, precisely. It covers the `Project` arm: injection still reaches the scan
   * and the pruned column stays out of the entry. It does NOT demonstrate that injecting BELOW
   * the Project is necessary -- an earlier docstring claimed it "would have caught" an
   * above-the-Project version, and that is false. `pushable` here reads only `dyear`, which is
   * in `cteDef.output`, so injecting above the Project resolves fine and
   * this test passes either way. The hazard is unreachable from SQL by construction: a column a
   * reference FILTERS on is folded into `newAttrSet`
   * (`PushdownPredicatesAndPruneColumnsForCTEDef:97-99`)
   * and therefore never pruned. It becomes reachable only through that rule's `:143-146` branch,
   * which pairs a freshly recomputed `newAttrSet` with the OLD stored `preds` -- and that needs
   * a later fixed-point iteration to drop a reference predicate, which no query here does.
   * Injecting below the Project is reasoned-safe, not test-covered.
   */
  test("a pruned body column does not break the injection") {
    withTable("sales") {
      createSales("sales")
      // `max(amt) AS peak` is in the CTE body and referenced by nobody, so column pruning
      // narrows the body and the `Project` arm runs.
      val q =
        """WITH yt AS (
          |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total,
          |         max(amt) AS peak
          |  FROM sales GROUP BY cust, yr)
          |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
          |WHERE t1.dyear = 2001 AND t1.year_total > 0 AND t2.dyear = 2002
          |""".stripMargin
      var off: Seq[Row] = Nil
      withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
        off = spark.sql(q).collect().toSeq
      }
      assert(off.head.getLong(0) === 200, s"the fixture must return 200, got $off")
      withSQLConf(conf(keep = true, inject = true): _*) {
        val scans = scanFilters(q)
        assert(scans.exists(p => p.contains("2001") || p.contains("2002")),
          s"the injection must still reach the scan when the body is pruned; found $scans")
        assert(!allFilters(q).exists(_.contains("peak")),
          "the pruned column must not appear in the cached body")
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        assert(spark.sql(q).collect().toSeq === off,
          "injecting below the pruning Project must not change the answer")
      }
    }
  }

  /**
   * A reference predicate holding an UNCORRELATED subquery over a grouping column. Extraction
   * admits it -- `SubqueryExpression.references` is only its `outerAttrs`
   * (`subquery.scala:82-83`), empty here, so `dyear IN (SELECT ...)` has references `{dyear}`,
   * a subset of the grouping-derived output -- and `narrowsRows` does not reject it either,
   * since an `Unevaluable` is not foldable. So `canInject` is the only thing standing between
   * this and a subquery inside the cached body.
   *
   * Two reasons that matters, and the SECOND is why this guard exists at all:
   *
   *   - `collectCTERefIds` walks subqueries, so an injected condition can pull CTE ids into
   *     the body's scope that the body itself never referenced. One naming a def outside this
   *     `WithCTE` makes `outOfScope.nonEmpty` fire and the def is not cached AT ALL --
   *     turning the conf on would DISABLE caching.
   *   - the sibling `keepMergedPredicate` shape is NOT equivalent here, which is the reasoning
   *     error that had this guard deleted once. `preds` is a snapshot from catalyst's own
   *     batch, while `RewritePredicateSubquery` (`Optimizer.scala:242-243`) runs after
   *     `operatorOptimizationBatch` (`:202`), so `cteDef.child`'s copy of the predicate has
   *     already become a join while the snapshot is still a raw `InSubquery`.
   *
   * The assertion is that the body took the whole-merged-predicate path (`year_total` present)
   * rather than the injected one. Disabling the guard makes injection happen, `year_total`
   * disappears, and this fails.
   */
  test("a subquery in the extracted part is not injected") {
    withTable("sales", "years") {
      createSales("sales")
      spark.range(2000, 2006).selectExpr("CAST(id AS INT) AS yr").write.saveAsTable("years")
      spark.sql("ANALYZE TABLE years COMPUTE STATISTICS FOR ALL COLUMNS")
      val q =
        """WITH yt AS (
          |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total
          |  FROM sales GROUP BY cust, yr)
          |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
          |WHERE t1.dyear IN (SELECT yr FROM years WHERE yr = 2001) AND t1.year_total > 0
          |  AND t2.dyear IN (SELECT yr FROM years WHERE yr = 2002)
          |""".stripMargin
      var off: Seq[Row] = Nil
      withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
        off = spark.sql(q).collect().toSeq
      }
      assert(off.head.getLong(0) === 200, s"the fixture must return 200, got $off")
      withSQLConf(conf(keep = true, inject = true): _*) {
        val filters = allFilters(q)
        assert(filters.exists(_.contains("year_total")),
          s"the extracted part holds a subquery, so injection must be declined and the whole " +
          s"merged predicate cached instead -- `year_total` is the marker of that path. " +
          s"Found $filters")
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        assert(spark.sql(q).collect().toSeq === off,
          "declining to inject must not change the answer")
      }
    }
  }

  /**
   * A body that already constrains the very column the extracted part reads. Injecting there
   * removes ZERO rows while still making the entry query-specific -- all of the cost, none of
   * the benefit.
   *
   * TPC-DS q74 is this shape and it is why the guard exists: its body says
   * `d_year IN (2001, 2001+1)` (`q74.sql:13,28`) and the extracted part is
   * `year = 2001 OR year = 2002`, a tautology over that body. Measured on local sf100, first
   * execution 20236ms at keepOff against 39559ms at keepWhole and 45934ms injected -- the
   * worst of the three shapes for no rows removed.
   *
   * The fixture mirrors that: `WHERE yr IN (2001, 2002)` inside the CTE, references filtering
   * on exactly those two years. Contrast `withMixedShape`, whose body has no year filter and
   * whose injection takes date_dim from 73049 rows to 20000 on real q4.
   *
   * Note the canonical forms differ on purpose -- `In` in the body against `Or`-of-`EqualTo`
   * from extraction. That is why the check is a value-set containment and not
   * `constraints.contains`, and it is also why `PruneFilters` does not delete the injected
   * filter by itself.
   *
   * THE ASSERTION HAD TO BE REWRITTEN. The first version asserted `year_total` was absent,
   * which is true on BOTH sides of the branch under test: the pre-pushdown body has no
   * predicate at all, and the injected body has only the year part -- `year_total` marks the
   * whole-merged-predicate shape, a third path neither reaches. Mutation-disabling the guard
   * left it green. What distinguishes the two is the `Or`-of-`EqualTo` FORM: the body's own
   * `yr IN (2001, 2002)` is part of the query and must stay, while a
   * `yr = 2001 OR yr = 2002` next to it can only have come from injecting the extracted part.
   */
  test("a predicate the body already implies is not injected") {
    withTable("sales") {
      createSales("sales")
      val q =
        """WITH yt AS (
          |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total
          |  FROM sales WHERE yr IN (2001, 2002) GROUP BY cust, yr)
          |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
          |WHERE t1.dyear = 2001 AND t1.year_total > 0 AND t2.dyear = 2002
          |""".stripMargin
      var off: Seq[Row] = Nil
      withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
        off = spark.sql(q).collect().toSeq
      }
      assert(off.head.getLong(0) === 200, s"the fixture must return 200, got $off")
      withSQLConf(conf(keep = true, inject = true): _*) {
        val filters = allFilters(q)
        // The redundant injection, if it happened, is the disjunctive form. The body's own
        // `IN` list is a different string and is expected to be present.
        val injectedForm = filters.filter(p => p.contains(" OR ") && p.contains("2001"))
        assert(injectedForm.isEmpty,
          s"the extracted `yr = 2001 OR yr = 2002` is implied by the body's own " +
          s"`yr IN (2001, 2002)`, so it removes no rows and must NOT be injected -- on q74 " +
          s"that redundancy cost 20236ms -> 45934ms. Found $injectedForm in $filters")
        assert(filters.exists(_.contains("IN (2001, 2002)")),
          s"the body's OWN year filter is part of the query and must survive; a guard that " +
          s"removed it would be dropping rows. Found $filters")
        assert(!filters.exists(_.contains("year_total")),
          s"and it must fall back to the PRE-PUSHDOWN body, not to the " +
          s"whole-merged-predicate one -- on q74 that shape measured 39559ms. Found $filters")
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        assert(spark.sql(q).collect().toSeq === off, "declining must not change the answer")
      }
      // And the payoff of declining: the body stayed query-independent, so a second query
      // asking for a DIFFERENT year subset shares the entry. The subset must differ, or the
      // assertion is decoration: with the guard removed both queries would inject the SAME
      // `dyear = 2001 OR dyear = 2002` (`Or` is a `CommutativeExpression`, so `indexKey`
      // canonicalizes the two orderings together) and the count would be 1 either way.
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      val q2 =
        """WITH yt AS (
          |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total
          |  FROM sales WHERE yr IN (2001, 2002) GROUP BY cust, yr)
          |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
          |WHERE t1.dyear = 2001 AND t1.year_total > 0 AND t2.dyear = 2001
          |""".stripMargin
      withSQLConf(conf(keep = true, inject = true): _*) {
        spark.sql(q).collect()
        spark.sql(q2).collect()
      }
      assert(spark.sharedState.autoCTECacheManager.numEntries === 1,
        "nothing was injected for either query, so both share the query-independent body. " +
        "Were the implied part injected, q asks for {2001, 2002} and q2 for {2001} alone, so " +
        "the two bodies would differ and this would be 2.")
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }

  /**
   * A body carrying runtime Bloom filters the pre-pushdown snapshot cannot have. Injecting
   * would DROP them, so the guard declines and the whole-merged-predicate body is cached
   * instead -- the one case where `keepMergedPredicate`'s shape is the better of the two.
   *
   * WHY THE SNAPSHOT CANNOT HAVE THEM. `InjectRuntimeFilter` runs in `SparkOptimizer`'s own
   * batch (`:96`), long after `PushdownPredicatesAndPruneColumnsForCTEDef` wrote
   * `originalPlanWithPredicates` in catalyst's operator-optimization fixed point
   * (`Optimizer.scala:140`). It reaches `cteDef.child` -- defs are children of `WithCTE` --
   * but never the snapshot, which is a case-class FIELD. `cacheQuery` re-optimizes and
   * re-derives SOME filters, but the per-key-pair dedup in `InjectRuntimeFilter:317-319`
   * stops one fresh pass reproducing what two accumulated passes had.
   *
   * MEASURED on local sf100, TPC-DS q64: `cteDef.child` carries 6 Bloom filters, the injected
   * body 2, and the missing two are the ones above catalog_sales and store_returns -- the
   * `cs_ui` aggregate's input goes from 107K rows back to 143,997,065 (1345x) and the first
   * execution from 11382ms to 36270ms. Over the 18 caching queries only q64 and q74 lose
   * filters this way; q4 and q11 have zero in either shape and GAIN 3 and 2 once the injected
   * predicate gives `InjectRuntimeFilter` something selective to build from.
   *
   * The fixture needs the thresholds lowered because the defaults (10MB creation side, 10GB
   * application-side scan) are sized for real warehouses -- `InjectRuntimeFilterSuite:345`
   * does the same thing for the same reason. The join is on a column the small side filters
   * selectively, which is what `InjectRuntimeFilter` looks for.
   */
  test("a body that would lose runtime Bloom filters is not injected") {
    withTable("sales", "dim") {
      createSales("sales")
      // A small, selectively-filtered dimension: the Bloom filter creation side.
      spark.range(0, 200).selectExpr("CAST(id AS INT) AS cust", "id % 4 AS bucket")
        .write.saveAsTable("dim")
      spark.sql("ANALYZE TABLE dim COMPUTE STATISTICS FOR ALL COLUMNS")
      val q =
        """WITH yt AS (
          |  SELECT s.cust AS customer_id, s.yr AS dyear, sum(s.amt) AS year_total
          |  FROM sales s JOIN dim d ON s.cust = d.cust
          |  WHERE d.bucket = 1
          |  GROUP BY s.cust, s.yr)
          |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
          |WHERE t1.dyear = 2001 AND t1.year_total > 0 AND t2.dyear = 2002
          |""".stripMargin
      val bloomConf = Seq(
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "10000000",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")
      var off: Seq[Row] = Nil
      withSQLConf((Seq(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") ++ bloomConf): _*) {
        off = spark.sql(q).collect().toSeq
      }
      // `d.bucket = 1` keeps 50 of the 200 customers, so the answer is 50. Pinned because
      // every assertion below compares against `off`, and a fixture returning 0 would satisfy
      // them all.
      assert(off.head.getLong(0) === 50, s"the fixture must return 50, got $off")

      // FIXTURE STRENGTH, and only that. Taken on the WHOLE-PREDICATE body -- the shape the
      // guard selects -- rather than on `inject=true`, because a mutation disabling the guard
      // would make an `inject=true` count fall to the injected body's, possibly 0, and `assume`
      // would CANCEL this test instead of failing it. A mutation run looking for failures would
      // see nothing.
      //
      // It is a PROXY for the guard's premise, not the premise. The guard compares
      // `countRuntimeFilters(cteDef.child)` against the snapshot on the LOGICAL trees; this
      // counts the physical plan `cacheQuery` produced after re-optimization. A bloom surviving
      // re-optimization implies one existed, so the direction holds -- but a re-optimization
      // that dropped it would cancel this test even though the guard fired correctly.
      var wholeBlooms = 0
      withSQLConf((conf(keep = true, inject = false) ++ bloomConf): _*) {
        wholeBlooms = cachedBodies(q).map(bloomCount).sum
      }
      assume(wholeBlooms > 0,
        "no runtime Bloom filter was built for this fixture, so the guard cannot be " +
        "observed; the thresholds or the join shape need adjusting")

      withSQLConf((conf(keep = true, inject = true) ++ bloomConf): _*) {
        val filters = allFilters(q)
        // SHAPE, not count. The `year_total` marker is what distinguishes the two bodies here;
        // a count assertion alongside it would be the same reading spelled twice, because when
        // the guard fires `inject=true` takes `case Some(pushable) => cteDef.child`, the exact
        // branch `inject=false` takes, so the counts are equal by construction. And a count
        // could not stand in for the marker either: the loss the guard exists for is a
        // two-accumulated-passes effect on a MULTI-join body (q64, 6 filters -> 2), while this
        // fixture's single join re-derives its one filter under either shape. Locally the guard
        // is pinned by shape; the count effect is only observable at cluster scale.
        assert(filters.exists(_.contains("year_total")),
          s"the body carries Bloom filters the snapshot predates, so injection must be " +
          s"declined in favour of the whole merged predicate -- on q64 injecting cost " +
          s"11382ms -> 36270ms. Found $filters")
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        assert(spark.sql(q).collect().toSeq === off, "declining must not change the answer")
      }
    }
  }

  /**
   * An `IsNull` conjunct must NOT be dropped when reducing the extracted predicate to a value
   * set, even though `IsNotNull` is.
   *
   * The two call sites of `valueSetOf` need different things from it. On the BODY side an
   * over-large set is safe -- it makes `subsetOf` harder, so implication is claimed less
   * often. On the PREDICATE side every value in the set must actually make the predicate TRUE,
   * so an over-large set claims implication the body does not provide, and the predicate is
   * dropped although it would have removed rows.
   *
   * The shape: `dyear IS NULL AND customer_id IN (1, 2, 3)` over a body that already pins
   * `cust IN (1, 2, 3)`. Dropping the `IsNull` leaves `(customer_id, {1,2,3})`, the body's
   * constraint is the same set, `subsetOf` succeeds, and the guard declines -- discarding a
   * predicate that removes every row whose `yr` is NOT null, which here is most of the table.
   * Keeping the `IsNull` makes `valueSetOf` return `None` for the conjunction, which is the
   * conservative answer, and the predicate is injected.
   *
   * `narrowsRows` already draws this line (`IsNotNull` does not narrow, `IsNull` does), so the
   * asymmetry is consistency with the neighbouring function rather than a special case.
   */
  test("an IsNull conjunct is not dropped when reducing to a value set") {
    withTable("nsales") {
      spark.sql("DROP TABLE IF EXISTS nsales")
      // `yr` is NULL for the first 200 rows and a real year afterwards, so
      // `dyear IS NULL` selects a proper subset rather than nothing.
      spark.range(0, 1200)
        .selectExpr(
          "CAST(id % 200 AS INT) AS cust",
          "CASE WHEN id < 200 THEN NULL ELSE 2000 + CAST(id / 400 AS INT) END AS yr",
          "id AS amt")
        .write.saveAsTable("nsales")
      spark.sql("ANALYZE TABLE nsales COMPUTE STATISTICS FOR ALL COLUMNS")
      val q =
        """WITH yt AS (
          |  SELECT cust AS customer_id, yr AS dyear, sum(amt) AS year_total
          |  FROM nsales WHERE cust IN (1, 2, 3) GROUP BY cust, yr)
          |SELECT count(*) FROM yt t1 JOIN yt t2 ON t1.customer_id = t2.customer_id
          |WHERE t1.dyear IS NULL AND t1.customer_id IN (1, 2, 3)
          |  AND t2.dyear IS NULL AND t2.customer_id IN (1, 2, 3)
          |""".stripMargin
      var off: Seq[Row] = Nil
      withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "false") {
        off = spark.sql(q).collect().toSeq
      }
      assert(off.head.getLong(0) > 0,
        s"the fixture must return rows, or the answer comparison below is vacuous. Got $off")
      withSQLConf(conf(keep = true, inject = true): _*) {
        // `dropIsNotNull = false` is NOT what makes the injected part observable -- `allFilters`
        // only ever drops `IsNotNull`, and the injected conjunct is `IsNull(yr)`, which survives
        // either way. It is passed so the analyzer's own `isnotnull` conjuncts show up too, which
        // makes a failure message say what the body actually carries.
        val filters = allFilters(q, dropIsNotNull = false)
        assert(filters.exists(_.toLowerCase(java.util.Locale.ROOT).contains("is null")),
          s"`dyear IS NULL AND customer_id IN (1,2,3)` is NOT implied by the body's " +
          s"`cust IN (1,2,3)` -- it also removes every non-null-`yr` row -- so it must be " +
          s"injected. Dropping the IsNull when reducing to a value set is what wrongly " +
          s"claims implication here. Found $filters")
        spark.sharedState.autoCTECacheManager.clearAll(spark)
        assert(spark.sql(q).collect().toSeq === off,
          "injecting the null check must not change the answer")
      }
    }
  }

  /**
   * The default ships ON. Every other test sets the conf explicitly, so without this one a
   * flipped `createWithDefault` would revert the shipped behaviour with the suite still green.
   */
  test("the conf is on by default") {
    assert(SQLConf.AUTO_CTE_CACHE_INJECT_IMPLIED_PREDICATE.defaultValue.contains(true),
      "injection ships on; turning it off returns keepMergedPredicate's whole-predicate body")
    withMixedShape { q =>
      withSQLConf(
        SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
        SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "-1",
        SQLConf.AUTO_CTE_CACHE_KEEP_MERGED_PREDICATE.key -> "true",
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.PLAN_STATS_ENABLED.key -> "true") {
        val filters = allFilters(q)
        assert(filters.exists(p => p.contains("2001") || p.contains("2002")),
          s"with the inject conf left at its default the implied part must be in the body; " +
          s"found $filters")
        assert(!filters.exists(_.contains("year_total")),
          s"and the whole merged predicate must not be; found $filters")
      }
    }
  }
}
