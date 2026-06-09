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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.{AutoCTECacheManager, QueryExecution}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.StorageLevel

/**
 * The lifecycle half of auto-CTE caching: what expiry does to data a query is still
 * using, and what the two Guava knobs can and cannot express.
 *
 * Expiry is not the same operation as `UNCACHE TABLE`. A `DataFrame` is lazy, so one
 * built an hour ago and collected now still holds the `InMemoryRelation` its plan was
 * rewritten to. Unpersisting that relation because the tracker's idle timer fired
 * turns the next `collect()` into a full recompute of a body that was already
 * materialised -- strictly worse than never having cached it, and reachable on the
 * default 1h TTL without anyone configuring anything. So expiry stops NEW plans from
 * reusing the entry and leaves the blocks to `ContextCleaner`, which frees them once
 * the last plan referencing them is unreachable. `clearAll` still unpersists, because
 * its callers (`spark.catalog.clearCache()`, test teardown) asked for exactly that.
 *
 * The two knobs are static confs. Guava fixes `expireAfterAccess` and `maximumWeight`
 * when the cache is built, once per `SparkContext`, so a session-level `SET` could
 * only ever be accepted and then ignored. Making them static turns that into an error
 * at `SET` time, which is the whole fix for the "silently ineffective" half.
 */
class AutoCteCacheLifecycleSuite extends QueryTest with SharedSparkSession {

  /** A leaf whose size estimate is whatever the test says, including values beyond Long. */
  private case class FixedStatsLeaf(bytes: BigInt) extends LeafNode {
    override def output: Seq[Attribute] = Nil
    override def computeStats(): Statistics = Statistics(sizeInBytes = bytes)
  }

  private def prepareData(): Unit = {
    spark.sql("DROP TABLE IF EXISTS auto_cte_lifecycle")
    spark.range(0, 200)
      .selectExpr("id AS key", "id * 2 AS value")
      .write.mode("overwrite").saveAsTable("auto_cte_lifecycle")
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS auto_cte_lifecycle")
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      spark.sharedState.cacheManager.clearCache()
    } finally {
      super.afterAll()
    }
  }

  private val bodySql =
    "SELECT key, sum(value) AS total FROM auto_cte_lifecycle GROUP BY key"

  test("SET is rejected for the two startup-only confs") {
    // The point of making them static. Before, `SET` succeeded, `spark.conf.get`
    // reported the new value, and the cache went on using the one from startup --
    // undetectable from outside.
    Seq(SQLConf.AUTO_CTE_CACHE_TTL.key, SQLConf.AUTO_CTE_CACHE_MAX_SIZE.key).foreach { key =>
      assert(SQLConf.isStaticConfigKey(key),
        s"$key is read once when SharedState builds the tracker, so it must be static; " +
        s"otherwise SET silently has no effect")
      val e = intercept[AnalysisException](spark.sql(s"SET $key=5min"))
      assert(e.getMessage.contains(key),
        s"expected the error to name $key, got: ${e.getMessage}")
    }
  }

  test("maxSize=0 means unlimited, not evict-everything") {
    // `maximumWeight(0)` makes Guava drop each entry as it is inserted, so every query
    // would pay a full materialisation and then throw it away -- worse than the feature
    // being off. 0 now joins -1 as "no bound".
    val mgr = new AutoCTECacheManager(ttlMs = 0, maxSizeBytes = 0)
    prepareData()
    val plan = spark.sql(bodySql).queryExecution.optimizedPlan
    mgr.trackEntry(1L, plan)
    assert(mgr.numEntries == 1,
      "maxSizeBytes = 0 must not install a weight bound; the entry was evicted on insert")
  }

  /**
   * The weigher has to stay in the `BigInt` domain. Without CBO and column statistics a
   * multi-way join estimate is a product of its children's sizes, so it routinely runs past
   * `Long.MaxValue`, and `BigInt#toLong` wraps there -- silently, and to either sign. A
   * negative weight makes Guava throw `IllegalStateException: Weights must be non-negative`
   * from inside `cache.put`; a wrapped positive one over `maximumWeight` makes it evict the
   * entry as it is inserted. Both turn `spark.sql.auto.cte.cache.maxSize` into a knob that
   * cannot be used, which is the point of the finding.
   *
   * Asserted on the weight rather than on `numEntries` after a real insert: whether a wrapped
   * `toLong` lands negative, small, or huge depends on the low 64 bits of that particular
   * estimate, so an end-to-end fixture only reddens for some sizes. An earlier version of this
   * test used an 8-way self join (estimate 4e26) and survived the mutation for exactly that
   * reason -- the wrap happened to land on a small positive number.
   */
  test("weigher: a size beyond Long range is weighed as unknown, not as a wrapped Long") {
    val mgr = new AutoCTECacheManager(ttlMs = 0, maxSizeBytes = 1024L * 1024 * 1024)
    assert(mgr.weighEntry(FixedStatsLeaf(BigInt(Long.MaxValue) + 1)) == 1,
      "an estimate that cannot be represented as a Long must weigh as unknown (1), so the " +
      "entry is kept; wrapping it lands anywhere from Int.MinValue to Int.MaxValue")
    assert(mgr.weighEntry(FixedStatsLeaf(BigInt(3L * 1024 * 1024 * 1024))) == Int.MaxValue,
      "a real estimate above 2GiB must saturate at the largest weight Guava can represent")
    assert(mgr.weighEntry(FixedStatsLeaf(BigInt(4096))) == 4096,
      "an estimate inside Int range must be weighed as itself")
  }

  /**
   * Caches `bodySql` through `CacheManager` exactly the way `ReplaceCTERefWithCache`
   * does -- same normalisation, same plan object handed to both the store and the
   * tracker -- and returns that plan plus the resulting relation.
   *
   * Going through the rule end-to-end instead would not let the test hold the plan
   * `CacheManager` keyed on: the rule caches the CTE BODY, while a `DataFrame` built
   * over the query exposes the whole query's plan. Tracking the wrong plan makes
   * `sameResult` never match, and then an assertion that expiry did not unpersist
   * passes because expiry touched nothing at all -- which is exactly how the first
   * version of this test managed to survive its own mutation.
   */
  private def cacheBodyLikeTheRuleDoes(): (LogicalPlan, InMemoryRelation) = {
    val cm = spark.sharedState.cacheManager
    val normalized =
      QueryExecution.normalize(spark, spark.sql(bodySql).queryExecution.optimizedPlan)
    cm.cacheQuery(spark, normalized, tableName = Some("auto_cte_probe"),
      StorageLevel.MEMORY_ONLY)
    val relation = cm.lookupCachedData(normalized)
      .getOrElse(fail("cacheQuery did not register the body"))
      .cachedRepresentation
    // Force materialisation so `isCachedColumnBuffersLoaded` means something.
    relation.cacheBuilder.cachedColumnBuffers.count()
    assert(relation.cacheBuilder.isCachedColumnBuffersLoaded,
      "the probe failed to materialise; the tests below would not discriminate")
    (normalized, relation)
  }

  test("expiry stops reuse but leaves the materialised blocks alone") {
    prepareData()
    spark.sharedState.cacheManager.clearCache()
    val (plan, relation) = cacheBodyLikeTheRuleDoes()

    val mgr = new AutoCTECacheManager(ttlMs = 1, maxSizeBytes = 0)
    mgr.trackEntry(1L, plan)
    Thread.sleep(20)
    mgr.evictStaleEntries(spark)

    assert(spark.sharedState.cacheManager.lookupCachedData(plan).isEmpty,
      "an expired entry must stop being reused by NEW plans, otherwise the TTL " +
      "expresses nothing")
    assert(relation.cacheBuilder.isCachedColumnBuffersLoaded,
      "expiry must NOT unpersist: a DataFrame built while the entry was live still " +
      "holds this relation, and dropping its blocks turns its next collect() into a " +
      "full recompute of a body that was already materialised")
    spark.sharedState.cacheManager.clearCache()
  }

  test("clearAll does unpersist, unlike expiry") {
    prepareData()
    spark.sharedState.cacheManager.clearCache()
    val (plan, relation) = cacheBodyLikeTheRuleDoes()

    val mgr = new AutoCTECacheManager(ttlMs = 0, maxSizeBytes = 0)
    mgr.trackEntry(2L, plan)
    mgr.clearAll(spark)

    assert(spark.sharedState.cacheManager.lookupCachedData(plan).isEmpty,
      "clearAll must drop the entry")
    assert(!relation.cacheBuilder.isCachedColumnBuffersLoaded,
      "clearAll backs spark.catalog.clearCache(), an explicit request for the data to " +
      "be gone, so it must release the blocks rather than wait for ContextCleaner")
  }

  test("a pre-built DataFrame still answers correctly after its entry expires") {
    // End-to-end counterpart of the relation-level test above: the rewritten plan holds
    // the InMemoryRelation, the entry expires, and the answer must be unchanged.
    prepareData()
    withSQLConf(SQLConf.AUTO_REUSED_CTE_ENABLED.key -> "true",
        SQLConf.AUTO_CTE_CACHE_MIN_SIZE_BYTES.key -> "0") {
      spark.sharedState.autoCTECacheManager.clearAll(spark)
      val cteSql =
        s"""WITH c AS ($bodySql)
           |SELECT a.key, a.total + b.total AS s FROM c a JOIN c b ON a.key = b.key""".stripMargin

      val expected = spark.sql(cteSql).collect().toSeq
      assert(expected.nonEmpty, "fixture produced no rows, the rest proves nothing")

      val df = spark.sql(cteSql)
      val relations = df.queryExecution.optimizedPlan.collectWithSubqueries {
        case r: InMemoryRelation => r
      }
      assert(relations.nonEmpty,
        "the plan must reference an InMemoryRelation for this test to mean anything; " +
        "auto-CTE did not cache the body")

      // Expire through the real tracker, which holds the body plan the rule gave it.
      spark.sharedState.autoCTECacheManager.evictStaleEntries(spark)

      assert(df.collect().toSeq.map(_.toString).sorted == expected.map(_.toString).sorted,
        "the pre-built df must still return the same answer after expiry")
      spark.sharedState.autoCTECacheManager.clearAll(spark)
    }
  }
}
