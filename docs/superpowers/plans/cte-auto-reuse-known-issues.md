# CTE Auto-Reuse: Known Issues & Future Improvements

## Issue 1: Divergent-Predicate Detection False Positive

**Status:** Resolved

**Solution:** Moved `CleanUpTempCTEInfo` from its original batch in the base
`Optimizer` to after `ReplaceCTERefWithCache` in `SparkOptimizer`'s
"Replace CTE with Repartition" batch. This preserves `originalPlanWithPredicates`
so `hasDivergentPredicates` can read the per-reference predicates directly —
no more false positives from natural Or filters in CTE bodies.

**Investigated alternatives that didn't work:**
- Walking the outer query plan to collect per-reference predicates above
  `CTERelationRef` nodes. Failed because the optimizer pushes join conditions
  as `Filter` nodes above individual refs, indistinguishable from user WHERE
  clauses.
- Examining CTE child for `Filter(Or(...))`. Had false positives for natural
  Or filters in CTE bodies.

---

## Issue 2: Memory Pressure Integration

**Status:** Not implemented, deferred

**Problem:** Currently, auto-cached CTE entries are evicted only by TTL
(idle timeout) and maxSize (total weight limit). There is no eviction
triggered by actual memory pressure — if the storage memory pool is
under pressure (e.g., other cached tables, broadcast variables, shuffle
data competing for memory), auto-CTE entries are not proactively evicted.

**Impact:** With the default `maxSize=-1` (unlimited) and `MEMORY_AND_DISK`
storage level, cached CTE data spills to disk under memory pressure rather
than being evicted. This is handled by Spark's storage system and does not
cause OOM, but disk spilling degrades performance.

**Fix:** Register an eviction callback with `UnifiedMemoryManager` that
triggers `AutoCTECacheManager` eviction (e.g., invalidate LRU entries)
when storage memory exceeds a configurable threshold. This requires changes
to `MemoryManager` in Spark's core module to support eviction callbacks —
a significant scope expansion beyond the CTE caching feature.

**Workaround:** Users can set `spark.sql.auto.cte.cache.maxSize` to a
reasonable value (e.g., `2g`) to cap total cache size and prevent
excessive memory/disk usage.
