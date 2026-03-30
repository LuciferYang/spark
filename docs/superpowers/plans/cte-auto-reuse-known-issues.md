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

## Issue 2: Memory Pressure — Spill Priority for Auto-CTE Blocks

**Status:** Deferred (acceptable with current behavior)

**Problem:** When storage memory is tight, Spark's `MemoryStore` evicts
(spills to disk) cached blocks using pure LRU ordering with no priority
distinction. Auto-CTE blocks — which are opportunistic optimizations —
compete equally with higher-priority data (explicit `CACHE TABLE`,
broadcast tables). Ideally, auto-CTE blocks should spill first.

**Current behavior:** With `MEMORY_AND_DISK` storage level, auto-CTE
blocks spill to disk automatically under memory pressure. The data
remains accessible (just slower). On high-speed disks, this is acceptable.
No OOM risk.

**Ideal fix:** Priority-based eviction in `MemoryStore.evictBlocksToFreeSpace()`
so auto-CTE blocks (lower priority) spill before user-requested caches
(higher priority). This requires changes to Spark's core storage module
(`MemoryStore`, `BlockManager`) to support eviction priorities — out of
scope for this feature.

**Workaround:** Users can set `spark.sql.auto.cte.cache.maxSize` to cap
total auto-CTE cache size and reduce pressure on the storage memory pool.
