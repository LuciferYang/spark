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

**Status:** Resolved

**Solution:** Added `evictionPriority` field to `StorageLevel` (lower =
evict first, default 0). `MemoryStore.evictBlocksToFreeSpace` now sorts
eviction candidates by priority before LRU order. Auto-CTE caching uses
`MEMORY_AND_DISK.withEvictionPriority(-1)`, so its blocks spill to disk
before user-requested caches (priority 0) when memory is tight.

**Implementation:**
- `StorageLevel`: new `evictionPriority` field + `withEvictionPriority()` method
- `MemoryStore`: two-phase eviction (collect candidates, stable-sort by priority)
- `BlockInfoManager`: `getEvictionPriority()` to read priority from `BlockInfo`
- `AutoCTECache`: uses `StorageLevel.MEMORY_AND_DISK.withEvictionPriority(-1)`
