# CTE Auto-Reuse Cache Design

## Problem Statement

Queries with repeated subexpressions (CTEs referenced multiple times, multi-part
TPC-DS queries) pay redundant I/O and computation costs. Aliyun EMR achieves
8x-150x speedups on TPC-DS part-b queries by caching CTE results as in-memory
tables that persist across queries within a session.

## TPC-DS Impact

| Category | Queries | Speedup | Mechanism |
|----------|---------|---------|-----------|
| Cross-query reuse | q23b, q24b, q39b | **8x-150x** | Part-b reuses part-a's cached CTE |
| Within-query reuse | q2, q58, q81 | **1.2x-1.5x** | CTE scanned once, read from cache N times |
| **Regression** | q1, q31, q39a | **1.4x-1.9x worse** | Cache blocks predicate pushdown |

## Configs

```
spark.sql.auto.reused.cte.enabled (boolean, default false, SESSION binding)
```
When true, non-inlined CTEs with expensive operators are automatically cached
as InMemoryRelation on first execution. Subsequent references (within or across
queries) read from cache. Per-session config — can be toggled per query.

```
spark.sql.auto.cte.cache.ttl (duration, default 1h, NOT_APPLICABLE binding)
```
Idle timeout for auto-cached CTE entries. Each cache access resets the clock.
Entries not accessed within this duration are evicted. Set to 0 to disable
eviction (entries persist until session ends). Read at SparkContext init only.

```
spark.sql.auto.cte.cache.maxSize (bytes, default -1, NOT_APPLICABLE binding)
```
Maximum total estimated size for auto-cached CTE entries. LRU eviction when
exceeded, using plan statistics as weight estimates. Set to -1 for unlimited
(default). Read at SparkContext init only.

## Architecture

### Component 1: Optimizer Rule (ReplaceCTERefWithCache)

Runs in the "Replace CTE with Repartition" batch in `SparkOptimizer`, before
`ReplaceCTERefWithRepartition`. The two rules cooperate:

```
WithCTE(child, cteDefs)
  → ReplaceCTERefWithCache:
      For each cteDef:
        if shouldAutoCache(cteDef):
          Cache via CacheManager → replace CTERelationRef with InMemoryRelation
        else:
          Skip → leave in WithCTE for next rule
  → ReplaceCTERefWithRepartition:
      Handle remaining (skipped) CTEs with repartition-based shuffle reuse
```

**Caching decision (`shouldAutoCache`):**

```
shouldAutoCache(cteDef) =
  !hasDivergentPredicates(cteDef) &&
  isExpensiveEnough(cteDef.child)
```

- **`isExpensiveEnough`**: CTE plan must contain at least one Join, Aggregate,
  Sort, or Window. Scan-only CTEs are cheap to recompute.

- **`hasDivergentPredicates`**: After `PushdownPredicatesAndPruneColumnsForCTEDef`
  runs, divergent predicates from different CTE references are combined as
  `Filter(Or(pred1, pred2), ...)` in the CTE child. If the Or branches are
  semantically different, caching would block predicate pushdown → skip.
  Note: `originalPlanWithPredicates` is cleared by `CleanUpTempCTEInfo` before
  this rule runs, so detection examines the plan structure directly. Known
  limitation: natural top-level `Filter(Or(...))` in the CTE body may produce
  false positives (conservatively skips caching; falls back to repartition).

**Cross-query reuse**: `CacheManager.lookupCachedData` normalizes plans via
`QueryExecution.normalize()` and matches via `sameResult()`. This enables cache
hits across queries — e.g., `rand()` seeds are normalized away, so two queries
defining the same CTE body with `rand()` share one cache entry.

**Skipped CTEs**: CTEs that fail the heuristic are preserved in a rebuilt
`WithCTE` node so `ReplaceCTERefWithRepartition` can handle them with proper
shuffle reuse. `CTERelationRef` nodes for skipped CTEs pass through unchanged.

### Component 2: Cache Lifecycle (AutoCTECacheManager)

Built on Guava `Cache` with `expireAfterAccess` (idle timeout) and optional
`maximumWeight` (size-based LRU). Lives in `SharedState` (shared across
sessions), constructed once at SparkContext init with immutable config.

```
AutoCTECacheManager(ttlMs, maxSizeBytes)
  └── Guava Cache
        ├── expireAfterAccess(ttlMs)     — idle timeout, each access resets
        ├── maximumWeight(maxSizeBytes)   — LRU by plan stats estimate
        └── RemovalListener              — queues evicted plans for uncaching
```

**Why a separate manager?** `CacheManager` stores the actual cached data
(InMemoryRelation). `AutoCTECacheManager` only tracks *which* entries were
created by auto-CTE caching, so TTL/size eviction doesn't affect entries
from explicit `CACHE TABLE`.

**Access tracking**: On cache hit, `recordAccessByPlan` refreshes Guava's
access time via `getIfPresent`. Uses reference equality (O(1) fast path for
within-query hits), falling back to `sameResult` (for cross-query hits).

**Eviction flow**: `evictStaleEntries` is called at the start of each query
(before caching new CTEs). It calls `cache.cleanUp()` to trigger Guava's
lazy expiration, then drains the pending uncache queue — calling
`CacheManager.uncacheQuery` for each evicted plan.

**Data invalidation**: Auto-cached CTEs benefit from `CacheManager`'s existing
invalidation. When DML operations (INSERT, etc.) modify an underlying table,
`CacheManager.recacheByPlan/recacheTableOrView` rebuilds affected cache entries
with fresh data — including auto-CTE entries.

### Interaction with Existing Optimization

| Existing Feature | Interaction |
|-----------------|-------------|
| `InlineCTE` | Inlined CTEs (deterministic or refCount=1) are NOT cached — already expanded. Only non-inlined CTEs reach this rule. |
| `PushdownPredicatesAndPruneColumnsForCTEDef` | Runs BEFORE this rule. Pushed-down predicates inform the divergent-predicates check. |
| `CleanUpTempCTEInfo` | Clears `originalPlanWithPredicates` BEFORE this rule. Divergent predicates detected via plan structure instead. |
| `ReplaceCTERefWithRepartition` | Handles CTEs skipped by our heuristic. Both rules coexist in the same batch. |
| `ReusedExchange` | CTE cache is at the logical/data level. ReusedExchange is at the physical/shuffle level. Both coexist. |
| `CacheManager` invalidation | Auto-CTE entries are regular `CachedData` entries — DML triggers automatic refresh. |

## Key Files

| File | Change |
|------|--------|
| `SQLConf.scala` | 3 new configs |
| `SharedState.scala` | Creates `AutoCTECacheManager` with config from SparkConf |
| `SparkOptimizer.scala` | Adds `ReplaceCTERefWithCache` to "Replace CTE with Repartition" batch |
| `AutoCTECache.scala` | `ReplaceCTERefWithCache` optimizer rule + `AutoCTECacheManager` (Guava Cache) |
| `AutoCTECacheSuite.scala` | 7 tests: enable/disable, correctness, within-query reuse, cross-query reuse, scan-only skip, TTL eviction |

## Future Work

- **Divergent-predicate detection improvement**: Current detection via
  `Filter(Or(...))` has false positives for natural Or filters. A more robust
  approach would require preserving `originalPlanWithPredicates` past
  `CleanUpTempCTEInfo`, or collecting per-reference predicates at rule time.
- **TPC-DS benchmarking**: Verify speedups on multi-part queries (q23, q24, q39)
  and no regressions on q1, q31, q39a.
- **Memory pressure integration**: Evict auto-CTE entries when storage memory
  is under pressure, via `MemoryManager` callback.

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| CTE cache blocks predicate pushdown (q1, q31) | `hasDivergentPredicates` heuristic; feature disabled by default |
| Memory exhaustion from large CTE caches | TTL eviction (default 1h) + optional maxSize limit |
| Stale cache returning outdated data | Cache key is normalized plan via `sameResult()`; DML triggers `CacheManager` invalidation |
| Interference with explicit CACHE TABLE | `AutoCTECacheManager` tracks auto-CTE entries separately |
