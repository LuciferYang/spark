# CTE Auto-Reuse + Auto-Clear Cache Design

## Problem Statement

Queries with repeated subexpressions (CTEs referenced multiple times, multi-part
TPC-DS queries) pay redundant I/O and computation costs. Aliyun EMR achieves
8x-150x speedups on TPC-DS part-b queries by caching CTE results as in-memory
tables that persist across queries within a session.

## Aliyun EMR Behavior (inferred from execution plans)

**Configs:**
- `spark.sql.auto.reused.cte.enabled = true` — auto-cache CTE results
- `spark.sql.auto.clear.cte.cache.enabled = true` — enable automatic cache eviction
- `spark.sql.legacy.ctePrecedencePolicy = LEGACY` — outer CTEs take precedence (existing Spark config)

**Mechanism:** Non-inlined CTEs with multiple references are materialized as
`InMemoryRelation` (columnar cache). The cache has a **long lifecycle** —
it persists across SQL statements within the same session. The `auto.clear`
mechanism evicts entries only under specific conditions (memory pressure,
TTL expiry, access staleness), not after every query.

**Observed plan nodes:** `InMemoryTableScan` with CTE names (e.g., `ssales`,
`frequent_ss_items`) or auto-generated UUIDs (e.g., `wss_4341787d...`).
All cached data is CTE results (joins, aggregations), never raw base tables.

## TPC-DS Impact

| Category | Queries | Speedup | Mechanism |
|----------|---------|---------|-----------|
| Cross-query reuse | q23b, q24b, q39b | **8x-150x** | Part-b reuses part-a's cached CTE |
| Within-query reuse | q2, q58, q81 | **1.2x-1.5x** | CTE scanned once, read from cache N times |
| **Regression** | q1, q31, q39a | **1.4x-1.9x worse** | Cache blocks predicate pushdown |

## Design

### Cache Lifecycle Model

```
  CTE materialized          Access         Access          Eviction
       │                      │              │                │
       ▼                      ▼              ▼                ▼
  ┌─────────┐  ──────────  ┌─────────┐  ──────────  ┌─────────────────┐
  │ CACHED  │──── read ───▶│ CACHED  │──── read ───▶│    EVICTED      │
  │(warm)   │              │(warm)   │              │(memory pressure │
  └─────────┘              └─────────┘              │ OR TTL expired  │
                                                    │ OR stale)       │
                                                    └─────────────────┘
```

The cache is **long-lived** within a SparkSession:
- Created when a CTE is first executed with `auto.reused.cte.enabled = true`
- Persists across SQL statements — subsequent queries hit the cache
- Evicted by `auto.clear` when: memory pressure, TTL expired, or access staleness

### Configs

```
spark.sql.auto.reused.cte.enabled (boolean, default false)
```
When true, non-inlined CTEs with refCount >= 2 are automatically materialized
as InMemoryRelation on first execution. Subsequent references (within or across
queries) read from cache.

```
spark.sql.auto.clear.cte.cache.enabled (boolean, default true)
```
When true, enables automatic eviction of CTE caches based on:
- **Memory pressure**: evict LRU entries when storage memory exceeds threshold
- **Access staleness**: evict entries not accessed for `cte.cache.ttl` duration
- **Size limit**: evict when total CTE cache exceeds `cte.cache.maxSize`

When false, CTE caches persist until session ends or explicit `UNCACHE TABLE`.

```
spark.sql.auto.clear.cte.cache.ttl (duration, default 1h)
```
Time-to-live for CTE cache entries since last access. Only effective when
`auto.clear.cte.cache.enabled = true`.

```
spark.sql.auto.clear.cte.cache.maxSize (bytes, default -1 / unlimited)
```
Maximum total size for auto-cached CTE entries. LRU eviction when exceeded.
Set to -1 for unlimited (default). Since the storage level is MEMORY_AND_DISK,
the cache spills to disk when memory is full, so unlimited is safe — TTL-based
eviction is the primary lifecycle control.

### Component 1: CTE Auto-Materialization

**Integration point:** Modify the existing CTE execution path. Currently,
non-inlined CTEs are re-executed per reference. The change:

```
Current flow:
  WithCTE(plan, cteDefs)
    → Physical planning: each CTERelationRef triggers full execution of CTERelationDef

New flow:
  WithCTE(plan, cteDefs)
    → Physical planning: first CTERelationRef triggers execution + cache
    → Subsequent CTERelationRef reads from InMemoryRelation
```

**Cache key:** The CTE's normalized analyzed plan (via `QueryExecution.normalize()`).
This enables cross-query cache hits when a later query defines a CTE with the
same logical plan.

**Implementation approach:**

1. In `CTERelationDef`, add a flag: `autoCache: Boolean`
2. In `InlineCTE`, when deciding NOT to inline (refCount >= 2), set `autoCache = true`
   - Apply smart heuristic to avoid regressions (see Component 3)
3. At physical planning time, for each `CTERelationDef` with `autoCache = true`:
   - Check if an equivalent plan already exists in `CacheManager`
   - If yes: replace with `InMemoryRelation` (cache hit)
   - If no: execute the CTE, cache result via `CacheManager`, then replace

**Key files:**
- `InlineCTE.scala` — mark CTEs for auto-caching
- `CacheManager.scala` — add `cacheAutoCTE()` method
- Physical planning (e.g., `SparkStrategies.scala` or new rule) — intercept
  `CTERelationDef` and replace with cached/materialized version

### Component 2: Auto-Clear Cache Management

**Integration point:** Extend `CacheManager` with eviction policies.

**Tracking per cache entry:**
```scala
case class AutoCTECacheEntry(
    cachedData: CachedData,
    createdAt: Long,          // timestamp
    lastAccessedAt: Long,     // updated on each read
    sizeInBytes: Long,        // cached data size
    isAutoCTE: Boolean)       // distinguish from explicit CACHE TABLE
```

**Eviction triggers:**
1. After each query completes: check total auto-CTE cache size
2. On memory pressure callback from `MemoryManager`
3. Periodic background check (optional)

**Eviction order:** LRU based on `lastAccessedAt`

**Key files:**
- `CacheManager.scala` — add eviction logic for auto-CTE entries
- `QueryExecution.scala` — trigger eviction check after query completes

### Component 3: Smart Caching Heuristic (avoid regressions)

The biggest risk is caching CTEs that block predicate pushdown (q1, q31, q39a).

**Heuristic: cache a CTE only when the benefit outweighs the cost.**

```
shouldAutoCache(cteDef, refs) = {
  autoReusedCTEEnabled &&
  refCount >= 2 &&
  !hasDivergentPredicates(cteDef, refs) &&
  isExpensiveEnough(cteDef)
}
```

**`hasDivergentPredicates`**: Check if different CTE references apply substantially
different filter predicates. If yes, caching blocks pushdown → skip.

Implementation: after `PushdownPredicatesAndPruneColumnsForCTEDef` runs,
examine `CTERelationDef.originalPlanWithPredicates`. If the original plan
(without pushed predicates) differs significantly from the plan after pushdown,
the predicates are divergent.

Simpler v1: check if the CTE's `originalPlanWithPredicates` has multiple distinct
predicate sets. If yes, different references need different data subsets → don't cache.

**`isExpensiveEnough`**: Only cache CTEs whose plan contains at least one expensive
operator (Join, Aggregate, Sort, Window). Simple scan-only CTEs are cheap to
recompute and caching them wastes memory.

### Interaction with Existing Optimization

| Existing Feature | Interaction |
|-----------------|-------------|
| `InlineCTE` | Inlined CTEs are NOT cached (already expanded into the plan). Only non-inlined CTEs are candidates. |
| `PushdownPredicatesAndPruneColumnsForCTEDef` | Runs BEFORE caching decision. The pushed-down predicates inform the divergent-predicates check. |
| `MergeSubplans` (SPARK-40193) | Orthogonal. MergeSubplans handles implicit sharing (duplicate scans). CTE auto-reuse handles explicit sharing (WITH clause). |
| `ReusedExchange` | CTE cache is at the logical/data level. ReusedExchange is at the physical/shuffle level. Both can coexist. |
| `AQE` | AQE statistics can inform caching decisions (skip caching very large CTE results). |

## Implementation Phases

### Phase 1: Core Auto-Materialization
- Add `AUTO_REUSE_CTE_ENABLED` config
- Modify `InlineCTE` to mark CTEs for caching
- Implement cache-on-first-access at physical planning
- Unit tests + e2e correctness tests

### Phase 2: Cross-Query Cache Hit
- Implement normalized plan matching in `CacheManager`
- Verify cross-query reuse works (q23a→q23b pattern)
- Benchmark with TPC-DS multi-part queries

### Phase 3: Auto-Clear Eviction
- Add `AUTO_CLEAR_CTE_CACHE_ENABLED`, `TTL`, `MAX_SIZE` configs
- Implement LRU eviction with access tracking
- Memory pressure integration

### Phase 4: Smart Heuristic
- Implement divergent-predicate detection
- Implement expensive-plan check
- Verify no regressions on q1, q31, q39a patterns

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| CTE cache blocks predicate pushdown (q1, q31) | Phase 4 smart heuristic; disabled by default |
| Memory exhaustion from large CTE caches | Auto-clear with maxSize + LRU eviction |
| Stale cache returning outdated data | Cache key includes full normalized plan; any plan change = cache miss |
| Interference with explicit CACHE TABLE | Track auto-CTE caches separately (isAutoCTE flag) |
