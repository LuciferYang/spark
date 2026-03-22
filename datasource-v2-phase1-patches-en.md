# Phase 1: Flag Flip + Feature Completion -- Complete ✅

## Status: Fully Complete

All 7 Patches are complete. 201/201 tests pass, 0 regressions. V2 file write path fully enabled, no V1 fallback.

## Summary

| Patch | Goal | Status | Key Changes |
|-------|------|--------|-------------|
| 1 | Cache invalidation | ✅ | `refreshCache()` uses `recacheByPath` + `fileIndex.refresh()` for FileTable |
| 2 | checkPartitioningMatchesV2Table | ✅ | Skip partition matching check for FileTable |
| 3 | Data type validation unification | ✅ No changes needed | V2 already consistent with V1 |
| 4 | Flag flip + Delete FallBack | ✅ | Delete `FallBackFileSourceV2`, `V2_FILE_WRITE_ENABLED` config; fix non-existent path write and fileIndex refresh |
| 5 | ErrorIfExists/Ignore V2 path | ✅ | Path existence check (matching V1 semantics); INSERT INTO format.\`path\` via ResolveSQLOnFile V2 resolution |
| 5a | V2 partitioned write fixes | ✅ | RequiresDistributionAndOrdering sorting; userSpecifiedPartitioning plumbing; supportsDataType skips partition columns; lazy->val description fix for Parquet summary |
| 6 | SupportsPartitionManagement | ✅ | SHOW/ADD/DROP PARTITION; partitionSchema fallback to userSpecifiedPartitioning |
| 7 | customPartitionLocations | ✅ | createPartition supports location; partition operations sync to catalog metastore; CatalogFileIndex supports custom path reads; syncNewPartitionsToCatalog auto-registers new partitions from INSERT INTO |

## V2 Write Path Full Coverage

**DataFrame API**:
- `mode("append")` -> V2 AppendData ✅
- `mode("overwrite")` -> V2 OverwriteByExpression ✅
- `mode("error")` path exists -> V2 throws PATH_ALREADY_EXISTS ✅
- `mode("error")` path does not exist -> V2 AppendData ✅
- `mode("ignore")` path exists -> V2 skip (LocalRelation) ✅
- `mode("ignore")` path does not exist -> V2 AppendData ✅

**SQL**:
- `INSERT INTO table` -> V2 ✅
- `INSERT OVERWRITE table` -> V2 ✅
- `CREATE TABLE AS SELECT` -> V2 ✅
- `INSERT INTO format.\`path\`` -> V2 (ResolveSQLOnFile) ✅

**Partition DDL**:
- `SHOW PARTITIONS` -> V2 SupportsPartitionManagement ✅
- `ALTER TABLE ADD PARTITION` -> V2 (with LOCATION support) ✅
- `ALTER TABLE DROP PARTITION` -> V2 ✅

## Branch Mapping for Each Patch

| Patch | Branch |
|-------|--------|
| 1-4 | `dsv2-phase1-patch1-cache-invalidation` |
| 5-5a | `dsv2-phase1-patch5-catalog-options` |
| 6-7 | `dsv2-phase1-patch6-partition-management` |

## Remaining V1 Fallback Scenarios (All Are Correct Behavior)

- Provider does not support `BATCH_WRITE` (e.g., read-only providers), falls back to V1 via `fallbackFileFormat`
- User explicitly configures V1 via `USE_V1_SOURCE_LIST`
