# DataSource V2 Feature Gap Implementation Plan

## Context

In Apache Spark 4.2.0-SNAPSHOT, all built-in file formats (avro, csv, json, kafka, orc, parquet, text) still default to the V1 path (`spark.sql.sources.useV1SourceList`). The root cause is that the file source V2 write path is not available (SPARK-28396), resulting in missing functionality for partitioned writes, bucketing, Catalog integration, statistics, etc. This plan aims to progressively fill these gaps, ultimately enabling built-in file formats to run entirely on the V2 path.

## Dependency Overview

```
Phase 0 (File V2 Write Path)          <- Root blocker
  |-- Phase 1 (Catalog Integration)
  |     |-- Phase 2 (Statistics / ANALYZE TABLE)
  |     +-- Phase 4 (MSCK REPAIR TABLE)
  |-- Phase 3 (Bucketing)
  +-- Track C (Streaming Write, partial dependency)

Track A (JDBC Native V2)            <- Fully independent
Track B (DEFAULT Column Values)     <- Fully independent
Track D (V2 Catalog Views)          <- Fully independent
```

---

## Phase 0: File Source V2 Partitioned Write (Root Blocker)

**Goal**: Enable `FileWrite` to support partition columns, remove the `FallBackFileSourceV2` hack.

**JIRA**: SPARK-28396, SPARK-36340

**Key Changes**:

1. **`FileWrite.scala`** -- Core change
   - Currently `partitionColumns = Seq.empty` (line 131-133), `bucketSpec = None` (line 141)
   - Extract partition column info from `LogicalWriteInfo` (`FileTable.partitioning()` already returns partition schema)
   - Split `allColumns` into `dataColumns` and `partitionColumns` for `WriteJobDescription`
   - Support `PARTITION_OVERWRITE_MODE` (reference V1 `InsertIntoHadoopFsRelationCommand:65`)

2. **`DataFrameWriter.scala:598`** -- Remove FileDataSourceV2 write guard
   - `case Some(_: FileDataSourceV2) => None` -> Allow V2 write path

3. **`DataSourceV2Utils.scala:167`** -- Remove FileDataSourceV2 Catalog guard
   - `!p.isInstanceOf[FileDataSourceV2]` -> Remove this filter

4. **`FallBackFileSourceV2.scala`** -- Delete this file
   - Also remove from the analyzer rule chain in `BaseSessionStateBuilder` and `HiveSessionStateBuilder`

5. **Per-format Write classes** -- Pass partition information
   - `ParquetWrite`, `OrcWrite`, `CSVWrite`, `JsonWrite`, `TextWrite`

**Risk Control**: Add SQL config switch `spark.sql.sources.v2.file.write.enabled` (default false), flip to true once development is stable.

**Tests**:
- Add `FileDataSourceV2WriteSuite`: partitioned writes, dynamic partition overwrite
- Parameterize existing `ParquetQuerySuite`, `OrcQuerySuite` to cover both V1/V2 paths
- Enable currently disabled V2 write path tests such as `ParquetQuerySuite:1339`

**Complexity**: L

---

## Phase 1: File V2 Catalog Integration -- ✅ Complete

**Dependency**: Phase 0

**Status**: Fully complete. See `datasource-v2-phase1-patches.md` for details.

**Results**:
- `FallBackFileSourceV2` deleted, `V2_FILE_WRITE_ENABLED` config removed
- V2 file write path enabled by default, all 4 SaveModes and SQL write operations use V2
- `FileTable` implements `SupportsPartitionManagement` (SHOW/ADD/DROP PARTITION)
- Newly written partitions automatically registered to catalog metastore
- Cache invalidation via `recacheByPath` + `fileIndex.refresh()`
- `INSERT INTO format.\`path\`` syntax via V2 path
- Custom partition paths (ALTER TABLE ADD PARTITION LOCATION) fully supported
- 201 tests pass, 0 regressions

**Complexity**: L

---

## Phase 2: Statistics / ANALYZE TABLE — ✅ Complete

**Dependency**: Phase 1

**Results**:
- New `AnalyzeTableExec` and `AnalyzeColumnExec` (V2-native, no V1 command delegation)
- Statistics persisted via `TableCatalog.alterTable()` + `TableChange.setProperty()`
- Table-level: `spark.sql.statistics.totalSize`, `spark.sql.statistics.numRows`
- Column-level: `spark.sql.statistics.colStats.<col>.<stat>`
- Supports NOSCAN, FOR COLUMNS, FOR ALL COLUMNS
- `FileScan.estimateStatistics()` returns stored `numRows` (injected via options), enabling optimizer join strategy selection
- 206 tests passing, 0 regressions

**Complexity**: S (simpler than estimated)

---

## Phase 3: File V2 Bucketing — ✅ Complete

**Dependency**: Phase 0

**Results**:
- `FileWrite` uses `V1WritesUtils.getWriterBucketSpec()` to create `WriterBucketSpec`, replacing hardcoded `None`
- `FileTable.createFileWriteBuilder` passes `catalogTable.bucketSpec` to the write pipeline
- All 6 `*Table` and `*Write` classes updated to plumb `BucketSpec`
- `FileDataSourceV2.getTable` uses `collect` to skip `BucketTransform` (handled via `catalogTable.bucketSpec`)
- 207 tests passing, 0 regressions

**Complexity**: M (simpler than estimated — `V1WritesUtils.getWriterBucketSpec` reusable)

**Note**: Bucket pruning and bucket join optimization not implemented in this Phase (read-path optimization)

**Complexity**: L

---

## Phase 4: MSCK REPAIR TABLE

**Dependency**: Phase 1

**Results**:
- New `RepairTableExec` (V2-native)
- Scans FS partition dirs, compares with catalog, registers missing / drops orphaned
- Uses `FileTable.listPartitionIdentifiers()` and `SessionCatalog.createPartitions/dropPartitions`
- 208 tests passing, 0 regressions

**Complexity**: S

---

## Track A: JDBC Native V2 (Fully Independent)

**JIRA**: SPARK-32593, SPARK-32595

**Goal**: Replace `V1Scan`/`V1Write` delegation with native V2 `Scan`/`BatchWrite`.

**Changes**:
- `JDBCScan.scala` -- Migrate from `V1Scan` to native `Scan` + `Batch` + `PartitionReaderFactory`
- `JDBCWriteBuilder.scala` -- Migrate from `V1Write` to native `BatchWrite` + `DataWriter`; implement atomic truncate+append (SPARK-32595)

**Complexity**: M (per JIRA)

---

## Track B: DEFAULT Column Value Support for V2 Write (Fully Independent)

**JIRA**: SPARK-43752

**Goal**: `ResolveColumnDefaultInCommandInputQuery` supports V2 write commands.

**Changes**:
- `ResolveColumnDefaultInCommandInputQuery.scala:46` -- Extend matching to `AppendData`, `OverwriteByExpression`, `OverwritePartitionsDynamic`
- V2 write commands carry default value metadata via `DataSourceV2Relation.table.columns()`

**Complexity**: S

---

## Track C: Streaming File V2 Write (Partially Depends on Phase 0)

**JIRA**: SPARK-27484, SPARK-33638

**Goal**: File source V2 can be used as streaming Sink; streaming writes have logical plan nodes.

**Changes**:
1. `ResolveDataSource.scala:95` / `DataStreamWriter.scala:252` -- Remove `FileDataSourceV2` exclusion
2. `FileTable`'s `WriteBuilder` needs to produce `StreamingWrite`
3. `MicroBatchExecution.scala:349` -- Add write node before analysis (SPARK-27484)
4. `DataStreamWriter.toTable()` -- Complete V2 table creation semantics (SPARK-33638)

**Complexity**: L

---

## Track D: V2 Catalog Views (Fully Independent)

**Goal**: V2 catalog supports `CREATE VIEW`, `DROP VIEW`, `SHOW VIEWS`, etc.

Currently no `ViewCatalog` interface exists, requiring a new API design. This is the largest independent work item; it is recommended to produce a design document before implementation.

**Complexity**: XL

---

## Parallelization Recommendations

| Time Period | Developer 1 (Critical Path) | Developer 2 | Developer 3 | Developer 4 |
|-------------|----------------------------|-------------|-------------|-------------|
| Q1 First Half | Phase 0 | Track A (JDBC) | Track B (DEFAULT) | Track D (View Design) |
| Q1 Second Half | Phase 1 | Track A cont. | Track C (Streaming) | Phase 4 (MSCK) |
| Q2 First Half | Phase 2 (Statistics) | Phase 3 (Bucketing) | Track C cont. | Track D Implementation |
| Q2 Second Half | Integration Testing + Performance Benchmarks | Phase 3 cont. | Stabilization | Stabilization |

## Rollback Strategy

- Each Phase has an independent SQL config switch, default off
- After all Phases are stable, flip config defaults, and ultimately remove built-in formats from `useV1SourceList`
- Must ensure tables created by V1 can still be read and written correctly under the V2 path

## Validation Approach

- Unit tests: Dedicated test suites for each Phase
- Integration tests: End-to-end SQL tests covering both V1/V2 paths
- Performance benchmarks: TPC-DS / TPC-H comparison of V1 and V2 write/scan performance
- Compatibility tests: Tables written by V1 -> Read by V2, and vice versa
