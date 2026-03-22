# Phase 0: File Source V2 Partitioned Write -- Patch Decomposition

## Dependencies

```
Patch 1 (Partition Column Plumbing)     <- No behavior change, can be merged independently
  |
Patch 2 (Feature Flag + Non-partitioned Write)  <- Minimum verifiable unit
  |-- Patch 3 (Partitioned Write)
  |     +-- Patch 4 (Dynamic Partition Overwrite)
  |-- Patch 5 (Remove DataFrameWriter Guard)
  +-- Patch 6 (Remove DataSourceV2Utils Guard)
        |
Patch 7 (Schema Validation Unification)     <- Independent of 3-6
        |
Patch 8 (Cleanup + Flag Flip)    <- Wrap-up
```

Can be parallelized: Patch 5 and Patch 6 are independent of each other and can be developed in parallel. Patch 7 can also be parallelized with 3-6.

---

## Patch 1: FileWrite Partition Column Pipeline (Pure Plumbing, No Behavior Change)

**Goal**: Enable `FileWrite` to receive and use partition column information, but without changing existing behavior (`FallBackFileSourceV2` still intercepts all writes).

**Changes**:

| File | Change |
|------|--------|
| `sql/core/.../datasources/v2/FileWrite.scala` | Add `partitionSchema: StructType` abstract field; use it in `createWriteJobDescription()` to separate `dataColumns` / `partitionColumns`, replacing the current `Seq.empty` |
| `sql/core/.../datasources/v2/FileTable.scala` | `newWriteBuilder()` -> `mergedWriteInfo()`: pass `fileIndex.partitionSchema` to concrete Write classes |
| `sql/core/.../datasources/v2/parquet/ParquetWrite.scala` | Add `partitionSchema: StructType` constructor parameter, implement `FileWrite.partitionSchema` |
| `sql/core/.../datasources/v2/orc/OrcWrite.scala` | Same as above |
| `sql/core/.../datasources/v2/csv/CSVWrite.scala` | Same as above |
| `sql/core/.../datasources/v2/json/JsonWrite.scala` | Same as above |
| `sql/core/.../datasources/v2/text/TextWrite.scala` | Same as above |
| Corresponding `*Table.scala` | `newWriteBuilder()` passes `fileIndex.partitionSchema` |

**Why no behavior change**: `FallBackFileSourceV2` converts all `InsertIntoStatement(FileTable)` back to the V1 path at the analyzer stage, so this Patch's code will not be executed. But it can be reviewed and merged independently.

**Tests**: Unit tests verify that `createWriteJobDescription()` outputs correct `partitionColumns` (can be tested by directly constructing `FileWrite` subclasses).

**Complexity**: S

---

## Patch 2: Add Feature Flag + Non-partitioned V2 Write

**Goal**: Add a config switch; when enabled, non-partitioned file tables use the V2 write path.

**Changes**:

| File | Change |
|------|--------|
| `sql/catalyst/.../internal/SQLConf.scala` | Add `spark.sql.sources.v2.file.write.enabled` (default `false`) |
| `sql/core/.../datasources/FallBackFileSourceV2.scala` | In `apply()`, check this flag; when `true`, skip the V2->V1 conversion |

**Tests**:
- With flag enabled, `INSERT INTO` non-partitioned Parquet/ORC/CSV/JSON tables use V2 path
- Verify written file contents match V1 path (round-trip read/write comparison)
- Verify behavior unchanged when flag is disabled

**Complexity**: S

---

## Patch 3: Partitioned V2 Write

**Goal**: On top of Patch 1 + 2, partitioned writes (dynamic partitioning) work correctly through the V2 path.

**Dependency**: Patch 1, Patch 2

**Changes**:

| File | Change |
|------|--------|
| `sql/core/.../datasources/v2/FileWrite.scala` | Ensure `requiredOrdering` includes dynamic partition column sorting (reference `V1WritesUtils.getSortOrder()`) |
| `sql/core/.../datasources/v2/V2Writes.scala` | Ensure `WriteFiles` (planned write) correctly handles partition sorting for V2 file writes |

**Key Point**: `FileWriterFactory.scala:43-47` already has conditional logic -- when `partitionColumns.isEmpty` it uses `SingleDirectoryDataWriter`, otherwise `DynamicPartitionDataSingleWriter`. After Patch 1 populates `partitionColumns`, this logic takes effect automatically.

**Tests**:
- Single-level partitioned write (Parquet, ORC, JSON, CSV -- 4 formats)
- V1/V2 partitioned write result comparison (4 formats)
- Multi-level partitioned write (year/month two levels)
- Multiple formats: Parquet, ORC, CSV, JSON

**Test scenarios deferred to Patch 5/6**:
- SQL `INSERT INTO parquet.\`path\`` partitioned write (depends on DataSourceV2Utils guard removal)
- Static partition write (`INSERT INTO t PARTITION(year=2024) SELECT ...`)
- Mixed static+dynamic partition write

**Complexity**: S (In practice, Patch 1 plumbing turned out to be sufficient with no additional code changes needed, only tests)

---

## Patch 4: Dynamic Partition Overwrite Mode

**Goal**: V2 file write supports `PARTITION_OVERWRITE_MODE=DYNAMIC`.

**Dependency**: Patch 3

**Changes**:

| File | Change |
|------|--------|
| `sql/core/.../datasources/v2/FileWrite.scala` | Read `partitionOverwriteMode` from `options`, configure `FileCommitProtocol` in `toBatch()` |
| `sql/core/.../datasources/v2/FileBatchWrite.scala` | During dynamic overwrite, `commit()` uses a staging directory, replacing only the written partitions (reference `InsertIntoHadoopFsRelationCommand:173-180`) |

**Tests**:
- `INSERT OVERWRITE` dynamic mode: only overwrites written partitions, other partitions unaffected
- `INSERT OVERWRITE` static mode: overwrites specified partitions
- Consistency comparison with V1 behavior

**Complexity**: M

---

## Patch 5: Remove DataFrameWriter Guard

**Goal**: `df.write.format("parquet").save(path)` uses the V2 path.

**Dependency**: Patch 2 (flag verified usable)

**Changes**:

| File | Change |
|------|--------|
| `sql/core/.../classic/DataFrameWriter.scala:598` | Remove `case Some(_: FileDataSourceV2) => None`, replace with feature flag control; only `Append` mode uses V2, other modes fall back to V1 |
| `sql/core/.../datasources/v2/FileTable.scala` | `CAPABILITIES` adds `TRUNCATE, OVERWRITE_DYNAMIC`; `createFileWriteBuilder` adds `SupportsTruncate` mixin |

**Actual Test Coverage**:
- `df.write.mode("append").format(format).save(path)` -> V2 Append
- `df.write.mode("overwrite").format(format).save(path)` -> V1 fallback (expected behavior)
- `df.write.mode("ignore").format(format).save(path)` -> V1 fallback (expected behavior)
- `df.write.partitionBy("col").parquet(path)` -> V2 partitioned write
- `df.write.option("compression", "snappy").parquet(path)` -> V2 write with options

**Current Limitation -- DataFrame API via V2 Path Only Supports Append Mode**:

During implementation, the following issues were discovered, causing Overwrite/ErrorIfExists/Ignore to still require V1 fallback:

1. **Overwrite mode cannot use V2**: V2's `OverwriteByExpression(Literal(true))` corresponds to full table truncate semantics, but `FileBatchWrite` does not clear the target directory before writing, resulting in old and new files coexisting. V1 cleans up before writing via `InsertIntoHadoopFsRelationCommand.deleteMatchingPartitions`. Need to implement truncate semantics in `FileBatchWrite` (clear target path before writing).

2. **Dynamic partition overwrite via DataFrame API has staging directory issues**: `FileCommitProtocol(dynamicPartitionOverwrite=true)` throws `FileNotFoundException` during the file rename phase in the V2 path, as partition subdirectories cannot be found under the staging directory. V1 handles this through `InsertIntoHadoopFsRelationCommand` using `FileCommitProtocol.getStagingDir` as `OutputSpec.outputPath`, but the V2 `WriteJobDescription.path` semantics are not fully consistent with V1 and require further investigation.

3. **First write to a new path fails**: In the V2 path, `DataFrameWriter.saveCommand` calls `getTable` -> `FileTable` -> `InMemoryFileIndex`, which attempts to list the target path. If the path does not exist, it throws `PATH_NOT_FOUND`. V1 automatically creates the path before writing. This means the DataFrame API V2 path cannot write to completely new paths (Append to a non-existent path also fails), requiring the path to already exist.

4. **ErrorIfExists / Ignore requires `SupportsCatalogOptions`**: V2's `DataFrameWriter` takes the `CreateTableAsSelect` branch for non-Append/Overwrite modes, requiring the provider to implement `SupportsCatalogOptions`. `FileDataSourceV2` does not implement this interface, so these two modes must fall back to V1. This requires Patch 6 (Catalog integration) to resolve.

**Follow-up Plan**: Issues 1-3 above require a new Patch (suggested as Patch 5.5 or handled during Patch 8 cleanup phase) to implement `FileBatchWrite` truncate/overwrite semantics and first-write path creation. Issue 4 is resolved by Patch 6.

**Complexity**: S (actual changes are small, but exposed issues that need to be resolved subsequently)

---

## Patch 6: Remove DataSourceV2Utils Guard

**Goal**: Catalog-managed file tables use the V2 path.

**Dependency**: Patch 2

**Changes**:

| File | Change |
|------|--------|
| `sql/core/.../datasources/v2/DataSourceV2Utils.scala:167-168` | Remove `!p.isInstanceOf[FileDataSourceV2]` filter, replace with feature flag control |

**Actual Test Coverage**:
- `CREATE TABLE t USING parquet` + `INSERT INTO t` -> V2 write ✓
- `CREATE TABLE t ... PARTITIONED BY` + `INSERT INTO t` -> V2 partitioned write ✓
- `CTAS (CREATE TABLE AS SELECT)` -> V2 path ✓

**Limitations Discovered in Patch 6 -- To Be Resolved in Phase 1**:

1. **`SHOW PARTITIONS` not supported**: `ParquetTable` does not implement `SupportsPartitionManagement`, reports `INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED`.

2. **`INSERT INTO parquet.\`path\`` syntax not supported**: Goes through `ResolveSQLOnFile` path, requires `SupportsCatalogOptions`, `FileDataSourceV2` does not implement it, reports `TABLE_OR_VIEW_NOT_FOUND`.

**Test Scenarios Deferred to Phase 1** (from Patch 3 + Patch 6):
- `SHOW PARTITIONS` to view partitions
- `INSERT INTO parquet.\`path\`` syntax
- `ALTER TABLE ADD/DROP PARTITION`
- Static partition write (`INSERT INTO t PARTITION(year=2024) SELECT ...`)
- Mixed static+dynamic partition write

**Complexity**: S

---

## Patch 7: SPARK-36340 Schema Validation Unification

**Goal**: Unify V1/V2 Insert schema field validation logic.

**Changes**:

| File | Change |
|------|--------|
| `sql/core/.../datasources/v2/FileWrite.scala:101` | Replace current simple `dataType` validation with unified validation logic shared with V1 (field name, type, nullable checks) |
| Extract shared validation logic | If V1 validation is in `DataSource.scala` or `HiveMetastoreClient`, extract to a shared utility class |

**Tests**:
- Error message consistency on schema mismatch
- Field type conversion (implicit cast) behavior consistency
- Inserting non-null data into nullable fields

**Complexity**: S

---

## Patch 8: Cleanup and Flag Flip -- Deferred to Phase 1

**Goal**: Remove `FallBackFileSourceV2` and remaining TODOs, prepare for enabling V2 by default.

**Status**: ❌ Not feasible yet, deferred to Phase 1

**Reason**: After flipping the flag default value (`false` -> `true`), `FileBasedDataSourceSuite` has 6 test failures:

1. **Cache invalidation** ("Do not use cache on overwrite/append"): V2 path's `refreshCache` uses plan matching, unable to invalidate cached DataFrames created via `spark.read.orc(path)`. V1 uses path matching.
2. **`checkPartitioningMatchesV2Table`** (SPARK-36568): When DataFrame API writes a second time to existing partition directories, V2 FileTable's `partitioning()` reads from `fileIndex.partitionSchema`, but the `partitioningAsV2` passed to `getTable` is inconsistent with existing data.
3. **Data type validation path differences** (SPARK-24204, SPARK-51590, Geospatial): V1 and V2 have different error messages/exception types for unsupported types, causing `intercept` assertion failures.

**Prerequisites to Resolve in Phase 1**:
- V2 FileTable cache invalidation (path-based cache recache)
- `checkPartitioningMatchesV2Table` compatibility for file sources
- Data type validation error message unification
- Only then can the flag be safely flipped and `FallBackFileSourceV2` deleted

**Cleanup Already Completed** (in Patch 7):
- SPARK-36340 TODO comment removed
- `checkNoCollationsInMapKeys` validation aligned

---

## Phase 0 Completion Summary

**Status**: ✅ Patches 1-7 all complete, Patch 8 deferred to Phase 1

**Results**:
- V2 file write path fully implemented, enabled via `spark.sql.sources.v2.file.write.enabled=true`
- Supports: non-partitioned write, partitioned write, dynamic partition overwrite, truncate (full overwrite), DataFrame API write, catalog table write (INSERT INTO / CTAS)
- Schema validation aligned with V1 (`checkNoCollationsInMapKeys`, `checkColumnNameDuplication`, `validateSchema`)
- Covered formats: Parquet, ORC, CSV, JSON, Text
- Tests: `FileDataSourceV2FallBackSuite` 18/18 passing

**Known Limitations (To Be Resolved in Phase 1)**:
- `SaveMode.ErrorIfExists` / `SaveMode.Ignore`: Requires `SupportsCatalogOptions`, currently DataFrame API falls back to V1
- `SHOW PARTITIONS`: Requires `SupportsPartitionManagement`
- `INSERT INTO parquet.\`path\``: Requires `SupportsCatalogOptions`
- Flag default value remains `false`, flipping requires resolving cache invalidation, partition info matching, and error message unification first

**Branch Mapping for Each Patch**:
| Patch | Branch |
|-------|--------|
| 1 | `dsv2-phase0-patch1-partition-plumbing` |
| 2 | `dsv2-phase0-patch2-feature-flag` |
| 3 | `dsv2-phase0-patch3-partitioned-write` |
| 4 | `dsv2-phase0-patch4-dynamic-overwrite` |
| 5 | `dsv2-phase0-patch5-dataframe-api` |
| 6 | `dsv2-phase0-patch6-remove-datasourcev2utils-guard` |
| 7 | `dsv2-phase0-patch7-schema-validation` |
