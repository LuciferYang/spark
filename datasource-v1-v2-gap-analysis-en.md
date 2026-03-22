# DataSource V2 vs V1 Feature Gap Analysis Report

Based on a comprehensive analysis of the Spark 4.2.0-SNAPSHOT codebase, the following are features that V2 is still missing or incomplete relative to V1.

---

## Core Fact: All Built-in File Formats Still Default to V1

`spark.sql.sources.useV1SourceList` defaults to `"avro,csv,json,kafka,orc,parquet,text"`, meaning all major file formats use the V1 path. (`SQLConf.scala:4731`)

---

## I. File Source V2 Write Path Completely Unavailable

| Issue | JIRA | Location |
|-------|------|----------|
| File source V2 write path is hard-coded disabled | SPARK-28396 | `DataFrameWriter.scala:598` -- `case Some(_: FileDataSourceV2) => None` |
| File source V2 cannot work with catalog tables | SPARK-28396 | `DataSourceV2Utils.scala:167` |
| Partitioned write not implemented, `partitionColumns = Seq.empty` | SPARK-36340 | `FileWrite.scala:131-133` |
| `bucketSpec` hard-coded to `None` | -- | `FileWrite.scala:141` |
| `customPartitionLocations` hard-coded to `Map.empty` | -- | `FileWrite.scala` |
| Parquet V2 write path tests all disabled | -- | `ParquetQuerySuite.scala:1339` and many other places |

**Result**: All file format writes actually fall back to V1's `InsertIntoHadoopFsRelationCommand` through `FallBackFileSourceV2`.

---

## II. Bucketing

- **V1**: `HadoopFsRelation` carries `BucketSpec`, `FileSourceStrategy` supports bucket pruning, writes distribute files by bucket hash
- **V2 File Source**: `FileTable` does not expose `BucketSpec`, `FileWrite` hard-codes `bucketSpec = None`, `FileScan` has no bucket pruning logic
- **Note**: V2 has `KeyedPartitioning` (`spark.sql.sources.v2.bucketing.enabled`), but this is a storage-partition join optimization for non-file connectors, completely different from Hive-compatible file bucketing

---

## III. Statistics

| Capability | V1 | V2 |
|------------|----|----|
| Table size estimation | Supported (with compression factor) | Only ratio-based estimation from file size |
| Row count statistics | `CatalogStatistics.rowCount` | `FileScan.numRows()` always returns `empty` |
| Column-level statistics | `CatalogColumnStat` | None |
| `ANALYZE TABLE` | Fully supported | **Throws exception directly** (`DataSourceV2Strategy.scala:469`) |
| Auto-update statistics after write | `CommandUtils.updateTableStats()` | None |
| Statistics update after partition pruning | `FilterEstimation` propagates to `CatalogStatistics` | None |
| CSV V2 `SupportsReportStatistics` | -- | Not implemented (`FileBasedDataSourceSuite.scala:685`) |

---

## IV. Streaming

| Issue | JIRA | Description |
|-------|------|-------------|
| File source V2 does not support streaming read/write | -- | `ResolveDataSource.scala:95`, `DataStreamWriter.scala:252` explicitly exclude `FileDataSourceV2` |
| No streaming write logical plan node | SPARK-27484 | Cannot check `STREAMING_WRITE` capability during analysis phase |
| `toTable()` table creation incomplete | SPARK-33638 | V2 table creation semantics not fully covered |
| V1 fallback logic not formally an analyzer rule | SPARK-27483 | `ResolveDataSource.scala:119` |
| CommitLog V2->V1 backward compatibility missing | SPARK-50653 | `CommitLog.scala:67` |

---

## V. Missing SQL Commands

The following commands throw errors directly for V2 tables (`DataSourceV2Strategy.scala:468-548`):

- `ANALYZE TABLE` / `ANALYZE COLUMN`
- `ALTER TABLE ... RECOVER PARTITIONS`
- `ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]`
- `LOAD DATA`
- `SHOW CREATE TABLE AS SERDE`
- `MSCK REPAIR TABLE` (SPARK-34397)

---

## VI. View Support

V2 catalog **does not support view API** (`v2Commands.scala:1328`). `SHOW VIEWS`, `DROP VIEW`, `ALTER VIEW`, etc. all report `UNSUPPORTED_FEATURE.CATALOG_OPERATION` for V2 catalogs.

---

## VII. Other Missing Features

| Issue | JIRA | Location |
|-------|------|----------|
| `DEFAULT` column values not supported for V2 write commands | SPARK-43752 | `ResolveColumnDefaultInCommandInputQuery.scala:46` |
| JDBC V2 uses `V1Scan`/`V1Write` internal fallback | SPARK-32593/32595 | JDBC lacks nested column pruning, truncate+append is non-atomic |
| `DESCRIBE TABLE COLUMN` nested columns not supported for V2 | -- | `DataSourceV2SQLSuite.scala:245` |
| Non-catalog path semantics undefined | -- | `DataSourceV2Utils.scala:147` |
| `FileDataSourceV2` partition inference is a stub implementation | SPARK-29665 | `FileDataSourceV2.scala:83` |
| Single-pass analyzer does not support `DataSourceV2Relation` | -- | `ExplicitlyUnsupportedResolverFeature.scala` |
| Auto cache invalidation after write | -- | V1 has `cacheManager.recacheByPath`, V2 does not |
| `ifPartitionNotExists` semantics | -- | V1 supports skipping existing partitions during static partition insert, V2 does not |
| DataSourceRDD type erasure hack | SPARK-25083 | `DataSourceRDD.scala:140` |

---

## V2 Advantages Over V1 (For Reference)

V2 is not entirely behind; it is superior to V1 in the following areas:
- **Custom metrics** (`CustomMetric` API, 3.2.0+)
- **Aggregate pushdown** (`SupportsPushDownAggregates`)
- **LIMIT/OFFSET/TopN/TABLESAMPLE pushdown**
- **Join pushdown** (`SupportsPushDownJoin`)
- **Runtime filtering** (`SupportsRuntimeV2Filtering`)
- **Iterative partition predicate pushdown** (SPARK-55596)
- **Row-level operations** (`ReplaceData`, `WriteDelta`)

---

## JIRA Summary

| JIRA | Topic | Status | Notes |
|------|-------|--------|-------|
| SPARK-25083 | DataSourceRDD type erasure hack | **Closed (Fixed)** | Fixed in 3.0.0, but TODO comments still remain in code |
| SPARK-27483 | V1 fallback logic should become analyzer rule | **In Progress** | |
| SPARK-27484 | Streaming write has no logical plan node | **Open** | |
| SPARK-28396 | File source V2 write path unavailable | **Resolved (Won't Fix)** | JIRA closed but TODO still exists in code, actual issue unresolved |
| SPARK-29665 | FileDataSourceV2 stub implementation needs cleanup | **Closed (Fixed)** | Fixed in 3.0.0, but TODO comments still remain in code |
| SPARK-32593 | JDBC V2 lacks nested column pruning | **Open** | |
| SPARK-32595 | JDBC V2 truncate+append non-atomic | **Open** | |
| SPARK-33638 | Streaming V2 table creation incomplete | **Open** | |
| SPARK-34397 | MSCK REPAIR TABLE does not support V2 | **Open** | |
| SPARK-36340 | V2 Insert schema check and partitioned write not implemented | **Open** | |
| SPARK-43752 | DEFAULT column values not supported for V2 write commands | **Open** | |
| SPARK-50653 | CommitLog V2->V1 backward compatibility missing | **Open** | |

### Status Statistics

- **Open (8)**: SPARK-27484, SPARK-32593, SPARK-32595, SPARK-33638, SPARK-34397, SPARK-36340, SPARK-43752, SPARK-50653
- **In Progress (1)**: SPARK-27483
- **Closed/Fixed (2)**: SPARK-25083, SPARK-29665 (both fixed in 3.0.0, but TODO comments still remain in code)
- **Resolved/Won't Fix (1)**: SPARK-28396 (JIRA marked as Won't Fix, but the write path disabled TODO comment still exists in code, and the actual functionality gap remains)
