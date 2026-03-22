# DataSource V2 功能补齐实施计划

## Context

Apache Spark 4.2.0-SNAPSHOT 中，所有内置文件格式（avro, csv, json, kafka, orc, parquet, text）默认仍走 V1 路径（`spark.sql.sources.useV1SourceList`）。根本原因是文件源 V2 写路径不可用（SPARK-28396），导致分区写入、分桶、Catalog 集成、统计信息等功能全部缺失。本计划旨在逐步补齐这些功能，最终使内置文件格式可以完全运行在 V2 路径上。

## 依赖关系总览

```
Phase 0 (文件 V2 写路径)          ← 根阻塞项
  ├── Phase 1 (Catalog 集成)
  │     ├── Phase 2 (统计信息 / ANALYZE TABLE)
  │     └── Phase 4 (MSCK REPAIR TABLE)
  ├── Phase 3 (分桶)
  └── Track C (流式写入，部分依赖)

Track A (JDBC 原生 V2)            ← 完全独立
Track B (DEFAULT 列值)            ← 完全独立
Track D (V2 Catalog 视图)         ← 完全独立
```

---

## Phase 0: 文件源 V2 分区写入（根阻塞项）

**目标**: 使 `FileWrite` 支持分区列，移除 `FallBackFileSourceV2` hack。

**JIRA**: SPARK-28396, SPARK-36340

**关键改动**:

1. **`FileWrite.scala`** — 核心变更
   - 当前 `partitionColumns = Seq.empty`（行 131-133）、`bucketSpec = None`（行 141）
   - 从 `LogicalWriteInfo` 提取分区列信息（`FileTable.partitioning()` 已返回分区 schema）
   - 将 `allColumns` 分为 `dataColumns` 和 `partitionColumns` 传入 `WriteJobDescription`
   - 支持 `PARTITION_OVERWRITE_MODE`（参考 V1 `InsertIntoHadoopFsRelationCommand:65`）

2. **`DataFrameWriter.scala:598`** — 移除 FileDataSourceV2 写入守卫
   - `case Some(_: FileDataSourceV2) => None` → 允许 V2 写路径

3. **`DataSourceV2Utils.scala:167`** — 移除 FileDataSourceV2 Catalog 守卫
   - `!p.isInstanceOf[FileDataSourceV2]` → 移除此过滤

4. **`FallBackFileSourceV2.scala`** — 删除此文件
   - 同步从 `BaseSessionStateBuilder` 和 `HiveSessionStateBuilder` 的分析器规则链中移除

5. **各格式 Write 类** — 传递分区信息
   - `ParquetWrite`, `OrcWrite`, `CSVWrite`, `JsonWrite`, `TextWrite`

**风险控制**: 新增 SQL 配置开关 `spark.sql.sources.v2.file.write.enabled`（默认 false），开发稳定后翻转为 true。

**测试**:
- 新增 `FileDataSourceV2WriteSuite`：分区写入、动态分区覆盖
- 参数化现有 `ParquetQuerySuite`、`OrcQuerySuite` 同时覆盖 V1/V2 路径
- 启用 `ParquetQuerySuite:1339` 等当前被禁用的 V2 写路径测试

**复杂度**: L

---

## Phase 1: 文件 V2 Catalog 集成 — ✅ 完成

**依赖**: Phase 0

**状态**: 全部完成。详见 `datasource-v2-phase1-patches.md`。

**成果**:
- `FallBackFileSourceV2` 已删除，`V2_FILE_WRITE_ENABLED` 配置已移除
- V2 文件写入路径默认启用，所有 4 种 SaveMode 和 SQL 写入操作走 V2
- `FileTable` 实现 `SupportsPartitionManagement`（SHOW/ADD/DROP PARTITION）
- 写入后自动注册新分区到 catalog metastore
- Cache invalidation 通过 `recacheByPath` + `fileIndex.refresh()`
- `INSERT INTO format.\`path\`` 语法通过 V2 路径
- 自定义分区路径（ALTER TABLE ADD PARTITION LOCATION）完整支持
- 201 测试通过，0 回归

**复杂度**: L

---

## Phase 2: 统计信息 / ANALYZE TABLE — ✅ 完成

**依赖**: Phase 1

**成果**:
- 新增 `AnalyzeTableExec` 和 `AnalyzeColumnExec`（V2 原生，不依赖 V1 命令）
- 通过 `TableCatalog.alterTable()` + `TableChange.setProperty()` 持久化统计
- 表级属性：`spark.sql.statistics.totalSize`、`spark.sql.statistics.numRows`
- 列级属性：`spark.sql.statistics.colStats.<col>.<stat>`
- 支持 NOSCAN、FOR COLUMNS、FOR ALL COLUMNS
- `FileScan.estimateStatistics()` 返回存储的 `numRows`（通过 options 注入），优化器可利用行数统计
- 206 测试通过，0 回归

**复杂度**: S（实际比预估简单）

---

## Phase 3: 文件 V2 分桶 — ✅ 完成

**依赖**: Phase 0

**成果**:
- `FileWrite` 使用 `V1WritesUtils.getWriterBucketSpec()` 创建 `WriterBucketSpec`，替代硬编码 `None`
- `FileTable.createFileWriteBuilder` 从 `catalogTable.bucketSpec` 传递分桶信息
- 所有 6 个 `*Table` 和 `*Write` 类更新以传递 `BucketSpec`
- `FileDataSourceV2.getTable` 使用 `collect` 跳过 `BucketTransform`（通过 `catalogTable.bucketSpec` 处理）
- 207 测试通过，0 回归

**复杂度**: M（比预估简单，因为 `V1WritesUtils.getWriterBucketSpec` 可复用）

**注意**: 桶裁剪（bucket pruning）和桶 Join 优化未在此 Phase 实现，属于读取路径优化

---

## Phase 4: MSCK REPAIR TABLE — ✅ 完成

**依赖**: Phase 1

**成果**:
- 新增 `RepairTableExec`（V2 原生）
- 扫描 FS 分区目录，与 catalog 对比，注册缺失分区 / 删除孤立分区
- 使用 `FileTable.listPartitionIdentifiers()` 和 `SessionCatalog.createPartitions/dropPartitions`
- 208 测试通过，0 回归

**复杂度**: S

---

## Track A: JDBC 原生 V2 — 写入路径 ✅ 完成，读取路径待做

**JIRA**: SPARK-32593, SPARK-32595

**写入路径成果**（SPARK-32595 已修复）:
- 新增 `JDBCBatchWrite`（extends Write + BatchWrite）替代 V1Write
- Truncate + append 在同一 JDBC 连接中执行（原子性）
- `JDBCTable.capabilities` 从 `V1_BATCH_WRITE` 改为 `BATCH_WRITE`
- 79/79 JDBCV2Suite 测试通过

**读取路径**（SPARK-32593，待做）:
- `JDBCScan` 仍使用 `V1Scan`，需迁移到原生 `Scan` + `Batch`
- 需要实现 `InputPartition`、`PartitionReaderFactory`、`PartitionReader`
- 影响较小（JDBC 不支持嵌套列，V1Scan 的限制在 JDBC 场景下影响不大）

---

## Track B: DEFAULT 列值支持 V2 写（完全独立）

**JIRA**: SPARK-43752

**目标**: `ResolveColumnDefaultInCommandInputQuery` 支持 V2 写命令。

**改动**:
- `ResolveColumnDefaultInCommandInputQuery.scala:46` — 扩展匹配 `AppendData`、`OverwriteByExpression`、`OverwritePartitionsDynamic`
- V2 写命令通过 `DataSourceV2Relation.table.columns()` 携带默认值元数据

**复杂度**: S

---

## Track C: 流式文件 V2 写入（部分依赖 Phase 0）

**JIRA**: SPARK-27484, SPARK-33638

**目标**: 文件源 V2 可用于流式 Sink；流式写入有逻辑计划节点。

**改动**:
1. `ResolveDataSource.scala:95` / `DataStreamWriter.scala:252` — 移除 `FileDataSourceV2` 排除
2. `FileTable` 的 `WriteBuilder` 需产出 `StreamingWrite`
3. `MicroBatchExecution.scala:349` — 在分析前添加写入节点（SPARK-27484）
4. `DataStreamWriter.toTable()` — 完善 V2 表创建语义（SPARK-33638）

**复杂度**: L

---

## Track D: V2 Catalog 视图（完全独立）

**目标**: V2 catalog 支持 `CREATE VIEW`, `DROP VIEW`, `SHOW VIEWS` 等。

当前无 `ViewCatalog` 接口存在，需要设计新 API。这是最大的独立工作项，建议先产出设计文档后再实施。

**复杂度**: XL

---

## 并行化建议

| 时间段 | 开发者 1（关键路径） | 开发者 2 | 开发者 3 | 开发者 4 |
|--------|---------------------|----------|----------|----------|
| Q1 前半 | Phase 0 | Track A (JDBC) | Track B (DEFAULT) | Track D (视图设计) |
| Q1 后半 | Phase 1 | Track A 续 | Track C (流式) | Phase 4 (MSCK) |
| Q2 前半 | Phase 2 (统计) | Phase 3 (分桶) | Track C 续 | Track D 实施 |
| Q2 后半 | 集成测试 + 性能基准 | Phase 3 续 | 稳定化 | 稳定化 |

## 回滚策略

- 每个 Phase 都有独立 SQL 配置开关，默认 off
- 所有 Phase 稳定后翻转配置默认值，最终从 `useV1SourceList` 中移除内置格式
- 必须保证 V1 创建的表在 V2 路径下仍可正常读写

## 验证方式

- 单元测试：每个 Phase 的专项测试套件
- 集成测试：端到端 SQL 测试覆盖 V1/V2 双路径
- 性能基准：TPC-DS / TPC-H 对比 V1 和 V2 写入/扫描性能
- 兼容性测试：V1 写入的表 → V2 读取，反之亦然
