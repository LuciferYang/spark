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

## Phase 1: 文件 V2 Catalog 集成

**依赖**: Phase 0

**目标**: `V2SessionCatalog` 直接返回 `FileTable` 而非包装为 `V1Table`；写入后自动注册分区。

**关键改动**:

1. **`V2SessionCatalog.scala`** — `loadTable()` 对文件表返回原生 V2 `FileTable`
2. **`FileTable.scala`** — 实现 `SupportsPartitionManagement`（当表为 catalog 管理且有分区列时）
3. **`FileBatchWrite.scala`** — `commit()` 后发现新分区并通过 `SupportsPartitionManagement.createPartition()` 注册
4. **写后缓存失效** — 类似 V1 的 `cacheManager.recacheByPath`

**测试**: Catalog 管理的文件表创建、插入、`SHOW PARTITIONS`、`ALTER TABLE ADD/DROP PARTITION`

**复杂度**: L

---

## Phase 2: 统计信息 / ANALYZE TABLE

**依赖**: Phase 1

**目标**: V2 表支持 `ANALYZE TABLE` 和扫描级统计。

**JIRA**: 无独立 JIRA（属于 V2 统计基础设施）

**关键改动**:

1. **`DataSourceV2Strategy.scala:468-469`** — 移除无条件抛异常，替换为 V2 物理计划节点
2. **统计持久化** — 通过表属性存储（`spark.sql.statistics.totalSize`, `spark.sql.statistics.numRows` 等），使用 `TableCatalog.alterTable()` 更新
3. **`FileScan.scala:200`** — `numRows()` 从表属性读取已存储的统计值
4. **新增** `AnalyzeTableExec` / `AnalyzeColumnExec` V2 版本

**测试**: `ANALYZE TABLE COMPUTE STATISTICS`、`EXPLAIN COST` 反映统计值、优化器基于统计选择 join 策略

**复杂度**: M

---

## Phase 3: 文件 V2 分桶

**依赖**: Phase 0

**目标**: V2 文件写入支持 Hive 兼容分桶，读取支持桶裁剪。

**关键改动**:

1. **`FileTable.scala`** — 暴露 `BucketSpec`（从表属性或 catalog 元数据获取）
2. **`FileWrite.scala:141`** — `bucketSpec = None` → 使用表的实际 BucketSpec
3. **`FileScan.scala`** — 添加桶裁剪逻辑（参考 V1 `FileSourceStrategy.genBucketSet`，用 BitSet 过滤桶文件）
4. **桶 Join 优化** — V2 planner 识别分桶 V2 表用于 sort-merge join

**关键参考**: `FileSourceStrategy.scala:67-152`（桶裁剪）、`BucketingUtils.scala`（桶文件命名）

**测试**: 分桶写入验证（文件命名、桶 ID 正确性）、桶裁剪扫描、桶 Join

**复杂度**: L

---

## Phase 4: MSCK REPAIR TABLE

**依赖**: Phase 1

**JIRA**: SPARK-34397

**目标**: 实现 `RepairTable` V2 物理执行计划。

**关键改动**:

1. **`DataSourceV2Strategy.scala:547-548`** — 移除抛异常，映射到 `RepairTableExec`
2. **新增 `RepairTableExec.scala`** — 扫描文件系统分区目录 → 与 `SupportsPartitionManagement.listPartitionIdentifiers()` 对比 → 批量增删分区
3. 逻辑计划 `RepairTable` 已存在于 `v2Commands.scala:1669`

**复杂度**: M

---

## Track A: JDBC 原生 V2（完全独立）

**JIRA**: SPARK-32593, SPARK-32595

**目标**: 用原生 V2 `Scan`/`BatchWrite` 替换 `V1Scan`/`V1Write` 委托。

**改动**:
- `JDBCScan.scala` — 从 `V1Scan` 迁移到原生 `Scan` + `Batch` + `PartitionReaderFactory`
- `JDBCWriteBuilder.scala` — 从 `V1Write` 迁移到原生 `BatchWrite` + `DataWriter`；实现原子 truncate+append（SPARK-32595）

**复杂度**: M（每个 JIRA）

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
