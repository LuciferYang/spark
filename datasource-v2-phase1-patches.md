# Phase 1: Flag 翻转前置修复 + 功能补全 — Patch 分解

## 依赖关系

```
Patch 1 (Cache invalidation)           ← 修复 2 个测试
  ↓
Patch 2 (checkPartitioningMatchesV2Table) ← 修复 1 个测试
  ↓
Patch 3 (数据类型校验统一)               ← 修复 3 个测试
  ↓
Patch 4 (Flag 翻转 + 删除 FallBackFileSourceV2) ← 收尾
  ↓
Patch 5 (SupportsCatalogOptions)         ← ErrorIfExists/Ignore 支持
  ↓
Patch 6 (SupportsPartitionManagement)    ← SHOW PARTITIONS / ALTER TABLE
  ↓
Patch 7 (customPartitionLocations)       ← 自定义分区路径
```

Patch 1-3 互相独立，可并行开发。Patch 4 依赖 1-3 全部完成。Patch 5-7 互相独立，依赖 Patch 4。

---

## Patch 1: V2 文件写入 Cache Invalidation

**目标**: 修复 V2 文件写入后 cached DataFrame 未刷新的问题。

**修复测试**:
- "Do not use cache on overwrite"（1000 != 10）
- "Do not use cache on append"（1000 != 1010）

**根因分析**:
- V1 路径：`InsertIntoHadoopFsRelationCommand` 写入完成后调用 `cacheManager.recacheByPath(session, outputPath, fs)`，基于路径匹配 invalidate cache
- V2 路径：`DataSourceV2Strategy.refreshCache()` 对非 catalog 表调用 `cacheManager.recacheByPlan(session, r)`，基于 plan 匹配。但写入时创建的 `DataSourceV2Relation` 与读取时 cached 的 plan 不同，匹配失败
- `CacheManager.recacheByPath()` 已经支持 `FileTable` 节点（`CacheManager.scala:563-564`），只是 V2 写入路径没有调用它

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../v2/DataSourceV2Strategy.scala:68-74` | `refreshCache()` 中增加 FileTable 分支：从 `DataSourceV2Relation.table` 提取 `FileTable.paths`，调用 `cacheManager.recacheByPath()` |

**具体方案**:
```scala
private def refreshCache(r: DataSourceV2Relation)(): Unit = r match {
  case ExtractV2CatalogAndIdentifier(catalog, ident) =>
    val nameParts = ident.toQualifiedNameParts(catalog)
    cacheManager.recacheTableOrView(session, nameParts, includeTimeTravel = false)
  case _ =>
    // For file-based V2 tables, use path-based cache invalidation
    // (same as V1's InsertIntoHadoopFsRelationCommand)
    r.table match {
      case ft: FileTable if ft.paths.nonEmpty =>
        val path = new Path(ft.paths.head)
        val fs = path.getFileSystem(session.sessionState.newHadoopConf())
        cacheManager.recacheByPath(session, path, fs)
      case _ =>
        cacheManager.recacheByPlan(session, r)
    }
}
```

**测试**: 现有 `FileBasedDataSourceSuite` 的 "Do not use cache on overwrite/append" 通过

**复杂度**: S

---

## Patch 2: checkPartitioningMatchesV2Table 兼容 FileTable

**目标**: 修复 DataFrame API 写入已有分区目录时 `checkPartitioningMatchesV2Table` 报错。

**修复测试**:
- "SPARK-36568: FileScan statistics estimation takes read schema into account"

**根因分析**:
- 测试用 `df.write.partitionBy("k").orc(path)` 写入分区数据
- 第二次写入同一路径时，`DataFrameWriter.saveCommand()` 调用 `getTable` → `provider.getTable(df.schema.asNullable, partitioningAsV2.toArray, dsOptions)`
- 但 `checkPartitioningMatchesV2Table(table)` 比较 `partitioningColumns`（用户指定的 `partitionBy("k")`）与 `table.partitioning()`
- `FileTable.partitioning()` 从 `fileIndex.partitionSchema` 推断，但 `getTable` 传入的 `partitioningAsV2` 已经包含了分区信息，`FileTable` 应该使用它
- 实际问题：`supportsExternalMetadata()` 为 true 时，`getTable` 传入了 schema 和 partitioning，但 `FileTable` 的 `partitioning()` 实现可能没有使用传入的 partitioning

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../classic/DataFrameWriter.scala` | `checkPartitioningMatchesV2Table` 对 `FileTable` 跳过检查（FileTable 的分区信息由写入者控制，不需要与已有数据匹配） |

**备选方案**: 让 `FileTable` 在构造时保存传入的 `partitioning` 参数，`partitioning()` 优先返回它。需要检查 `FileTable` 的构造流程。

**测试**: 现有 `FileBasedDataSourceSuite` 的 SPARK-36568 测试通过

**复杂度**: S

---

## Patch 3: 数据类型校验错误消息统一

**目标**: 统一 V1/V2 对不支持数据类型的错误处理，使异常类型和消息一致。

**修复测试**:
- "SPARK-24204 error handling for unsupported Null data types - csv, orc"
- "SPARK-51590: unsupported the TIME data types in data sources"
- "Geospatial types are not supported in file data sources other than Parquet"

**根因分析**:
- V1 路径：数据类型校验在 `DataSource.validateSchema()` 中，抛出 `AnalysisException`（`UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE`）
- V2 路径：数据类型校验在 `FileTable.schema` 属性访问时触发（`FileTable.scala:86-89`），调用 `supportsDataType()` → 抛出 `QueryCompilationErrors.dataTypeUnsupportedByDataSourceError()`
- 问题 1：V2 的校验发生在 `DataSourceV2Relation.create()` 阶段（读取 `table.columns.asSchema`），而非写入阶段
- 问题 2：V2 校验在 `getTable` 时就触发了，而测试期望在 `write` 操作时抛出异常
- 问题 3：某些测试 `intercept` 的异常类型与 V2 抛出的不同

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../v2/FileTable.scala:86-89` | `dataSchema` 中的 `supportsDataType` 校验改为延迟到写入阶段（或在 `getTable` 时不校验写入 schema，只校验读取 schema） |
| `sql/core/.../v2/FileWrite.scala` | `validateInputs()` 中已有 `supportsDataType` 校验，确保错误消息与 V1 一致 |

**具体方案**:
- `FileTable.schema` 中的 `supportsDataType` 校验仅对读取路径生效
- 写入路径的校验统一在 `FileWrite.validateInputs()` 中，使用与 V1 相同的 `DataSource.validateSchema()` 逻辑（已在 Patch 7 中部分对齐）
- 需要确保 `getTable` 传入写入 schema 时不触发读取侧的类型校验

**测试**: 现有 3 个失败测试通过

**复杂度**: M

---

## Patch 4: Flag 翻转 + 删除 FallBackFileSourceV2

**目标**: 默认启用 V2 文件写入路径，移除 `FallBackFileSourceV2` hack。

**依赖**: Patch 1-3 全部完成，`FileBasedDataSourceSuite` 全部通过

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/catalyst/.../internal/SQLConf.scala` | `V2_FILE_WRITE_ENABLED` 默认值 `false` → `true` |
| `sql/core/.../datasources/FallBackFileSourceV2.scala` | **删除** |
| `sql/core/.../internal/BaseSessionStateBuilder.scala:228` | 从规则链中移除 `FallBackFileSourceV2` |
| `sql/hive/.../hive/HiveSessionStateBuilder.scala:128` | 同上 |
| `sql/catalyst/.../internal/SQLConf.scala` | 移除 `V2_FILE_WRITE_ENABLED` 配置项（不再需要） |
| `sql/core/.../classic/DataFrameWriter.scala` | `lookupV2Provider()` 中移除 `V2_FILE_WRITE_ENABLED` 守卫 |
| `sql/core/.../v2/DataSourceV2Utils.scala` | 移除 `V2_FILE_WRITE_ENABLED` 守卫 |

**前置验证**:
- `FileBasedDataSourceSuite` 全部通过
- `ParquetQuerySuite`、`OrcQuerySuite`、`CSVQuerySuite`、`JsonQuerySuite` 无回归
- `FileDataSourceV2FallBackSuite` 适配（部分测试需要调整，因为 fallback 行为不再存在）
- `CachedTableSuite` 无回归

**复杂度**: M（改动简单，但验证范围大）

---

## Patch 5: FileDataSourceV2 实现 SupportsCatalogOptions

**目标**: 让文件源 V2 provider 支持 catalog 操作，解锁 `SaveMode.ErrorIfExists` / `SaveMode.Ignore`。

**依赖**: Patch 4

**背景**:
- 当前 `DataFrameWriter.lookupV2Provider()` 对 `ErrorIfExists` / `Ignore` 模式回退 V1，因为这两种模式需要 `SupportsCatalogOptions` 来检查表是否存在
- `SupportsCatalogOptions` 要求实现 `extractIdentifier()` 和 `extractCatalog()`

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../v2/parquet/ParquetDataSourceV2.scala` | 实现 `SupportsCatalogOptions`：`extractIdentifier()` 从 path 生成 `Identifier`，`extractCatalog()` 返回 session catalog |
| `sql/core/.../v2/orc/OrcDataSourceV2.scala` | 同上 |
| `sql/core/.../v2/csv/CSVDataSourceV2.scala` | 同上 |
| `sql/core/.../v2/json/JsonDataSourceV2.scala` | 同上 |
| `sql/core/.../v2/text/TextDataSourceV2.scala` | 同上 |
| `sql/core/.../classic/DataFrameWriter.scala` | 移除 `ErrorIfExists` / `Ignore` 的 V1 回退守卫 |

**参考**: `KafkaSourceProvider` 已实现 `SupportsCatalogOptions`，可参考其模式。

**测试**:
- `SaveMode.ErrorIfExists`：表已存在时抛出 `TABLE_OR_VIEW_ALREADY_EXISTS`
- `SaveMode.Ignore`：表已存在时静默跳过
- `INSERT INTO parquet.\`path\`` 语法正常工作

**复杂度**: M

---

## Patch 6: FileTable 实现 SupportsPartitionManagement

**目标**: 支持 `SHOW PARTITIONS`、`ALTER TABLE ADD/DROP PARTITION` 等分区 DDL。

**依赖**: Patch 5（需要 catalog 集成）

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../v2/FileTable.scala` | 实现 `SupportsPartitionManagement` 接口 |
| 需要实现的方法 | `createPartition()`, `dropPartition()`, `listPartitionIdentifiers()`, `partitionSchema()` |

**实现思路**:
- `listPartitionIdentifiers()`: 扫描文件系统目录结构，解析 `key=value` 分区路径
- `createPartition()`: 创建分区目录（`fs.mkdirs`）
- `dropPartition()`: 删除分区目录
- `partitionSchema()`: 返回 `fileIndex.partitionSchema`

**测试**:
- `SHOW PARTITIONS` 列出所有分区
- `ALTER TABLE ADD PARTITION` 创建新分区目录
- `ALTER TABLE DROP PARTITION` 删除分区目录
- 写入后自动发现新分区

**复杂度**: M

---

## Patch 7: customPartitionLocations 支持

**目标**: 支持用户指定的自定义分区路径（`ALTER TABLE ADD PARTITION LOCATION`）。

**依赖**: Patch 6

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../v2/FileWrite.scala` | `customPartitionLocations` 从 `Map.empty` 改为从表元数据/catalog 获取 |
| `sql/core/.../v2/FileTable.scala` | `SupportsPartitionManagement.createPartition()` 支持 `location` 参数 |

**复杂度**: S

---

## 总结

| Patch | 目标 | 修复测试数 | 复杂度 | 可并行 |
|-------|------|-----------|--------|--------|
| 1 | Cache invalidation | 2 | S | ✅ |
| 2 | checkPartitioningMatchesV2Table | 1 | S | ✅ |
| 3 | 数据类型校验统一 | 3 | M | ✅ |
| 4 | Flag 翻转 + 删除 FallBack | — | M | ❌（依赖 1-3） |
| 5 | SupportsCatalogOptions | — | M | ✅（依赖 4） |
| 6 | SupportsPartitionManagement | — | M | ✅（依赖 5） |
| 7 | customPartitionLocations | — | S | ❌（依赖 6） |

Patch 1-3 可并行开发，是 Phase 1 的关键路径。Patch 4 是里程碑。Patch 5-7 是功能扩展。
