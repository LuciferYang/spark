# Phase 0: 文件源 V2 分区写入 — Patch 分解

## 依赖关系

```
Patch 1 (分区列 plumbing)     ← 无行为变更，可独立合入
  ↓
Patch 2 (Feature Flag + 非分区写入)  ← 最小可验证单元
  ├── Patch 3 (分区写入)
  │     └── Patch 4 (动态分区覆盖)
  ├── Patch 5 (移除 DataFrameWriter 守卫)
  └── Patch 6 (移除 DataSourceV2Utils 守卫)
        ↓
Patch 7 (Schema 校验统一)     ← 独立于 3-6
        ↓
Patch 8 (清理 + Flag 翻转)    ← 收尾
```

可并行的: Patch 5 和 Patch 6 互相独立，可并行开发。Patch 7 也可与 3-6 并行。

---

## Patch 1: FileWrite 分区列管线（纯 plumbing，无行为变更）

**目标**: 让 `FileWrite` 能接收和使用分区列信息，但暂不改变现有行为（`FallBackFileSourceV2` 仍拦截所有写入）。

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../datasources/v2/FileWrite.scala` | 新增 `partitionSchema: StructType` 抽象字段；`createWriteJobDescription()` 中用它分离 `dataColumns` / `partitionColumns`，替换当前 `Seq.empty` |
| `sql/core/.../datasources/v2/FileTable.scala` | `newWriteBuilder()` → `mergedWriteInfo()` 时，将 `fileIndex.partitionSchema` 传递给具体 Write 类 |
| `sql/core/.../datasources/v2/parquet/ParquetWrite.scala` | 构造函数增加 `partitionSchema: StructType` 参数，实现 `FileWrite.partitionSchema` |
| `sql/core/.../datasources/v2/orc/OrcWrite.scala` | 同上 |
| `sql/core/.../datasources/v2/csv/CSVWrite.scala` | 同上 |
| `sql/core/.../datasources/v2/json/JsonWrite.scala` | 同上 |
| `sql/core/.../datasources/v2/text/TextWrite.scala` | 同上 |
| 对应的 `*Table.scala` | `newWriteBuilder()` 传递 `fileIndex.partitionSchema` |

**为什么无行为变更**: `FallBackFileSourceV2` 在分析器阶段就把所有 `InsertIntoStatement(FileTable)` 转回了 V1 路径，这个 Patch 的代码不会被执行到。但可以独立 review 和合入。

**测试**: 单元测试验证 `createWriteJobDescription()` 输出的 `partitionColumns` 正确（可通过直接构造 `FileWrite` 子类测试）。

**复杂度**: S

---

## Patch 2: 新增 Feature Flag + 非分区 V2 写入

**目标**: 添加配置开关，启用后非分区文件表走 V2 写路径。

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/catalyst/.../internal/SQLConf.scala` | 新增 `spark.sql.sources.v2.file.write.enabled`（默认 `false`） |
| `sql/core/.../datasources/FallBackFileSourceV2.scala` | `apply()` 中检查该 flag，为 `true` 时跳过 V2→V1 转换 |

**测试**:
- 启用 flag 后，`INSERT INTO` 非分区 Parquet/ORC/CSV/JSON 表走 V2 路径
- 验证写出的文件内容与 V1 路径一致（round-trip 读写对比）
- 验证 flag 关闭时行为不变

**复杂度**: S

---

## Patch 3: 分区 V2 写入

**目标**: 在 Patch 1 + 2 基础上，分区写入（动态分区）通过 V2 路径正确工作。

**依赖**: Patch 1, Patch 2

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../datasources/v2/FileWrite.scala` | 确保 `requiredOrdering` 包含动态分区列排序（参考 `V1WritesUtils.getSortOrder()`） |
| `sql/core/.../datasources/v2/V2Writes.scala` | 确保 `WriteFiles`（planned write）对 V2 文件写入也正确处理分区排序 |

**关键点**: `FileWriterFactory.scala:43-47` 已有条件判断——`partitionColumns.isEmpty` 时用 `SingleDirectoryDataWriter`，否则用 `DynamicPartitionDataSingleWriter`。Patch 1 填充了 `partitionColumns` 后，这段逻辑自动生效。

**测试**:
- 单级分区写入（Parquet、ORC、JSON、CSV 4 格式）
- V1/V2 分区写入结果对比（4 格式）
- 多级分区写入（year/month 两级）
- 多种格式: Parquet、ORC、CSV、JSON

**延迟到 Patch 5/6 的测试场景**:
- SQL `INSERT INTO parquet.\`path\`` 分区写入（依赖 DataSourceV2Utils 守卫移除）
- 静态分区写入（`INSERT INTO t PARTITION(year=2024) SELECT ...`）
- 混合静态+动态分区写入

**复杂度**: S（实际发现 Patch 1 plumbing 已足够，无需额外代码改动，仅需测试）

---

## Patch 4: 动态分区覆盖模式

**目标**: V2 文件写入支持 `PARTITION_OVERWRITE_MODE=DYNAMIC`。

**依赖**: Patch 3

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../datasources/v2/FileWrite.scala` | 从 `options` 读取 `partitionOverwriteMode`，在 `toBatch()` 中配置 `FileCommitProtocol` |
| `sql/core/.../datasources/v2/FileBatchWrite.scala` | 动态覆盖时，`commit()` 使用 staging 目录，仅替换写入的分区（参考 `InsertIntoHadoopFsRelationCommand:173-180`） |

**测试**:
- `INSERT OVERWRITE` 动态模式: 只覆盖写入的分区，其他分区不受影响
- `INSERT OVERWRITE` 静态模式: 覆盖指定分区
- 与 V1 行为一致性对比

**复杂度**: M

---

## Patch 5: 移除 DataFrameWriter 守卫

**目标**: `df.write.format("parquet").save(path)` 走 V2 路径。

**依赖**: Patch 2（flag 已验证可用）

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../classic/DataFrameWriter.scala:598` | 移除 `case Some(_: FileDataSourceV2) => None`，改为受 feature flag 控制；仅 `Append` 模式走 V2，其他模式回退 V1 |
| `sql/core/.../datasources/v2/FileTable.scala` | `CAPABILITIES` 增加 `TRUNCATE, OVERWRITE_DYNAMIC`；`createFileWriteBuilder` 增加 `SupportsTruncate` mixin |

**实际测试覆盖**:
- `df.write.mode("append").format(format).save(path)` → V2 Append
- `df.write.mode("overwrite").format(format).save(path)` → V1 回退（预期行为）
- `df.write.mode("ignore").format(format).save(path)` → V1 回退（预期行为）
- `df.write.partitionBy("col").parquet(path)` → V2 分区写入
- `df.write.option("compression", "snappy").parquet(path)` → V2 带选项写入

**当前限制 — DataFrame API 通过 V2 路径仅支持 Append 模式**:

实现过程中发现以下问题，导致 Overwrite/ErrorIfExists/Ignore 仍需回退 V1：

1. **Overwrite 模式无法走 V2**: V2 的 `OverwriteByExpression(Literal(true))` 对应全表 truncate 语义，但 `FileBatchWrite` 不会在写入前清空目标目录，导致旧文件和新文件并存。V1 通过 `InsertIntoHadoopFsRelationCommand.deleteMatchingPartitions` 在写入前清理。需要在 `FileBatchWrite` 中实现 truncate 语义（写入前清空目标路径）。

2. **动态分区覆盖 via DataFrame API 有 staging 目录问题**: `FileCommitProtocol(dynamicPartitionOverwrite=true)` 在 V2 路径的文件 rename 阶段报 `FileNotFoundException`，staging 目录下找不到分区子目录。V1 通过 `InsertIntoHadoopFsRelationCommand` 使用 `FileCommitProtocol.getStagingDir` 作为 `OutputSpec.outputPath` 来处理，但 V2 的 `WriteJobDescription.path` 语义与 V1 不完全一致，需要进一步调查。

3. **首次写入新路径失败**: V2 路径中 `DataFrameWriter.saveCommand` 调用 `getTable` → `FileTable` → `InMemoryFileIndex`，后者尝试 list 目标路径。如果路径不存在则抛 `PATH_NOT_FOUND`。V1 会在写入前自动创建路径。这意味着通过 DataFrame API V2 路径无法写入全新路径（Append 到不存在的路径也会失败），需要路径已存在。

4. **ErrorIfExists / Ignore 需要 `SupportsCatalogOptions`**: V2 的 `DataFrameWriter` 对非 Append/Overwrite 模式走 `CreateTableAsSelect` 分支，要求 provider 实现 `SupportsCatalogOptions`。`FileDataSourceV2` 不实现此接口，因此这两个模式必须回退 V1。需要 Patch 6（Catalog 集成）解决。

**后续计划**: 以上问题 1-3 需要新增一个 Patch（建议作为 Patch 5.5 或在 Patch 8 清理阶段处理），实现 `FileBatchWrite` 的 truncate/overwrite 语义和首次写入路径创建。问题 4 由 Patch 6 解决。

**复杂度**: S（实际改动小，但暴露了后续需要解决的问题）

---

## Patch 6: 移除 DataSourceV2Utils 守卫

**目标**: Catalog 管理的文件表走 V2 路径。

**依赖**: Patch 2

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../datasources/v2/DataSourceV2Utils.scala:167-168` | 移除 `!p.isInstanceOf[FileDataSourceV2]` 过滤，改为受 feature flag 控制 |

**实际测试覆盖**:
- `CREATE TABLE t USING parquet` + `INSERT INTO t` → V2 写入 ✓
- `CREATE TABLE t ... PARTITIONED BY` + `INSERT INTO t` → V2 分区写入 ✓
- `CTAS (CREATE TABLE AS SELECT)` → V2 路径 ✓

**Patch 6 发现的限制 — 需后续 Phase 1 解决**:

1. **`SHOW PARTITIONS`** 不支持：`ParquetTable` 未实现 `SupportsPartitionManagement`，报 `INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED`。

2. **`INSERT INTO parquet.\`path\`` 语法不支持**：走 `ResolveSQLOnFile` 路径，需要 `SupportsCatalogOptions`，`FileDataSourceV2` 未实现，报 `TABLE_OR_VIEW_NOT_FOUND`。

**延迟到 Phase 1 的测试场景**（来自 Patch 3 + Patch 6）:
- `SHOW PARTITIONS` 查看分区
- `INSERT INTO parquet.\`path\`` 语法
- `ALTER TABLE ADD/DROP PARTITION`
- 静态分区写入（`INSERT INTO t PARTITION(year=2024) SELECT ...`）
- 混合静态+动态分区写入

**复杂度**: S

---

## Patch 7: SPARK-36340 Schema 校验统一

**目标**: 统一 V1/V2 Insert 的 schema 字段校验逻辑。

**改动**:

| 文件 | 改动 |
|------|------|
| `sql/core/.../datasources/v2/FileWrite.scala:101` | 替换当前简单的 `dataType` 校验，使用与 V1 统一的校验逻辑（字段名、类型、nullable 检查） |
| 提取共用校验逻辑 | 若 V1 校验在 `DataSource.scala` 或 `HiveMetastoreClient` 中，提取到共享工具类 |

**测试**:
- Schema 不匹配时的错误消息一致性
- 字段类型转换（隐式 cast）行为一致
- Nullable 字段插入非空数据

**复杂度**: S

---

## Patch 8: 清理和 Flag 翻转 — 延迟到 Phase 1

**目标**: 移除 `FallBackFileSourceV2` 和残留 TODO，准备默认启用 V2。

**状态**: ❌ 暂不可行，延迟到 Phase 1

**原因**: 翻转 flag 默认值（`false` → `true`）后，`FileBasedDataSourceSuite` 有 6 个测试失败：

1. **Cache invalidation**（"Do not use cache on overwrite/append"）：V2 路径的 `refreshCache` 通过 plan 匹配，无法 invalidate 通过 `spark.read.orc(path)` 创建的 cached DataFrame。V1 通过路径匹配。
2. **`checkPartitioningMatchesV2Table`**（SPARK-36568）：DataFrame API 第二次写入已有分区目录时，V2 FileTable 的 `partitioning()` 从 `fileIndex.partitionSchema` 读取，但 `getTable` 传入的 `partitioningAsV2` 与已有数据不一致。
3. **数据类型校验路径差异**（SPARK-24204, SPARK-51590, Geospatial）：V1 和 V2 对不支持类型的错误消息/异常类型不同，导致 `intercept` 断言失败。

**Phase 1 需要解决的前置问题**:
- V2 FileTable cache invalidation（基于路径的 cache recache）
- `checkPartitioningMatchesV2Table` 对 file source 的兼容
- 数据类型校验错误消息统一
- 然后才能安全翻转 flag 并删除 `FallBackFileSourceV2`

**已完成的清理**（在 Patch 7 中）:
- SPARK-36340 TODO 注释已移除
- `checkNoCollationsInMapKeys` 校验已对齐
