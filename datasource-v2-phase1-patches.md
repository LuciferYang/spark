# Phase 1: Flag 翻转 + 功能补全 — 已完成 ✅

## 状态: 全部完成

所有 7 个 Patch 已完成。201/201 测试通过，0 回归。V2 文件写入路径完全启用，无 V1 回退。

## 总结

| Patch | 目标 | 状态 | 关键改动 |
|-------|------|------|---------|
| 1 | Cache invalidation | ✅ | `refreshCache()` 对 FileTable 用 `recacheByPath` + `fileIndex.refresh()` |
| 2 | checkPartitioningMatchesV2Table | ✅ | 对 FileTable 跳过分区匹配检查 |
| 3 | 数据类型校验统一 | ✅ 无需改动 | V2 已与 V1 一致 |
| 4 | Flag 翻转 + 删除 FallBack | ✅ | 删除 `FallBackFileSourceV2`、`V2_FILE_WRITE_ENABLED` 配置项；修复非存在路径写入和 fileIndex 刷新 |
| 5 | ErrorIfExists/Ignore V2 路径 | ✅ | 路径存在性检查（匹配 V1 语义）；INSERT INTO format.\`path\` 通过 ResolveSQLOnFile V2 解析 |
| 5a | V2 分区写入修复 | ✅ | RequiresDistributionAndOrdering 排序；userSpecifiedPartitioning plumbing；supportsDataType 跳过分区列；lazy→val description 修复 Parquet summary |
| 6 | SupportsPartitionManagement | ✅ | SHOW/ADD/DROP PARTITION；partitionSchema fallback to userSpecifiedPartitioning |
| 7 | customPartitionLocations | ✅ | createPartition 支持 location；分区操作同步 catalog metastore；CatalogFileIndex 支持自定义路径读取；syncNewPartitionsToCatalog 自动注册 INSERT INTO 的新分区 |

## V2 写入路径完整覆盖

**DataFrame API**:
- `mode("append")` → V2 AppendData ✅
- `mode("overwrite")` → V2 OverwriteByExpression ✅
- `mode("error")` 路径存在 → V2 抛 PATH_ALREADY_EXISTS ✅
- `mode("error")` 路径不存在 → V2 AppendData ✅
- `mode("ignore")` 路径存在 → V2 跳过（LocalRelation）✅
- `mode("ignore")` 路径不存在 → V2 AppendData ✅

**SQL**:
- `INSERT INTO table` → V2 ✅
- `INSERT OVERWRITE table` → V2 ✅
- `CREATE TABLE AS SELECT` → V2 ✅
- `INSERT INTO format.\`path\`` → V2（ResolveSQLOnFile）✅

**分区 DDL**:
- `SHOW PARTITIONS` → V2 SupportsPartitionManagement ✅
- `ALTER TABLE ADD PARTITION` → V2（含 LOCATION 支持）✅
- `ALTER TABLE DROP PARTITION` → V2 ✅

## 各 Patch 对应分支

| Patch | 分支 |
|-------|------|
| 1-4 | `dsv2-phase1-patch1-cache-invalidation` |
| 5-5a | `dsv2-phase1-patch5-catalog-options` |
| 6-7 | `dsv2-phase1-patch6-partition-management` |

## 仅剩的 V1 回退场景（均为正确行为）

- Provider 不支持 `BATCH_WRITE`（如只读 provider），通过 `fallbackFileFormat` 走 V1
- 用户通过 `USE_V1_SOURCE_LIST` 显式配置强制 V1
