# DataSource V2 vs V1 功能差距调研报告

基于对 Spark 4.2.0-SNAPSHOT 代码库的全面调研，以下是 V2 相对 V1 仍然缺失或不完整的功能。

---

## 核心事实：所有内置文件格式默认仍使用 V1

`spark.sql.sources.useV1SourceList` 默认值为 `"avro,csv,json,kafka,orc,parquet,text"`，即所有主要文件格式都走 V1 路径。（`SQLConf.scala:4731`）

---

## 一、文件源 V2 写路径完全不可用

| 问题 | JIRA | 位置 |
|------|------|------|
| 文件源 V2 写路径被硬编码禁用 | SPARK-28396 | `DataFrameWriter.scala:598` — `case Some(_: FileDataSourceV2) => None` |
| 文件源 V2 无法与 catalog 表协作 | SPARK-28396 | `DataSourceV2Utils.scala:167` |
| 分区写入未实现，`partitionColumns = Seq.empty` | SPARK-36340 | `FileWrite.scala:131-133` |
| `bucketSpec` 硬编码为 `None` | — | `FileWrite.scala:141` |
| `customPartitionLocations` 硬编码为 `Map.empty` | — | `FileWrite.scala` |
| Parquet V2 写路径测试全部禁用 | — | `ParquetQuerySuite.scala:1339` 等多处 |

**结果**：所有文件格式写入实际都通过 `FallBackFileSourceV2` 回退到 V1 的 `InsertIntoHadoopFsRelationCommand`。

---

## 二、Bucketing（分桶）

- **V1**：`HadoopFsRelation` 携带 `BucketSpec`，`FileSourceStrategy` 支持桶裁剪（bucket pruning），写入时按桶哈希分配文件
- **V2 文件源**：`FileTable` 不暴露 `BucketSpec`，`FileWrite` 硬编码 `bucketSpec = None`，`FileScan` 无桶裁剪逻辑
- **注意**：V2 有 `KeyedPartitioning`（`spark.sql.sources.v2.bucketing.enabled`），但这是面向非文件连接器的 storage-partition join 优化，与 Hive 兼容的文件分桶完全不同

---

## 三、统计信息（Statistics）

| 能力 | V1 | V2 |
|------|----|----|
| 表大小估算 | 支持（含压缩因子） | 仅基于文件大小比例估算 |
| 行数统计 | `CatalogStatistics.rowCount` | `FileScan.numRows()` 始终返回 `empty` |
| 列级统计 | `CatalogColumnStat` | 无 |
| `ANALYZE TABLE` | 完整支持 | **直接抛异常**（`DataSourceV2Strategy.scala:469`） |
| 写后自动更新统计 | `CommandUtils.updateTableStats()` | 无 |
| 分区裁剪后统计更新 | `FilterEstimation` 传播到 `CatalogStatistics` | 无 |
| CSV V2 `SupportsReportStatistics` | — | 未实现（`FileBasedDataSourceSuite.scala:685`） |

---

## 四、流式处理（Streaming）

| 问题 | JIRA | 说明 |
|------|------|------|
| 文件源 V2 不支持流式读写 | — | `ResolveDataSource.scala:95`、`DataStreamWriter.scala:252` 显式排除 `FileDataSourceV2` |
| 无流式写入逻辑计划节点 | SPARK-27484 | 导致无法在分析阶段检查 `STREAMING_WRITE` 能力 |
| `toTable()` 建表不完整 | SPARK-33638 | V2 表创建语义未完整覆盖 |
| V1 回退逻辑未正式成为分析器规则 | SPARK-27483 | `ResolveDataSource.scala:119` |
| CommitLog V2→V1 向后兼容缺失 | SPARK-50653 | `CommitLog.scala:67` |

---

## 五、SQL 命令缺失

以下命令对 V2 表直接报错（`DataSourceV2Strategy.scala:468-548`）：

- `ANALYZE TABLE` / `ANALYZE COLUMN`
- `ALTER TABLE ... RECOVER PARTITIONS`
- `ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]`
- `LOAD DATA`
- `SHOW CREATE TABLE AS SERDE`
- `MSCK REPAIR TABLE`（SPARK-34397）

---

## 六、View 支持

V2 catalog **不支持视图 API**（`v2Commands.scala:1328`）。`SHOW VIEWS`、`DROP VIEW`、`ALTER VIEW` 等对 V2 catalog 均报 `UNSUPPORTED_FEATURE.CATALOG_OPERATION`。

---

## 七、其他缺失

| 问题 | JIRA | 位置 |
|------|------|------|
| `DEFAULT` 列值不支持 V2 写命令 | SPARK-43752 | `ResolveColumnDefaultInCommandInputQuery.scala:46` |
| JDBC V2 使用 `V1Scan`/`V1Write` 内部回退 | SPARK-32593/32595 | JDBC 无嵌套列裁剪，truncate+append 非原子 |
| `DESCRIBE TABLE COLUMN` 嵌套列不支持 V2 | — | `DataSourceV2SQLSuite.scala:245` |
| 非 catalog 路径语义未定义 | — | `DataSourceV2Utils.scala:147` |
| `FileDataSourceV2` 分区推断是 stub 实现 | SPARK-29665 | `FileDataSourceV2.scala:83` |
| 单趟分析器不支持 `DataSourceV2Relation` | — | `ExplicitlyUnsupportedResolverFeature.scala` |
| 写后自动缓存失效 | — | V1 有 `cacheManager.recacheByPath`，V2 无 |
| `ifPartitionNotExists` 语义 | — | V1 支持静态分区插入时跳过已存在分区，V2 无 |
| DataSourceRDD 类型擦除 hack | SPARK-25083 | `DataSourceRDD.scala:140` |

---

## V2 相比 V1 的优势（供参考）

V2 并非全面落后，在以下方面优于 V1：
- **自定义指标**（`CustomMetric` API，3.2.0+）
- **聚合下推**（`SupportsPushDownAggregates`）
- **LIMIT/OFFSET/TopN/TABLESAMPLE 下推**
- **Join 下推**（`SupportsPushDownJoin`）
- **运行时过滤**（`SupportsRuntimeV2Filtering`）
- **迭代分区谓词下推**（SPARK-55596）
- **行级操作**（`ReplaceData`、`WriteDelta`）

---

## 涉及的 JIRA 汇总

| JIRA | 主题 | 状态 | 备注 |
|------|------|------|------|
| SPARK-25083 | DataSourceRDD 类型擦除 hack | **Closed (Fixed)** | 已在 3.0.0 修复，但代码中仍残留 TODO 注释 |
| SPARK-27483 | V1 回退逻辑应成为分析器规则 | **In Progress** | |
| SPARK-27484 | 流式写入无逻辑计划节点 | **Open** | |
| SPARK-28396 | 文件源 V2 写路径不可用 | **Resolved (Won't Fix)** | JIRA 已关闭但代码中 TODO 仍在，实际问题未解决 |
| SPARK-29665 | FileDataSourceV2 stub 实现需清理 | **Closed (Fixed)** | 已在 3.0.0 修复，但代码中仍残留 TODO 注释 |
| SPARK-32593 | JDBC V2 无嵌套列裁剪 | **Open** | |
| SPARK-32595 | JDBC V2 truncate+append 非原子 | **Open** | |
| SPARK-33638 | 流式 V2 表创建不完整 | **Open** | |
| SPARK-34397 | MSCK REPAIR TABLE 不支持 V2 | **Open** | |
| SPARK-36340 | V2 Insert schema 检查和分区写入未实现 | **Open** | |
| SPARK-43752 | DEFAULT 列值不支持 V2 写命令 | **Open** | |
| SPARK-50653 | CommitLog V2→V1 向后兼容缺失 | **Open** | |

### 状态统计

- **Open（8 个）**：SPARK-27484, SPARK-32593, SPARK-32595, SPARK-33638, SPARK-34397, SPARK-36340, SPARK-43752, SPARK-50653
- **In Progress（1 个）**：SPARK-27483
- **Closed/Fixed（2 个）**：SPARK-25083, SPARK-29665（均在 3.0.0 修复，但代码中仍有残留 TODO）
- **Resolved/Won't Fix（1 个）**：SPARK-28396（JIRA 标记为 Won't Fix，但代码中写路径禁用的 TODO 注释仍然存在，实际功能缺失依旧）
