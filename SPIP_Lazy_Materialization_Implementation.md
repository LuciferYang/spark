# Parquet 向量化读取延迟物化 (Lazy Materialization) 实现文档

## 1. 背景与目标
本项目实现了 Spark SPIP "Lazy Materialization for Parquet Read Performance Improvement"。目标是通过延迟列数据的解码和物化，减少在过滤查询中被过滤掉的行所带来的不必要计算开销，从而提升读取性能。

## 2. 核心设计与实现

### 2.1 新增类：`LazyColumnVector`
**位置**: `sql/core/src/main/java/org/apache/spark/sql/execution/vectorized/LazyColumnVector.java`

`LazyColumnVector` 是 `WritableColumnVector` 的装饰器（Wrapper），实现了延迟加载的核心逻辑。

*   **延迟加载机制**: 类内部维护了一个 `Runnable loadTask` 和一个布尔标记 `isLoaded`。
*   **拦截访问**: 所有的读取方法（如 `getInt`, `getLong`, `getArray`, `getUTF8String` 等）在调用底层向量之前，都会先调用 `ensureLoaded()`。如果 `isLoaded` 为 false，则执行 `loadTask` 并将标记置为 true。
*   **透明代理**: 对于写入方法（如 `putInt`, `putLong`），直接委托给底层向量。这是因为写入操作通常是在 `loadTask` 执行过程中调用的，不需要触发加载。
*   **资源管理**: `close()` 和 `reset()` 方法正确地委托给底层向量，并重置加载状态。

### 2.2 读取器修改：`VectorizedParquetRecordReader`
**位置**: `sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedParquetRecordReader.java`

对读取器进行了关键修改以支持延迟特性：

1.  **配置集成**: 在初始化时读取 `spark.sql.parquet.enableLazyMaterialization` 配置项。
2.  **向量分配**: 在 `allocateColumns` 方法中，如果启用了延迟物化，将分配的 `OnHeap` 或 `OffHeap` 向量包装在 `LazyColumnVector` 中。
3.  **批次读取逻辑 (`nextBatch`)**:
    *   在处理每一列时，检查是否为 `LazyColumnVector` 且启用了延迟物化。
    *   如果是，不再立即调用 `readBatch` 进行解码。
    *   而是构建一个闭包（Lambda），该闭包包含解码该列及其子列（对于嵌套类型）的逻辑：
        ```java
        lazyVector.setLoadTask(() -> {
            try {
                // ... 执行原本的 readBatch 和 assemble 逻辑 ...
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        ```
    *   将此闭包注册到 `LazyColumnVector`。
    *   当 Spark 执行引擎（如 `FilterExec` 或 `ProjectExec`）首次访问该列数据时，才会触发这个闭包的执行。

### 2.3 配置项
**位置**: `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala`

新增配置 `spark.sql.parquet.enableLazyMaterialization`：
*   **默认值**: `false` (遵循 SPIP 建议，默认关闭以确保稳定性，需显式开启)。
*   **文档**: "Enables lazy materialization for the vectorized Parquet reader..."
*   **作用**: 全局控制是否启用 Parquet 向量化读取的延迟物化特性。

## 3. 实现思路与优势
传统的 Parquet 向量化读取流程是：
`读取整个 Batch 的所有列数据 -> 进行 Filter -> 后续处理`
这意味着即使某一行（或一批行）在第一列的 Filter 中就被淘汰，后续列的数据依然被解码了。对于宽表或复杂类型的列，这种无效解码浪费了大量的 CPU 和 I/O 资源。

改进后的流程（延迟物化）：
1.  `VectorizedParquetRecordReader` 准备好 `LazyColumnVector` 容器，但暂时不填充数据。
2.  上层算子（如 `FilterExec`）请求排在前面的过滤列数据。
3.  `LazyColumnVector` 拦截请求，触发该过滤列的解码任务。
4.  `FilterExec` 根据数据计算过滤条件。
5.  如果整行被过滤，后续的列（Projection Columns）将永远不会被访问，从而完全跳过了这些列的解码和物化过程。

**优势**:
*   **性能提升**: 对于高选择性（High Selectivity）查询，即过滤掉大部分数据的查询，性能提升尤为明显。
*   **资源节省**: 减少了不必要的内存写入和 CPU 解码指令。

## 4. 验证与效果

### 4.1 单元测试
**文件**: `sql/core/src/test/java/org/apache/spark/sql/execution/vectorized/LazyColumnVectorSuite.java`
*   验证了 `LazyColumnVector` 在 `put` 操作时**不会**触发加载（保持 Lazy）。
*   验证了在 `get` 操作时**正确触发**加载，并且只加载一次（Idempotent）。
*   验证了 `reset` 操作能重置加载状态，支持向量复用。
*   验证了嵌套类型（Struct）的子列访问也能触发父列的加载。

### 4.2 集成测试
**文件**: `sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetLazyMaterializationSuite.scala`
*   **正确性验证**: 验证了在开启 `spark.sql.parquet.enableLazyMaterialization` 的情况下，查询结果与预期一致。
*   **场景覆盖**: 覆盖了基本过滤场景和嵌套类型场景，确保引入延迟机制后不会破坏现有的读取逻辑。

通过上述设计与实现，成功将 Lazy Materialization 特性引入 Spark 的 Parquet 向量化读取路径中，为后续的性能优化奠定了基础。
