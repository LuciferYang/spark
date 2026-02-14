# VectorizedPlainValuesReader 基准测试详细分析报告

本文档基于 `sql/core/benchmarks/VectorizedPlainValuesReaderJMHBenchmark.txt` 的测试结果，对 `VectorizedPlainValuesReader`（基准版本/旧版）与 `VectorizedSingleBufferPlainValuesReader`（优化版本/新版）进行详细的性能对比分析。

## 1. 详细场景对比分析

本节按场景类型对测试数据进行拆解对比。所有得分为 `ns/op`（越低越好），数据基于 `bufferSize=4096, numValues=1000000` 的测试项。

### 1.1 Binary 类型 (Variable Width)

Binary 类型的读取和跳过是本次优化的核心目标。

| 场景 | Buffer类型 | 旧版得分 (ns) | 新版得分 (ns) | 加速比 | 收益分析 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **纯跳过 (initAndSkipAll)** | Single | 3,137,365 | 1,167,932 | **2.69x** | 正向 (大幅收益) |
| **纯跳过 (initAndSkipAll)** | Multi | 11,989,921 | 4,860,070 | **2.47x** | 正向 (大幅收益) |
| **纯读取 (readBatch)** | Single | 7,582,985 | 6,147,747 | **1.23x** | 正向 (中等收益) |
| **纯读取 (readBatch)** | Multi | 35,360,837 | 9,825,568 | **3.60x** | 正向 (极大收益) |
| **混合 (Skip 90% Read 10%)** | Single | 3,977,057 | 1,643,974 | **2.42x** | 正向 (大幅收益) |
| **混合 (Skip 50% Read 50%)** | Single | 5,233,544 | 3,634,329 | **1.44x** | 正向 (显著收益) |

*   **分析**: Binary 类型在所有场景下均取得了显著的正向收益。Skip 操作的优化尤为明显，不再需要解析每一个值的长度，而是直接读取长度并累加 position。在 MultiBuffer 场景下的读取更是有 3.6倍 的提升，表明新实现有效减少了跨 Buffer 处理的开销。

### 1.2 Boolean 类型 (Bit Packing)

| 场景 | Buffer类型 | 旧版得分 (ns) | 新版得分 (ns) | 加速比 | 收益分析 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **纯读取 (readBooleans)** | Single | 202,296 | 225,034 | **0.90x** | **负向** (轻微回退) |
| **纯读取 (readBooleans)** | Multi | 454,437 | 243,306 | **1.87x** | 正向 (大幅收益) |
| **纯跳过 (skipBooleans)** | Single | 9.56 | 11.76 | 0.81x | **负向** (微乎其微) |
| **混合 (Skip 90% Read 10%)** | Single | 20,934 | 21,816 | 0.96x | 负向 (持平) |

*   **分析**:
    *   **SingleBuffer**: 旧版本针对 Boolean 的位操作在 SingleBuffer 下非常极致，新版本引入的抽象层级或逻辑调整导致了约 10% 的回退。Skip 操作虽然倍数上看慢了，但绝对值仅差 2ns，属于噪声范围，可忽略。
    *   **MultiBuffer**: 新版本表现优异，快了接近 2 倍。这是因为 Boolean 跨页时需要处理位偏移，旧版的通用逻辑开销较大，优化版处理得更好。

### 1.3 定长类型 (Int, Long, Float, Double) - 批量读取 (Batch Read)

| 类型 | 场景 | Buffer类型 | 旧版得分 (ns) | 新版得分 (ns) | 加速比 | 收益分析 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Int** | readIntegers | Single | 187,626 | 187,841 | 1.00x | 持平 |
| **Int** | readIntegers | Multi | 554,046 | 550,706 | 1.01x | 持平 |
| **Long** | readLongs | Single | 236,990 | 235,772 | 1.01x | 持平 |
| **Long** | readLongs | Multi | 974,133 | 971,640 | 1.00x | 持平 |
| **Double** | readDoubles | Single | 127,827 | 119,367 | 1.07x | 正向 (微小收益) |
| **Double** | readDoubles | Multi | 908,452 | 905,084 | 1.00x | 持平 |

*   **分析**:
    *   对于最常用的 SingleBuffer 场景，新旧版本均通过 `Unsafe` 或 `ByteBuffer.get` 进行批量内存拷贝，性能完全一致。
    *   对于 MultiBuffer，定长类型的优化空间有限（主要是内存拷贝），两者性能也基本持平。

### 1.4 定长类型 - 逐个读取 (Single Read)

用于非向量化路径（如 `row-by-row` 处理）。

| 类型 | 场景 | Buffer类型 | 旧版得分 (ns) | 新版得分 (ns) | 加速比 | 收益分析 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Int** | readIntegerSingle | Single | 7,945 | 4,736 | **1.68x** | 正向 (显著收益) |
| **Long** | readLongSingle | Single | 7,868 | 4,675 | **1.68x** | 正向 (显著收益) |
| **Double** | readDoubleSingle | Single | 7,733 | 4,615 | **1.68x** | 正向 (显著收益) |

*   **分析**: 新版本彻底移除了循环内的多余判断，对于单次调用 `readInt()` / `readLong()` 等 API，开销降低了 40% 以上。

### 1.5 无符号类型 (Unsigned)

| 类型 | 场景 | Buffer类型 | ��版得分 (ns) | 新版得分 (ns) | 加速比 | 收益分析 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **UInt** | readUnsignedIntegers | Single | 845,442 | 1,350,705 | **0.63x** | **负向** (回退) |
| **UInt** | readUnsignedIntegers | Multi | 1,133,868 | 1,686,012 | **0.67x** | **负向** (回退) |
| **ULong** | readUnsignedLongs | Single | 237,342,545 | 267,263,820 | 0.89x | **负向** (轻微回退) |

*   **分析**: 新版本在处理 Unsigned 转换（尤其是需要将 Byte/Short 转为 Unsigned Int 并存入 Long Column 时）似乎增加了一些开销。这是本次优化中主要的负向收益点。

---

## 2. 总结分析：SingleBufferInputStream 视角

当输入是连续的内存块 (`SingleBufferInputStream`) 时：

### 正向收益 (Pros)
1.  **Binary 类型全方位提升**:
    *   读取速度提升约 **23%**。
    *   跳过速度提升 **2.7倍**。
    *   混合读/跳场景提升 **1.4倍 - 2.4倍**。
2.  **单值读取 (Row-by-Row) 性能提升**:
    *   `readInteger`, `readLong`, `readDouble` 等 API 均有 **68%** 左右的性能提升。这对于复杂过滤器或 UDF 等非向量化执行的场景有益。
3.  **部分定长类型批量读取**:
    *   `readDoubles`, `readFloats` 有轻微提升 (~7%)。

### 持平 (Neutral)
1.  **定长类型 (Int/Long) 批量读取**:
    *   得益于 JVM 的 Unsafe copy 优化，新旧版本性能完全一致，优化没有破坏这一关键路径。

### 负向收益 (Cons)
1.  **Boolean 类型**:
    *   批量读取有约 **10%** 的回退（`202us` -> `225us` per 1000 loops）。考虑到 Boolean 解码本身非常快，这一回退可能是由于方法调用的内联未达到极致或新的位运算逻辑略繁琐。
2.  **Unsigned 类型**:
    *   `readUnsignedIntegers` 有显著回退 (~37% 变慢)。这通常发生在将较小类型的数据读取并提升为较大类型的 Unsigned 值时。

---

## 3. 总结分析：MultiBufferInputStream 视角

当输入是碎片化的内存页 (`MultiBufferInputStream`) 时：

### 正向收益 (Pros)
1.  **Binary 类型性能爆发**:
    *   读取性能提升 **3.6倍**。旧版本在跨页处理 Binary 数据时有显著开销，新版本通过简化状态管理极大地优化了这一点。
    *   跳过性能提升 **2.5倍**。
2.  **Boolean 类型大幅改善**:
    *   读取性能提升 **1.87倍**。旧版本在处理跨 Buffer 的位数据拼接时效率较低，新版本逻辑更优。
3.  **跳过操作 (Skip) 更加高效**:
    *   对于定长类型（如 `initAndSkipAllIntegers`），虽然绝对时间很短（微秒级）��但在 MultiBuffer 下新版本通常也有 50% - 70% 的提升（这在数据统计或索引跳跃场景很有用）。

### 持平 (Neutral)
1.  **定长类型 (Int/Long/Double) 批量读取**:
    *   性能基本持平 (1.0x)。这表明对于这些简单类型，性能瓶颈主要在于数据的搬运，而非控制流逻辑，且原版实现也并无明显缺陷。

### 负向收益 (Cons)
1.  **Unsigned 类型**:
    *   与 SingleBuffer 类似，`readUnsignedIntegers` 在 MultiBuffer 下也有约 **33%** 的性能回退。

---

## 4. 总体结论

新实现 `VectorizedSingleBufferPlainValuesReader` 成功达成其优化目标：

1.  **解决了 Binary 类型的性能瓶颈**：在极差的 MultiBuffer 读取场景下快了 **3.6倍**，在最常用的 SingleBuffer Skip 场景下快了 **2.7倍**。
2.  **增强了该 Reader 的通用性**：显著提升了单值读取（Single Read）的性能，使其不再仅限于向量化路径的高效使用。
3.  **稳健的兼容性**：对于 Parquet 最核心的 Int/Long Batch Read 路径，新实现完美保持了原有的高性能。

**建议**:
*   绝大多数场景（Int/Long/Binary/String/Double）推荐使用新实现。
*   如果特定负载极度依赖 `Boolean` 列的密集读取且全部命中 SingleBuffer 路径，可能会观测到轻��的 CPU 消耗增加，但在 IO 密集型场景下这通常被掩盖。
*   如果负载大量依赖 `Unsigned Integer` 类型（较少见），需评估该回退的影响。
