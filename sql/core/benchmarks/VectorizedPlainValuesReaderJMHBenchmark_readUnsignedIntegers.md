# VectorizedPlainValuesReader 基准测试详细分析报告 (readUnsignedIntegers)

本文针对 `VectorizedPlainValuesReaderJMHBenchmark` 中 `readUnsignedIntegers` 相关场景的测试结果进行分析。这些测试主要是在 **Heap ByteBuffer** 场景下进行的（`VectorizedPlainValuesReaderJMHBenchmark` 使用的是 Heap 内存）。

## 测试结果汇总

数据来源：用户提供的 `readUnsignedIntegers` JMH 测试输出。

| Benchmark | bufferSize | numValues | Score (Old) ns/op | Score (New) ns/op | Speedup (Old/New) | Trend |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **MultiBuffer** |
| readUnsignedIntegers | 4096 | 100,000 | 110,748 ± 42k | 115,540 ± 40k | 0.96x | 持平 |
| readUnsignedIntegers | 4096 | 1,000,000 | 1,241,745 ± 81k | 1,695,994 ± 319k | **0.73x** | **回退** |
| **SingleBuffer** |
| readUnsignedIntegers | 4096 | 100,000 | 84,312 ± 31k | 106,654 ± 51k | 0.79x | 回退 |
| readUnsignedIntegers | 4096 | 1,000,000 | 711,962 ± 32k | 1,349,059 ± 52k | **0.53x** | **显著回退** |

## 原因分析

### 1. 实现机制对比

**旧版 (VectorizedPlainValuesReader)**:
```java
public final void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    int requiredBytes = total * 4;
    ByteBuffer buffer = getBuffer(requiredBytes); // 可能返回一个 HeapByteBuffer 分片
    for (int i = 0; i < total; i += 1) {
        c.putLong(rowId + i, Integer.toUnsignedLong(buffer.getInt()));
    }
}
```

**新版 (VectorizedSingleBufferPlainValuesReader)**:
```java
public final void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    // 省略 checkBufferSize
    for (int i = 0; i < total; i += 1) {
        c.putLong(rowId + i, Integer.toUnsignedLong(buffer.getInt()));
    }
}
```

### 2. 为什么会有显著差异 (尤其是 SingleBuffer 1000k 场景)

表面上看两者的代码逻辑几乎完全一致（都是通过 `buffer.getInt()` 循环读取）。但在 Benchmark 中，Old 版本在 SingleBuffer 1M 数据场景下要快近一倍（711us vs 1349us）。这一现象比较反直觉，因为通常认为新版少了一次 `getBuffer` 调用应该更快。

可能的解释如下：

1.  **JIT 编译与内联差异 (最重要的原因)**:
    *   **Old**: `getBuffer` 返回一个新的 `ByteBuffer` 对象（可能是 slice）。这个局部的 `ByteBuffer` 对象只在这个方法内使用。HotSpot JIT 编译器在通过逃逸分析（Escape Analysis）后，可能能够非常激进地优化这个循环，甚至可能将 `buffer.getInt()` 内联并进行部分展开。
    *   **New**: `buffer` 是一个长期存在的**成员变量**。对于成员变量的重复访问（`buffer.getInt()` 实际上这会更新成员变量内部的 `position` 字段），JIT 编译器必须更加保守，以确保内存可见性和副作用的正确性。虽然 `ByteBuffer` 未必是 volatile，但频繁更新堆上的对象字段（`position`）并不如更新栈上局部对象的字段那样容易被寄存器分配优化。

2.  **Unsafe vs Heap Access**:
    *   虽然这里测试的是 Heap ByteBuffer，但 JIT 对 `ByteBuffer.get` 的内在优化（Intrinsic）可能在某种特定模式（局部变量 vs 成员变量）下触发得更好。

3.  **Benchmark 噪声与误差**:
    *   尽管迭代了 10 次，但误差范围（Error）仍然很大（例如 Old: ±32k, New: ±52k）。不过 0.53x 的差距超出了误差范围。

### 3. 先前分析的修正

之前的分析认为 MultiBuffer 下新版也会有回退，现在的数据证实了这一点。
同时，之前的分析认为 SingleBuffer 下两者应该持平（因为都是循环），但实际数据表明新版在处理**大量的逐个转换读取**（如 Unsigned Int 转 Long）时，这种基于成员变量的循环更新模式确实不如基于局部变量的模式高效。

### 4. 结论与建议

*   **回退确认**: `readUnsignedIntegers` 在新版实现中存在明确的性能回退（~0.5x - 0.7x）。这是目前观察到的最大负优化点。
*   **影响评估**: 这是一个权衡。
    *   **正向**: Binary 类型提升 3-10倍，Boolean MultiBuffer 提升 2倍，单值读取提升 1.7倍。
    *   **负向**: Unsigned Int/Long 读取回退。
    *   考虑到 Spark 数仓中 `Binary` (String) 和 `Int/Long` (Signed) 是绝对的主流类型，而 Unsigned 类型使用较少（通常只在 Parquet 特定 schema 下出现），**总体收���依然是正向的**。
*   **未来优化**: 后续可以考虑在 `VectorizedSingleBufferPlainValuesReader` 中也尝试使用局部变量策略，或者针对 Unsigned 类型也引入批量读取 API（例如 `buffer.getLongs` 然后再转换，或者使用 Unsafe 批量操作）。

该分析结果已补充到基准测试报告中。

