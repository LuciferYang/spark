# VectorizedPlainValuesReader 优化的 Benchmark 分析

## 优化回顾

commit `a0c73a179782a2652b31d82de7af24c1cfe27466` 包含两个优化：

### 优化1: `readBytes()` - 添加数组快速路径
```java
// 优化前：每次循环都调用 buffer.position()
for (int i = 0; i < total; i += 1) {
  c.putByte(rowId + i, buffer.get());
  buffer.position(buffer.position() + 3);  // 跳过3字节
}

// 优化后：使用数组直接访问
if (buffer.hasArray()) {
  byte[] array = buffer.array();
  int offset = buffer.arrayOffset() + buffer.position();
  for (int i = 0; i < total; i++) {
    c.putByte(rowId + i, array[offset + i * 4]);
  }
  buffer.position(buffer.position() + requiredBytes);
}
```

### 优化2: `readBoolean()` - 优化分支预测
```java
// 优化前：先检查，后读取
if (bitOffset == 0) {
  updateCurrentByte();
}
boolean v = (currentByte & (1 << bitOffset)) != 0;
bitOffset += 1;
if (bitOffset == 8) {
  bitOffset = 0;
}

// 优化后：先读取，后更新（减少一个分支）
boolean v = (currentByte & (1 << bitOffset)) != 0;
bitOffset += 1;
if (bitOffset == 8) {
  bitOffset = 0;
  updateCurrentByte();
}
```

---

## 调用链分析

### VectorizedPlainValuesReader 使用条件

根据 `VectorizedColumnReader.getValuesReader()` 方法：

```java
private ValuesReader getValuesReader(Encoding encoding) {
    return switch (encoding) {
      case PLAIN -> new VectorizedPlainValuesReader();  // 只有 PLAIN 编码
      case DELTA_BYTE_ARRAY -> new VectorizedDeltaByteArrayReader();
      case DELTA_LENGTH_BYTE_ARRAY -> new VectorizedDeltaLengthByteArrayReader();
      case DELTA_BINARY_PACKED -> new VectorizedDeltaBinaryPackedReader();
      case RLE -> new VectorizedRleValuesReader(1);  // 仅用于 BOOLEAN
      ...
    };
}
```

**关键发现**: `VectorizedPlainValuesReader` **只在 PLAIN 编码时使用**！

如果数据使用字典编码（Dictionary Encoding），则走 `VectorizedRleValuesReader` 路径，**不会使用** `VectorizedPlainValuesReader`。

### Benchmark 测试的前提条件

要测试 `VectorizedPlainValuesReader` 的优化效果，需要确保：
1. **禁用字典编码**，或者
2. **使用字典回退**（Dictionary Fallback）的数据

在 `DataSourceReadBenchmark` 中，数据是随机生成的，可能会触发字典回退，从而使用 PLAIN 编码。

### readBytes() 调用链

```
ParquetVectorUpdaterFactory.ByteUpdater.readValues()
  └── valuesReader.readBytes(total, values, offset)
        └── VectorizedPlainValuesReader.readBytes()

触发条件:
  - Parquet 类型: INT32
  - Spark 类型: ByteType
  - 在 getUpdater() 中: sparkType == DataTypes.ByteType → return new ByteUpdater()
```

**关键发现**: `ByteUpdater` 确实会被使用当 Spark schema 是 `ByteType` 时！

### readBoolean() 调用链

```
ParquetVectorUpdaterFactory.BooleanUpdater.readValue()
  └── valuesReader.readBoolean()
        └── VectorizedPlainValuesReader.readBoolean()

触发条件:
  - 单值读取场景（非批量）
  - 主要用于 defColumn 的处理
```

**重要**: `readBoolean()` 是单值方法，主要在非批量场景使用。批量场景使用 `readBooleans()`。

### readBooleans() 调用链

```
ParquetVectorUpdaterFactory.BooleanUpdater.readValues()
  └── valuesReader.readBooleans(total, values, offset)
        └── VectorizedPlainValuesReader.readBooleans()

触发条件:
  - Parquet 类型: BOOLEAN
  - Spark 类型: BooleanType
  - 批量读取场景
```

---

## 现有 Benchmark 覆盖分析

### DataSourceReadBenchmark.scala

```scala
// 在 runBenchmarkSuite 中
Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach {
  dataType => numericScanBenchmark(1024 * 1024 * 15, dataType)
}
```

| 测试类型 | 优化方法 | 是否能测试到 | 原因 |
|---------|---------|-------------|------|
| BooleanType | `readBooleans()` | ✅ 能 | BooleanUpdater 调用 readBooleans() |
| BooleanType | `readBoolean()` | ⚠️ 部分 | 仅在单值场景使用 |
| ByteType | `readBytes()` | ✅ 能 | ByteUpdater 调用 readBytes() |

**结论**: 现有的 `DataSourceReadBenchmark` **可以**测试这两个优化的效果！

### 但存在的问题

1. **调用链长，效果被稀释**：整个链路包含 SQL 解析、计划生成、向量化读取等多个环节
2. **其他开销占比高**：Parquet 解压、RLE 解码、definition level 处理等

---

## 建议方案

### 方案1: 使用现有 DataSourceReadBenchmark（推荐用于验证整体影响）

直接运行现有测试，对比优化前后的 `BooleanType` 和 `ByteType` 性能：

```bash
# 运行 BooleanType 和 ByteType 测试
build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.DataSourceReadBenchmark"
```

**优点**: 
- 不需要新增代码
- 反映真实端到端性能

**缺点**:
- 优化效果可能不明显（被其他开销稀释）

### 方案2: 新增 VectorizedPlainValuesReaderBenchmark（推荐用于验证优化本身）

创建直接测试 `VectorizedPlainValuesReader` 的微基准：

```scala
package org.apache.spark.sql.execution.benchmark

import java.nio.{ByteBuffer, ByteOrder}
import scala.util.Random

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.datasources.parquet.VectorizedPlainValuesReader
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{BooleanType, ByteType}

/**
 * Benchmark for VectorizedPlainValuesReader optimizations.
 *
 * To run this benchmark:
 * {{{
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.VectorizedPlainValuesReaderBenchmark"
 * }}}
 */
object VectorizedPlainValuesReaderBenchmark extends BenchmarkBase {

  private def readBooleansBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark(s"readBooleans ($values values)", values, output = output)
    
    // Boolean 数据：每8个值占1字节
    val numBytes = (values + 7) / 8
    val data = new Array[Byte](numBytes)
    Random.nextBytes(data)

    benchmark.addCase("VectorizedPlainValuesReader.readBooleans()") { _ =>
      val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      val in = ByteBufferInputStream.wrap(buffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)
      
      val column = new OnHeapColumnVector(values, BooleanType)
      try {
        reader.readBooleans(values, column, 0)
      } finally {
        column.close()
      }
    }

    benchmark.run()
  }

  private def readBooleanSingleBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark(s"readBoolean single ($values calls)", values, output = output)
    
    val numBytes = (values + 7) / 8
    val data = new Array[Byte](numBytes)
    Random.nextBytes(data)

    benchmark.addCase("VectorizedPlainValuesReader.readBoolean() loop") { _ =>
      val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      val in = ByteBufferInputStream.wrap(buffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)
      
      var sum = 0
      var i = 0
      while (i < values) {
        if (reader.readBoolean()) sum += 1
        i += 1
      }
    }

    benchmark.run()
  }

  private def readBytesBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark(s"readBytes ($values values)", values, output = output)
    
    // Byte 数据：每个值占4字节（INT32存储）
    val data = new Array[Byte](values * 4)
    Random.nextBytes(data)

    benchmark.addCase("readBytes() - heap buffer (fast path)") { _ =>
      val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      val in = ByteBufferInputStream.wrap(buffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)
      
      val column = new OnHeapColumnVector(values, ByteType)
      try {
        reader.readBytes(values, column, 0)
      } finally {
        column.close()
      }
    }

    benchmark.addCase("readBytes() - direct buffer (slow path)") { _ =>
      val directBuffer = ByteBuffer.allocateDirect(values * 4).order(ByteOrder.LITTLE_ENDIAN)
      directBuffer.put(data)
      directBuffer.flip()
      val in = ByteBufferInputStream.wrap(directBuffer)
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(values, in)
      
      val column = new OnHeapColumnVector(values, ByteType)
      try {
        reader.readBytes(values, column, 0)
      } finally {
        column.close()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("VectorizedPlainValuesReader.readBooleans()") {
      Seq(10000, 100000, 1000000, 10000000).foreach(readBooleansBenchmark)
    }
    
    runBenchmark("VectorizedPlainValuesReader.readBoolean() single value") {
      Seq(10000, 100000, 1000000).foreach(readBooleanSingleBenchmark)
    }
    
    runBenchmark("VectorizedPlainValuesReader.readBytes()") {
      Seq(10000, 100000, 1000000, 10000000).foreach(readBytesBenchmark)
    }
  }
}
```

**优点**:
- 直接测量优化效果
- 排除其他因素干扰
- 可以对比 heap buffer vs direct buffer

**缺点**:
- 不反映真实场景
- 需要新增代码

---

## 最终结论

### 问题1: 现有 Benchmark 能否体现优化效果？

**✅ 能**, 但效果会被稀释。`DataSourceReadBenchmark` 中的 `BooleanType` 和 `ByteType` 测试会覆盖这两个优化。

### 问题2: 直接测试 VectorizedPlainValuesReader 还是通过 VectorizedColumnReader？

**建议两层都测试**:

| 测试层级 | 用途 | 推荐场景 |
|---------|------|---------|
| VectorizedPlainValuesReader | 验证优化本身是否有效 | 开发阶段、PR review |
| DataSourceReadBenchmark | 验证对用户的实际影响 | 发布前、性能报告 |

### 问题3: 需要增加什么样的 Benchmark？

**建议新增 `VectorizedPlainValuesReaderBenchmark`**，用于：
1. 单独测试 `readBooleans()` 批量方法
2. 单独测试 `readBoolean()` 单值方法  
3. 对比 `readBytes()` 在 heap buffer vs direct buffer 下的性能
4. 提供清晰的优化效果数据

---

## 测试验证步骤

### 步骤1: 使用现有 Benchmark 验证整体影响

```bash
# 切换到优化前的代码
git checkout <before-commit>
build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.DataSourceReadBenchmark" 2>&1 | tee before.txt

# 切换到优化后的代码  
git checkout <after-commit>
build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.DataSourceReadBenchmark" 2>&1 | tee after.txt

# 对比 BooleanType 和 ByteType 的结果
diff before.txt after.txt
```

### 步骤2: 新增微基准验证优化本身

创建上面的 `VectorizedPlainValuesReaderBenchmark.scala`，然后运行：

```bash
build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.VectorizedPlainValuesReaderBenchmark"
```

这样可以清晰地看到：
- `readBoolean()` 分支优化的效果
- `readBytes()` 数组快速路径的效果

