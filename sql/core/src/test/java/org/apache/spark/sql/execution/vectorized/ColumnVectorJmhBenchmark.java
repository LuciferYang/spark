package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.Platform;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, jvmArgsAppend = {
  "-XX:+UseCompressedOops",
  "-XX:+UseCompressedClassPointers",
  "-XX:+UnlockDiagnosticVMOptions",
  "-XX:+DebugNonSafepoints"
})
@Measurement(iterations = 10, time = 1)
@Warmup(iterations = 10, time = 1)
public class ColumnVectorJmhBenchmark {

  @Param({"1024", "4096", "8192"})
  public int count;

  private OffHeapColumnVector offHeapVector;
  private OldOffHeapColumnVector oldOffHeapVector;
  private OnHeapColumnVector onHeapVector;
  private OldOnHeapColumnVector oldOnHeapVector;

  private byte[] inputBytes;

  @Setup
  public void setup() {
    offHeapVector = new OffHeapColumnVector(count, DataTypes.ShortType);
    oldOffHeapVector = new OldOffHeapColumnVector(count, DataTypes.ShortType);
    onHeapVector = new OnHeapColumnVector(count, DataTypes.ShortType);
    oldOnHeapVector = new OldOnHeapColumnVector(count, DataTypes.ShortType);

    inputBytes = new byte[count * 4];
    ByteBuffer buffer = ByteBuffer.wrap(inputBytes).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < count; i++) {
      buffer.putInt(i * 4, i);
    }
  }

  @TearDown
  public void tearDown() {
    offHeapVector.close();
    oldOffHeapVector.close();
    onHeapVector.close();
    oldOnHeapVector.close();
  }

  @Benchmark
  public void offHeapPutShortsFromInts() {
    offHeapVector.putShortsFromIntsLittleEndian(0, count, inputBytes, 0);
  }

  @Benchmark
  public void oldOffHeapPutShortsFromInts() {
    oldOffHeapVector.putShortsFromIntsLittleEndian(0, count, inputBytes, 0);
  }

  @Benchmark
  public void onHeapPutShortsFromInts() {
    onHeapVector.putShortsFromIntsLittleEndian(0, count, inputBytes, 0);
  }

  @Benchmark
  public void oldOnHeapPutShortsFromInts() {
    oldOnHeapVector.putShortsFromIntsLittleEndian(0, count, inputBytes, 0);
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
