/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.vectorized;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import org.apache.spark.sql.types.DataTypes;

/**
 * JMH Benchmark for comparing OnHeapColumnVector and OldOnHeapColumnVector
 * putIntsLittleEndian and putLongsLittleEndian performance.
 *
 * To run:
 * {{{
 *   build/mvn test-compile -pl sql/core -DskipTests
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.vectorized.OnHeapColumnVectorJMHBenchmark"
 * }}}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class OnHeapColumnVectorJMHBenchmark {

  @Param({"4096"})
  public int count;

  @Param({"65536"})
  public int i;

  private OnHeapColumnVector onHeapVectorInt;
  private OnHeapColumnVector onHeapVectorLong;
  private OldOnHeapColumnVector oldOnHeapVectorInt;
  private OldOnHeapColumnVector oldOnHeapVectorLong;

  private byte[] inputBytesInt;
  private byte[] inputBytesLong;

  @Setup
  public void setup() {
    onHeapVectorInt = new OnHeapColumnVector(count, DataTypes.IntegerType);
    onHeapVectorLong = new OnHeapColumnVector(count, DataTypes.LongType);
    oldOnHeapVectorInt = new OldOnHeapColumnVector(count, DataTypes.IntegerType);
    oldOnHeapVectorLong = new OldOnHeapColumnVector(count, DataTypes.LongType);

    inputBytesInt = new byte[count * 4];
    inputBytesLong = new byte[count * 8];
  }

  @TearDown
  public void tearDown() {
    onHeapVectorInt.close();
    onHeapVectorLong.close();
    oldOnHeapVectorInt.close();
    oldOnHeapVectorLong.close();
  }

  @Benchmark
  public void onHeapPutIntsLittleEndian() {
    for (int n = 0; n < i; n++) {
      onHeapVectorInt.putIntsLittleEndian(0, count, inputBytesInt, 0);
    }
  }

  @Benchmark
  public void oldOnHeapPutIntsLittleEndian() {
    for (int n = 0; n < i; n++) {
      oldOnHeapVectorInt.putIntsLittleEndian(0, count, inputBytesInt, 0);
    }
  }

  @Benchmark
  public void onHeapPutLongsLittleEndian() {
    for (int n = 0; n < i; n++) {
      onHeapVectorLong.putLongsLittleEndian(0, count, inputBytesLong, 0);
    }
  }

  @Benchmark
  public void oldOnHeapPutLongsLittleEndian() {
    for (int n = 0; n < i; n++) {
      oldOnHeapVectorLong.putLongsLittleEndian(0, count, inputBytesLong, 0);
    }
  }

    public static void main(String[] args) throws RunnerException {
        String filter = args.length > 0 ?
                args[0] : OnHeapColumnVectorJMHBenchmark.class.getSimpleName();
        Options opt = new OptionsBuilder()
                .include(filter)
                .build();


        new Runner(opt).run();
    }
}
