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
package org.apache.spark.util

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}


/**
 * Benchmark for EnumSet vs HashSet hold enumeration type
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class> --jars <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/FilesReadStringBenchmark-results.txt".
 * }}}
 */
object FilesReadStringBenchmark extends BenchmarkBase {

  def testReadToString(size: Int): Unit = {

    val benchmark = new Benchmark(
      s"Test read string from file with file size = ${size * 10}",
      size,
      output = output)

    val tmpDir = Utils.createTempDir()
    val file = new File(tmpDir, "test.txt")
    Utils.tryWithResource(Files.newOutputStream(file.toPath)) { os =>
      val data = "0123456789".getBytes(Charset.forName("UTF-8"))
      for (_ <- 0 until size) {
        os.write(data)
      }
    }

    benchmark.addCase("Use Java") { _: Int =>
      java.nio.file.Files.readString(file.toPath, Charset.forName("UTF-8"))
    }

    benchmark.addCase("Use Guava") { _: Int =>
      com.google.common.io.Files.toString(file, com.google.common.base.Charsets.UTF_8)
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    testReadToString(10)
    testReadToString(100)
    testReadToString(1000)
    testReadToString(10000)
    testReadToString(100000)
    testReadToString(1000000)
  }
}
