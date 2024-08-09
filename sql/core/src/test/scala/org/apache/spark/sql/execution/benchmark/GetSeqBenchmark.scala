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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark

/**
 * Benchmark for encode
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/EncodeBenchmark-results.txt".
 * }}}
 */
object GetSeqBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  def testEncodeArray(rows: Int, size: Int): Unit = {

    val data = (0 until size).toArray
    val seq = Seq.fill(rows)(data)

    val benchmark = new Benchmark(
      s"Test $rows array($size) to rows",
      rows,
      output = output)

    benchmark.addCase("Array to Row") { _: Int =>
      val ret = seq.toDF().collect()
    }

    benchmark.run()
  }

  def testRowGetSeq(valuesPerIteration: Int, size: Int): Unit = {

    val data = (0 until size).toArray
    val row = Seq(data).toDF().collect().head

    val benchmark = new Benchmark(
      s"Test get seq with $size from row",
      valuesPerIteration,
      output = output)

    benchmark.addCase("Get Seq") { _: Int =>

      for (_ <- 0L until valuesPerIteration) {
        val ret = row.getSeq(0)
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000
    testRowGetSeq(valuesPerIteration, 10)
    testRowGetSeq(valuesPerIteration, 100)
    testRowGetSeq(valuesPerIteration, 1000)
    testRowGetSeq(valuesPerIteration, 10000)
    testRowGetSeq(valuesPerIteration, 100000)

    val rows = 10000
    testEncodeArray(rows, 10)
    testEncodeArray(rows, 100)
    testEncodeArray(rows, 1000)
    testEncodeArray(rows, 10000)
  }
}
