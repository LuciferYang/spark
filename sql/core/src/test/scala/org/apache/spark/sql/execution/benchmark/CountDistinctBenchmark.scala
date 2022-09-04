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
import org.apache.spark.internal.config.UI.{UI_ENABLED, UI_PORT}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/CountDistinctBenchmark-results.txt".
 * }}}
 */
object CountDistinctBenchmark extends SqlBasedBenchmark {
  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 1)
      .config(UI_PORT.key, 8836)
      .config(UI_ENABLED.key, true)
      .getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Benchmark count distinct") {
      withTempPath { dir =>
        import org.apache.spark.internal.config.Tests.IS_TESTING
        System.clearProperty(IS_TESTING.key)
        val N = 2000000
        val columns = Range(0, 100).map(i => s"id % $i AS id$i")

        spark.range(N).selectExpr(columns: _*).write.mode("Overwrite").parquet(dir.getCanonicalPath)

        Seq(1, 2, 5, 10, 15, 25, 30, 40, 50, 60, 100).foreach { cnt =>
          val selectExps = columns.take(cnt).map(_.split(" ").last).map(c => s"count(distinct $c)")

          val benchmark = new Benchmark("Benchmark count distinct", N, minNumIters = 1)
          benchmark.addCase(s"$cnt count distinct with codegen") { _ =>
            withSQLConf(
              "spark.sql.codegen.wholeStage" -> "true",
              "spark.sql.codegen.factoryMode" -> "FALLBACK") {
              spark.read.parquet(dir.getCanonicalPath).selectExpr(selectExps: _*)
                .write.format("noop").mode("Overwrite").save()
            }
          }

          benchmark.addCase(s"$cnt count distinct without codegen") { _ =>
            withSQLConf(
              "spark.sql.codegen.wholeStage" -> "false",
              "spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
              spark.read.parquet(dir.getCanonicalPath).selectExpr(selectExps: _*)
                .write.format("noop").mode("Overwrite").save()
            }
          }
          benchmark.run()
        }
      }
    }
  }
}