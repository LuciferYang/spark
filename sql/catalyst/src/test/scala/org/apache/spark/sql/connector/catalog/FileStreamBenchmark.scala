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

package org.apache.spark.sql.connector.catalog

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.RandomUtils

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.util.Utils

object FileStreamBenchmark extends BenchmarkBase {

  import java.io.{File, FileInputStream, InputStream}

  def prepareData(): File = {
    val data = RandomUtils.nextBytes(4096)
    val dir = Utils.createTempDir()
    val file = FileUtils.getFile(dir, "test.data")
    Utils.tryWithResource(Files.newOutputStream(file.toPath)) { outputStream =>
      (0 until 4096).foreach(_ => outputStream.write(data))
    }
    file
  }

  def doRead(input: InputStream): Unit = {
    val buf = new Array[Byte](4096)
    (0 until 4096).foreach(_ => input.read(buf))
  }

  def testRead(file: File): Unit = {
    val valuesPerIteration = 1000

    val benchmark = new Benchmark(s"Test Read", valuesPerIteration, output = output)

    benchmark.addCase("new FileInputStream") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Utils.tryWithResource(new FileInputStream(file)) { input =>
          doRead(input)
        }
      }
    }

    benchmark.addCase("Files.newInputStream") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        Utils.tryWithResource(Files.newInputStream(file.toPath)) { input =>
          doRead(input)
        }
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val file = prepareData()
    testRead(file)
  }
}
