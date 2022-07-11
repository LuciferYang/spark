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

import org.apache.spark.benchmark.BenchmarkBase

object MethodHandleBenchmark extends BenchmarkBase {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val valuesPerIteration = 100000
    import java.lang.invoke.MethodHandles

    import org.apache.spark.benchmark.Benchmark
    val benchmark = new Benchmark(s"Test method Getter", valuesPerIteration, output = output)

    val obj = new TestObj(1)

    val valueField = classOf[TestObj].getDeclaredField("value")
    valueField.setAccessible(true)

    benchmark.addCase("Use reflect Getter") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        valueField.get(obj)
      }
    }

    val lookup = MethodHandles.lookup()
    val valueHandle = lookup.unreflectGetter(valueField)

    benchmark.addCase("Use handle Getter") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        valueHandle.invoke(obj)
      }
    }

    benchmark.run()


    val benchmark1 = new Benchmark(s"Test method Invoke", valuesPerIteration, output = output)
    val addMethod = classOf[TestObj].getDeclaredMethod("add", classOf[String])
    addMethod.setAccessible(true)
    benchmark1.addCase("Use reflect Invoke") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        addMethod.invoke(obj, "2")
      }
    }
    val addMethodHandle = lookup.unreflect(addMethod)
    benchmark1.addCase("Use handle Invoke") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        addMethodHandle.invoke(obj, "2")
      }
    }
    benchmark1.run()
  }
}

class TestObj(val value: Int) {
  def add(other: String): String = s"$value + $other"
}
