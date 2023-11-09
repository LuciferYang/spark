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

import java.lang.invoke.{MethodHandle, MethodHandles, MethodType}
import java.nio.ByteBuffer

import sun.misc.Unsafe
import sun.nio.ch.DirectBuffer

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.Utils

/**
 * Benchmark for EnumSet vs HashSet hold enumeration type
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/EnumTypeSetBenchmark-results.txt".
 * }}}
 */
object RefBenchmark extends BenchmarkBase {

  private def bufferCleanerByRef: DirectBuffer => Unit = {
    val cleanerMethod = Utils.classForName("sun.misc.Unsafe")
      .getMethod("invokeCleaner", classOf[ByteBuffer])
    val unsafeField = classOf[Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    val unsafe = unsafeField.get(null).asInstanceOf[Unsafe]
    buffer: DirectBuffer => cleanerMethod.invoke(unsafe, buffer)
  }

  private def bufferCleanerByMH: ByteBuffer => Unit = {
    val cleanerClass = Utils.classForName("jdk.internal.ref.Cleaner")
    val directBufferClass = Utils.classForName("sun.nio.ch.DirectBuffer")
    val byteBufferLookup: MethodHandles.Lookup =
      MethodHandles.privateLookupIn(directBufferClass, MethodHandles.lookup())
    val cleanerMethod: MethodHandle = byteBufferLookup
      .findVirtual(directBufferClass, "cleaner", MethodType.methodType(cleanerClass))
    val cleanerLookup: MethodHandles.Lookup =
      MethodHandles.privateLookupIn(cleanerClass, MethodHandles.lookup())
    val cleanMethod: MethodHandle =
      cleanerLookup.findVirtual(cleanerClass, "clean", MethodType.methodType(classOf[Unit]))
    buffer: ByteBuffer => cleanMethod.invoke(cleanerMethod.invoke(buffer))
  }

  private def bufferCleanerByRef2: ByteBuffer => Unit = {
    val unsafeField = classOf[Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    val unsafe = unsafeField.get(null).asInstanceOf[Unsafe]
    buffer: ByteBuffer => unsafe.invokeCleaner(buffer)
  }

  private def bufferCleanerByRef3: ByteBuffer => Unit = {
    val unsafeClass = Utils.classForName("sun.misc.Unsafe")
    val cleanerMethod = unsafeClass.getMethod("invokeCleaner", classOf[ByteBuffer])
    val unsafeField = unsafeClass.getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    val unsafe = unsafeField.get(null)
    buffer: ByteBuffer => cleanerMethod.invoke(unsafe, buffer)
  }

  def testCreateFunction(valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      "Test create bufferCleaner function", valuesPerIteration, output = output)

    benchmark.addCase("Use Refection") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val r = bufferCleanerByRef
      }
    }

    benchmark.addCase("Use Refection 2") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val r = bufferCleanerByRef2
      }
    }

    benchmark.addCase("Use Refection 3") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val r = bufferCleanerByRef3
      }
    }

    benchmark.addCase("Use MethodHandle") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val r = bufferCleanerByMH
      }
    }

    benchmark.run()
  }

  def testInvokeFunction(valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      "Test invoke bufferCleaner function", valuesPerIteration, output = output)

    val ref = bufferCleanerByRef
    val bufRef = ByteBuffer.allocateDirect(10)
    benchmark.addCase("Use Refection") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        ref(bufRef.asInstanceOf[DirectBuffer])
      }
    }

    val ref2 = bufferCleanerByRef2
    val bufRef2 = ByteBuffer.allocateDirect(10)
    benchmark.addCase("Use Refection 2") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        ref2(bufRef2)
      }
    }

    val ref3 = bufferCleanerByRef3
    val bufRef3 = ByteBuffer.allocateDirect(10)
    benchmark.addCase("Use Refection 3") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        ref3(bufRef3)
      }
    }

    val mh = bufferCleanerByMH
    val bufMh = ByteBuffer.allocateDirect(10)
    benchmark.addCase("Use MethodHandles") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        mh(bufMh)
      }
    }
    benchmark.run()
  }

  def testInvokeFunction2(valuesPerIteration: Int): Unit = {

    val benchmark = new Benchmark(
      "Test invoke bufferCleaner function", valuesPerIteration, output = output)

    val ref = bufferCleanerByRef
    val bufRefs = (0 until valuesPerIteration).map(_ => ByteBuffer.allocateDirect(10))
    benchmark.addCase("Use Refection") { _: Int =>
      bufRefs.foreach(buf => ref(buf.asInstanceOf[DirectBuffer]))
    }

    val ref2 = bufferCleanerByRef2
    val bufRefs2 = (0 until valuesPerIteration).map(_ => ByteBuffer.allocateDirect(10))
    benchmark.addCase("Use Refection 2") { _: Int =>
      bufRefs2.foreach(buf => ref2(buf))
    }

    val ref3 = bufferCleanerByRef3
    val bufRefs3 = (0 until valuesPerIteration).map(_ => ByteBuffer.allocateDirect(10))
    benchmark.addCase("Use Refection 3") { _: Int =>
      bufRefs3.foreach(buf => ref3(buf))
    }

    val mh = bufferCleanerByMH
    val bufMhs = (0 until valuesPerIteration).map(_ => ByteBuffer.allocateDirect(10))
    benchmark.addCase("Use MethodHandles") { _: Int =>
      bufMhs.foreach(buf => mh(buf))
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val valuesPerIteration = 1000000
     testCreateFunction(valuesPerIteration)
     testInvokeFunction(valuesPerIteration)
     testInvokeFunction2(valuesPerIteration)
  }
}
