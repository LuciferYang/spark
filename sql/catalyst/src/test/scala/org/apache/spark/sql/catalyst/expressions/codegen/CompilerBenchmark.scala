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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.io.{ByteArrayOutputStream, StringWriter}
import java.net.URI
import java.util.Collections
import javax.tools._

import scala.jdk.CollectionConverters._

import org.codehaus.janino.ClassBodyEvaluator

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark comparing Janino ClassBodyEvaluator vs JDK javax.tools.JavaCompiler
 * on representative Spark-generated code at different complexity levels.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "catalyst/Test/runMain
 *        org.apache.spark.sql.catalyst.expressions.codegen.CompilerBenchmark"
 *   2. generate result:
 *        SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain
 *        org.apache.spark.sql.catalyst.expressions.codegen.CompilerBenchmark"
 *        Results will be written to "benchmarks/CompilerBenchmark-results.txt".
 * }}}
 */
object CompilerBenchmark extends BenchmarkBase {

  // ---------------------------------------------------------------
  // JDK in-memory compiler infrastructure
  // ---------------------------------------------------------------

  private class InMemoryJavaFileObject(name: String, code: String)
    extends SimpleJavaFileObject(
      URI.create("string:///" + name.replace('.', '/') + JavaFileObject.Kind.SOURCE.extension),
      JavaFileObject.Kind.SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = code
  }

  private class InMemoryClassFileObject(name: String)
    extends SimpleJavaFileObject(
      URI.create("bytes:///" + name.replace('.', '/') + JavaFileObject.Kind.CLASS.extension),
      JavaFileObject.Kind.CLASS) {
    val bos = new ByteArrayOutputStream()
    override def openOutputStream(): java.io.OutputStream = bos
    def getBytes: Array[Byte] = bos.toByteArray
  }

  private class InMemoryFileManager(delegate: StandardJavaFileManager)
    extends ForwardingJavaFileManager[StandardJavaFileManager](delegate) {

    val classFiles = new java.util.concurrent.ConcurrentHashMap[String, InMemoryClassFileObject]()

    override def getJavaFileForOutput(
        location: JavaFileManager.Location,
        className: String,
        kind: JavaFileObject.Kind,
        sibling: FileObject): JavaFileObject = {
      val fileObject = new InMemoryClassFileObject(className)
      classFiles.put(className, fileObject)
      fileObject
    }
  }

  private class InMemoryClassLoader(
      fileManager: InMemoryFileManager,
      parent: ClassLoader) extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = {
      val classFile = fileManager.classFiles.get(name)
      if (classFile == null) {
        throw new ClassNotFoundException(name)
      }
      val bytes = classFile.getBytes
      defineClass(name, bytes, 0, bytes.length)
    }
  }

  // ---------------------------------------------------------------
  // Compile with Janino (same path as Spark's CodeGenerator.doCompile)
  // ---------------------------------------------------------------

  private def compileWithJanino(classBody: String): Class[_] = {
    val evaluator = new ClassBodyEvaluator()
    evaluator.setParentClassLoader(Thread.currentThread().getContextClassLoader)
    evaluator.setClassName("org.apache.spark.codegen.bench.Generated")
    evaluator.setDefaultImports(Array[String](): _*)
    evaluator.cook("generated.java", classBody)
    evaluator.getClazz()
  }

  // ---------------------------------------------------------------
  // Compile with JDK compiler (javax.tools.JavaCompiler)
  // ---------------------------------------------------------------

  private val jdkCompiler = ToolProvider.getSystemJavaCompiler

  private def compileWithJdk(fullSource: String): Class[_] = {
    val className = "org.apache.spark.codegen.bench.Generated"
    val fileObject = new InMemoryJavaFileObject(className, fullSource)
    val diagnostics = new DiagnosticCollector[JavaFileObject]()
    val standardFm = jdkCompiler.getStandardFileManager(diagnostics, null, null)
    val fileManager = new InMemoryFileManager(standardFm)

    val task = jdkCompiler.getTask(
      new StringWriter(), fileManager, diagnostics, null, null,
      Collections.singletonList(fileObject))

    if (!task.call()) {
      val errors = diagnostics.getDiagnostics.asScala
        .filter(_.getKind == Diagnostic.Kind.ERROR)
        .map(_.getMessage(null))
        .mkString("\n")
      throw new RuntimeException(s"JDK compilation failed:\n$errors")
    }

    val loader = new InMemoryClassLoader(fileManager, Thread.currentThread().getContextClassLoader)
    loader.loadClass(className)
  }

  // Cached file manager wrapping a single shared StandardJavaFileManager.
  // We rotate the captured class bytes after each compile so the cache survives reuse.
  private val sharedStandardFm: StandardJavaFileManager =
    jdkCompiler.getStandardFileManager(null, null, null)

  private val sharedJdkOpts: java.util.List[String] = java.util.Arrays.asList(
    "-proc:none",        // skip annotation processing
    "-g:none",           // skip debug info
    "-nowarn",           // suppress warnings
    "-implicit:none",    // do not compile implicit dependencies
    "-Xlint:none"        // disable all lints
  )

  private def compileWithJdkOptimized(fullSource: String): Class[_] = {
    val className = "org.apache.spark.codegen.bench.Generated"
    val fileObject = new InMemoryJavaFileObject(className, fullSource)
    val fileManager = new InMemoryFileManager(sharedStandardFm)

    val task = jdkCompiler.getTask(
      null,           // null Writer → swallow output, avoid StringWriter allocation
      fileManager,
      null,           // null DiagnosticListener → use default
      sharedJdkOpts,
      null,
      Collections.singletonList(fileObject))

    if (!task.call()) {
      throw new RuntimeException("JDK compilation failed")
    }

    val loader = new InMemoryClassLoader(fileManager, Thread.currentThread().getContextClassLoader)
    loader.loadClass(className)
  }

  // ---------------------------------------------------------------
  // Code templates at different complexity levels
  // ---------------------------------------------------------------

  /**
   * Small: a simple single-method class (~20 lines).
   * Represents a trivial filter predicate.
   */
  private def smallClassBody: String =
    """
    public boolean evaluate(long value) {
      return (value & 1) == 1;
    }
    """

  private def smallFullSource: String =
    s"""
    package org.apache.spark.codegen.bench;
    public class Generated {
      public boolean evaluate(long value) {
        return (value & 1) == 1;
      }
    }
    """

  /**
   * Medium: a class with several fields, constructor, and multiple methods (~80 lines).
   * Represents a typical UnsafeProjection with a few columns.
   */
  private def mediumClassBody: String = {
    val fields = (0 until 10).map(i => s"  private long field_$i;").mkString("\n")
    val initFields = (0 until 10).map(i => s"    this.field_$i = $i;").mkString("\n")
    val methods = (0 until 5).map { i =>
      s"""
      public long compute_$i(long input) {
        long result = input;
        for (int j = 0; j < 10; j++) {
          result = result * 31 + field_$i + j;
        }
        return result;
      }
      """
    }.mkString("\n")

    s"""
$fields
    private boolean isNull;
    private Object[] references;

    public void init(Object[] refs) {
      this.references = refs;
$initFields
      this.isNull = false;
    }

$methods

    public long apply(long input) {
      long sum = 0;
      ${(0 until 5).map(i => s"sum += compute_$i(input);").mkString("\n      ")}
      return sum;
    }
    """
  }

  private def mediumFullSource: String = {
    val fields = (0 until 10).map(i => s"  private long field_$i;").mkString("\n")
    val initFields = (0 until 10).map(i => s"    this.field_$i = $i;").mkString("\n")
    val methods = (0 until 5).map { i =>
      s"""
  public long compute_$i(long input) {
    long result = input;
    for (int j = 0; j < 10; j++) {
      result = result * 31 + field_$i + j;
    }
    return result;
  }
      """
    }.mkString("\n")

    s"""
package org.apache.spark.codegen.bench;
public class Generated {
$fields
  private boolean isNull;
  private Object[] references;

  public void init(Object[] refs) {
    this.references = refs;
$initFields
    this.isNull = false;
  }

$methods

  public long apply(long input) {
    long sum = 0;
    ${(0 until 5).map(i => s"sum += compute_$i(input);").mkString("\n    ")}
    return sum;
  }
}
    """
  }

  /**
   * Large: a class with many fields, many methods, and inner classes (~500 lines).
   * Represents a wide-schema projection with 50+ columns and split methods.
   */
  private def largeClassBody(numColumns: Int): String = {
    val fields = (0 until numColumns).map(i => s"  private long value_$i;").mkString("\n")
    val nullFields = (0 until numColumns).map(i => s"  private boolean isNull_$i;").mkString("\n")
    val initFields = (0 until numColumns).map { i =>
      s"    this.value_$i = 0L;\n    this.isNull_$i = true;"
    }.mkString("\n")

    val writeMethods = (0 until numColumns).grouped(10).zipWithIndex.map { case (cols, groupIdx) =>
      val body = cols.map { i =>
        s"""      if (!isNull_$i) {
        value_$i = value_$i * 31 + $i;
      } else {
        value_$i = $i;
        isNull_$i = false;
      }"""
      }.mkString("\n")
      s"""
    public void writeFields_$groupIdx(long input) {
$body
    }
      """
    }.mkString("\n")

    val applyBody = (0 until numColumns).grouped(10).zipWithIndex.map { case (_, groupIdx) =>
      s"      writeFields_$groupIdx(input);"
    }.mkString("\n")

    val sumBody = (0 until numColumns).map(i => s"value_$i").mkString(" + ")

    s"""
$fields
$nullFields

    public void init() {
$initFields
    }

$writeMethods

    public long apply(long input) {
$applyBody
      return $sumBody;
    }
    """
  }

  private def largeFullSource(numColumns: Int): String = {
    val fields = (0 until numColumns).map(i => s"  private long value_$i;").mkString("\n")
    val nullFields = (0 until numColumns).map(i =>
      s"  private boolean isNull_$i;").mkString("\n")
    val initFields = (0 until numColumns).map { i =>
      s"    this.value_$i = 0L;\n    this.isNull_$i = true;"
    }.mkString("\n")

    val writeMethods = (0 until numColumns).grouped(10).zipWithIndex.map { case (cols, groupIdx) =>
      val body = cols.map { i =>
        s"""    if (!isNull_$i) {
      value_$i = value_$i * 31 + $i;
    } else {
      value_$i = $i;
      isNull_$i = false;
    }"""
      }.mkString("\n")
      s"""
  public void writeFields_$groupIdx(long input) {
$body
  }
      """
    }.mkString("\n")

    val applyBody = (0 until numColumns).grouped(10).zipWithIndex.map { case (_, groupIdx) =>
      s"    writeFields_$groupIdx(input);"
    }.mkString("\n")

    val sumBody = (0 until numColumns).map(i => s"value_$i").mkString(" + ")

    s"""
package org.apache.spark.codegen.bench;
public class Generated {
$fields
$nullFields

  public void init() {
$initFields
  }

$writeMethods

  public long apply(long input) {
$applyBody
    return $sumBody;
  }
}
    """
  }

  // ---------------------------------------------------------------
  // Benchmark runner
  // ---------------------------------------------------------------

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // Thorough warm-up so the JDK compiler's own JIT compilation does not skew results.
    val large50Body = largeClassBody(50)
    val large50Source = largeFullSource(50)
    val large200Body = largeClassBody(200)
    val large200Source = largeFullSource(200)

    (0 until 30).foreach { _ =>
      compileWithJanino(smallClassBody)
      compileWithJanino(mediumClassBody)
      compileWithJanino(large50Body)
      compileWithJdk(smallFullSource)
      compileWithJdk(mediumFullSource)
      compileWithJdkOptimized(smallFullSource)
      compileWithJdkOptimized(mediumFullSource)
      compileWithJdkOptimized(large50Source)
    }

    runBenchmark("Compiler Benchmark: Janino vs JDK JavaCompiler") {

      val numIters = 30

      def runCase(name: String, code: (String, String, String)): Unit = {
        val (janinoBody, jdkSource, _) = code
        val bench = new Benchmark(name, 1, minNumIters = numIters, output = output)
        bench.addCase("Janino ClassBodyEvaluator", numIters = numIters) { _ =>
          compileWithJanino(janinoBody)
        }
        bench.addCase("JDK JavaCompiler (default)", numIters = numIters) { _ =>
          compileWithJdk(jdkSource)
        }
        bench.addCase("JDK JavaCompiler (optimized: reused FM + skip flags)",
          numIters = numIters) { _ =>
          compileWithJdkOptimized(jdkSource)
        }
        bench.run()
      }

      runCase("Small code (trivial filter, ~20 lines)",
        (smallClassBody, smallFullSource, ""))
      runCase("Medium code (projection with 10 fields, 5 methods, ~80 lines)",
        (mediumClassBody, mediumFullSource, ""))
      runCase("Large code (50 columns, split methods, ~500 lines)",
        (large50Body, large50Source, ""))
      runCase("Very large code (200 columns, split methods, ~2000 lines)",
        (large200Body, large200Source, ""))
    }
  }
}
