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

import java.io.File

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.jdk.CollectionConverters._

import sbt._
import sbt.Keys._
import com.github.sbt.junit.jupiter.sbt.JupiterPlugin.autoImport._

object SparkParallelTestGrouping {
  // Settings for parallelizing tests. The basic strategy here is to run the slowest suites (or
  // collections of suites) in their own forked JVMs, allowing us to gain parallelism within a
  // SBT project. Here, we take an opt-in approach where the default behavior is to run all
  // tests sequentially in a single JVM, requiring us to manually opt-in to the extra parallelism.
  //
  // There are a reasons why such an opt-in approach is good:
  //
  //    1. Launching one JVM per suite adds significant overhead for short-running suites. In
  //       addition to JVM startup time and JIT warmup, it appears that initialization of Derby
  //       metastores can be very slow so creating a fresh warehouse per suite is inefficient.
  //
  //    2. When parallelizing within a project we need to give each forked JVM a different tmpdir
  //       so that the metastore warehouses do not collide. Unfortunately, it seems that there are
  //       some tests which have an overly tight dependency on the default tmpdir, so those fragile
  //       tests need to continue re-running in the default configuration (or need to be rewritten).
  //       Fixing that problem would be a huge amount of work for limited payoff in most cases
  //       because most test suites are short-running.
  //

  private val testsWhichShouldRunInTheirOwnDedicatedJvm = Set(
    "org.apache.spark.DistributedSuite",
    "org.apache.spark.scheduler.HealthTrackerIntegrationSuite",
    "org.apache.spark.sql.catalyst.expressions.DateExpressionsSuite",
    "org.apache.spark.sql.catalyst.expressions.HashExpressionsSuite",
    "org.apache.spark.sql.catalyst.expressions.CastSuite",
    "org.apache.spark.sql.catalyst.expressions.MathExpressionsSuite",
    "org.apache.spark.sql.hive.HiveExternalCatalogSuite",
    "org.apache.spark.sql.hive.StatisticsSuite",
    "org.apache.spark.sql.hive.client.HiveClientVersions",
    "org.apache.spark.sql.hive.HiveExternalCatalogVersionsSuite",
    "org.apache.spark.ml.classification.LogisticRegressionSuite",
    "org.apache.spark.ml.classification.LinearSVCSuite",
    "org.apache.spark.sql.SQLQueryTestSuite",
    "org.apache.spark.sql.hive.client.HadoopVersionInfoSuite",
    "org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperationSuite",
    "org.apache.spark.sql.hive.thriftserver.ThriftServerQueryTestSuite",
    "org.apache.spark.sql.hive.thriftserver.SparkSQLEnvSuite",
    "org.apache.spark.sql.hive.thriftserver.ui.ThriftServerPageSuite",
    "org.apache.spark.sql.hive.thriftserver.ui.HiveThriftServer2ListenerSuite",
    "org.apache.spark.sql.kafka010.KafkaDelegationTokenSuite",
    "org.apache.spark.sql.streaming.RocksDBStateStoreStreamingAggregationSuite",
    "org.apache.spark.shuffle.KubernetesLocalDiskShuffleDataIOSuite",
    "org.apache.spark.sql.hive.HiveScalaReflectionSuite"
  ) ++ sys.env.get("DEDICATED_JVM_SBT_TESTS").map(_.split(",")).getOrElse(Array.empty).toSet

  private val DEFAULT_TEST_GROUP = "default_test_group"
  private val HIVE_EXECUTION_TEST_GROUP = "hive_execution_test_group"

  private def testNameToTestGroup(name: String): String = name match {
    case _ if testsWhichShouldRunInTheirOwnDedicatedJvm.contains(name) => name
    // Different with the cases in testsWhichShouldRunInTheirOwnDedicatedJvm, here we are grouping
    // all suites of `org.apache.spark.sql.hive.execution.*` into a single group, instead of
    // launching one JVM per suite.
    case _ if name.contains("org.apache.spark.sql.hive.execution") => HIVE_EXECUTION_TEST_GROUP
    case _ => DEFAULT_TEST_GROUP
  }

  lazy val settings = Seq(
    (Test / testGrouping) := {
      val tests: Seq[TestDefinition] = (Test / definedTests).value
      val defaultForkOptions = ForkOptions(
        javaHome = javaHome.value,
        outputStrategy = outputStrategy.value,
        bootJars = Vector.empty[java.io.File],
        workingDirectory = Some(baseDirectory.value),
        runJVMOptions = (Test / javaOptions).value.toVector,
        connectInput = connectInput.value,
        envVars = (Test / envVars).value
      )
      tests.groupBy(test => testNameToTestGroup(test.name)).map { case (groupName, groupTests) =>
        val forkOptions = {
          if (groupName == DEFAULT_TEST_GROUP) {
            defaultForkOptions
          } else {
            defaultForkOptions.withRunJVMOptions(defaultForkOptions.runJVMOptions ++
              Seq(s"-Djava.io.tmpdir=${baseDirectory.value}/target/tmp/$groupName"))
          }
        }
        new Tests.Group(
          name = groupName,
          tests = groupTests,
          runPolicy = Tests.SubProcess(forkOptions))
      }
    }.toSeq
  )
}

object TestSettings {
  import BuildCommons._
  private val defaultExcludedTags = Seq("org.apache.spark.tags.ChromeUITest",
    "org.apache.spark.deploy.k8s.integrationtest.YuniKornTag",
    "org.apache.spark.internal.io.cloud.IntegrationTestSuite") ++
    (if (System.getProperty("os.name").startsWith("Mac OS X") &&
        System.getProperty("os.arch").equals("aarch64")) {
      Seq("org.apache.spark.tags.ExtendedLevelDBTest")
    } else Seq.empty)

  lazy val settings = Seq (
    // Fork new JVMs for tests and set Java options for those
    fork := true,
    // Setting SPARK_DIST_CLASSPATH is a simple way to make sure any child processes
    // launched by the tests have access to the correct test-time classpath.
    (Test / envVars) ++= {
      val baseEnvVars = Map(
        "SPARK_DIST_CLASSPATH" ->
          (Test / fullClasspath).value.files.map(_.getAbsolutePath)
            .mkString(File.pathSeparator).stripSuffix(File.pathSeparator),
        "SPARK_PREPEND_CLASSES" -> "1",
        "SPARK_SCALA_VERSION" -> scalaBinaryVersion.value,
        "SPARK_TESTING" -> "1",
        "JAVA_HOME" -> sys.env.get("JAVA_HOME").getOrElse(sys.props("java.home")),
        "SPARK_BEELINE_OPTS" -> "-DmyKey=yourValue"
      )

      if (sys.props("os.name").contains("Mac OS X")) {
        baseEnvVars + ("OBJC_DISABLE_INITIALIZE_FORK_SAFETY" -> "YES")
      } else {
        baseEnvVars
      }
    },

    // Copy system properties to forked JVMs so that tests know proxy settings
    (Test / javaOptions) ++= {
      val q = "\""
      sys.props.toList
        .filter {
          case (key, value) => key.startsWith("http.") || key.startsWith("https.")
        }
        .map {
          case (key, value) => s"-D$key=$q$value$q"
        }
    },

    (Test / javaOptions) += s"-Djava.io.tmpdir=$testTempDir",
    (Test / javaOptions) += "-Dspark.test.home=" + sparkHome,
    (Test / javaOptions) += "-Dspark.testing=1",
    (Test / javaOptions) += "-Dspark.port.maxRetries=100",
    (Test / javaOptions) += "-Dspark.master.rest.enabled=false",
    (Test / javaOptions) += "-Dspark.memory.debugFill=true",
    (Test / javaOptions) += "-Dspark.ui.enabled=false",
    (Test / javaOptions) += "-Dspark.ui.showConsoleProgress=false",
    (Test / javaOptions) += "-Dspark.unsafe.exceptionOnMemoryLeak=true",
    (Test / javaOptions) += "-Dspark.hadoop.hadoop.caller.context.enabled=true",
    (Test / javaOptions) += "-Dhive.conf.validation=false",
    (Test / javaOptions) += "-Dsun.io.serialization.extendedDebugInfo=false",
    (Test / javaOptions) += "-Dderby.system.durability=test",
    (Test / javaOptions) ++= {
      if ("true".equals(System.getProperty("java.net.preferIPv6Addresses"))) {
        Seq("-Djava.net.preferIPv6Addresses=true")
      } else {
        Seq.empty
      }
    },
    (Test / javaOptions) ++= System.getProperties.asScala.filter(_._1.startsWith("spark"))
      .map { case (k,v) => s"-D$k=$v" }.toSeq,
    (Test / javaOptions) += "-ea",
    (Test / javaOptions) += s"-XX:ErrorFile=${baseDirectory.value}/target/hs_err_pid%p.log",
    (Test / javaOptions) ++= {
      val metaspaceSize = sys.env.get("METASPACE_SIZE").getOrElse("1300m")
      val heapSize = sys.env.get("HEAP_SIZE").getOrElse("4g")
      val extraTestJavaArgs = Array("-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dio.netty.allocator.type=pooled",
        "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE",
        "-Dio.netty.noUnsafe=false",
        "--enable-native-access=ALL-UNNAMED").mkString(" ")
      s"-Xmx$heapSize -Xss4m -XX:MaxMetaspaceSize=$metaspaceSize -XX:ReservedCodeCacheSize=128m -Dfile.encoding=UTF-8 $extraTestJavaArgs"
        .split(" ").toSeq
    },
    javaOptions ++= {
      val metaspaceSize = sys.env.get("METASPACE_SIZE").getOrElse("1300m")
      val heapSize = sys.env.get("HEAP_SIZE").getOrElse("4g")
      s"-Xmx$heapSize -XX:MaxMetaspaceSize=$metaspaceSize".split(" ").toSeq
    },
    (Test / javaOptions) ++= {
      val jdwpEnabled = sys.props.getOrElse("test.jdwp.enabled", "false").toBoolean

      if (jdwpEnabled) {
        val jdwpAddr = sys.props.getOrElse("test.jdwp.address", "localhost:0")
        val jdwpServer = sys.props.getOrElse("test.jdwp.server", "y")
        val jdwpSuspend = sys.props.getOrElse("test.jdwp.suspend", "y")
        ("-agentlib:jdwp=transport=dt_socket," +
          s"suspend=$jdwpSuspend,server=$jdwpServer,address=$jdwpAddr").split(" ").toSeq
      } else {
        Seq.empty
      }
    },
    // Exclude tags defined in a system property
    (Test / testOptions) += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.exclude.tags").map { tags =>
        tags.split(",").flatMap { tag => Seq("-l", tag) }.toSeq
      }.getOrElse(Nil): _*),
    (Test / testOptions) += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.default.exclude.tags").map(tags => tags.split(",").toSeq)
        .map(tags => tags.filter(!_.trim.isEmpty)).getOrElse(defaultExcludedTags)
        .flatMap(tag => Seq("-l", tag)): _*),
    (Test / testOptions) += Tests.Argument(jupiterTestFramework,
      sys.props.get("test.exclude.tags").map { tags =>
        Seq("--exclude-tag=" + tags)
      }.getOrElse(Nil): _*),
    // Include tags defined in a system property
    (Test / testOptions) += Tests.Argument(TestFrameworks.ScalaTest,
      sys.props.get("test.include.tags").map { tags =>
        tags.split(",").flatMap { tag => Seq("-n", tag) }.toSeq
      }.getOrElse(Nil): _*),
    (Test / testOptions) += Tests.Argument(jupiterTestFramework,
      sys.props.get("test.include.tags").map { tags =>
        Seq("--include-tags=" + tags)
      }.getOrElse(Nil): _*),
    // Show full stack trace and duration in test cases.
    (Test / testOptions) += Tests.Argument("-oDF"),
    // Slowpoke notifications: receive notifications every 5 minute of tests that have been running
    // longer than two minutes.
    (Test / testOptions) += Tests.Argument(TestFrameworks.ScalaTest, "-W", "120", "300"),
    (Test / testOptions) += Tests.Argument(jupiterTestFramework, "-v", "-a"),
    // Enable Junit testing.
    libraryDependencies += "com.github.sbt.junit" % "jupiter-interface" % "0.17.0" % "test",
    // `parallelExecutionInTest` controls whether test suites belonging to the same SBT project
    // can run in parallel with one another. It does NOT control whether tests execute in parallel
    // within the same JVM (which is controlled by `testForkedParallel`) or whether test cases
    // within the same suite can run in parallel (which is a ScalaTest runner option which is passed
    // to the underlying runner but is not a SBT-level configuration). This needs to be `true` in
    // order for the extra parallelism enabled by `SparkParallelTestGrouping` to take effect.
    // The `SERIAL_SBT_TESTS` check is here so the extra parallelism can be feature-flagged.
    (Test / parallelExecution) := { if (sys.env.contains("SERIAL_SBT_TESTS")) false else true },
    // Make sure the test temp directory exists.
    (Test / resourceGenerators) += Def.macroValueI((Test / resourceManaged) map { outDir: File =>
      var dir = new File(testTempDir)
      if (!dir.isDirectory()) {
        // Because File.mkdirs() can fail if multiple callers are trying to create the same
        // parent directory, this code tries to create parents one at a time, and avoids
        // failures when the directories have been created by somebody else.
        val stack = new ListBuffer[File]()
        while (!dir.isDirectory()) {
          stack.prepend(dir)
          dir = dir.getParentFile()
        }

        while (stack.nonEmpty) {
          val d = stack.remove(0)
          require(d.mkdir() || d.isDirectory(), s"Failed to create directory $d")
        }
      }
      Seq.empty[File]
    }).value,
    (Global / concurrentRestrictions) := {
      // The number of concurrent test groups is empirically chosen based on experience
      // with Jenkins flakiness.
      if (sys.env.contains("SERIAL_SBT_TESTS")) (Global / concurrentRestrictions).value
      else Seq(Tags.limit(Tags.ForkedTestGroup, 4))
    }
  )

}
