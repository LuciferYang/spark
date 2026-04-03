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

import java.io._
import java.nio.charset.StandardCharsets.UTF_8

import sbt._
import sbt.Keys._
import sbt.librarymanagement.{ VersionNumber, SemanticSelector }
import com.etsy.sbt.checkstyle.CheckstylePlugin.autoImport._
import com.here.bom.Bom
import sbtpomreader.SbtPomKeys
import com.typesafe.tools.mima.plugin.MimaKeys
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import org.scalastyle.sbt.Tasks

import sbtprotoc.ProtocPlugin.autoImport._

/**
 * Overrides to work around sbt's dependency resolution being different from Maven's.
 */
object DependencyOverrides {
  lazy val jacksonVersion = sys.props.get("fasterxml.jackson.version").getOrElse("2.21.2")
  lazy val jacksonDeps = Bom.dependencies("com.fasterxml.jackson" % "jackson-bom" % jacksonVersion)
  lazy val settings = jacksonDeps ++ Seq(
    dependencyOverrides ++= {
      val guavaVersion = sys.props.get("guava.version").getOrElse(
        SbtPomKeys.effectivePom.value.getProperties.get("guava.version").asInstanceOf[String])
      val jlineVersion =
        SbtPomKeys.effectivePom.value.getProperties.get("jline.version").asInstanceOf[String]
      val avroVersion =
        SbtPomKeys.effectivePom.value.getProperties.get("avro.version").asInstanceOf[String]
      val slf4jVersion =
        SbtPomKeys.effectivePom.value.getProperties.get("slf4j.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "jline" % "jline" % jlineVersion,
        "org.apache.avro" % "avro" % avroVersion,
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.scala-lang" % "scalap" % scalaVersion.value
      ) ++ jacksonDeps.key.value
    }
  )
}

/**
 * This excludes library dependencies in sbt, which are specified in maven but are
 * not needed by sbt build.
 */
object ExcludedDependencies {
  lazy val settings = Seq(
    libraryDependencies ~= { libs => libs.filterNot(_.name == "groovy-all") },
    excludeDependencies ++= Seq(
      ExclusionRule(organization = "ch.qos.logback"),
      ExclusionRule("org.lz4", "lz4-java"),
      ExclusionRule("org.slf4j", "slf4j-simple"),
      ExclusionRule("javax.servlet", "javax.servlet-api"))
  )
}

/**
 * This excludes the spark-connect-shims module from a module when it is not part of the connect
 * client dependencies.
 */
object ExcludeShims {
  import bloop.integrations.sbt.BloopKeys

  val shimmedProjects = Set("spark-sql-api", "spark-connect-common", "spark-connect-client-jdbc", "spark-connect-client-jvm")
  val classPathFilter = TaskKey[Classpath => Classpath]("filter for classpath")

  // Filter for bloopInternalClasspath which is Seq[(File, File)]
  type BloopClasspath = Seq[(java.io.File, java.io.File)]
  val bloopClasspathFilter = TaskKey[BloopClasspath => BloopClasspath]("filter for bloop classpath")

  lazy val settings = Seq(
    classPathFilter := {
      if (!shimmedProjects(moduleName.value)) {
        cp => cp.filterNot(_.data.name.contains("spark-connect-shims"))
      } else {
        identity _
      }
    },
    bloopClasspathFilter := {
      if (!shimmedProjects(moduleName.value)) {
        // Note: bloop output directories use "connect-shims" (without "spark-" prefix)
        cp => cp.filterNot { case (f1, f2) =>
          f1.getPath.contains("connect-shims") || f2.getPath.contains("connect-shims")
        }
      } else {
        identity _
      }
    },
    Compile / internalDependencyClasspath :=
      classPathFilter.value((Compile / internalDependencyClasspath).value),
    Compile / internalDependencyAsJars :=
      classPathFilter.value((Compile / internalDependencyAsJars).value),
    Runtime / internalDependencyClasspath :=
      classPathFilter.value((Runtime / internalDependencyClasspath).value),
    Runtime / internalDependencyAsJars :=
      classPathFilter.value((Runtime / internalDependencyAsJars).value),
    Test / internalDependencyClasspath :=
      classPathFilter.value((Test / internalDependencyClasspath).value),
    Test / internalDependencyAsJars :=
      classPathFilter.value((Test / internalDependencyAsJars).value),
    // Filter bloop's internal classpath for correct IDE integration
    Compile / BloopKeys.bloopInternalClasspath :=
      bloopClasspathFilter.value((Compile / BloopKeys.bloopInternalClasspath).value),
    Runtime / BloopKeys.bloopInternalClasspath :=
      bloopClasspathFilter.value((Runtime / BloopKeys.bloopInternalClasspath).value),
    Test / BloopKeys.bloopInternalClasspath :=
      bloopClasspathFilter.value((Test / BloopKeys.bloopInternalClasspath).value),
  )
}

/**
 * Project to pull previous artifacts of Spark for generating Mima excludes.
 */
object OldDeps {

  import BuildCommons.protoVersion

  lazy val project = Project("oldDeps", file("dev"))
    .settings(oldDepsSettings)
    .disablePlugins(sbtpomreader.PomReaderPlugin)

  lazy val allPreviousArtifactKeys = Def.settingDyn[Seq[Set[ModuleID]]] {
    SparkBuild.mimaProjects
      .map { project => (project / MimaKeys.mimaPreviousArtifacts) }
      .map(k => Def.setting(k.value))
      .join
  }

  def oldDepsSettings() = Defaults.coreDefaultSettings ++ Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := protoVersion,
    name := "old-deps",
    libraryDependencies := allPreviousArtifactKeys.value.flatten
  )
}

object Unidoc {

  import BuildCommons._
  import sbtunidoc.BaseUnidocPlugin
  import sbtunidoc.JavaUnidocPlugin
  import sbtunidoc.ScalaUnidocPlugin
  import sbtunidoc.BaseUnidocPlugin.autoImport._
  import sbtunidoc.GenJavadocPlugin.autoImport._
  import sbtunidoc.JavaUnidocPlugin.autoImport._
  import sbtunidoc.ScalaUnidocPlugin.autoImport._

  private val ignoredPathSegments = Set(
    "org/apache/spark/deploy",
    "org/apache/spark/examples",
    "org/apache/spark/internal",
    "org/apache/spark/memory",
    "org/apache/spark/network",
    "org/apache/spark/rpc",
    "org/apache/spark/executor",
    "org/apache/spark/ExecutorAllocationClient",
    "org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend",
    "python",
    "org/apache/spark/kafka010",
    "org/apache/spark/types/variant",
    "org/apache/spark/ui/flamegraph",
    "org/apache/spark/util/collection",
    "org/apache/spark/util/io",
    "org/apache/spark/util/kvstore",
    "org/apache/spark/sql/artifact",
    "org/apache/spark/sql/avro",
    "org/apache/spark/sql/catalyst",
    "org/apache/spark/sql/connect/",
    "org/apache/spark/sql/classic/",
    "org/apache/spark/sql/execution",
    "org/apache/spark/sql/internal",
    "org/apache/spark/sql/metricview",
    "org/apache/spark/sql/pipelines",
    "org/apache/spark/sql/scripting",
    "org/apache/spark/sql/ml",
    "org/apache/spark/sql/hive",
    "org/apache/spark/sql/catalog/v2/utils",
    "org/apache/spark/errors",
    "org/apache/spark/sql/errors",
    "org/apache/hive",
    "org/apache/spark/sql/v2/avro",
    "SSLOptions"
  )

  private def shouldIgnoreUndocumented(f: File): Boolean = {
    val path = f.getCanonicalPath
    f.getName.contains("$") ||
      ignoredPathSegments.exists(path.contains) ||
      (path.contains("org/apache/spark/shuffle") &&
        !path.contains("org/apache/spark/shuffle/api")) ||
      (path.contains("org/apache/spark/unsafe") &&
        !path.contains("org/apache/spark/unsafe/types/CalendarInterval"))
  }

  protected def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    packages.map(_.filterNot(shouldIgnoreUndocumented))
  }

  private def ignoreClasspaths(classpaths: Seq[Classpath]): Seq[Classpath] = {
    classpaths
      .map(_.filterNot(_.data.getCanonicalPath.matches(""".*kafka-clients-0\.10.*""")))
      .map(_.filterNot(_.data.getCanonicalPath.matches(""".*kafka_2\..*-0\.10.*""")))
      .map(_.filterNot(_.data.getCanonicalPath.contains("apache-rat")))
      .map(_.filterNot(_.data.getCanonicalPath.contains("connect-shims")))
  }

  val unidocSourceBase = settingKey[String]("Base URL of source links in Scaladoc.")

  lazy val settings = BaseUnidocPlugin.projectSettings ++
                      ScalaUnidocPlugin.projectSettings ++
                      JavaUnidocPlugin.projectSettings ++
                      Seq (
    publish := {},

    (ScalaUnidoc / unidoc / unidocAllClasspaths) := {
      ignoreClasspaths((ScalaUnidoc / unidoc / unidocAllClasspaths).value)
    },

    (JavaUnidoc / unidoc / unidocAllClasspaths) := {
      ignoreClasspaths((JavaUnidoc / unidoc / unidocAllClasspaths).value)
    },

    // Skip actual catalyst, but include the subproject.
    // Catalyst is not public API and contains quasiquotes which break scaladoc.
    (ScalaUnidoc / unidoc / unidocAllSources) := {
      ignoreUndocumentedPackages((ScalaUnidoc / unidoc / unidocAllSources).value)
    },

    // Skip class names containing $ and some internal packages in Javadocs
    (JavaUnidoc / unidoc / unidocAllSources) := {
      ignoreUndocumentedPackages((JavaUnidoc / unidoc / unidocAllSources).value)
        .map(_.filterNot(_.getCanonicalPath.contains("org/apache/hadoop")))
    },

    (JavaUnidoc / unidoc / javacOptions) := {
      Seq(
        "-windowtitle", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
        "-public",
        "-noqualifier", "java.lang",
        "-tag", """example:a:Example\:""",
        "-tag", """note:a:Note\:""",
        "-tag", "group:X",
        "-tag", "tparam:X",
        "-tag", "constructor:X",
        "-tag", "todo:X",
        "-tag", "groupname:X",
        "-tag", "inheritdoc",
        "--ignore-source-errors", "-notree"
      )
    },

    // Use GitHub repository for Scaladoc source links
    unidocSourceBase := s"https://github.com/apache/spark/tree/v${version.value}",

    (ScalaUnidoc / unidoc / scalacOptions) ++= Seq(
      "-groups", // Group similar methods together based on the @group annotation.
      "-skip-packages", "org.apache.hadoop",
      "-sourcepath", (ThisBuild / baseDirectory).value.getAbsolutePath
    ) ++ (
      // Add links to sources when generating Scaladoc for a non-snapshot release
      if (!isSnapshot.value) {
        Opts.doc.sourceUrl(unidocSourceBase.value + "€{FILE_PATH_EXT}")
      } else {
        Seq()
      }
    ),
    (ScalaUnidoc / unidoc / unidocProjectFilter) :=
      inAnyProject -- inProjects(OldDeps.project, repl, examples, tools, kubernetes,
        yarn, tags, streamingKafka010, sqlKafka010, connectCommon, connect, connectJdbc,
        connectClient, connectShims, protobuf, profiler),
    (JavaUnidoc / unidoc / unidocProjectFilter) :=
      inAnyProject -- inProjects(OldDeps.project, repl, examples, tools, kubernetes,
        yarn, tags, streamingKafka010, sqlKafka010, connectCommon, connect, connectJdbc,
        connectClient, connectShims, protobuf, profiler),
  )
}

object Checkstyle {
  lazy val settings = Seq(
    checkstyleSeverityLevel := CheckstyleSeverityLevel.Error,
    (Compile / checkstyle / javaSource) := baseDirectory.value / "src/main/java",
    (Test / checkstyle / javaSource) := baseDirectory.value / "src/test/java",
    checkstyleConfigLocation := CheckstyleConfigLocation.File("dev/checkstyle.xml"),
    checkstyleOutputFile := baseDirectory.value / "target/checkstyle-output.xml",
    (Test / checkstyleOutputFile) := baseDirectory.value / "target/checkstyle-output.xml"
  )
}
