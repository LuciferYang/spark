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
import java.lang.{Runtime => JRuntime}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

import scala.io.Source
import scala.util.Properties
import scala.jdk.CollectionConverters._

import sbt._
import sbt.Classpaths.publishOrSkip
import sbt.Keys._
import sbtpomreader.{PomBuild, SbtPomKeys}
import com.typesafe.tools.mima.plugin.MimaKeys
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import org.scalastyle.sbt.Tasks
import sbtassembly.AssemblyPlugin.autoImport._

import sbtprotoc.ProtocPlugin.autoImport._

object BuildCommons {

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val sqlProjects@Seq(sqlApi, catalyst, sql, hive, hiveThriftServer, tokenProviderKafka010, sqlKafka010, avro, protobuf) =
    Seq("sql-api", "catalyst", "sql", "hive", "hive-thriftserver", "token-provider-kafka-0-10",
      "sql-kafka-0-10", "avro", "protobuf").map(ProjectRef(buildLocation, _))

  val streamingProjects@Seq(streaming, streamingKafka010) =
    Seq("streaming", "streaming-kafka-0-10").map(ProjectRef(buildLocation, _))

  val connectProjects@Seq(connectCommon, connect, connectJdbc, connectClient, connectShims) =
    Seq("connect-common", "connect", "connect-client-jdbc", "connect-client-jvm", "connect-shims")
      .map(ProjectRef(buildLocation, _))

  val allProjects@Seq(
    core, graphx, mllib, mllibLocal, repl, networkCommon, networkShuffle, launcher, unsafe, tags, sketch, kvstore,
    commonUtils, commonUtilsJava, variant, pipelines, _*
  ) = Seq(
    "core", "graphx", "mllib", "mllib-local", "repl", "network-common", "network-shuffle", "launcher", "unsafe",
    "tags", "sketch", "kvstore", "common-utils", "common-utils-java", "variant", "pipelines"
  ).map(ProjectRef(buildLocation, _)) ++ sqlProjects ++ streamingProjects ++ connectProjects

  val optionallyEnabledProjects@Seq(kubernetes, yarn,
    sparkGangliaLgpl, streamingKinesisAsl, profiler,
    dockerIntegrationTests, hadoopCloud, kubernetesIntegrationTests) =
    Seq("kubernetes", "yarn",
      "ganglia-lgpl", "streaming-kinesis-asl", "profiler",
      "docker-integration-tests", "hadoop-cloud", "kubernetes-integration-tests").map(ProjectRef(buildLocation, _))

  val assemblyProjects@Seq(networkYarn, streamingKafka010Assembly, streamingKinesisAslAssembly) =
    Seq("network-yarn", "streaming-kafka-0-10-assembly", "streaming-kinesis-asl-assembly")
      .map(ProjectRef(buildLocation, _))

  val copyJarsProjects@Seq(assembly, examples) = Seq("assembly", "examples")
    .map(ProjectRef(buildLocation, _))

  val tools = ProjectRef(buildLocation, "tools")
  // Root project.
  val spark = ProjectRef(buildLocation, "spark")
  val sparkHome = buildLocation

  val testTempDir = s"$sparkHome/target/tmp"

  val javaVersion = settingKey[String]("source and target JVM version for javac and scalac")

  // Google Protobuf version used for generating the protobuf.
  // SPARK-41247: needs to be consistent with `protobuf.version` in `pom.xml`.
  val protoVersion = "4.33.5"
}

object SparkBuild extends PomBuild {

  import BuildCommons._
  import sbtunidoc.GenJavadocPlugin
  import sbtunidoc.GenJavadocPlugin.autoImport._

  lazy val checkJavaVersion = taskKey[Unit]("Check Java Version")
  lazy val checkJavaVersionSettings: Seq[Setting[?]] = Seq(
    checkJavaVersion := {
      val currentVersion = JRuntime.version()
      val currentVersionFeature = currentVersion.feature()
      val currentVersionUpdate = currentVersion.update()
      val minimumVersion = JRuntime.Version.parse(
        SbtPomKeys.effectivePom.value.getProperties
          .get("java.minimum.version").asInstanceOf[String])
      val minimumVersionFeature = minimumVersion.feature()
      val minimumVersionUpdate = minimumVersion.update()
      val isCompatible = currentVersionFeature > minimumVersionFeature ||
        (currentVersionFeature == minimumVersionFeature &&
          currentVersionUpdate >= minimumVersionUpdate)
      if (!isCompatible) {
        throw new MessageOnlyException(
          "The Java version used to build the project is outdated. " +
            s"Please use Java $minimumVersion or later.")
      }
    },
    (Compile / compile) := ((Compile / compile) dependsOn checkJavaVersion).value,
    (Test / compile) := ((Test / compile) dependsOn checkJavaVersion).value
  )

  val projectsMap: scala.collection.mutable.Map[String, Seq[Setting[_]]] =
    scala.collection.mutable.Map.empty

  override val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES")
      .orElse(Properties.propOrNone("sbt.maven.profiles")) match {
      case None => Seq("sbt")
      case Some(v) =>
        v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
    }
    if (profiles.contains("jdwp-test-debug")) {
      sys.props.put("test.jdwp.enabled", "true")
    }
    if (profiles.contains("user-defined-protoc")) {
      val sparkProtocExecPath = Properties.envOrNone("SPARK_PROTOC_EXEC_PATH")
      val connectPluginExecPath = Properties.envOrNone("CONNECT_PLUGIN_EXEC_PATH")
      if (sparkProtocExecPath.isDefined) {
        sys.props.put("spark.protoc.executable.path", sparkProtocExecPath.get)
      }
      if (connectPluginExecPath.isDefined) {
        sys.props.put("connect.plugin.executable.path", connectPluginExecPath.get)
      }
    }
    profiles
  }

  Properties.envOrNone("SBT_MAVEN_PROPERTIES") match {
    case Some(v) =>
      v.split("(\\s+|,)").filterNot(_.isEmpty).foreach { prop =>
        val idx = prop.indexOf('=')
        if (idx > 0) System.setProperty(prop.substring(0, idx), prop.substring(idx + 1))
      }
    case _ =>
  }

  override val userPropertiesMap = System.getProperties.asScala.toMap

  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val SbtCompile = config("sbt") extend(Compile)

  lazy val sparkGenjavadocSettings: Seq[sbt.Def.Setting[_]] = GenJavadocPlugin.projectSettings ++ Seq(
    scalacOptions ++= Seq(
      "-P:genjavadoc:strictVisibility=true" // hide package private types
    )
  )

  lazy val scalaStyleRules = Project("scalaStyleRules", file("scalastyle"))
    .settings(
      libraryDependencies += "org.scalastyle" %% "scalastyle" % "1.0.0"
    )

  lazy val scalaStyleOnCompile = taskKey[Unit]("scalaStyleOnCompile")

  lazy val scalaStyleOnTest = taskKey[Unit]("scalaStyleOnTest")

  // We special case the 'println' lint rule to only be a warning on compile, because adding
  // printlns for debugging is a common use case and is easy to remember to remove.
  val scalaStyleOnCompileConfig: String = {
    val in = "scalastyle-config.xml"
    val out = "scalastyle-on-compile.generated.xml"
    val replacements = Map(
      """customId="println" level="error"""" -> """customId="println" level="warn""""
    )
    val source = Source.fromFile(in)
    try {
      var contents = source.getLines.mkString("\n")
      for ((k, v) <- replacements) {
        require(contents.contains(k), s"Could not rewrite '$k' in original scalastyle config.")
        contents = contents.replace(k, v)
      }
      val pw = new PrintWriter(out)
      try {
        pw.write(contents)
      } finally {
        pw.close()
      }
      out
    } finally {
      source.close()
    }
  }

  // Return a cached scalastyle task for a given configuration (usually Compile or Test)
  private def cachedScalaStyle(config: Configuration) = Def.task {
    val logger = streams.value.log
    // We need a different cache dir per Configuration, otherwise they collide
    val cacheDir = target.value / s"scalastyle-cache-${config.name}"
    val cachedFun = FileFunction.cached(cacheDir, FilesInfo.lastModified, FilesInfo.exists) {
      (inFiles: Set[File]) => {
        val args: Seq[String] = Seq.empty
        val scalaSourceV = Seq(file((config / scalaSource).value.getAbsolutePath))
        val configV = (ThisBuild / baseDirectory).value / scalaStyleOnCompileConfig
        val configUrlV = (config / scalastyleConfigUrl).value
        val streamsV = ((config / streams).value: @sbtUnchecked)
        val failOnErrorV = true
        val failOnWarningV = false
        val scalastyleTargetV = (config / scalastyleTarget).value
        val configRefreshHoursV = (config / scalastyleConfigRefreshHours).value
        val targetV = (config / target).value
        val configCacheFileV = (config / scalastyleConfigUrlCacheFile).value

        logger.info(s"Running scalastyle on ${name.value} in ${config.name}")
        Tasks.doScalastyle(args, configV, configUrlV, failOnErrorV, failOnWarningV, scalaSourceV,
          scalastyleTargetV, streamsV, configRefreshHoursV, targetV, configCacheFileV)

        Set.empty
      }
    }

    cachedFun(findFiles((config / scalaSource).value))
  }

  private def findFiles(file: File): Set[File] = if (file.isDirectory) {
    file.listFiles().toSet.flatMap(findFiles) + file
  } else {
    Set(file)
  }

  def enableScalaStyle: Seq[sbt.Def.Setting[_]] = Seq(
    scalaStyleOnCompile := cachedScalaStyle(Compile).value,
    scalaStyleOnTest := cachedScalaStyle(Test).value,
    (scalaStyleOnCompile / logLevel) := Level.Warn,
    (scalaStyleOnTest / logLevel) := Level.Warn,
    (Compile / compile) := {
      scalaStyleOnCompile.value
      (Compile / compile).value
    },
    (Test / compile) := {
      scalaStyleOnTest.value
      (Test / compile).value
    }
  )

  lazy val compilerWarningSettings: Seq[sbt.Def.Setting[_]] = Seq(
    (Compile / scalacOptions) ++= {
      Seq(
        // replace -Xfatal-warnings with fine-grained configuration, since 2.13.2
        // verbose warning on deprecation, error on all others
        // see `scalac -Wconf:help` for details
        // since 2.13.15, "-Wconf:cat=deprecation:wv,any:e" no longer takes effect and needs to
        // be changed to "-Wconf:any:e", "-Wconf:cat=deprecation:wv",
        // please refer to the details: https://github.com/scala/scala/pull/10708
        "-Wconf:any:e",
        "-Wconf:cat=deprecation:wv",
        // 2.13-specific warning hits to be muted (as narrowly as possible) and addressed separately
        "-Wunused:imports",
        "-Wconf:msg=^(?=.*?method|value|type|object|trait|inheritance)(?=.*?deprecated)(?=.*?since 2.13).+$:e",
        "-Wconf:msg=^(?=.*?Widening conversion from)(?=.*?is deprecated because it loses precision).+$:e",
        // SPARK-45610 Convert "Auto-application to `()` is deprecated" to compile error, as it will become a compile error in Scala 3.
        "-Wconf:cat=deprecation&msg=Auto-application to \\`\\(\\)\\` is deprecated:e",
        // SPARK-35574 Prevent the recurrence of compilation warnings related to `procedure syntax is deprecated`
        "-Wconf:cat=deprecation&msg=procedure syntax is deprecated:e",
        // SPARK-45627 Symbol literals are deprecated in Scala 2.13 and it's a compile error in Scala 3.
        "-Wconf:cat=deprecation&msg=symbol literal is deprecated:e",
        // SPARK-45627 `enum`, `export` and `given` will become keywords in Scala 3,
        // so they are prohibited from being used as variable names in Scala 2.13 to
        // reduce the cost of migration in subsequent versions.
        "-Wconf:cat=deprecation&msg=it will become a keyword in Scala 3:e",
        // SPARK-46938 to prevent enum scan on pmml-model, under spark-mllib module.
        "-Wconf:cat=other&site=org.dmg.pmml.*:w",
        // SPARK-49937 ban call the method `SparkThrowable#getErrorClass`
        "-Wconf:cat=deprecation&msg=method getErrorClass in trait SparkThrowable is deprecated:e"
      )
    }
  )

  val noLintOnCompile = sys.env.contains("NOLINT_ON_COMPILE") &&
      !sys.env.get("NOLINT_ON_COMPILE").contains("false")
  lazy val sharedSettings = checkJavaVersionSettings ++
                            sparkGenjavadocSettings ++
                            compilerWarningSettings ++
      (if (noLintOnCompile) Nil else enableScalaStyle) ++ Seq(
    (Compile / exportJars) := true,
    (Test / exportJars) := false,
    javaHome := sys.env.get("JAVA_HOME")
      .orElse(sys.props.get("java.home"))
      .map(file),
    publishMavenStyle := true,
    unidocGenjavadocVersion := "0.19",

    // Override SBT's default resolvers:
    resolvers :=
      sys.env.get("MAVEN_MIRROR_URL").map("maven-mirror" at _).toSeq ++
      Seq(
        // Google Mirror of Maven Central, placed first so that it's used instead of flaky Maven Central.
        // See https://storage-download.googleapis.com/maven-central/index.html for more info.
        "gcs-maven-central-mirror" at "https://maven-central.storage-download.googleapis.com/maven2/",
        DefaultMavenRepository,
        Resolver.mavenLocal,
        Resolver.file("ivyLocal", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
      ),
    externalResolvers := resolvers.value,
    otherResolvers := SbtPomKeys.mvnLocalRepository(dotM2 => Seq(Resolver.file("dotM2", dotM2))).value,
    (MavenCompile / publishLocalConfiguration) := PublishConfiguration()
        .withResolverName("dotM2")
        .withArtifacts(packagedArtifacts.value.toVector)
        .withLogging(ivyLoggingLevel.value),
    (SbtCompile / publishLocalConfiguration) := PublishConfiguration()
        .withResolverName("ivyLocal")
        .withArtifacts(packagedArtifacts.value.toVector)
        .withLogging(ivyLoggingLevel.value),
    (MavenCompile / publishMavenStyle) := true,
    (SbtCompile / publishMavenStyle) := false,
    (MavenCompile / publishLocal) := publishOrSkip((MavenCompile / publishLocalConfiguration),
      (publishLocal / skip)).value,
    (SbtCompile / publishLocal) := publishOrSkip((SbtCompile / publishLocalConfiguration),
      (publishLocal / skip)).value,
    publishLocal := Seq((MavenCompile / publishLocal), (SbtCompile / publishLocal)).dependOn.value,

    javaOptions ++= {
      // for `dev.ludovic.netlib.blas` which implements such hardware-accelerated BLAS operations
      Seq("--add-modules=jdk.incubator.vector")
    },

    (Compile / doc / javacOptions) ++= {
      Seq("-Xdoclint:all", "-Xdoclint:-missing")
    },

    javaVersion := SbtPomKeys.effectivePom.value.getProperties.get("java.version").asInstanceOf[String],

    (Compile / javacOptions) ++= Seq(
      "-encoding", UTF_8.name(),
      "-g",
      "-proc:full",
      "--release", javaVersion.value
    ),
    // This -target and Xlint:unchecked options cannot be set in the Compile configuration scope since
    // `javadoc` doesn't play nicely with them; see https://github.com/sbt/sbt/issues/355#issuecomment-3817629
    // for additional discussion and explanation.
    (Compile / compile / javacOptions) ++= Seq(
      "-Xlint:unchecked"
    ),

    (Compile / scalacOptions) ++= Seq(
      "-release", javaVersion.value,
      "-sourcepath", (ThisBuild / baseDirectory).value.getAbsolutePath  // Required for relative source links in scaladoc
    ),

    SbtPomKeys.profiles := profiles,

    // Remove certain packages from Scaladoc
    (Compile / doc / scalacOptions) := Seq(
      "-groups",
      "-skip-packages", Seq(
        "org.apache.spark.api.python",
        "org.apache.spark.deploy",
        "org.apache.spark.kafka010",
        "org.apache.spark.network",
        "org.apache.spark.sql.avro",
        "org.apache.spark.sql.metricview",
        "org.apache.spark.sql.pipelines",
        "org.apache.spark.sql.scripting",
        "org.apache.spark.types.variant",
        "org.apache.spark.ui.flamegraph",
        "org.apache.spark.util.collection"
      ).mkString(":"),
      "-doc-title", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
    ),

    // disable Mima check for all modules,
    // to be enabled in specific ones that have previous artifacts
    MimaKeys.mimaFailOnNoPrevious := false,

    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := protoVersion,
  )

  // Shared assembly settings used by SparkConnectCommon, SparkConnect, SparkConnectJdbc,
  // SparkConnectClient, and SparkProtobuf.
  lazy val commonAssemblySettings: Seq[Setting[_]] = Seq(
    (assembly / test) := {},
    (assembly / logLevel) := Level.Info,
    (assembly / assemblyPackageScala / assembleArtifact) := false
  )

  val defaultAssemblyMergeStrategy: Setting[_] =
    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/services/") =>
        MergeStrategy.filterDistinctLines
      case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }

  // Shared protoc executable settings used by Core, SQL, SparkProtobuf, and SparkConnectCommon.
  def protocExecutableSettings: Seq[Setting[_]] = {
    sys.props.get("spark.protoc.executable.path") match {
      case Some(path) => Seq(PB.protocExecutable := file(path))
      case None => Seq.empty
    }
  }

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  (allProjects ++ optionallyEnabledProjects ++ assemblyProjects ++ copyJarsProjects ++ Seq(spark, tools))
    .foreach(enable(sharedSettings ++ DependencyOverrides.settings ++
      ExcludedDependencies.settings ++ (if (noLintOnCompile) Nil else Checkstyle.settings) ++
      ExcludeShims.settings))

  /* Enable tests settings for all projects except examples, assembly and tools */
  (allProjects ++ optionallyEnabledProjects).foreach(enable(TestSettings.settings))

  val mimaProjects = allProjects.filterNot { x =>
    Seq(
      spark, hive, hiveThriftServer, repl, networkCommon, networkShuffle, networkYarn,
      unsafe, tags, tokenProviderKafka010, sqlKafka010, pipelines, connectCommon, connect,
      connectJdbc, connectClient, variant, connectShims, profiler, commonUtilsJava
    ).contains(x)
  }

  mimaProjects.foreach { x =>
    enable(MimaBuild.mimaSettings(sparkHome, x))(x)
  }

  /* Generate and pick the spark build info from extra-resources */
  enable(CommonUtils.settings)(commonUtils)

  enable(Core.settings)(core)

  /*
   * Set up tasks to copy dependencies during packaging. This step can be disabled in the command
   * line, so that dev/mima can run without trying to copy these files again and potentially
   * causing issues.
   */
  if (!"false".equals(System.getProperty("copyDependencies"))) {
    copyJarsProjects.foreach(enable(CopyDependencies.settings))
  }

  /* Enable Assembly for all assembly projects */
  assemblyProjects.foreach(enable(Assembly.settings))

  /* Package pyspark artifacts in a separate zip file for YARN. */
  enable(PySparkAssembly.settings)(assembly)

  /* Enable unidoc only for the root spark project */
  enable(Unidoc.settings)(spark)

  /* Sql-api ANTLR generation settings */
  enable(SqlApi.settings)(sqlApi)

  /* Spark SQL Core settings */
  enable(SQL.settings)(sql)

  /* Hive console settings */
  enable(Hive.settings)(hive)

  enable(HiveThriftServer.settings)(hiveThriftServer)

  enable(SparkConnectCommon.settings)(connectCommon)
  enable(SparkConnect.settings)(connect)
  enable(SparkConnectJdbc.settings)(connectJdbc)
  enable(SparkConnectClient.settings)(connectClient)

  /* Protobuf settings */
  enable(SparkProtobuf.settings)(protobuf)

  enable(KubernetesIntegrationTests.settings)(kubernetesIntegrationTests)

  enable(YARN.settings)(yarn)

  if (profiles.contains("sparkr")) {
    enable(SparkR.settings)(core)
  }

  /**
   * Adds the ability to run the spark shell directly from SBT without building an assembly
   * jar.
   *
   * Usage: `build/sbt sparkShell`
   */
  val sparkShell = taskKey[Unit]("start a spark-shell.")
  val sparkPackage = inputKey[Unit](
    s"""
       |Download and run a spark package.
       |Usage `builds/sbt "sparkPackage <group:artifact:version> <MainClass> [args]
     """.stripMargin)
  val sparkSql = taskKey[Unit]("starts the spark sql CLI.")

  enable(Seq(
    (run / connectInput) := true,
    fork := true,
    (run / outputStrategy) := Some (StdoutOutput),

    javaOptions += "-Xmx2g",

    sparkShell := {
      (Compile / runMain).toTask(" org.apache.spark.repl.Main -usejavacp").value
    },

    sparkPackage := {
      import complete.DefaultParsers._
      val packages :: className :: otherArgs = spaceDelimited("<group:artifact:version> <MainClass> [args]").parsed.toList
      val scalaRun = (run / runner).value
      val classpath = (Runtime / fullClasspath).value
      val args = Seq("--packages", packages, "--class", className, (LocalProject("core") / Compile / Keys.`package`)
        .value.getCanonicalPath) ++ otherArgs
      println(args)
      scalaRun.run("org.apache.spark.deploy.SparkSubmit", classpath.map(_.data), args, streams.value.log)
    },

    (Compile / javaOptions) += "-Dspark.master=local",

    sparkSql := {
      (Compile / runMain).toTask(" org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver").value
    }
  ))(assembly)

  enable(Seq(sparkShell := (LocalProject("assembly") / sparkShell).value))(spark)

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    } ++ Seq[Project](OldDeps.project)
  }

  if (!sys.env.contains("SERIAL_SBT_TESTS")) {
    allProjects.foreach(enable(SparkParallelTestGrouping.settings))
  }
}

