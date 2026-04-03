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

import sbt._
import sbt.Keys._
import sbtpomreader.SbtPomKeys
import sbtassembly.AssemblyPlugin.autoImport._

import sbtprotoc.ProtocPlugin.autoImport._

object CommonUtils {
  import scala.sys.process.Process
  def buildenv = Process(Seq("uname")).!!.trim.replaceFirst("[^A-Za-z0-9].*", "").toLowerCase
  def bashpath = Process(Seq("where", "bash")).!!.split("[\r\n]+").head.replace('\\', '/')
  lazy val settings = Seq(
    (Compile / resourceGenerators) += Def.task {
      val buildScript = baseDirectory.value + "/../../build/spark-build-info"
      val targetDir = baseDirectory.value + "/target/extra-resources/"
      // support Windows build under cygwin/mingw64, etc
      val bash = buildenv match {
        case "cygwin" | "msys2" | "mingw64" | "clang64" => bashpath
        case _ => "bash"
      }
      val command = Seq(bash, buildScript, targetDir, version.value)
      Process(command).!!
      val propsFile = baseDirectory.value / "target" / "extra-resources" / "spark-version-info.properties"
      Seq(propsFile)
    }.taskValue
  )
}

object Core {
  import BuildCommons.protoVersion
  lazy val settings = Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := BuildCommons.protoVersion,
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      Seq(
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },
    // Use Maven's output directory so sbt and Maven can share generated sources.
    // Core uses protoc-jar-maven-plugin which outputs to target/generated-sources.
    (Compile / PB.targets) := Seq(
      PB.gens.java -> target.value / "generated-sources"
    )
  ) ++ SparkBuild.protocExecutableSettings
}

object SparkProtobuf {
  import BuildCommons.protoVersion

  lazy val settings = SparkBuild.commonAssemblySettings ++ Seq(SparkBuild.defaultAssemblyMergeStrategy) ++ Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := BuildCommons.protoVersion,

    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf",

    dependencyOverrides += "com.google.protobuf" % "protobuf-java" % protoVersion,

    (Test / PB.protoSources) += (Test / sourceDirectory).value / "resources" / "protobuf",

    (Test / PB.protocOptions) += "--include_imports",

    (Test / PB.targets) := Seq(
      PB.gens.java -> target.value / "generated-test-sources",
      PB.gens.descriptorSet -> target.value / "generated-test-sources/descriptor-set-sbt.desc",
      // The above creates single descriptor file with all the proto files. This is different from
      // Maven, which create one descriptor file for each proto file.
    ),

    // SPARK-52227: Include `spark-protobuf-*.jar`, `unused-*.jar`,`protobuf-*.jar`in assembly.
    // This needs to be consistent with the content of `maven-shade-plugin`.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      val validPrefixes = Set("spark-protobuf", "protobuf-")
      cp filterNot { v =>
        validPrefixes.exists(v.data.getName.startsWith)
      }
    },

    (assembly / assemblyShadeRules) := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.spark_protobuf.protobuf.@1").inAll,
    ),
  ) ++ SparkBuild.protocExecutableSettings
}

/**
 * These settings run the Kubernetes integration tests.
 * Docker images will have the "dev" tag, and will be overwritten every time the
 * integration tests are run. The integration tests are actually bound to the "test" phase,
 * so running "test" on this module will run the integration tests.
 *
 * There are two ways to run the tests:
 * - the "tests" task builds docker images and runs the test, so it's a little slow.
 * - the "run-its" task just runs the tests on a pre-built set of images.
 *
 * Note that this does not use the shell scripts that the maven build uses, which are more
 * configurable. This is meant as a quick way for developers to run these tests against their
 * local changes.
 */
object KubernetesIntegrationTests {
  import BuildCommons._
  import scala.sys.process.Process

  val dockerBuild = TaskKey[Unit]("docker-imgs", "Build the docker images for ITs.")
  val runITs = TaskKey[Unit]("run-its", "Only run ITs, skip image build.")
  val imageRepo = sys.props.getOrElse("spark.kubernetes.test.imageRepo", "docker.io/kubespark")
  val imageTag = sys.props.get("spark.kubernetes.test.imageTag")
  val namespace = sys.props.get("spark.kubernetes.test.namespace")
  val deployMode = sys.props.get("spark.kubernetes.test.deployMode")

  // Hack: this variable is used to control whether to build docker images. It's updated by
  // the tasks below in a non-obvious way, so that you get the functionality described in
  // the scaladoc above.
  private var shouldBuildImage = true

  lazy val settings = Seq(
    dockerBuild := {
      if (shouldBuildImage) {
        val dockerTool = s"$sparkHome/bin/docker-image-tool.sh"
        val bindingsDir = s"$sparkHome/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings"
        val javaImageTag = sys.props.get("spark.kubernetes.test.javaImageTag")
        val dockerFile = sys.props.getOrElse("spark.kubernetes.test.dockerFile",
            s"$sparkHome/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile")
        val pyDockerFile = sys.props.getOrElse("spark.kubernetes.test.pyDockerFile",
            s"$bindingsDir/python/Dockerfile")
        var rDockerFile = sys.props.getOrElse("spark.kubernetes.test.rDockerFile",
            s"$bindingsDir/R/Dockerfile")
        val excludeTags = sys.props.getOrElse("test.exclude.tags", "").split(",")
        if (excludeTags.exists(_.equalsIgnoreCase("r"))) {
          rDockerFile = ""
        }
        val extraOptions = if (javaImageTag.isDefined) {
          Seq("-b", s"java_image_tag=${javaImageTag.get}")
        } else {
          Seq("-f", s"$dockerFile")
        }
        val cmd = Seq(dockerTool,
          "-r", imageRepo,
          "-t", imageTag.getOrElse("dev"),
          "-p", pyDockerFile,
          "-R", rDockerFile) ++
          (if (deployMode != Some("minikube")) Seq.empty else Seq("-m")) ++
          extraOptions :+
          "build"
        val ec = Process(cmd).!
        if (ec != 0) {
          throw new IllegalStateException(s"Process '${cmd.mkString(" ")}' exited with $ec.")
        }
        if (deployMode == Some("cloud")) {
          val cmd = Seq(dockerTool, "-r", imageRepo, "-t", imageTag.getOrElse("dev"), "push")
          val ret = Process(cmd).!
          if (ret != 0) {
            throw new IllegalStateException(s"Process '${cmd.mkString(" ")}' exited with $ret.")
          }
        }
      }
      shouldBuildImage = true
    },
    runITs := Def.taskDyn {
      shouldBuildImage = false
      Def.task {
        (Test / test).value
      }
    }.value,
    (Test / test) := (Test / test).dependsOn(dockerBuild).value,
    (Test / javaOptions) ++= Seq(
      s"-Dspark.kubernetes.test.deployMode=${deployMode.getOrElse("minikube")}",
      s"-Dspark.kubernetes.test.imageRepo=${imageRepo}",
      s"-Dspark.kubernetes.test.imageTag=${imageTag.getOrElse("dev")}",
      s"-Dspark.kubernetes.test.unpackSparkDir=$sparkHome"
    ),
    (Test / javaOptions) ++= namespace.map("-Dspark.kubernetes.test.namespace=" + _),
    // Force packaging before building images, so that the latest code is tested.
    dockerBuild := dockerBuild
      .dependsOn(assembly / Compile / packageBin)
      .dependsOn(examples / Compile / packageBin)
      .value
  )
}

object SqlApi {
  import com.simplytyped.Antlr4Plugin
  import com.simplytyped.Antlr4Plugin.autoImport._

  lazy val settings = Antlr4Plugin.projectSettings ++ Seq(
    (Antlr4 / antlr4Version) := SbtPomKeys.effectivePom.value.getProperties.get("antlr4.version").asInstanceOf[String],
    (Antlr4 / antlr4PackageName) := Some("org.apache.spark.sql.catalyst.parser"),
    (Antlr4 / antlr4GenListener) := true,
    (Antlr4 / antlr4GenVisitor) := true,
    (Antlr4 / antlr4TreatWarningsAsErrors) := true,
    // Use Maven's output directory so sbt and Maven can share generated sources
    (Antlr4 / javaSource) := target.value / "generated-sources" / "antlr4"
  )
}

object SQL {
  import BuildCommons.protoVersion
  lazy val settings = Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := BuildCommons.protoVersion,
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      Seq(
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },
    // Use Maven's output directory so sbt and Maven can share generated sources.
    // sql/core uses protoc-jar-maven-plugin which outputs to target/generated-sources.
    (Compile / PB.targets) := Seq(
      PB.gens.java -> target.value / "generated-sources"
    )
  ) ++ SparkBuild.protocExecutableSettings
}

object Hive {

  lazy val settings = Seq(
    // Specially disable assertions since some Hive tests fail them
    (Test / javaOptions) := (Test / javaOptions).value.filterNot(_ == "-ea"),
    // Hive tests need higher metaspace size
    (Test / javaOptions) := (Test / javaOptions).value.filterNot(_.contains("MaxMetaspaceSize")),
    (Test / javaOptions) += "-XX:MaxMetaspaceSize=2g",
    // SPARK-45265: HivePartitionFilteringSuite addPartitions related tests generate supper long
    // direct sql against derby server, which may cause stack overflow error when derby do sql
    // parsing.
    // We need to increase the Xss for the test. Meanwhile, QueryParsingErrorsSuite requires a
    // smaller size of Xss to mock a FAILED_TO_PARSE_TOO_COMPLEX error, so we need to set for
    // hive moudle specifically.
    (Test / javaOptions) := (Test / javaOptions).value.filterNot(_.contains("Xss")),
    // SPARK-45265: The value for `-Xss` should be consistent with the configuration value for
    // `scalatest-maven-plugin` in `sql/hive/pom.xml`
    (Test / javaOptions) += "-Xss64m",
    // Supporting all SerDes requires us to depend on deprecated APIs, so we turn off the warnings
    // only for this subproject.
    scalacOptions := (scalacOptions map { currentOpts: Seq[String] =>
      currentOpts.filterNot(_ == "-deprecation")
    }).value,
    // Some of our log4j jars make it impossible to submit jobs from this JVM to Hive Map/Reduce
    // in order to generate golden files.  This is only required for developers who are adding new
    // new query tests.
    (Test / fullClasspath) := (Test / fullClasspath).value.filterNot { f => f.toString.contains("jcl-over") }
  )
}

object HiveThriftServer {
  lazy val settings = Seq(
    excludeDependencies ++= Seq(
      ExclusionRule("org.apache.hive", "hive-llap-common"),
      ExclusionRule("org.apache.hive", "hive-llap-client"))
  )
}

object YARN {
  val genConfigProperties = TaskKey[Unit]("gen-config-properties",
    "Generate config.properties which contains a setting whether Hadoop is provided or not")
  val propFileName = "config.properties"
  val hadoopProvidedProp = "spark.yarn.isHadoopProvided"
  val buildTestDeps = TaskKey[Unit]("buildTestDeps", "Build needed dependencies for test.")

  lazy val settings = Seq(
    Compile / unmanagedResources :=
      (Compile / unmanagedResources).value.filter(!_.getName.endsWith(s"$propFileName")),
    genConfigProperties := {
      val file = (Compile / classDirectory).value / s"org/apache/spark/deploy/yarn/$propFileName"
      val isHadoopProvided = SbtPomKeys.effectivePom.value.getProperties.get(hadoopProvidedProp)
      sbt.IO.write(file, s"$hadoopProvidedProp = $isHadoopProvided")
    },
    Compile / copyResources := (Def.taskDyn {
      val c = (Compile / copyResources).value
      Def.task {
        (Compile / genConfigProperties).value
        c
      }
    }).value,

    buildTestDeps := {
      (LocalProject("assembly") / Compile / Keys.`package`).value
    },
    test := ((Test / test) dependsOn (buildTestDeps)).value,

    testOnly := ((Test / testOnly) dependsOn (buildTestDeps)).evaluated
  )
}

object SparkR {
  import scala.sys.process.Process

  val buildRPackage = taskKey[Unit]("Build the R package")
  lazy val settings = Seq(
    buildRPackage := {
      val postfix = if (File.separator == "\\") ".bat" else ".sh"
      val command = baseDirectory.value / ".." / "R" / s"install-dev$postfix"
      Process(command.toString).!!
    },
    (Compile / compile) := (Def.taskDyn {
      val c = (Compile / compile).value
      Def.task {
        (Compile / buildRPackage).value
        c
      }
    }).value
  )
}
