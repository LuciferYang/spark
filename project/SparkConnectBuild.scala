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

import java.util.Locale

import sbt._
import sbt.Keys._
import sbtpomreader.SbtPomKeys
import sbtassembly.AssemblyPlugin.autoImport._

import sbtprotoc.ProtocPlugin.autoImport._

object SparkConnectCommon {
  import BuildCommons.protoVersion

  lazy val settings = SparkBuild.commonAssemblySettings ++ Seq(SparkBuild.defaultAssemblyMergeStrategy) ++ Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := BuildCommons.protoVersion,

    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.version").asInstanceOf[String]
      val guavaFailureaccessVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.failureaccess.version").asInstanceOf[String]
      val grpcVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "io.grpc.version").asInstanceOf[String]
      Seq(
        "io.grpc" % "protoc-gen-grpc-java" % grpcVersion asProtocPlugin(),
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.guava" % "failureaccess" % guavaFailureaccessVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },

    dependencyOverrides ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.version").asInstanceOf[String]
      val guavaFailureaccessVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.failureaccess.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.guava" % "failureaccess" % guavaFailureaccessVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion
      )
    },

    // Exclude `pmml-model-*.jar`, `scala-collection-compat_*.jar`,`jsr305-*.jar` and
    // `netty-*.jar` and `unused-1.0.0.jar` from assembly.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      cp filter { v =>
        val name = v.data.getName
        name.startsWith("pmml-model-") || name.startsWith("scala-collection-compat_") ||
          name.startsWith("jsr305-") || name.startsWith("netty-") || name == "unused-1.0.0.jar"
      }
    }
  ) ++ {
    val sparkProtocExecPath = sys.props.get("spark.protoc.executable.path")
    val connectPluginExecPath = sys.props.get("connect.plugin.executable.path")
    // Use Maven's output directory so sbt and Maven can share generated sources
    if (sparkProtocExecPath.isDefined && connectPluginExecPath.isDefined) {
      Seq(
        (Compile / PB.targets) := Seq(
          PB.gens.java -> target.value / "generated-sources" / "protobuf" / "java",
          PB.gens.plugin(name = "grpc-java", path = connectPluginExecPath.get) -> target.value / "generated-sources" / "protobuf" / "grpc-java"
        ),
        PB.protocExecutable := file(sparkProtocExecPath.get)
      )
    } else {
      Seq(
        (Compile / PB.targets) := Seq(
          PB.gens.java -> target.value / "generated-sources" / "protobuf" / "java",
          PB.gens.plugin("grpc-java") -> target.value / "generated-sources" / "protobuf" / "grpc-java"
        )
      )
    }
  }
}

object SparkConnect {
  import BuildCommons.protoVersion

  lazy val settings = SparkBuild.commonAssemblySettings ++ Seq(SparkBuild.defaultAssemblyMergeStrategy) ++ Seq(
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.version").asInstanceOf[String]
      val guavaFailureaccessVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.failureaccess.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.guava" % "failureaccess" % guavaFailureaccessVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },

    dependencyOverrides ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.version").asInstanceOf[String]
      val guavaFailureaccessVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.failureaccess.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.guava" % "failureaccess" % guavaFailureaccessVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion
      )
    },

    // SPARK-46733: Include `spark-connect-*.jar`, `unused-*.jar`, `annotations-*.jar`,
    // `grpc-*.jar`, `protobuf-*.jar`, `gson-*.jar`, `animal-sniffer-annotations-*.jar`,
    // `perfmark-api-*.jar`, `proto-google-common-protos-*.jar` in assembly.
    // This needs to be consistent with the content of `maven-shade-plugin`.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      val validPrefixes = Set("spark-connect", "unused-", "annotations-",
        "grpc-", "protobuf-", "gson", "animal-sniffer-annotations",
        "perfmark-api", "proto-google-common-protos")
      cp filterNot { v =>
        validPrefixes.exists(v.data.getName.startsWith)
      }
    },

    (assembly / assemblyShadeRules) := Seq(
      ShadeRule.rename("io.grpc.**" -> "org.sparkproject.connect.grpc.@1").inAll,
      ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.connect.protobuf.@1").inAll,
      ShadeRule.rename("android.annotation.**" -> "org.sparkproject.connect.android_annotation.@1").inAll,
      ShadeRule.rename("io.perfmark.**" -> "org.sparkproject.connect.io_perfmark.@1").inAll,
      ShadeRule.rename("org.codehaus.mojo.animal_sniffer.**" -> "org.sparkproject.connect.animal_sniffer.@1").inAll,
      ShadeRule.rename("com.google.gson.**" -> "org.sparkproject.connect.gson.@1").inAll,
      ShadeRule.rename("com.google.api.**" -> "org.sparkproject.connect.google_protos.api.@1").inAll,
      ShadeRule.rename("com.google.apps.**" -> "org.sparkproject.connect.google_protos.apps.@1").inAll,
      ShadeRule.rename("com.google.cloud.**" -> "org.sparkproject.connect.google_protos.cloud.@1").inAll,
      ShadeRule.rename("com.google.geo.**" -> "org.sparkproject.connect.google_protos.geo.@1").inAll,
      ShadeRule.rename("com.google.logging.**" -> "org.sparkproject.connect.google_protos.logging.@1").inAll,
      ShadeRule.rename("com.google.longrunning.**" -> "org.sparkproject.connect.google_protos.longrunning.@1").inAll,
      ShadeRule.rename("com.google.rpc.**" -> "org.sparkproject.connect.google_protos.rpc.@1").inAll,
      ShadeRule.rename("com.google.shopping.**" -> "org.sparkproject.connect.google_protos.shopping.@1").inAll,
      ShadeRule.rename("com.google.type.**" -> "org.sparkproject.connect.google_protos.type.@1").inAll
    )
  )
}

object SparkConnectJdbc {
  import BuildCommons.protoVersion
  val buildTestDeps = TaskKey[Unit]("buildTestDeps", "Build needed dependencies for test.")

  lazy val settings = SparkBuild.commonAssemblySettings ++ Seq(SparkBuild.defaultAssemblyMergeStrategy) ++ Seq(
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },
    dependencyOverrides ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion
      )
    },

    buildTestDeps := {
      (LocalProject("assembly") / Compile / Keys.`package`).value
      (LocalProject("catalyst") / Test / Keys.`package`).value
    },

    // SPARK-42538: Make sure the `${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars` is available for testing.
    // At the same time, the build of `connect`, `connect-client-jdbc`, `connect-client-jvm` and `sql` will be triggered by `assembly` build,
    // so no additional configuration is required.
    test := ((Test / test) dependsOn (buildTestDeps)).value,

    testOnly := ((Test / testOnly) dependsOn (buildTestDeps)).evaluated,

    (Test / javaOptions) += "-Darrow.memory.debug.allocator=true",

    // Only include `spark-connect-client-jdbc-*.jar` in assembly.
    // This needs to be consistent with the content of `maven-shade-plugin`.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      val validPrefixes = Set("spark-connect-client-jdbc")
      cp filterNot { v =>
        validPrefixes.exists(v.data.getName.startsWith)
      }
    },

    (assembly / assemblyShadeRules) := Seq(
      ShadeRule.rename("io.grpc.**" -> "org.sparkproject.connect.client.io.grpc.@1").inAll,
      ShadeRule.rename("com.google.**" -> "org.sparkproject.connect.client.com.google.@1").inAll,
      ShadeRule.rename("io.netty.**" -> "org.sparkproject.connect.client.io.netty.@1").inAll,
      ShadeRule.rename("io.perfmark.**" -> "org.sparkproject.connect.client.io.perfmark.@1").inAll,
      ShadeRule.rename("org.codehaus.**" -> "org.sparkproject.connect.client.org.codehaus.@1").inAll,
      ShadeRule.rename("android.annotation.**" -> "org.sparkproject.connect.client.android.annotation.@1").inAll
    )
  )
}

object SparkConnectClient {
  import BuildCommons.protoVersion
  val buildTestDeps = TaskKey[Unit]("buildTestDeps", "Build needed dependencies for test.")

  lazy val settings = SparkBuild.commonAssemblySettings ++ Seq(SparkBuild.defaultAssemblyMergeStrategy) ++ Seq(
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    },
    dependencyOverrides ++= {
      val guavaVersion =
        SbtPomKeys.effectivePom.value.getProperties.get(
          "guava.version").asInstanceOf[String]
      Seq(
        "com.google.guava" % "guava" % guavaVersion,
        "com.google.protobuf" % "protobuf-java" % protoVersion
      )
    },

    buildTestDeps := {
      (LocalProject("assembly") / Compile / Keys.`package`).value
      (LocalProject("catalyst") / Test / Keys.`package`).value
    },

    // SPARK-42538: Make sure the `${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars` is available for testing.
    // At the same time, the build of `connect`, `connect-client-jdbc`, `connect-client-jvm` and `sql` will be triggered by `assembly` build,
    // so no additional configuration is required.
    test := ((Test / test) dependsOn (buildTestDeps)).value,

    testOnly := ((Test / testOnly) dependsOn (buildTestDeps)).evaluated,

    (Test / javaOptions) += "-Darrow.memory.debug.allocator=true",

    // Exclude `pmml-model-*.jar`, `scala-collection-compat_*.jar`, `jspecify-*.jar`,
    // `error_prone_annotations-*.jar`, `listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar`,
    // `j2objc-annotations-*.jar` and `unused-1.0.0.jar` from assembly.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      cp filter { v =>
        val name = v.data.getName
        name.startsWith("pmml-model-") || name.startsWith("scala-collection-compat_") ||
          name.startsWith("jspecify-") || name.startsWith("error_prone_annotations") ||
          name.startsWith("listenablefuture") || name.startsWith("j2objc-annotations") ||
          name == "unused-1.0.0.jar"
      }
    },

    (assembly / assemblyShadeRules) := Seq(
      ShadeRule.rename("io.grpc.**" -> "org.sparkproject.connect.client.io.grpc.@1").inAll,
      ShadeRule.rename("com.google.**" -> "org.sparkproject.connect.client.com.google.@1").inAll,
      ShadeRule.rename("io.netty.**" -> "org.sparkproject.connect.client.io.netty.@1").inAll,
      ShadeRule.rename("io.perfmark.**" -> "org.sparkproject.connect.client.io.perfmark.@1").inAll,
      ShadeRule.rename("org.codehaus.**" -> "org.sparkproject.connect.client.org.codehaus.@1").inAll,
      ShadeRule.rename("android.annotation.**" -> "org.sparkproject.connect.client.android.annotation.@1").inAll
    )
  )
}

