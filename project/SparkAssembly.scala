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
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.Locale

import sbt._
import sbt.Keys._
import sbtpomreader.SbtPomKeys
import sbtassembly.AssemblyPlugin.autoImport._

object Assembly {
  import sbtassembly.AssemblyPlugin.autoImport._

  val hadoopVersion = taskKey[String]("The version of hadoop that spark is compiled against.")

  lazy val settings = baseAssemblySettings ++ Seq(
    (assembly / test) := {},
    hadoopVersion := {
      sys.props.get("hadoop.version")
        .getOrElse(SbtPomKeys.effectivePom.value.getProperties.get("hadoop.version").asInstanceOf[String])
    },
    (assembly / assemblyJarName) := {
      lazy val hadoopVersionValue = hadoopVersion.value
      if (moduleName.value.contains("streaming-kafka-0-10-assembly")
        || moduleName.value.contains("streaming-kinesis-asl-assembly")) {
        s"${moduleName.value}-${version.value}.jar"
      } else {
        s"${moduleName.value}-${version.value}-hadoop${hadoopVersionValue}.jar"
      }
    },
    (Test / assembly / assemblyJarName) := s"${moduleName.value}-test-${version.value}.jar",
    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf")
                                                               => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).matches("meta-inf.*\\.sf$")
                                                               => MergeStrategy.discard
      case "log4j2.properties"                                 => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/services/")
                                                               => MergeStrategy.filterDistinctLines
      case "reference.conf"                                    => MergeStrategy.concat
      case _                                                   => MergeStrategy.first
    }
  )
}

object PySparkAssembly {
  import sbtassembly.AssemblyPlugin.autoImport._
  import java.util.zip.{ZipOutputStream, ZipEntry}

  lazy val settings = Seq(
    // Use a resource generator to copy all .py files from python/pyspark into a managed directory
    // to be included in the assembly. We can't just add "python/" to the assembly's resource dir
    // list since that will copy unneeded / unwanted files.
    (Compile / resourceGenerators) += Def.macroValueI((Compile / resourceManaged) map { outDir: File =>
      val src = new File(BuildCommons.sparkHome, "python/pyspark")
      val zipFile = new File(BuildCommons.sparkHome , "python/lib/pyspark.zip")
      zipFile.delete()
      zipRecursive(src, zipFile)
      Seq.empty[File]
    }).value
  )

  private def zipRecursive(source: File, destZipFile: File) = {
    val destOutput = new ZipOutputStream(new FileOutputStream(destZipFile))
    try {
      addFilesToZipStream("", source, destOutput)
      destOutput.flush()
    } finally {
      destOutput.close()
    }
  }

  private def addFilesToZipStream(parent: String, source: File, output: ZipOutputStream): Unit = {
    if (source.isDirectory()) {
      output.putNextEntry(new ZipEntry(parent + source.getName()))
      for (file <- source.listFiles()) {
        addFilesToZipStream(parent + source.getName() + File.separator, file, output)
      }
    } else {
      val in = new FileInputStream(source)
      output.putNextEntry(new ZipEntry(parent + source.getName()))
      try {
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            output.write(buf, 0, n)
          }
        }
        output.closeEntry()
      } finally {
        in.close()
      }
    }
  }

}

object CopyDependencies {
  import scala.sys.process.Process

  val copyDeps = TaskKey[Unit]("copyDeps", "Copies needed dependencies to the build directory.")
  val destPath = (Compile / crossTarget) { _ / "jars"}

  lazy val settings = Seq(
    copyDeps := {
      val dest = destPath.value
      if (!dest.isDirectory() && !dest.mkdirs()) {
        throw new IOException("Failed to create jars directory.")
      }

      // For the SparkConnect build, we manually call the assembly target to
      // produce the shaded Jar which happens automatically in the case of Maven.
      // Later, when the dependencies are copied, we manually copy the shaded Jar only.
      val fid = (LocalProject("connect") / assembly).value
      val fidClient = (LocalProject("connect-client-jvm") / assembly).value
      val fidProtobuf = (LocalProject("protobuf") / assembly).value
      val noProvidedSparkJars: Boolean = sys.env.getOrElse("NO_PROVIDED_SPARK_JARS", "1") == "1" ||
        sys.env.getOrElse("NO_PROVIDED_SPARK_JARS", "true")
          .toLowerCase(Locale.getDefault()) == "true"

      (Compile / dependencyClasspath).value.map(_.data)
        .filter { jar => jar.isFile() }
        // Do not copy the Spark Connect JAR as it is unshaded in the SBT build.
        .foreach { jar =>
          val destJar = new File(dest, jar.getName())
          if (destJar.isFile()) {
            destJar.delete()
          }

          if (jar.getName.contains("spark-connect-common")) {
            // Don't copy the spark connect common JAR as it is shaded in the spark connect.
          } else if (jar.getName.contains("connect-client-jdbc")) {
            // Do not place Spark Connect JDBC driver jar as it is not built-in.
          } else if (jar.getName.contains("connect-client-jvm")) {
            // Do not place Spark Connect client jars as it is not built-in.
          } else if (noProvidedSparkJars && jar.getName.contains("spark-avro")) {
            // Do not place Spark Avro jars as it is not built-in.
          } else if (jar.getName.contains("spark-connect")) {
            Files.copy(fid.toPath, destJar.toPath)
          } else if (jar.getName.contains("spark-protobuf")) {
            if (!noProvidedSparkJars) {
              Files.copy(fidProtobuf.toPath, destJar.toPath)
            }
          } else {
            Files.copy(jar.toPath(), destJar.toPath())
          }
        }

      // Here we get the full classpathes required for Spark Connect client including
      // ammonite, and exclude all spark-* jars. After that, we add the shaded Spark
      // Connect client assembly manually.
      Def.taskDyn {
        if (moduleName.value.contains("assembly")) {
          Def.task {
            val replClasspathes = (LocalProject("connect-client-jvm") / Compile / dependencyClasspath)
              .value.map(_.data).filter(_.isFile())
            val scalaBinaryVer = SbtPomKeys.effectivePom.value.getProperties.get(
              "scala.binary.version").asInstanceOf[String]
            val sparkVer = SbtPomKeys.effectivePom.value.getProperties.get(
              "spark.version").asInstanceOf[String]
            val dest = destPath.value
            val destDir = new File(dest, "connect-repl").toPath
            Files.createDirectories(destDir)

            val sourceAssemblyJar = Paths.get(
              BuildCommons.sparkHome.getAbsolutePath, "sql", "connect", "client",
              "jvm", "target", s"scala-$scalaBinaryVer", s"spark-connect-client-jvm-assembly-$sparkVer.jar")
            val destAssemblyJar = Paths.get(destDir.toString, s"spark-connect-client-jvm-assembly-$sparkVer.jar")
            Files.copy(sourceAssemblyJar, destAssemblyJar, StandardCopyOption.REPLACE_EXISTING)

            replClasspathes.foreach { f =>
              val destFile = Paths.get(destDir.toString, f.getName)
              if (!f.getName.startsWith("spark-")) {
                Files.copy(f.toPath, destFile, StandardCopyOption.REPLACE_EXISTING)
              }
            }
          }.dependsOn(LocalProject("connect-client-jvm") / assembly)
        } else {
          Def.task {}
        }
      }.value

      // Copy the Spark Connect JDBC driver assembly manually.
      Def.taskDyn {
        if (moduleName.value.contains("assembly")) {
          Def.task {
            val scalaBinaryVer = SbtPomKeys.effectivePom.value.getProperties.get(
              "scala.binary.version").asInstanceOf[String]
            val sparkVer = SbtPomKeys.effectivePom.value.getProperties.get(
              "spark.version").asInstanceOf[String]
            val dest = destPath.value
            val destDir = new File(dest, "connect-repl").toPath
            Files.createDirectories(destDir)

            val sourceAssemblyJar = Paths.get(
              BuildCommons.sparkHome.getAbsolutePath, "sql", "connect", "client",
              "jdbc", "target", s"scala-$scalaBinaryVer", s"spark-connect-client-jdbc-assembly-$sparkVer.jar")
            val destAssemblyJar = Paths.get(destDir.toString, s"spark-connect-client-jdbc-assembly-$sparkVer.jar")
            Files.copy(sourceAssemblyJar, destAssemblyJar, StandardCopyOption.REPLACE_EXISTING)
            ()
          }.dependsOn(LocalProject("connect-client-jdbc") / assembly)
        } else {
          Def.task {}
        }
      }.value
    },
    (Compile / packageBin / crossTarget) := destPath.value,
    (Compile / packageBin) := (Compile / packageBin).dependsOn(copyDeps).value
  )

}
