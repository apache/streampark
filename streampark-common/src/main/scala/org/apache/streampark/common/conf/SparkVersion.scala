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

package org.apache.streampark.common.conf

import org.apache.streampark.common.util.{CommandUtils, Logger, SparkEnvUtils}
import org.apache.streampark.common.util.Implicits._

import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.function.Consumer
import java.util.regex.Pattern

import scala.collection.mutable

/** @param sparkHome actual spark home that must be a readable local path */
class SparkVersion(val sparkHome: String) extends Serializable with Logger {

  private[this] lazy val SPARK_VER_PATTERN = Pattern.compile("^(\\d+\\.\\d+)(\\.)?.*$")

  private[this] lazy val SPARK_VERSION_PATTERN = Pattern.compile("\\s{2}version\\s(\\d+\\.\\d+\\.\\d+)")

  private[this] lazy val SPARK_SCALA_VERSION_PATTERN = Pattern.compile("Using\\sScala\\sversion\\s(\\d+\\.\\d+)")

  private[this] lazy val SPARK_RELEASE_VERSION_PATTERN = Pattern.compile("^Spark\\s+(\\d+\\.\\d+\\.\\d+)")

  private[this] lazy val SPARK_CORE_JAR_PATTERN =
    Pattern.compile("^spark-core_(\\d+\\.\\d+)-(\\d+\\.\\d+\\.\\d+)\\.jar$")

  val (version, scalaVersion) = {
    parseFromSparkCoreJar()
      .orElse(parseFromReleaseFile())
      .orElse(parseFromSparkSubmit())
      .getOrElse(
        throw new IllegalStateException(
          s"[StreamPark] parse spark version failed for sparkHome: $sparkHome. " +
            "Please check whether $SPARK_HOME/jars/spark-core_*.jar or RELEASE exists."))
  }

  lazy val majorVersion: String = {
    if (version == null) {
      null
    } else {
      val matcher = SPARK_VER_PATTERN.matcher(version)
      matcher.matches()
      matcher.group(1)
    }
  }

  lazy val fullVersion: String = s"${version}_$scalaVersion"

  /** Resolved JAVA_HOME for Spark CLI and SparkLauncher, based on spark-env.sh or auto-detection. */
  lazy val javaHome: Option[String] = SparkEnvUtils.resolveJavaHome(sparkHome, version)

  lazy val sparkLib: File = {
    require(sparkHome != null, "[StreamPark] sparkHome must not be null.")
    require(new File(sparkHome).exists(), "[StreamPark] sparkHome must be exists.")
    val lib = new File(s"$sparkHome/jars")
    require(
      lib.exists() && lib.isDirectory,
      s"[StreamPark] $sparkHome/jars must be exists and must be directory.")
    lib
  }

  def checkVersion(throwException: Boolean = true): Boolean = {
    version.split("\\.").map(_.trim.toInt) match {
      case Array(v, _, _) if v == 2 || v == 3 || v == 4 => true
      case _ =>
        if (throwException) {
          throw new UnsupportedOperationException(s"Unsupported spark version: $version")
        } else {
          false
        }
    }
  }

  private def parseFromSparkCoreJar(): Option[(String, String)] = {
    val jarsDir = new File(s"$sparkHome/jars")
    if (!jarsDir.exists() || !jarsDir.isDirectory) {
      None
    } else {
      jarsDir.listFiles().collectFirst {
        case file if SPARK_CORE_JAR_PATTERN.matcher(file.getName).matches() =>
          val matcher = SPARK_CORE_JAR_PATTERN.matcher(file.getName)
          matcher.matches()
          val parsed = matcher.group(2) -> matcher.group(1)
          logInfo(s"Spark version parsed from spark-core jar name: ${parsed._1}, scala: ${parsed._2}")
          parsed
      }
    }
  }

  private def parseFromReleaseFile(): Option[(String, String)] = {
    val releaseFile = new File(s"$sparkHome/RELEASE")
    if (!releaseFile.exists()) {
      None
    } else {
      val firstLine = FileUtils.readFileToString(releaseFile, StandardCharsets.UTF_8).trim.split("\n").headOption.getOrElse("")
      val matcher = SPARK_RELEASE_VERSION_PATTERN.matcher(firstLine)
      if (matcher.find()) {
        parseFromSparkCoreJar().map { case (_, scalaVer) =>
          val parsed = matcher.group(1) -> scalaVer
          logInfo(s"Spark version parsed from RELEASE file: ${parsed._1}, scala: ${parsed._2}")
          parsed
        }
      } else {
        None
      }
    }
  }

  private def hintSparkVersion(): String = {
    parseFromSparkCoreJar().map(_._1).orElse {
      val releaseFile = new File(s"$sparkHome/RELEASE")
      if (!releaseFile.exists()) {
        None
      } else {
        val firstLine =
          FileUtils.readFileToString(releaseFile, StandardCharsets.UTF_8).trim.split("\n").headOption.getOrElse("")
        val matcher = SPARK_RELEASE_VERSION_PATTERN.matcher(firstLine)
        if (matcher.find()) Some(matcher.group(1)) else None
      }
    }.getOrElse("3.0.0")
  }

  private def parseFromSparkSubmit(): Option[(String, String)] = {
    var sparkVersion: String = null
    var scalaVersion: String = null
    val javaHomeExport = SparkEnvUtils
      .resolveJavaHome(sparkHome, hintSparkVersion())
      .map(javaHome => s"export JAVA_HOME=$javaHome&&")
      .getOrElse("")
    val cmd = List(s"export SPARK_HOME=$sparkHome&&${javaHomeExport}$sparkHome/bin/spark-submit --version")
    val buffer = new mutable.StringBuilder

    CommandUtils.execute(
      sparkHome,
      cmd,
      new Consumer[String]() {
        override def accept(out: String): Unit = {
          buffer.append(out).append("\n")
          val matcher = SPARK_VERSION_PATTERN.matcher(out)
          if (matcher.find) {
            sparkVersion = matcher.group(1)
          } else {
            val matcher1 = SPARK_SCALA_VERSION_PATTERN.matcher(out)
            if (matcher1.find) {
              scalaVersion = matcher1.group(1)
            }
          }
        }
      })

    logInfo(buffer.toString())
    if (sparkVersion != null && scalaVersion != null) {
      logInfo(s"Spark version parsed from spark-submit: $sparkVersion, scala: $scalaVersion")
      buffer.clear()
      Some(sparkVersion -> scalaVersion)
    } else {
      None
    }
  }

  override def toString: String =
    s"""
       |----------------------------------------- spark version -----------------------------------
       |     sparkHome    : $sparkHome
       |     sparkVersion : $version
       |     scalaVersion : $scalaVersion
       |     javaHome       : ${javaHome.getOrElse("not resolved")}
       |-------------------------------------------------------------------------------------------
       |""".stripMargin

}
