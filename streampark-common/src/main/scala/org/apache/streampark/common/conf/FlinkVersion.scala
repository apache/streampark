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

import org.apache.streampark.common.util.{CommandUtils, Logger}
import org.apache.streampark.common.util.Implicits._

import java.io.File
import java.net.URL
import java.util.function.Consumer
import java.util.regex.Pattern

import scala.collection.mutable

/** @param flinkHome actual flink home that must be a readable local path */
class FlinkVersion(val flinkHome: String) extends Serializable with Logger {

  private[this] lazy val FLINK_VER_PATTERN = Pattern.compile("^(\\d+\\.\\d+)(\\.)?.*$")

  private[this] lazy val FLINK_VERSION_PATTERN = Pattern.compile("^Version: (.*), Commit ID: (.*)$")

  private[this] lazy val FLINK_SCALA_VERSION_PATTERN =
    Pattern.compile("^flink-dist_(\\d\\.\\d+).*.jar$")

  private[this] lazy val APACHE_FLINK_VERSION_PATTERN = Pattern.compile("(^\\d+\\.\\d+\\.\\d+)")

  private[this] lazy val OTHER_FLINK_VERSION_PATTERN = Pattern.compile("(\\d+\\.\\d+)(-*)")

  lazy val scalaVersion: String = {
    val matcher = FLINK_SCALA_VERSION_PATTERN.matcher(flinkDistJar.getName)
    if (matcher.matches()) {
      matcher.group(1)
    } else {
      // flink 1.15 + on support scala 2.12
      "2.12"
    }
  }

  lazy val fullVersion: String = s"${version}_$scalaVersion"

  lazy val flinkLib: File = {
    require(flinkHome != null, "[StreamPark] flinkHome must not be null.")
    require(new File(flinkHome).exists(), "[StreamPark] flinkHome must be exists.")
    val lib = new File(s"$flinkHome/lib")
    require(
      lib.exists() && lib.isDirectory,
      s"[StreamPark] $flinkHome/lib must be exists and must be directory.")
    lib
  }

  lazy val flinkLibs: List[URL] = flinkLib.listFiles().map(_.toURI.toURL).toList

  lazy val version: String = {
    val cmd = List(
      s"java -classpath ${flinkDistJar.getName} org.apache.flink.client.cli.CliFrontend --version")
    var flinkVersion: String = null
    val buffer = new mutable.StringBuilder
    CommandUtils.execute(
      flinkLib.getAbsolutePath,
      cmd,
      new Consumer[String]() {
        override def accept(out: String): Unit = {
          buffer.append(out).append("\n")
          val matcher = FLINK_VERSION_PATTERN.matcher(out)
          if (matcher.find) {
            val version = matcher.group(1)
            val matcher1 = APACHE_FLINK_VERSION_PATTERN.matcher(version)
            if (matcher1.find) {
              flinkVersion = version
            } else {
              val matcher2 = OTHER_FLINK_VERSION_PATTERN.matcher(version)
              if (matcher2.find) {
                flinkVersion = version
              }
            }
          }
        }
      })

    logInfo(buffer.toString())
    if (flinkVersion == null) {
      throw new IllegalStateException(s"[StreamPark] parse flink version failed. $buffer")
    }
    buffer.clear()
    flinkVersion
  }

  // flink major version, like "1.13", "1.14"
  lazy val majorVersion: String = {
    if (version == null) {
      null
    } else {
      val matcher = FLINK_VER_PATTERN.matcher(version)
      matcher.matches()
      matcher.group(1)
    }
  }

  lazy val flinkDistJar: File = {
    val distJar = flinkLib.listFiles().filter(_.getName.matches("flink-dist.*\\.jar"))
    distJar match {
      case x if x.isEmpty =>
        throw new IllegalArgumentException(s"[StreamPark] can no found flink-dist jar in $flinkLib")
      case x if x.length > 1 =>
        throw new IllegalArgumentException(
          s"[StreamPark] found multiple flink-dist jar in $flinkLib")
      case _ =>
    }
    distJar.head
  }

  def checkVersion(throwException: Boolean = true): Boolean = {
    version.split("\\.").map(_.trim.toInt) match {
      case Array(1, v, _) if v >= 12 && v <= 20 => true
      case _ =>
        if (throwException) {
          throw new UnsupportedOperationException(s"Unsupported flink version: $version")
        } else {
          false
        }
    }
  }

  def checkVersion(sinceVersion: Int): Boolean = {
    version.split("\\.").map(_.trim.toInt) match {
      case Array(1, v, _) if v >= sinceVersion => true
      case _ => false
    }
  }

  // StreamPark flink shims version, like "streampark-flink-shims_flink-1.13"
  private lazy val shimsVersion: String = s"streampark-flink-shims_flink-$majorVersion"

  override def toString: String =
    s"""
       |----------------------------------------- flink version -----------------------------------
       |     flinkHome    : $flinkHome
       |     distJarName  : ${flinkDistJar.getName}
       |     flinkVersion : $version
       |     majorVersion : $majorVersion
       |     scalaVersion : $scalaVersion
       |     shimsVersion : $shimsVersion
       |-------------------------------------------------------------------------------------------
       |""".stripMargin

}
