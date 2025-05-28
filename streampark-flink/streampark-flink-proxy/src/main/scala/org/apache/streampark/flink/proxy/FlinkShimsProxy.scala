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

package org.apache.streampark.flink.proxy

import org.apache.streampark.common.conf.{ConfigKeys, FlinkVersion}
import org.apache.streampark.common.constants.Constants
import org.apache.streampark.common.util.{ChildFirstClassLoader, ClassLoaderObjectInputStream, ClassLoaderUtils, Logger}
import org.apache.streampark.common.util.Implicits._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectOutputStream}
import java.net.URL
import java.util.function.{Function => JavaFunc}
import java.util.regex.Pattern

import scala.collection.mutable.{ListBuffer, Map => MutableMap}

object FlinkShimsProxy extends Logger {

  private[this] val SHIMS_CLASS_LOADER_CACHE = MutableMap[String, ClassLoader]()

  private[this] val VERIFY_SQL_CLASS_LOADER_CACHE = MutableMap[String, ClassLoader]()

  private[this] val FLINK_JAR_PATTERN = Pattern.compile("flink-(.*).jar", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  private[this] val INCLUDE_PATTERN: Pattern = Pattern.compile("(streampark-shaded-jackson-)(.*).jar", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  private[this] def getFlinkShimsResourcePattern(majorVersion: String) =
    Pattern.compile(s"flink-(.*)-$majorVersion(.*).jar", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  private[this] lazy val FLINK_SHIMS_PREFIX = "streampark-flink-shims_flink"

  private[this] lazy val PARENT_FIRST_PATTERNS = List(
    "java.",
    "javax.xml",
    "org.slf4j",
    "org.apache.log4j",
    "org.apache.logging",
    "org.apache.commons.logging",
    "org.apache.commons.cli",
    "ch.qos.logback",
    "org.xml",
    "org.w3c",
    "org.apache.hadoop")

  /**
   * Get shimsClassLoader to execute for scala API
   *
   * @param flinkVersion
   *   flinkVersion
   * @param func
   *   execute function
   * @tparam T
   * @return
   */
  def proxy[T](flinkVersion: FlinkVersion, func: ClassLoader => T): T = {
    val shimsClassLoader = getFlinkShimsClassLoader(flinkVersion)
    ClassLoaderUtils.runAsClassLoader[T](shimsClassLoader, () => func(shimsClassLoader))
  }

  /**
   * Get shimsClassLoader to execute for java API
   *
   * @param flinkVersion
   *   flinkVersion
   * @param func
   *   execute function
   * @tparam T
   * @return
   */
  def proxy[T](flinkVersion: FlinkVersion, func: JavaFunc[ClassLoader, T]): T = {
    val shimsClassLoader = getFlinkShimsClassLoader(flinkVersion)
    ClassLoaderUtils.runAsClassLoader[T](shimsClassLoader, () => func(shimsClassLoader))
  }

  // need to load all flink-table dependencies compatible with different versions
  private def getVerifySqlLibClassLoader(flinkVersion: FlinkVersion): ClassLoader = {
    logInfo(s"Add verify sql lib,flink version: $flinkVersion")
    VERIFY_SQL_CLASS_LOADER_CACHE.getOrElseUpdate(
      s"${flinkVersion.fullVersion}", {
        val getFlinkTable: File => Boolean = _.getName.startsWith("flink-table")
        // 1) flink/lib/flink-table*
        val libTableURL = getFlinkHomeLib(flinkVersion.flinkHome, "lib", getFlinkTable)

        // 2) After version 1.15 need add flink/opt/flink-table*
        val optTableURL = getFlinkHomeLib(flinkVersion.flinkHome, "opt", getFlinkTable)
        val shimsUrls = ListBuffer[URL](libTableURL ++ optTableURL: _*)

        // 3) add only streampark shims jar
        addShimsUrls(
          flinkVersion,
          file => {
            if (file.getName.startsWith("streampark-flink-shims")) {
              shimsUrls += file.toURI.toURL
            }
          })

        new ChildFirstClassLoader(
          shimsUrls.toArray,
          Thread.currentThread().getContextClassLoader,
          PARENT_FIRST_PATTERNS,
          jarName => loadJarFilter(jarName, flinkVersion))
      })
  }

  private def loadJarFilter(jarName: String, flinkVersion: FlinkVersion): Boolean = {
    val childFirstPattern = getFlinkShimsResourcePattern(flinkVersion.majorVersion)
    FLINK_JAR_PATTERN.matcher(jarName).matches && !childFirstPattern.matcher(jarName).matches
  }

  private def addShimsUrls(flinkVersion: FlinkVersion, addShimUrl: File => Unit): Unit = {
    val appHome = System.getProperty(ConfigKeys.KEY_APP_HOME)
    require(
      appHome != null,
      String.format("%s is not found on System env.", ConfigKeys.KEY_APP_HOME))

    val libPath = new File(s"$appHome/lib")
    require(libPath.exists())

    val majorVersion = flinkVersion.majorVersion
    val scalaVersion = flinkVersion.scalaVersion

    libPath
      .listFiles()
      .foreach((jar: File) => {
        val jarName = jar.getName
        if (jarName.endsWith(Constants.JAR_SUFFIX)) {
          if (jarName.startsWith(FLINK_SHIMS_PREFIX)) {
            val prefixVer =
              s"$FLINK_SHIMS_PREFIX-${majorVersion}_$scalaVersion"
            if (jarName.startsWith(prefixVer)) {
              addShimUrl(jar)
              logInfo(s"Include flink shims jar lib: $jarName")
            }
          } else {
            if (INCLUDE_PATTERN.matcher(jarName).matches()) {
              addShimUrl(jar)
              logInfo(s"Include jar lib: $jarName")
            } else if (jarName.matches(s"^streampark-.*_$scalaVersion.*$$")) {
              addShimUrl(jar)
              logInfo(s"Include streampark lib: $jarName")
            }
          }
        }
      })
  }

  /**
   * Get ClassLoader to verify sql
   *
   * @param flinkVersion
   *   flinkVersion
   * @param func
   *   execute function
   * @tparam T
   * @return
   */
  def proxyVerifySql[T](flinkVersion: FlinkVersion, func: JavaFunc[ClassLoader, T]): T = {
    val shimsClassLoader = getVerifySqlLibClassLoader(flinkVersion)
    ClassLoaderUtils.runAsClassLoader[T](shimsClassLoader, () => func(shimsClassLoader))
  }

  private[this] def getFlinkShimsClassLoader(flinkVersion: FlinkVersion): ClassLoader = {
    logInfo(s"add flink shims urls classloader,flink version: $flinkVersion")

    SHIMS_CLASS_LOADER_CACHE.getOrElseUpdate(
      s"${flinkVersion.fullVersion}", {
        // 1) flink/lib
        val libURL = getFlinkHomeLib(
          flinkVersion.flinkHome,
          "lib",
          file => !file.getName.startsWith("log4j") && file.getName.endsWith(".jar"))
        val shimsUrls = ListBuffer[URL](libURL: _*)

        // 2) add all shims jar
        addShimsUrls(
          flinkVersion,
          file => {
            if (file != null) {
              shimsUrls += file.toURI.toURL
            }
          })

        new ChildFirstClassLoader(
          shimsUrls.toArray,
          Thread.currentThread().getContextClassLoader,
          PARENT_FIRST_PATTERNS,
          jarName => loadJarFilter(jarName, flinkVersion))
      })
  }

  private[this] def getFlinkHomeLib(
      flinkHome: String,
      childDir: String,
      filterFun: File => Boolean): List[URL] = {
    val file = new File(flinkHome, childDir)
    require(file.isDirectory, s"FLINK_HOME $file does not exist")
    file.listFiles.filter(filterFun).map(_.toURI.toURL).toList
  }

  @throws[Exception]
  def getObject[T](loader: ClassLoader, obj: Object): T = {
    val arrayOutputStream = new ByteArrayOutputStream
    new ObjectOutputStream(arrayOutputStream)
      .using(out => {
        out.writeObject(obj)
        val byteArrayInputStream =
          new ByteArrayInputStream(arrayOutputStream.toByteArray)
        new ClassLoaderObjectInputStream(loader, byteArrayInputStream)
          .using(_.readObject())
      })
      .asInstanceOf[T]
  }

}
