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

package org.apache.streampark.flink.client.bean

import org.apache.streampark.common.conf.{FlinkVersion, Workspace}
import org.apache.streampark.common.conf.ConfigKeys._
import org.apache.streampark.common.constants.Constants
import org.apache.streampark.common.enums._
import org.apache.streampark.common.util._
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.flink.packer.pipeline.{BuildResult, ShadedBuildResponse}
import org.apache.streampark.flink.util.FlinkUtils
import org.apache.streampark.shaded.com.fasterxml.jackson.databind.ObjectMapper

import org.apache.commons.collections.MapUtils
import org.apache.commons.io.FileUtils
import org.apache.flink.runtime.jobgraph.{SavepointConfigOptions, SavepointRestoreSettings}

import javax.annotation.Nullable

import java.io.File
import java.net.URL

import scala.util.Try

case class SubmitRequest(
    flinkVersion: FlinkVersion,
    deployMode: FlinkDeployMode,
    properties: JavaMap[String, Any],
    flinkYaml: String,
    jobType: FlinkJobType,
    id: Long,
    jobId: String,
    appName: String,
    appConf: String,
    applicationType: ApplicationType,
    savePoint: String,
    restoreMode: FlinkRestoreMode,
    args: String,
    @Nullable clusterId: String,
    @Nullable hadoopUser: String,
    @Nullable buildResult: BuildResult,
    @Nullable extraParameter: JavaMap[String, Any],
    @Nullable kubernetesNamespace: String,
    @Nullable flinkRestExposedType: FlinkK8sRestExposedType) {

  private[this] lazy val appProperties: Map[String, String] = getParameterMap(
    KEY_FLINK_PROPERTY_PREFIX)

  lazy val appOption: Map[String, String] = getParameterMap(KEY_FLINK_OPTION_PREFIX)

  lazy val appMain: String = this.jobType match {
    case FlinkJobType.FLINK_SQL =>
      Constants.STREAMPARK_FLINKSQL_CLIENT_CLASS
    case FlinkJobType.PYFLINK => Constants.PYTHON_FLINK_DRIVER_CLASS_NAME
    case _ => appProperties(KEY_FLINK_APPLICATION_MAIN_CLASS)
  }

  lazy val effectiveAppName: String =
    if (this.appName == null) appProperties(KEY_FLINK_APP_NAME)
    else this.appName

  lazy val libs: List[URL] = {
    val path = s"${Workspace.local.APP_WORKSPACE}/$id/lib"
    Try(new File(path).listFiles().map(_.toURI.toURL).toList)
      .getOrElse(List.empty[URL])
  }

  lazy val classPaths: List[URL] = flinkVersion.flinkLibs ++ libs

  lazy val flinkSQL: String = extraParameter.get(KEY_FLINK_SQL()).toString

  lazy val allowNonRestoredState: Boolean = Try(
    properties
      .get(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key)
      .toString
      .toBoolean)
    .getOrElse(false)

  lazy val savepointRestoreSettings: SavepointRestoreSettings = {
    savePoint match {
      case sp if Try(sp.isEmpty).getOrElse(true) =>
        SavepointRestoreSettings.none
      case sp => SavepointRestoreSettings.forPath(sp, allowNonRestoredState)
    }
  }

  lazy val userJarFile: File = {
    deployMode match {
      case FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION => null
      case _ =>
        checkBuildResult()
        new File(buildResult.asInstanceOf[ShadedBuildResponse].shadedJarPath)
    }
  }

  lazy val safePackageProgram: Boolean = {
    // ref FLINK-21164 FLINK-9844 packageProgram.close()
    // must be flink 1.12.2 and above
    flinkVersion.version.split("\\.").map(_.trim.toInt) match {
      case Array(a, b, c) if a >= 1 => b > 12 || (b == 12 && c >= 2)
      case _ => false
    }
  }

  def hasProp(key: String): Boolean = MapUtils.isNotEmpty(properties) && properties.containsKey(key)

  def getProp(key: String): Any = properties.get(key)

  def hasExtra(key: String): Boolean = MapUtils.isNotEmpty(extraParameter) && extraParameter.containsKey(key)

  def getExtra(key: String): Any = extraParameter.get(key)

  private[this] def getParameterMap(prefix: String = ""): Map[String, String] = {
    if (this.appConf == null) {
      return Map.empty[String, String]
    }
    val format = this.appConf.substring(0, 7)
    if (format == "json://") {
      val json = this.appConf.drop(7)
      new ObjectMapper()
        .readValue[JavaMap[String, String]](json, classOf[JavaMap[String, String]])
        .toMap
        .filter(_._2 != null)
    } else {
      lazy val content = DeflaterUtils.unzipString(this.appConf.trim.drop(7))
      val map = format match {
        case "yaml://" => PropertiesUtils.fromYamlText(content)
        case "conf://" => PropertiesUtils.fromHoconText(content)
        case "prop://" => PropertiesUtils.fromPropertiesText(content)
        case "hdfs://" =>
          /**
           * If the configuration file is HDFS mode, you need to copy the HDFS related configuration
           * file to resources.
           */
          val text = HdfsUtils.read(this.appConf)
          val extension = this.appConf.split("\\.").last.toLowerCase
          extension match {
            case "yml" | "yaml" => PropertiesUtils.fromYamlText(text)
            case "conf" => PropertiesUtils.fromHoconText(text)
            case "properties" => PropertiesUtils.fromPropertiesText(text)
            case _ =>
              throw new IllegalArgumentException(
                "[StreamPark] Usage: application config format error,must be [yaml|conf|properties]")
          }
        case _ =>
          throw new IllegalArgumentException("[StreamPark] application config format error.")
      }
      map
        .filter(_._1.startsWith(prefix))
        .filter(_._2.nonEmpty)
        .map(x => x._1.drop(prefix.length) -> x._2)
    }
  }

  private[client] lazy val hdfsWorkspace = {

    /**
     * The flink version and configuration in the native flink and hdfs must be kept exactly the
     * same.
     */
    val workspace = Workspace.remote
    val flinkHome = flinkVersion.flinkHome
    val flinkHomeDir = new File(flinkHome)
    val flinkName = if (FileUtils.isSymlink(flinkHomeDir)) {
      flinkHomeDir.getCanonicalFile.getName
    } else {
      flinkHomeDir.getName
    }
    val flinkHdfsHome = s"${workspace.APP_FLINK}/$flinkName"
    HdfsWorkspace(
      flinkName,
      flinkHome,
      flinkLib = s"$flinkHdfsHome/lib",
      flinkPlugins = s"$flinkHdfsHome/plugins",
      flinkDistJar = FlinkUtils.getFlinkDistJar(flinkHome),
      appJars = workspace.APP_JARS)
  }

  @throws[Exception]
  def checkBuildResult(): Unit = {
    deployMode match {
      case FlinkDeployMode.KUBERNETES_NATIVE_SESSION =>
        AssertUtils.required(
          buildResult != null,
          s"[flink-submit] current job: ${this.effectiveAppName} was not yet built, buildResult is empty" +
            s",clusterId=$clusterId," +
            s",namespace=$kubernetesNamespace")
        AssertUtils.required(
          buildResult.pass,
          s"[flink-submit] current job ${this.effectiveAppName} build failed, clusterId" +
            s",clusterId=$clusterId," +
            s",namespace=$kubernetesNamespace")
      case _ =>
        AssertUtils.required(
          this.buildResult != null,
          s"[flink-submit] current job: ${this.effectiveAppName} was not yet built, buildResult is empty")
        AssertUtils.required(
          this.buildResult.pass,
          s"[flink-submit] current job ${this.effectiveAppName} build failed, please check")
    }
  }

}

/**
 * @param flinkName
 * @param flinkHome
 * @param flinkDistJar
 * @param flinkLib
 * @param flinkPlugins
 * @param appJars
 * @param appPlugins
 */
case class HdfsWorkspace(
    flinkName: String,
    flinkHome: String,
    flinkDistJar: String,
    flinkLib: String,
    flinkPlugins: String,
    appJars: String)
