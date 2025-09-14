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

import org.apache.streampark.common.conf.{ConfigConst, FlinkVersion, Workspace}
import org.apache.streampark.common.conf.ConfigConst._
import org.apache.streampark.common.enums._
import org.apache.streampark.common.util.{DeflaterUtils, HdfsUtils, PropertiesUtils}
import org.apache.streampark.flink.packer.pipeline.{BuildResult, ShadedBuildResponse}
import org.apache.streampark.flink.util.FlinkUtils
import org.apache.streampark.shaded.com.fasterxml.jackson.databind.ObjectMapper

import org.apache.commons.io.FileUtils
import org.apache.flink.runtime.jobgraph.{SavepointConfigOptions, SavepointRestoreSettings}

import javax.annotation.Nullable

import java.io.File
import java.util.{Map => JavaMap}

import scala.collection.JavaConversions._
import scala.util.Try

case class SubmitRequest(
    flinkVersion: FlinkVersion,
    executionMode: ExecutionMode,
    properties: JavaMap[String, Any],
    flinkYaml: String,
    developmentMode: DevelopmentMode,
    id: Long,
    jobId: String,
    appName: String,
    appConf: String,
    applicationType: ApplicationType,
    savePoint: String,
    args: String,
    @Nullable buildResult: BuildResult,
    @Nullable extraParameter: JavaMap[String, Any],
    @Nullable clusterId: String,
    @Nullable kubernetesNamespace: String,
    @Nullable flinkRestExposedType: FlinkK8sRestExposedType) {

  private lazy val appProperties: Map[String, String] = getParameterMap(KEY_FLINK_PROPERTY_PREFIX)

  lazy val appOption: Map[String, String] = getParameterMap(KEY_FLINK_OPTION_PREFIX)

  lazy val appMain: String = this.developmentMode match {
    case DevelopmentMode.FLINK_SQL => ConfigConst.STREAMPARK_FLINKSQL_CLIENT_CLASS
    case _ => appProperties(KEY_FLINK_APPLICATION_MAIN_CLASS)
  }

  lazy val effectiveAppName: String =
    if (this.appName == null) appProperties(KEY_FLINK_APP_NAME) else this.appName

  lazy val flinkSQL: String = extraParameter.get(KEY_FLINK_SQL()).toString

  lazy val allowNonRestoredState: Boolean = Try(
    properties.get(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key).toString.toBoolean)
    .getOrElse(false)

  lazy val savepointRestoreSettings: SavepointRestoreSettings = {
    savePoint match {
      case sp if Try(sp.isEmpty).getOrElse(true) => SavepointRestoreSettings.none
      case sp => SavepointRestoreSettings.forPath(sp, allowNonRestoredState)
    }
  }

  lazy val userJarFile: File = {
    executionMode match {
      case ExecutionMode.KUBERNETES_NATIVE_APPLICATION => null
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

  def hasProp(key: String): Boolean = properties.containsKey(key)

  def getProp(key: String): Any = properties.get(key)

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
          // 如果配置文件为hdfs方式,则需要用户将hdfs相关配置文件copy到resources下...
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

    /** 必须保持本机flink和hdfs里的flink版本和配置都完全一致. */
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
      appJars = workspace.APP_JARS
    )
  }

  @throws[Exception]
  def checkBuildResult(): Unit = {
    executionMode match {
      case ExecutionMode.KUBERNETES_NATIVE_SESSION | ExecutionMode.KUBERNETES_NATIVE_APPLICATION =>
        if (buildResult == null) {
          throw new Exception(
            s"[flink-submit] current job: ${this.effectiveAppName} was not yet built, buildResult is empty" +
              s",clusterId=$clusterId," +
              s",namespace=$kubernetesNamespace")
        }
        if (!buildResult.pass) {
          throw new Exception(
            s"[flink-submit] current job ${this.effectiveAppName} build failed, clusterId" +
              s",clusterId=$clusterId," +
              s",namespace=$kubernetesNamespace")
        }
      case _ =>
        if (this.buildResult == null) {
          throw new Exception(
            s"[flink-submit] current job: ${this.effectiveAppName} was not yet built, buildResult is empty")
        }
        if (!this.buildResult.pass) {
          throw new Exception(
            s"[flink-submit] current job ${this.effectiveAppName} build failed, please check")
        }
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
 */
case class HdfsWorkspace(
    flinkName: String,
    flinkHome: String,
    flinkDistJar: String,
    flinkLib: String,
    flinkPlugins: String,
    appJars: String)
