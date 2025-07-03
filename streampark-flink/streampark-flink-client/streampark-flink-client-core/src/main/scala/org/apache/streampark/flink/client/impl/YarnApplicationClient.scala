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

package org.apache.streampark.flink.client.impl

import org.apache.streampark.common.conf.Workspace
import org.apache.streampark.common.constants.Constants
import org.apache.streampark.common.enums.FlinkJobType
import org.apache.streampark.common.fs.FsOperator
import org.apache.streampark.common.util.{AssertUtils, FileUtils, HdfsUtils}
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.flink.client.`trait`.YarnClientTrait
import org.apache.streampark.flink.client.bean._
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse

import org.apache.flink.client.deployment.application.ApplicationConfiguration
import org.apache.flink.configuration._
import org.apache.flink.python.PythonOptions
import org.apache.flink.yarn.YarnClusterDescriptor
import org.apache.flink.yarn.configuration.YarnConfigOptions
import org.apache.hadoop.yarn.api.records.ApplicationId

import java.util
import java.util.Collections

import scala.collection.mutable.ListBuffer

/** yarn application mode submit */
object YarnApplicationClient extends YarnClientTrait {

  private[this] lazy val workspace = Workspace.remote

  override def setConfig(submitRequest: SubmitRequest, flinkConfig: Configuration): Unit = {
    super.setConfig(submitRequest, flinkConfig)
    val providedLibs = {
      val array = ListBuffer(
        submitRequest.hdfsWorkspace.flinkLib,
        submitRequest.hdfsWorkspace.flinkPlugins,
        submitRequest.hdfsWorkspace.appJars)
      submitRequest.jobType match {
        case FlinkJobType.FLINK_SQL =>
          array += s"${workspace.APP_SHIMS}/flink-${submitRequest.flinkVersion.majorVersion}"
          val jobLib = s"${workspace.APP_WORKSPACE}/${submitRequest.id}/lib"
          if (HdfsUtils.exists(jobLib)) {
            array += jobLib
          }
        case _ =>
      }
      array.toList
    }

    flinkConfig
      // yarn.provided.lib.dirs
      .safeSet(YarnConfigOptions.PROVIDED_LIB_DIRS, providedLibs.asJava)
      // flinkDistJar
      .safeSet(YarnConfigOptions.FLINK_DIST_JAR, submitRequest.hdfsWorkspace.flinkDistJar)
      // pipeline.jars
      .safeSet(
        PipelineOptions.JARS,
        Collections.singletonList(
          submitRequest.buildResult
            .asInstanceOf[ShadedBuildResponse]
            .shadedJarPath))
      // yarn application name
      .safeSet(YarnConfigOptions.APPLICATION_NAME, submitRequest.effectiveAppName)
      // yarn application Type
      .safeSet(YarnConfigOptions.APPLICATION_TYPE, submitRequest.applicationType.getName)

    if (submitRequest.jobType == FlinkJobType.PYFLINK) {
      val pyVenv: String = workspace.APP_PYTHON_VENV
      AssertUtils.required(FsOperator.hdfs.exists(pyVenv), s"$pyVenv File does not exist")

      val localLib: String =
        s"${Workspace.local.APP_WORKSPACE}/${submitRequest.id}/lib"
      if (FileUtils.exists(localLib) && FileUtils.directoryNotBlank(localLib)) {
        flinkConfig.safeSet(PipelineOptions.JARS, util.Arrays.asList(localLib))
      }

      // yarn.ship-files
      val shipFiles = new util.ArrayList[String]()
      shipFiles.add(submitRequest.userJarFile.getParentFile.getAbsolutePath)

      flinkConfig
        .safeSet(YarnConfigOptions.SHIP_FILES, shipFiles)
        // python.files
        .safeSet(PythonOptions.PYTHON_FILES, submitRequest.userJarFile.getParentFile.getName)
        // python.archives
        .safeSet(PythonOptions.PYTHON_ARCHIVES, pyVenv)
        // python.client.executable
        .safeSet(PythonOptions.PYTHON_CLIENT_EXECUTABLE, Constants.PYTHON_EXECUTABLE)
        // python.executable
        .safeSet(PythonOptions.PYTHON_EXECUTABLE, Constants.PYTHON_EXECUTABLE)

      val args: util.List[String] =
        flinkConfig.get(ApplicationConfiguration.APPLICATION_ARGS)
      // Caused by: java.lang.UnsupportedOperationException
      val argsList: util.ArrayList[String] = new util.ArrayList[String](args)
      argsList.add("-pym")
      argsList.add(submitRequest.userJarFile.getName.dropRight(Constants.PYTHON_SUFFIX.length))
      flinkConfig.safeSet(ApplicationConfiguration.APPLICATION_ARGS, argsList)
    }

    logInfo(s"""
               |------------------------------------------------------------------
               |Effective submit configuration: $flinkConfig
               |------------------------------------------------------------------
               |""".stripMargin)
  }

  override def doSubmit(
      submitRequest: SubmitRequest,
      flinkConfig: Configuration): SubmitResponse = {
    val (clusterSpecification, clusterDescriptor: YarnClusterDescriptor) = getYarnClusterDeployDescriptor(flinkConfig, submitRequest.hadoopUser)
    logInfo(s"""
               |------------------------<<specification>>-------------------------
               |$clusterSpecification
               |------------------------------------------------------------------
               |""".stripMargin)

    val applicationConfiguration =
      ApplicationConfiguration.fromConfiguration(flinkConfig)
    var applicationId: ApplicationId = null
    var jobManagerUrl: String = null
    val clusterClient = clusterDescriptor
      .deployApplicationCluster(clusterSpecification, applicationConfiguration)
      .getClusterClient
    applicationId = clusterClient.getClusterId
    jobManagerUrl = clusterClient.getWebInterfaceURL
    logInfo(s"""
               |-------------------------<<applicationId>>------------------------
               |Flink Job Started: applicationId: $applicationId
               |__________________________________________________________________
               |""".stripMargin)

    val resp =
      SubmitResponse(applicationId.toString, flinkConfig.toMap, jobManagerUrl = jobManagerUrl)
    closeSubmit(submitRequest, clusterClient, clusterDescriptor)
    resp
  }

}
