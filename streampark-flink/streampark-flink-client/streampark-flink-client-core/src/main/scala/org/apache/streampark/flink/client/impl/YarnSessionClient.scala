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

import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.common.util.Utils
import org.apache.streampark.flink.client.`trait`.YarnClientTrait
import org.apache.streampark.flink.client.bean._

import org.apache.commons.lang3.StringUtils
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration._
import org.apache.flink.yarn.YarnClusterDescriptor
import org.apache.flink.yarn.configuration.{YarnConfigOptions, YarnDeploymentTarget}
import org.apache.hadoop.yarn.api.records.{ApplicationId, FinalApplicationStatus}
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException
import org.apache.hadoop.yarn.util.ConverterUtils

import java.util

/** Submit Job to YARN Session Cluster */
object YarnSessionClient extends YarnClientTrait {

  /**
   * @param submitRequest
   * @param flinkConfig
   */
  override def setConfig(submitRequest: SubmitRequest, flinkConfig: Configuration): Unit = {
    super.setConfig(submitRequest, flinkConfig)
    flinkConfig
      .safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
    logInfo(s"""
               |------------------------------------------------------------------
               |Effective submit configuration: $flinkConfig
               |------------------------------------------------------------------
               |""".stripMargin)
  }

  /**
   * @param deployRequest
   * @param flinkConfig
   */
  def deployClusterConfig(deployRequest: DeployRequest, flinkConfig: Configuration): Unit = {
    val shipFiles = new util.ArrayList[String]()
    shipFiles.add(s"${deployRequest.flinkVersion.flinkHome}/lib")
    shipFiles.add(s"${deployRequest.flinkVersion.flinkHome}/plugins")

    flinkConfig
      // flinkDistJar
      .safeSet(YarnConfigOptions.FLINK_DIST_JAR, deployRequest.hdfsWorkspace.flinkDistJar)
      // flink lib
      .safeSet(YarnConfigOptions.SHIP_FILES, shipFiles)
      // yarnDeployment Target
      .safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
      // conf dir
      .safeSet(DeploymentOptionsInternal.CONF_DIR, s"${deployRequest.flinkVersion.flinkHome}/conf")

    logInfo(s"""
               |------------------------------------------------------------------
               |Effective submit configuration: $flinkConfig
               |------------------------------------------------------------------
               |""".stripMargin)
  }

  @throws[Exception]
  override def doSubmit(
      submitRequest: SubmitRequest,
      flinkConfig: Configuration): SubmitResponse = {
    val (yarnClusterId: ApplicationId, clusterDescriptor: YarnClusterDescriptor) = getYarnClusterDescriptor(flinkConfig)
    val programJobGraph =
      super.getJobGraph(flinkConfig, submitRequest, submitRequest.userJarFile)
    val packageProgram = programJobGraph._1
    val jobGraph = programJobGraph._2
    val client = clusterDescriptor.retrieve(yarnClusterId).getClusterClient
    val jobId = client.submitJob(jobGraph).get().toString
    val jobManagerUrl = client.getWebInterfaceURL

    logInfo(s"""
               |-------------------------<<applicationId>>------------------------
               |Flink Job Started: jobId: $jobId , applicationId: ${yarnClusterId.toString}
               |__________________________________________________________________
               |""".stripMargin)
    val resp = SubmitResponse(yarnClusterId.toString, flinkConfig.toMap, jobId, jobManagerUrl)
    closeSubmit(submitRequest, packageProgram, client, clusterDescriptor)
    resp
  }

  override def doCancel(
      cancelRequest: CancelRequest,
      flinkConfig: Configuration): CancelResponse = {
    flinkConfig
      .safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
    super.doCancel(cancelRequest, flinkConfig)
  }

  override def doTriggerSavepoint(
      request: TriggerSavepointRequest,
      flinkConfig: Configuration): SavepointResponse = {
    flinkConfig
      .safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
    super.doTriggerSavepoint(request, flinkConfig)
  }

  def deploy(deployRequest: DeployRequest): DeployResponse = {
    logInfo(
      s"""
         |--------------------------------------- flink yarn sesion start ---------------------------------------
         |    userFlinkHome    : ${deployRequest.flinkVersion.flinkHome}
         |    flinkVersion     : ${deployRequest.flinkVersion.version}
         |    deployMode       : ${deployRequest.deployMode.name()}
         |    clusterId        : ${deployRequest.clusterId}
         |    properties       : ${deployRequest.properties.mkString(",")}
         |-------------------------------------------------------------------------------------------------------
         |""".stripMargin)
    var clusterDescriptor: YarnClusterDescriptor = null
    var client: ClusterClient[ApplicationId] = null
    try {
      val flinkConfig =
        extractConfiguration(deployRequest.flinkVersion.flinkHome, deployRequest.properties)
      deployClusterConfig(deployRequest, flinkConfig)
      val yarnClusterDescriptor = getYarnClusterDeployDescriptor(flinkConfig)
      clusterDescriptor = yarnClusterDescriptor._2
      if (StringUtils.isNotBlank(deployRequest.clusterId)) {
        try {
          val applicationStatus =
            clusterDescriptor.getYarnClient
              .getApplicationReport(ApplicationId.fromString(deployRequest.clusterId))
              .getFinalApplicationStatus
          if (FinalApplicationStatus.UNDEFINED == applicationStatus) {
            // application is running
            val yarnClient = clusterDescriptor
              .retrieve(ApplicationId.fromString(deployRequest.clusterId))
              .getClusterClient
            if (yarnClient.getWebInterfaceURL != null) {
              return DeployResponse(yarnClient.getWebInterfaceURL, yarnClient.getClusterId.toString)
            }
          }
        } catch {
          case _: ApplicationNotFoundException =>
            logInfo("this applicationId have not managed by yarn ,need deploy ...")
        }
      }
      val clientProvider = clusterDescriptor.deploySessionCluster(yarnClusterDescriptor._1)
      client = clientProvider.getClusterClient
      if (client.getWebInterfaceURL != null) {
        DeployResponse(client.getWebInterfaceURL, client.getClusterId.toString)
      } else {
        DeployResponse(error = new RuntimeException("get the cluster getWebInterfaceURL failed."))
      }
    } catch {
      case e: Exception =>
        logError(s"start flink session fail in ${deployRequest.deployMode} mode")
        throw e
    } finally {
      Utils.close(client, clusterDescriptor)
    }
  }
  def shutdown(shutDownRequest: ShutDownRequest): ShutDownResponse = {
    var clusterDescriptor: YarnClusterDescriptor = null
    var client: ClusterClient[ApplicationId] = null
    try {
      val flinkConfig = getFlinkDefaultConfiguration(shutDownRequest.flinkVersion.flinkHome)
      shutDownRequest.properties.foreach(m =>
        m._2 match {
          case v if v != null => flinkConfig.setString(m._1, m._2.toString)
          case _ =>
        })
      flinkConfig.safeSet(YarnConfigOptions.APPLICATION_ID, shutDownRequest.clusterId)
      flinkConfig.safeSet(DeploymentOptions.TARGET, YarnDeploymentTarget.SESSION.getName)
      val yarnClusterDescriptor = getYarnClusterDescriptor(flinkConfig)
      clusterDescriptor = yarnClusterDescriptor._2
      val shutDownState = FinalApplicationStatus.UNDEFINED.equals(
        clusterDescriptor.getYarnClient
          .getApplicationReport(ApplicationId.fromString(shutDownRequest.clusterId))
          .getFinalApplicationStatus)
      if (shutDownState) {
        val clientProvider =
          clusterDescriptor.retrieve(yarnClusterDescriptor._1)
        client = clientProvider.getClusterClient
        client.shutDownCluster()
      }
      logInfo(s"the ${shutDownRequest.clusterId}'s final status is ${clusterDescriptor.getYarnClient
          .getApplicationReport(ConverterUtils.toApplicationId(shutDownRequest.clusterId))
          .getFinalApplicationStatus}")
      ShutDownResponse(shutDownRequest.clusterId)
    } catch {
      case e: Exception =>
        logError(s"shutdown flink session fail in ${shutDownRequest.deployMode} mode")
        throw e
    } finally {
      Utils.close(client, clusterDescriptor)
    }
  }
}
