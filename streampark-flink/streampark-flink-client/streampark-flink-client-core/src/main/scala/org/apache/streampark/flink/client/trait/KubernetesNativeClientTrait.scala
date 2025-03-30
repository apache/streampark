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

package org.apache.streampark.flink.client.`trait`

import org.apache.streampark.common.enums.{ExecutionMode, FlinkK8sRestExposedType}
import org.apache.streampark.flink.client.bean._
import org.apache.streampark.flink.kubernetes.PodTemplateTool
import org.apache.streampark.flink.packer.pipeline.DockerImageBuildResponse

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.JobID
import org.apache.flink.client.deployment.ClusterSpecification
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration._
import org.apache.flink.kubernetes.{KubernetesClusterClientFactory, KubernetesClusterDescriptor}
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.ServiceExposedType

import javax.annotation.Nonnull

import java.util.{HashMap => JavaHashMap, Map => JavaMap}

import scala.language.postfixOps

/** kubernetes native mode submit */
trait KubernetesNativeClientTrait extends FlinkClientTrait {

  override def setConfig(submitRequest: SubmitRequest, flinkConfig: Configuration): Unit = {
    // extract from submitRequest
    flinkConfig
      .safeSet(KubernetesConfigOptions.CLUSTER_ID, submitRequest.clusterId)
      .safeSet(KubernetesConfigOptions.NAMESPACE, submitRequest.kubernetesNamespace)
      .safeSet(
        KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
        covertToServiceExposedType(submitRequest.flinkRestExposedType))

    if (submitRequest.buildResult != null) {
      if (submitRequest.executionMode == ExecutionMode.KUBERNETES_NATIVE_APPLICATION) {
        val buildResult = submitRequest.buildResult.asInstanceOf[DockerImageBuildResponse]
        buildResult.podTemplatePaths.foreach(
          p => {
            if (PodTemplateTool.KUBERNETES_POD_TEMPLATE.key.equals(p._1)) {
              flinkConfig.safeSet(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE, p._2)
            } else if (PodTemplateTool.KUBERNETES_JM_POD_TEMPLATE.key.equals(p._1)) {
              flinkConfig.safeSet(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE, p._2)
            } else if (PodTemplateTool.KUBERNETES_TM_POD_TEMPLATE.key.equals(p._1)) {
              flinkConfig.safeSet(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE, p._2)
            }
          })
      }
    }

    // add flink conf configuration, mainly to set the log4j configuration
    if (!flinkConfig.contains(DeploymentOptionsInternal.CONF_DIR)) {
      flinkConfig.safeSet(
        DeploymentOptionsInternal.CONF_DIR,
        s"${submitRequest.flinkVersion.flinkHome}/conf")
    }

    if (flinkConfig.get(KubernetesConfigOptions.NAMESPACE).isEmpty) {
      flinkConfig.removeConfig(KubernetesConfigOptions.NAMESPACE)
    }

    // add pod labels, mainly to facilitate the management of k8s resources
    addPodLabels(flinkConfig, KubernetesConfigOptions.JOB_MANAGER_LABELS, submitRequest)
    addPodLabels(flinkConfig, KubernetesConfigOptions.TASK_MANAGER_LABELS, submitRequest)

    logInfo(s"""
               |------------------------------------------------------------------
               |Effective submit configuration: $flinkConfig
               |------------------------------------------------------------------
               |""".stripMargin)
  }

  // Tip: Perhaps it would be better to let users freely specify the savepoint directory
  @throws[Exception]
  override def doCancel(
      cancelRequest: CancelRequest,
      flinkConfig: Configuration): CancelResponse = {
    executeClientAction(
      cancelRequest,
      flinkConfig,
      (jobId, client) => {
        val resp = super.cancelJob(cancelRequest, jobId, client)
        CancelResponse(resp)
      }
    )
  }

  private[client] def executeClientAction[O, R <: SavepointRequestTrait](
      request: R,
      flinkConfig: Configuration,
      actFunc: (JobID, ClusterClient[_]) => O): O = {
    val hints = s"[flink-client] execute ${request.getClass.getSimpleName} for flink job failed,"
    require(
      StringUtils.isNotBlank(request.clusterId),
      s"$hints, clusterId is null, mode=${flinkConfig.get(DeploymentOptions.TARGET)}")

    flinkConfig
      .safeSet(KubernetesConfigOptions.CLUSTER_ID, request.clusterId)
      .safeSet(KubernetesConfigOptions.NAMESPACE, request.kubernetesNamespace)

    var clusterDescriptor: KubernetesClusterDescriptor = null
    var client: ClusterClient[String] = null

    try {
      clusterDescriptor = getK8sClusterDescriptor(flinkConfig)
      client = clusterDescriptor
        .retrieve(flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID))
        .getClusterClient
      actFunc(JobID.fromHexString(request.jobId), client)
    } catch {
      case e: Exception =>
        logger.error(s"$hints mode=${flinkConfig.get(DeploymentOptions.TARGET)}, request=$request")
        throw e
    } finally {
      if (client != null) client.close()
      if (clusterDescriptor != null) clusterDescriptor.close()
    }
  }

  @throws[Exception]
  override def doTriggerSavepoint(
      request: TriggerSavepointRequest,
      flinkConfig: Configuration): SavepointResponse = {
    executeClientAction(
      request,
      flinkConfig,
      (jobId, clusterClient) => {
        val actionResult = super.triggerSavepoint(request, jobId, clusterClient)
        SavepointResponse(actionResult)
      })
  }

  // noinspection DuplicatedCode
  /*
    tips:
    The default kubernetes cluster communication information will be obtained from ./kube/conf file.

    If you need to customize the kubernetes cluster context, such as multiple target kubernetes clusters
    or multiple kubernetes api-server accounts, there are two ways to achieve this:
     1. Get the KubernetesClusterDescriptor by manually, building the FlinkKubeClient and specify
         the kubernetes context contents in the FlinkKubeClient.
     2. Specify an explicit key kubernetes.config.file in flinkConfig instead of the default value.
   */
  def getK8sClusterDescriptorAndSpecification(
      flinkConfig: Configuration): (KubernetesClusterDescriptor, ClusterSpecification) = {
    val clientFactory = new KubernetesClusterClientFactory()
    val clusterDescriptor = clientFactory.createClusterDescriptor(flinkConfig)
    val clusterSpecification = clientFactory.getClusterSpecification(flinkConfig)
    (clusterDescriptor, clusterSpecification)
  }

  def getK8sClusterDescriptor(flinkConfig: Configuration): KubernetesClusterDescriptor = {
    val clientFactory = new KubernetesClusterClientFactory()
    clientFactory.createClusterDescriptor(flinkConfig)
  }

  protected def flinkConfIdentifierInfo(@Nonnull conf: Configuration): String =
    s"executionMode=${conf.get(DeploymentOptions.TARGET)}, clusterId=${conf.get(
        KubernetesConfigOptions.CLUSTER_ID)}, " +
      s"namespace=${conf.get(KubernetesConfigOptions.NAMESPACE)}"

  private def covertToServiceExposedType(exposedType: FlinkK8sRestExposedType): ServiceExposedType =
    exposedType match {
      case FlinkK8sRestExposedType.CLUSTER_IP => ServiceExposedType.ClusterIP
      case FlinkK8sRestExposedType.LOAD_BALANCER => ServiceExposedType.LoadBalancer
      case FlinkK8sRestExposedType.NODE_PORT => ServiceExposedType.NodePort
      case _ => ServiceExposedType.LoadBalancer
    }

  def getDefaultKubernetesConf(k8sConf: String): String = {
    if (k8sConf != null) {
      val homePath: String = System.getProperty("user.home")
      return k8sConf.replace("~", homePath)
    }
    null
  }

  private def addPodLabels(
      flinkConfig: Configuration,
      opt: ConfigOption[JavaMap[String, String]],
      submitRequest: SubmitRequest): Unit = {
    val labels = flinkConfig
      .getOptional[JavaMap[String, String]](opt)
      .orElse(new JavaHashMap[String, String]())
    labels.put("jobId", submitRequest.jobId)
    flinkConfig.safeSet(opt, labels)
  }

}
