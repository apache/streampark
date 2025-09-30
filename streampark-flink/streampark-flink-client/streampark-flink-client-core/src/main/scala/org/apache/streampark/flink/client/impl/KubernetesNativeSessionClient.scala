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

import org.apache.streampark.common.conf.ConfigKeys
import org.apache.streampark.common.enums.FlinkDeployMode
import org.apache.streampark.common.util.{Logger, Utils}
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.flink.client.`trait`.KubernetesNativeClientTrait
import org.apache.streampark.flink.client.bean._
import org.apache.streampark.flink.client.tool.FlinkSessionSubmitHelper
import org.apache.streampark.flink.core.FlinkKubernetesClient
import org.apache.streampark.flink.kubernetes.KubernetesRetriever
import org.apache.streampark.flink.kubernetes.enums.FlinkK8sDeployMode
import org.apache.streampark.flink.kubernetes.ingress.IngressController
import org.apache.streampark.flink.kubernetes.model.ClusterKey

import org.apache.commons.lang3.StringUtils
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration._
import org.apache.flink.kubernetes.KubernetesClusterDescriptor
import org.apache.flink.kubernetes.configuration.{KubernetesConfigOptions, KubernetesDeploymentTarget}
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.ServiceExposedType
import org.apache.flink.kubernetes.kubeclient.{FlinkKubeClient, FlinkKubeClientFactory}

import java.io.File

/** Kubernetes native session mode submit. */
object KubernetesNativeSessionClient extends KubernetesNativeClientTrait with Logger {

  @throws[Exception]
  override def doSubmit(
      submitRequest: SubmitRequest,
      flinkConfig: Configuration): SubmitResponse = {
    // require parameters
    require(
      StringUtils.isNotBlank(submitRequest.clusterId),
      s"[flink-submit] submit flink job failed, clusterId is null, mode=${flinkConfig
          .get(DeploymentOptions.TARGET)}")
    super.trySubmit(submitRequest, flinkConfig, submitRequest.userJarFile)(
      jobGraphSubmit,
      restApiSubmit)
  }

  /** Submit flink session job via rest api. */
  @throws[Exception]
  def restApiSubmit(
      submitRequest: SubmitRequest,
      flinkConfig: Configuration,
      fatJar: File): SubmitResponse = {

    // get jm rest url of flink session cluster
    val clusterKey = ClusterKey(
      FlinkK8sDeployMode.SESSION,
      submitRequest.kubernetesNamespace,
      submitRequest.clusterId)
    val jmRestUrl = KubernetesRetriever
      .retrieveFlinkRestUrl(clusterKey)
      .getOrElse(
        throw new Exception(
          s"[flink-submit] retrieve flink session rest url failed, clusterKey=$clusterKey"))
    // submit job via rest api
    val jobId =
      FlinkSessionSubmitHelper.submitViaRestApi(jmRestUrl, fatJar, flinkConfig)
    SubmitResponse(clusterKey.clusterId, flinkConfig.toMap, jobId, jmRestUrl)
  }

  /** Submit flink session job with building JobGraph via ClusterClient api. */
  @throws[Exception]
  def jobGraphSubmit(
      submitRequest: SubmitRequest,
      flinkConfig: Configuration,
      jarFile: File): SubmitResponse = {
    val clusterDescriptor = getK8sClusterDescriptor(flinkConfig)
    // build JobGraph
    val packageProgramJobGraph =
      super.getJobGraph(flinkConfig, submitRequest, jarFile)
    val packageProgram = packageProgramJobGraph._1
    val jobGraph = packageProgramJobGraph._2
    // retrieve client and submit JobGraph
    val client = clusterDescriptor
      .retrieve(flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID))
      .getClusterClient
    val submitResult = client.submitJob(jobGraph)
    val jobId = submitResult.get().toString
    val result =
      SubmitResponse(client.getClusterId, flinkConfig.toMap, jobId, client.getWebInterfaceURL)
    logInfo(
      s"[flink-submit] flink job has been submitted. ${flinkConfIdentifierInfo(flinkConfig)}, jobId: $jobId")
    closeSubmit(submitRequest, packageProgram, client, client)
    result
  }

  override def doCancel(
      cancelRequest: CancelRequest,
      flinkConfig: Configuration): CancelResponse = {
    flinkConfig.safeSet(
      DeploymentOptions.TARGET,
      FlinkDeployMode.KUBERNETES_NATIVE_SESSION.getName)
    super.doCancel(cancelRequest, flinkConfig)
  }

  @throws[Exception]
  def deploy(deployRequest: DeployRequest): DeployResponse = {
    logInfo(
      s"""
         |--------------------------------------- kubernetes cluster start ---------------------------------------
         |    userFlinkHome    : ${deployRequest.flinkVersion.flinkHome}
         |    flinkVersion     : ${deployRequest.flinkVersion.version}
         |    deployMode       : ${deployRequest.deployMode.name()}
         |    clusterId        : ${deployRequest.clusterId}
         |    namespace        : ${deployRequest.k8sParam.kubernetesNamespace}
         |    exposedType      : ${deployRequest.k8sParam.flinkRestExposedType}
         |    serviceAccount   : ${deployRequest.k8sParam.serviceAccount}
         |    flinkImage       : ${deployRequest.k8sParam.flinkImage}
         |    properties       : ${deployRequest.properties.mkString(",")}
         |--------------------------------------------------------------------------------------------------------
         |""".stripMargin)

    val flinkConfig = getFlinkK8sConfig(deployRequest)
    val kubeClient = FlinkKubeClientFactory.getInstance.fromConfiguration(flinkConfig, "client")

    var clusterDescriptor: KubernetesClusterDescriptor = null
    var client: ClusterClient[String] = null

    try {
      val kubernetesClusterDescriptor = getK8sClusterDescriptorAndSpecification(flinkConfig)
      clusterDescriptor = kubernetesClusterDescriptor._1

      val kubeClientWrapper = new FlinkKubernetesClient(kubeClient)
      val kubeService = kubeClientWrapper.getService(deployRequest.clusterId)

      if (kubeService.isPresent) {
        client = clusterDescriptor.retrieve(deployRequest.clusterId).getClusterClient
      } else {
        client = clusterDescriptor
          .deploySessionCluster(kubernetesClusterDescriptor._2)
          .getClusterClient
      }
      var webInterfaceURL = client.getWebInterfaceURL
      val ingressDomain = flinkConfig.getString(ConfigKeys.STREAMPARK_INGRESS_MODE, "")
      if (StringUtils.isNotBlank(ingressDomain)) {
        webInterfaceURL = IngressController.configureIngress(
          ingressDomain,
          deployRequest.clusterId,
          deployRequest.k8sParam.kubernetesNamespace,
          flinkConfig)
        logInfo(s"create ingress with url: $webInterfaceURL with cluster: ${client.getClusterId}")
      }
      DeployResponse(address = webInterfaceURL, clusterId = client.getClusterId)
    } catch {
      case e: Exception => DeployResponse(error = e)
    } finally {
      Utils.close(client, clusterDescriptor, kubeClient)
    }
  }

  def shutdown(shutDownRequest: ShutDownRequest): ShutDownResponse = {
    var kubeClient: FlinkKubeClient = null
    try {
      val flinkConfig = getFlinkK8sConfig(shutDownRequest)
      kubeClient = FlinkKubeClientFactory.getInstance.fromConfiguration(flinkConfig, "client")
      val kubeClientWrapper = new FlinkKubernetesClient(kubeClient)
      val stopAndCleanupState =
        shutDownRequest.clusterId != null && kubeClientWrapper
          .getService(shutDownRequest.clusterId)
          .isPresent
      if (stopAndCleanupState) {
        kubeClient.stopAndCleanupCluster(shutDownRequest.clusterId)
        IngressController.deleteIngress(shutDownRequest.clusterId, shutDownRequest.k8sParam.kubernetesNamespace, flinkConfig)
        ShutDownResponse(shutDownRequest.clusterId)
      } else {
        null
      }
    } catch {
      case e: Exception =>
        logError(s"shutdown flink session fail in ${shutDownRequest.deployMode} mode")
        e.printStackTrace()
        throw e
    } finally {
      Utils.close(kubeClient)
    }
  }

  override def doTriggerSavepoint(
      triggerSavepointRequest: TriggerSavepointRequest,
      flinkConfig: Configuration): SavepointResponse = {
    flinkConfig.safeSet(
      DeploymentOptions.TARGET,
      FlinkDeployMode.KUBERNETES_NATIVE_SESSION.getName)
    super.doTriggerSavepoint(triggerSavepointRequest, flinkConfig)
  }

  private[this] def getFlinkK8sConfig[R <: DeployRequestTrait](deployRequest: R): Configuration = {
    extractConfiguration(deployRequest.flinkVersion.flinkHome, deployRequest.properties)
      .safeSet(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName)
      .safeSet(KubernetesConfigOptions.NAMESPACE, deployRequest.k8sParam.kubernetesNamespace)
      .safeSet(
        KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT,
        deployRequest.k8sParam.serviceAccount)
      .safeSet(KubernetesConfigOptions.CLUSTER_ID, deployRequest.clusterId)
      .safeSet(KubernetesConfigOptions.CONTAINER_IMAGE, deployRequest.k8sParam.flinkImage)
      .safeSet(
        KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
        ServiceExposedType.valueOf(deployRequest.k8sParam.flinkRestExposedType.getName))
      .safeSet(
        KubernetesConfigOptions.KUBE_CONFIG_FILE,
        getDefaultKubernetesConf(deployRequest.k8sParam.kubeConf))
      .safeSet(DeploymentOptionsInternal.CONF_DIR, s"${deployRequest.flinkVersion.flinkHome}/conf")
  }

}
