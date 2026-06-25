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

package org.apache.streampark.flink.kubernetes.ingress

import org.apache.streampark.common.util.Implicits.AutoCloseImplicits

import org.apache.flink.client.deployment.ClusterRetrieveException
import org.apache.flink.client.program.ClusterClientProvider
import org.apache.flink.client.program.rest.RestClusterClient
import org.apache.flink.configuration.{Configuration, JobManagerOptions, RestOptions}
import org.apache.flink.kubernetes.KubernetesClusterDescriptor
import org.apache.flink.kubernetes.artifact.{DefaultKubernetesArtifactUploader, KubernetesArtifactUploader}
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices
import org.apache.flink.runtime.rpc.{AddressResolution, FatalErrorHandler}
import org.apache.flink.util.Preconditions.checkNotNull
import org.slf4j.LoggerFactory

import java.net.URL

class IngressClusterDescriptor(flinkConfig: Configuration, clientFactory: FlinkKubeClientFactory, artifactUploader: KubernetesArtifactUploader)
  extends KubernetesClusterDescriptor(flinkConfig, clientFactory, artifactUploader) {

  val LOG = LoggerFactory.getLogger(classOf[KubernetesClusterDescriptor])

  val clusterId = checkNotNull(flinkConfig.get(KubernetesConfigOptions.CLUSTER_ID), "ClusterId must be specified!")
  val namespace = flinkConfig.get(KubernetesConfigOptions.NAMESPACE)

  private def createClusterClientProvider(clusterId: String): ClusterClientProvider[String] = {
    () =>
      val configuration = new Configuration(flinkConfig)
      val client = FlinkKubeClientFactory.getInstance.fromConfiguration(configuration, "client")
      client.using(client => {
        val restEndpoint = client.getRestEndpoint(clusterId)
        if (restEndpoint.isPresent) {
          configuration.set(RestOptions.ADDRESS, restEndpoint.get.getAddress)
          configuration.set[Integer](RestOptions.PORT, restEndpoint.get.getPort)
        } else {
          throw new RuntimeException(new ClusterRetrieveException(s"Could not get the rest endpoint of $clusterId"))
        }
      })
      var webMonitorAddress = getWebMonitorAddress(configuration)
      val ingressURL = IngressController.getIngressUrlAddress(namespace, clusterId, flinkConfig)
      if (ingressURL.isDefined) {
        val webInterfaceURL = new URL(ingressURL.get)
        webMonitorAddress = webInterfaceURL.getProtocol + "://" + webInterfaceURL.getHost + ":80" + webInterfaceURL.getPath
        configuration.set(JobManagerOptions.ADDRESS, webInterfaceURL.getHost)
        configuration.set[Integer](JobManagerOptions.PORT, 80)
        configuration.set(RestOptions.PATH, webInterfaceURL.getPath)
        configuration.set(RestOptions.PATH, webInterfaceURL.getPath)
      }
      new RestClusterClient[String](
        configuration,
        clusterId,
        (effectiveConfiguration: Configuration, fatalErrorHandler: FatalErrorHandler) =>
          new StandaloneClientHAServices(webMonitorAddress))
  }

  @throws[Exception]
  private def getWebMonitorAddress(configuration: Configuration) = {
    var resolution = AddressResolution.TRY_ADDRESS_RESOLUTION
    val serviceType = configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE)
    if (serviceType.isClusterIP) {
      resolution = AddressResolution.NO_ADDRESS_RESOLUTION
      LOG.warn(
        s"Please note that Flink client operations(e.g. cancel, list, stop,"
          + " savepoint, etc.) won't work from outside the Kubernetes cluster"
          + s" since '${KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE.key}' has been set to $serviceType.")
    }
    HighAvailabilityServicesUtils.getWebMonitorAddress(configuration, resolution)
  }

  override def retrieve(clusterId: String): ClusterClientProvider[String] = {
    val clusterClientProvider = createClusterClientProvider(clusterId)
    clusterClientProvider.getClusterClient.using(clusterClient =>
      LOG.info(s"Retrieve flink cluster $clusterId successfully, JobManager Web Interface: ${clusterClient.getWebInterfaceURL}"))
    clusterClientProvider
  }
}

object IngressClusterDescriptor {
  def createClusterDescriptor(configuration: Configuration): IngressClusterDescriptor = {
    checkNotNull(configuration.get(KubernetesConfigOptions.CLUSTER_ID), "ClusterId must be specified!")
    new IngressClusterDescriptor(
      configuration,
      FlinkKubeClientFactory.getInstance(),
      new DefaultKubernetesArtifactUploader())
  }
}
