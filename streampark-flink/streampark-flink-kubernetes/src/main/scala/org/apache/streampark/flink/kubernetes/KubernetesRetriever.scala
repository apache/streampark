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

package org.apache.streampark.flink.kubernetes

import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.common.util.Logger
import org.apache.streampark.flink.kubernetes.enums.FlinkK8sDeployMode
import org.apache.streampark.flink.kubernetes.ingress.IngressController
import org.apache.streampark.flink.kubernetes.model.ClusterKey

import org.apache.flink.client.cli.ClientOptions
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.{Configuration, DeploymentOptions, RestOptions}
import org.apache.flink.kubernetes.KubernetesClusterDescriptor
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient, KubernetesClientException}
import org.apache.hc.core5.util.Timeout

import javax.annotation.Nullable

import scala.util.{Failure, Success, Try}

object KubernetesRetriever extends Logger {

  // see org.apache.flink.client.cli.ClientOptions.CLIENT_TIMEOUT}
  val FLINK_CLIENT_TIMEOUT_SEC: Timeout =
    Timeout.ofMilliseconds(ClientOptions.CLIENT_TIMEOUT.defaultValue().toMillis)

  // see org.apache.flink.configuration.RestOptions.AWAIT_LEADER_TIMEOUT
  val FLINK_REST_AWAIT_TIMEOUT_SEC: Timeout =
    Timeout.ofMilliseconds(RestOptions.AWAIT_LEADER_TIMEOUT.defaultValue())

  private val DEPLOYMENT_LOST_TIME = collection.mutable.Map[String, Long]()

  /** get new KubernetesClient */
  @throws(classOf[KubernetesClientException])
  def newK8sClient(): KubernetesClient = {
    new DefaultKubernetesClient()
  }

  /** check connection of kubernetes cluster */
  def checkK8sConnection(): Boolean = {
    Try(newK8sClient().getVersion != null).getOrElse(false)
  }

  private val clusterClientServiceLoader =
    new DefaultClusterClientServiceLoader()

  /** get new flink cluster client of kubernetes mode */
  def newFinkClusterClient(
      clusterId: String,
      @Nullable namespace: String,
      executeMode: FlinkK8sDeployMode.Value): Option[ClusterClient[String]] = {
    // build flink config
    val flinkConfig = new Configuration()
    flinkConfig.setString(DeploymentOptions.TARGET, executeMode.toString)
    flinkConfig.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId)
    flinkConfig.set(ClientOptions.CLIENT_TIMEOUT, ClientOptions.CLIENT_TIMEOUT.defaultValue())
    flinkConfig.set(
      RestOptions.AWAIT_LEADER_TIMEOUT,
      RestOptions.AWAIT_LEADER_TIMEOUT.defaultValue())
    flinkConfig.set(RestOptions.RETRY_MAX_ATTEMPTS, RestOptions.RETRY_MAX_ATTEMPTS.defaultValue())
    if (Try(namespace.isEmpty).getOrElse(true)) {
      flinkConfig.setString(
        KubernetesConfigOptions.NAMESPACE,
        KubernetesConfigOptions.NAMESPACE.defaultValue())
    } else {
      flinkConfig.setString(KubernetesConfigOptions.NAMESPACE, namespace)
    }
    // retrieve flink cluster client
    clusterClientServiceLoader
      .getClusterClientFactory(flinkConfig)
      .createClusterDescriptor(flinkConfig)
      .asInstanceOf[KubernetesClusterDescriptor]
      .using(clusterProvider =>
        Try {
          clusterProvider
            .retrieve(flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID))
            .getClusterClient
        } match {
          case Success(v) => Some(v)
          case Failure(e) =>
            logError(s"Get flinkClient error, the error is: $e")
            None
        })
  }

  /**
   * check whether deployment exists on kubernetes cluster
   *
   * @param namespace
   *   deployment namespace
   * @param deploymentName
   *   deployment name
   */
  def isDeploymentExists(namespace: String, deploymentName: String): Boolean = {

    KubernetesRetriever
      .newK8sClient()
      .using(client => {
        client
          .apps()
          .deployments()
          .inNamespace(namespace)
          .withLabel("type", "flink-native-kubernetes")
          .list()
          .getItems
          .exists(_.getMetadata.getName == deploymentName)
      }) {
        e =>
          logWarn(
            s"""
               |[StreamPark] check deploymentExists WARN,
               |namespace: $namespace,
               |deploymentName: $deploymentName,
               |error: $e
               |""".stripMargin)
          val key = s"${namespace}_$deploymentName"
          DEPLOYMENT_LOST_TIME.get(key) match {
            case Some(time) =>
              val timeOut = 1000 * 60 * 3L
              if (System.currentTimeMillis() - time >= timeOut) {
                logError(
                  s"""
                     |[StreamPark] check deploymentExists Failed,
                     |namespace: $namespace,
                     |deploymentName: $deploymentName,
                     |detail: deployment: $deploymentName Not Found more than 3 minutes, $e
                     |""".stripMargin)
                DEPLOYMENT_LOST_TIME -= key
                return false
              }
              return true
            case _ =>
              DEPLOYMENT_LOST_TIME += key -> System.currentTimeMillis()
              true
          }
      }
  }

  /** retrieve flink jobManager rest url */
  def retrieveFlinkRestUrl(clusterKey: ClusterKey): Option[String] = {
    val client = KubernetesRetriever
      .newFinkClusterClient(clusterKey.clusterId, clusterKey.namespace, clusterKey.executeMode)
      .getOrElse(return None)
    val url =
      IngressController.getIngressUrlAddress(clusterKey.namespace, clusterKey.clusterId, client)
    logger.info(s"retrieve flink jobManager rest url: $url")
    Some(url)
  }

}
