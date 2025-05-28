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

package org.apache.streampark.flink.kubernetes.helper

import org.apache.streampark.common.util.{Logger, SystemPropertyUtils}
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.flink.kubernetes.KubernetesRetriever

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Pod
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.DefaultKubernetesClient

import java.io.File

import scala.util.{Success, Try}

object KubernetesDeploymentHelper extends Logger {

  private[this] def getPods(nameSpace: String, deploymentName: String): List[Pod] = {
    KubernetesRetriever
      .newK8sClient()
      .using(client => {
        Try {
          client.pods
            .inNamespace(nameSpace)
            .withLabels {
              client.apps.deployments
                .inNamespace(nameSpace)
                .withName(deploymentName)
                .get
                .getSpec
                .getSelector
                .getMatchLabels
            }
            .list
            .getItems
            .toList
        }.getOrElse(List.empty[Pod])
      })
  }

  def isDeploymentError(nameSpace: String, deploymentName: String): Boolean = {
    Try {
      val pods = getPods(nameSpace, deploymentName)
      val podStatus = pods.head.getStatus
      podStatus.getPhase match {
        case "Unknown" => true
        case "Failed" => true
        case "Pending" => false
        case _ =>
          podStatus.getContainerStatuses.head.getLastState.getTerminated != null
      }
    }.getOrElse(true)
  }

  private[this] def deleteDeployment(nameSpace: String, deploymentName: String): Unit = {
    KubernetesRetriever
      .newK8sClient()
      .using(client => {
        val map = client.apps.deployments.inNamespace(nameSpace)
        map.withLabel("app", deploymentName).delete
        map.withName(deploymentName).delete()
      })
  }

  private[this] def deleteConfigMap(nameSpace: String, deploymentName: String): Unit = {
    KubernetesRetriever
      .newK8sClient()
      .using(client => {
        val map = client.configMaps().inNamespace(nameSpace)
        map.withLabel("app", deploymentName).delete
        map.withName(deploymentName).delete()
      })
  }

  def delete(nameSpace: String, deploymentName: String): Unit = {
    deleteDeployment(nameSpace, deploymentName)
    deleteConfigMap(nameSpace, deploymentName)
  }

  def checkConnection(): Boolean = {
    Try(new DefaultKubernetesClient) match {
      case Success(client) =>
        client.close()
        true
      case _ => false
    }
  }

  def watchDeploymentLog(nameSpace: String, jobName: String, jobId: String): String = {
    KubernetesRetriever
      .newK8sClient()
      .using(client => {
        val path = KubernetesDeploymentHelper.getJobLog(jobId)
        val file = new File(path)
        val log = client.apps.deployments
          .inNamespace(nameSpace)
          .withName(jobName)
          .getLog
        Files.asCharSink(file, Charsets.UTF_8).write(log)
        path
      })
  }

  def watchPodTerminatedLog(nameSpace: String, jobName: String, jobId: String): String = {
    KubernetesRetriever
      .newK8sClient()
      .using(client =>
        Try {
          val podName = getPods(nameSpace, jobName).head.getMetadata.getName
          val path = KubernetesDeploymentHelper.getJobErrorLog(jobId)
          val file = new File(path)
          val log = client.pods
            .inNamespace(nameSpace)
            .withName(podName)
            .terminated()
            .withPrettyOutput
            .getLog
          Files.asCharSink(file, Charsets.UTF_8).write(log)
          path
        }.getOrElse(null))(error => throw error)
  }

  private[kubernetes] def getJobLog(jobId: String): String = {
    val tmpPath = SystemPropertyUtils.getTmpdir()
    s"$tmpPath/$jobId.log"
  }

  private[kubernetes] def getJobErrorLog(jobId: String): String = {
    val tmpPath = SystemPropertyUtils.getTmpdir()
    s"$tmpPath/${jobId}_err.log"
  }

}
