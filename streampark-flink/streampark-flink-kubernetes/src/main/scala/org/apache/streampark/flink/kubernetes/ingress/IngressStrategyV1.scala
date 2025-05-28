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

import org.apache.streampark.common.util.Implicits._

import org.apache.flink.client.program.ClusterClient
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.DefaultKubernetesClient

import scala.util.{Failure, Success, Try}

class IngressStrategyV1 extends IngressStrategy {

  override def getIngressUrl(
      nameSpace: String,
      clusterId: String,
      clusterClient: ClusterClient[_]): String = {
    new DefaultKubernetesClient().using(client =>
      Try {
        Option(
          Try(
            client.network.v1
              .ingresses()
              .inNamespace(nameSpace)
              .withName(clusterId)
              .get())
            .getOrElse(null)) match {
          case Some(ingress) =>
            Option(ingress)
              .map(ingress => ingress.getSpec.getRules.head)
              .map(rule => rule.getHost -> rule.getHttp.getPaths.head.getPath)
              .map { case (host, path) =>
                val newPath = Option(path).filter(_.nonEmpty).map(_.replaceAll("\\/+$", "")).getOrElse("")
                s"http://$host$newPath"
              }
              .getOrElse(clusterClient.using(_.getWebInterfaceURL))
          case None => clusterClient.using(_.getWebInterfaceURL)
        }
      } match {
        case Success(value) => value
        case Failure(e) =>
          throw new RuntimeException(s"[StreamPark] get ingressUrlAddress error: $e")
      })
  }

  private[this] def touchIngressBackendRestPort(
      client: DefaultKubernetesClient,
      clusterId: String,
      nameSpace: String): Int = {
    var ports = client.services
      .inNamespace(nameSpace)
      .withName(s"$clusterId-$REST_SERVICE_IDENTIFICATION")
      .get()
      .getSpec
      .getPorts
    ports =
      ports.filter(servicePort => servicePort.getName.equalsIgnoreCase(REST_SERVICE_IDENTIFICATION))
    ports.map(servicePort => servicePort.getTargetPort.getIntVal).head
  }

  override def configureIngress(domainName: String, clusterId: String, nameSpace: String): Unit = {
    new DefaultKubernetesClient().using(client => {
      val ownerReference = getOwnerReference(nameSpace, clusterId, client)
      val ingressBackendRestServicePort =
        touchIngressBackendRestPort(client, clusterId, nameSpace)
      val ingress = new IngressBuilder()
        .withNewMetadata()
        .withName(clusterId)
        .addToAnnotations(buildIngressAnnotations(clusterId, nameSpace))
        .addToLabels(buildIngressLabels(clusterId))
        .addToOwnerReferences(ownerReference) // Add OwnerReference
        .endMetadata()
        .withNewSpec()
        .withIngressClassName(ingressClass)
        .addNewRule()
        .withHost(domainName)
        .withNewHttp()
        .addNewPath()
        .withPath(s"/$nameSpace/$clusterId/")
        .withPathType("ImplementationSpecific")
        .withNewBackend()
        .withNewService()
        .withName(s"$clusterId-$REST_SERVICE_IDENTIFICATION")
        .withNewPort()
        .withNumber(ingressBackendRestServicePort)
        .endPort()
        .endService()
        .endBackend()
        .endPath()
        .addNewPath()
        .withPath(s"/$nameSpace/$clusterId" + "(/|$)(.*)")
        .withPathType("ImplementationSpecific")
        .withNewBackend()
        .withNewService()
        .withName(s"$clusterId-$REST_SERVICE_IDENTIFICATION")
        .withNewPort()
        .withNumber(ingressBackendRestServicePort)
        .endPort()
        .endService()
        .endBackend()
        .endPath()
        .endHttp()
        .endRule()
        .endSpec()
        .build()
      client.network.v1.ingresses().inNamespace(nameSpace).create(ingress)
    })
  }
}
