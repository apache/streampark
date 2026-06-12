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

package org.apache.streampark.common.conf

/** Flink kubernetes Configuration for v1 version */
object K8sFlinkConfig {

  //  ======= deprecated =======
  @deprecated
  val jobStatusTrackTaskTimeoutSec: InternalOption = InternalOption(
    key = "streampark.flink-k8s.tracking.polling-task-timeout-sec.job-status",
    defaultValue = 120L,
    classType = classOf[java.lang.Long],
    description = "run timeout seconds of single flink-k8s metrics tracking task")

  @deprecated
  val jobStatusTrackCacheTimeoutSec: InternalOption = InternalOption(
    key = "streampark.flink-k8s.tracking.cache-timeout-sec.job-status",
    defaultValue = 300,
    classType = classOf[java.lang.Integer],
    description = "status cache timeout seconds of single flink-k8s job status tracking task")

  @deprecated
  val metricTrackTaskTimeoutSec: InternalOption = InternalOption(
    key = "streampark.flink-k8s.tracking.polling-task-timeout-sec.cluster-metric",
    defaultValue = 120L,
    classType = classOf[java.lang.Long],
    description = "run timeout seconds of single flink-k8s job status tracking task")

  val maxK8sEventBusThreadNum: InternalOption = InternalOption(
    key = "streampark.flink-k8s.tracking.max-event-bus-thread-num",
    defaultValue = 10,
    classType = classOf[java.lang.Integer],
    description = "max event bus thread num")

  val jobStatueTrackTaskIntervalSec: InternalOption = InternalOption(
    key = "streampark.flink-k8s.tracking.polling-interval-sec.job-status",
    defaultValue = 5L,
    classType = classOf[java.lang.Long],
    description = "interval seconds between two single flink-k8s metrics tracking task")

  @deprecated
  val metricTrackTaskIntervalSec: InternalOption = InternalOption(
    key = "streampark.flink-k8s.tracking.polling-interval-sec.cluster-metric",
    defaultValue = 5L,
    classType = classOf[java.lang.Long],
    description = "interval seconds between two single flink-k8s metrics tracking task")

  @deprecated
  val silentStateJobKeepTrackingSec: InternalOption = InternalOption(
    key = "streampark.flink-k8s.tracking.silent-state-keep-sec",
    defaultValue = 60,
    classType = classOf[java.lang.Integer],
    description = "retained tracking time for SILENT state flink tasks")

  /**
   * If an ingress controller is specified in the configuration, the ingress class
   * kubernetes.io/ingress.class must be specified when creating the ingress, since there are often
   * multiple ingress controllers in a production environment.
   */
  val ingressClass: InternalOption = InternalOption(
    key = "streampark.flink-k8s.ingress.class",
    defaultValue = "nginx",
    classType = classOf[java.lang.String],
    description = "Direct ingress to the ingress controller.")

  /** kubernetes default namespace */
  @deprecated
  val DEFAULT_KUBERNETES_NAMESPACE = "default"

}
