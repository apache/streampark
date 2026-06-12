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

import org.apache.streampark.common.conf.{InternalConfigHolder, K8sFlinkConfig}

/**
 * @param jobStatusWatcherConf
 *   configuration for flink job status tracking process
 * @param metricWatcherConf
 *   configuration for flink metric tracking process
 */
case class FlinkTrackConfig(
    jobStatusWatcherConf: JobStatusWatcherConfig,
    metricWatcherConf: MetricWatcherConfig,
    threadNumberConfig: ThreadNumberConfig)

/**
 * configuration for FlinkMetricWatcher
 *
 * @param requestTimeoutSec
 *   run timeout of single tracking task
 * @param requestIntervalSec
 *   interval seconds between two single tracking task
 */
case class MetricWatcherConfig(requestTimeoutSec: Long, requestIntervalSec: Long)

/**
 * configuration for FlinkJobStatusWatcher
 *
 * @param requestTimeoutSec
 *   run timeout of single tracking task
 * @param requestIntervalSec
 *   interval seconds between two single tracking task
 * @param silentStateJobKeepTrackingSec
 *   retained tracking time for SILENT state flink tasks
 * @param jobStatusCacheTimeOutSec
 *   job status cache time out of single tracking task, must bigger than silentStateJobKeepTrackingSec
 */
case class JobStatusWatcherConfig(
    requestTimeoutSec: Long,
    requestIntervalSec: Long,
    silentStateJobKeepTrackingSec: Int,
    jobStatusCacheTimeOutSec: Int)

case class ThreadNumberConfig(maxK8sEventBusThreadNum: Int)

object FlinkTrackConfig {
  def defaultConf: FlinkTrackConfig =
    FlinkTrackConfig(JobStatusWatcherConfig.defaultConf, MetricWatcherConfig.defaultConf, ThreadNumberConfig.defaultConf)

  def debugConf: FlinkTrackConfig =
    FlinkTrackConfig(JobStatusWatcherConfig.debugConf, MetricWatcherConfig.debugConf, ThreadNumberConfig.debugConf)

  /** create from ConfigHub */
  def fromConfigHub: FlinkTrackConfig = FlinkTrackConfig(
    JobStatusWatcherConfig(
      InternalConfigHolder.get(K8sFlinkConfig.jobStatusTrackTaskTimeoutSec),
      InternalConfigHolder.get(K8sFlinkConfig.jobStatueTrackTaskIntervalSec),
      InternalConfigHolder.get(K8sFlinkConfig.silentStateJobKeepTrackingSec),
      InternalConfigHolder.get(K8sFlinkConfig.jobStatusTrackCacheTimeoutSec)),
    MetricWatcherConfig(
      InternalConfigHolder.get(K8sFlinkConfig.metricTrackTaskTimeoutSec),
      InternalConfigHolder.get(K8sFlinkConfig.metricTrackTaskIntervalSec)),
    ThreadNumberConfig(
      InternalConfigHolder.get(K8sFlinkConfig.maxK8sEventBusThreadNum)))

}

object JobStatusWatcherConfig {
  def defaultConf: JobStatusWatcherConfig = JobStatusWatcherConfig(
    requestTimeoutSec = 120,
    requestIntervalSec = 5,
    silentStateJobKeepTrackingSec = 60,
    jobStatusCacheTimeOutSec = 300)

  def debugConf: JobStatusWatcherConfig = JobStatusWatcherConfig(
    requestTimeoutSec = 120,
    requestIntervalSec = 2,
    silentStateJobKeepTrackingSec = 5,
    jobStatusCacheTimeOutSec = 30)
}

object MetricWatcherConfig {
  def defaultConf: MetricWatcherConfig =
    MetricWatcherConfig(requestTimeoutSec = 120, requestIntervalSec = 10)

  def debugConf: MetricWatcherConfig =
    MetricWatcherConfig(requestTimeoutSec = 120, requestIntervalSec = 2)
}

object ThreadNumberConfig {
  def defaultConf: ThreadNumberConfig = ThreadNumberConfig(maxK8sEventBusThreadNum = 10)

  def debugConf: ThreadNumberConfig = ThreadNumberConfig(maxK8sEventBusThreadNum = 1)
}
