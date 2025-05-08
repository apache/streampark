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

import org.apache.streampark.flink.kubernetes.enums.FlinkJobState
import org.apache.streampark.flink.kubernetes.enums.FlinkK8sDeployMode.{APPLICATION, SESSION}
import org.apache.streampark.flink.kubernetes.event.{BuildInEvent, FlinkJobStateEvent, FlinkJobStatusChangeEvent}
import org.apache.streampark.flink.kubernetes.model._
import org.apache.streampark.flink.kubernetes.watcher._

import com.google.common.eventbus.{AllowConcurrentEvents, Subscribe}

import javax.annotation.Nullable

/** Default K8sFlinkTrackMonitor implementation. */
class DefaultFlinkK8sWatcher(conf: FlinkTrackConfig = FlinkTrackConfig.defaultConf)
  extends FlinkK8sWatcher {

  // cache pool for storage tracking result
  implicit val watchController: FlinkK8sWatchController =
    new FlinkK8sWatchController()

  // eventBus for change event
  implicit lazy val eventBus: ChangeEventBus = {
    val eventBus = new ChangeEventBus(conf.threadNumberConfig)
    eventBus.registerListener(new BuildInEventListener)
    eventBus
  }

  // remote server tracking watcher
  private val k8sEventWatcher = new FlinkK8sEventWatcher()
  private val jobStatusWatcher = new FlinkJobStatusWatcher(conf.jobStatusWatcherConf)
  private val metricsWatcher = new FlinkMetricWatcher(conf.metricWatcherConf)
  private val checkpointWatcher = new FlinkCheckpointWatcher(conf.metricWatcherConf)

  private[this] val allWatchers =
    Array[FlinkWatcher](k8sEventWatcher, jobStatusWatcher, metricsWatcher, checkpointWatcher)

  override def registerListener(listener: AnyRef): Unit =
    eventBus.registerListener(listener)

  override def start(): Unit = allWatchers.foreach(_.start())

  override def stop(): Unit = allWatchers.foreach(_.stop())

  override def restart(): Unit = allWatchers.foreach(_.restart())

  override def close(): Unit = {
    allWatchers.foreach(_.close())
    watchController.close()
  }

  def doWatching(trackId: TrackId): Unit = {
    if (trackId.isLegal) {
      watchController.trackIds.set(trackId)
    }
  }

  def unWatching(trackId: TrackId): Unit = {
    watchController.canceling.set(trackId)
  }

  override def isInWatching(trackId: TrackId): Boolean =
    watchController.isInWatching(trackId)

  override def getJobStatus(trackId: TrackId): Option[JobStatusCV] = Option(
    watchController.jobStatuses.get(trackId))

  override def getJobStatus(trackIds: Set[TrackId]): Map[CacheKey, JobStatusCV] =
    watchController.jobStatuses.getAsMap(trackIds)

  override def getAllJobStatus: Map[CacheKey, JobStatusCV] =
    watchController.jobStatuses.asMap()

  override def getAccGroupMetrics(groupId: String): FlinkMetricCV =
    watchController.collectAccGroupMetric(groupId)

  override def getClusterMetrics(clusterKey: ClusterKey): Option[FlinkMetricCV] = Option(
    watchController.flinkMetrics.get(clusterKey))

  override def getAllWatchingIds: Set[TrackId] =
    watchController.getAllWatchingIds()

  override def checkIsInRemoteCluster(trackId: TrackId): Boolean = {
    if (!trackId.isLegal) false;
    else {
      val nonLost = (state: FlinkJobState.Value) =>
        state != FlinkJobState.LOST || state != FlinkJobState.SILENT
      trackId.executeMode match {
        case SESSION =>
          jobStatusWatcher
            .touchSessionJob(trackId)
            .exists(e => nonLost(e.jobState))
        case APPLICATION =>
          jobStatusWatcher
            .touchApplicationJob(trackId)
            .exists(e => nonLost(e.jobState))
        case _ => false
      }
    }
  }

  override def postEvent(event: BuildInEvent, sync: Boolean): Unit = {
    if (sync) {
      eventBus.postSync(event)
    } else {
      eventBus.postAsync(event)
    }
  }

  @Nullable override def getRemoteRestUrl(trackId: TrackId): String =
    watchController.endpoints.get(trackId.toClusterKey)

  /** Build-in Event Listener of K8sFlinkTrackMonitor. */
  private class BuildInEventListener {

    /**
     * Watch the FlinkJobOperaEvent, then update relevant cache record and trigger a new
     * FlinkJobStatusChangeEvent.
     */
    // noinspection UnstableApiUsage
    @Subscribe
    @AllowConcurrentEvents
    def subscribeFlinkJobStateEvent(event: FlinkJobStateEvent): Unit = {
      if (event.trackId.isLegal) {
        val latest = watchController.jobStatuses.get(event.trackId)
        // determine if the current event should be ignored
        val shouldIgnore: Boolean = (latest, event) match {
          case (preCache, _) if preCache == null => false
          // discard current event when the job state is consistent
          case (preCache, event) if preCache.jobState == event.jobState => true
          // discard current event when current event is too late
          case (preCache, event) if event.pollTime <= preCache.pollAckTime =>
            true
          case _ => false
        }
        if (!shouldIgnore) {
          // update relevant cache
          val newCache = {
            if (latest != null) {
              latest.copy(jobState = event.jobState)
            } else {
              JobStatusCV(
                jobState = event.jobState,
                jobId = event.trackId.jobId,
                pollEmitTime = event.pollTime,
                pollAckTime = System.currentTimeMillis)
            }
          }
          watchController.jobStatuses.put(event.trackId, newCache)
          // post new FlinkJobStatusChangeEvent
          eventBus.postAsync(FlinkJobStatusChangeEvent(event.trackId, newCache))
        }
      }
    }

  }
}
