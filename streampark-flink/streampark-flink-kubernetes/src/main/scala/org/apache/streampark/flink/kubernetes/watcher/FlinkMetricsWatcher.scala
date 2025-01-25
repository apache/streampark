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

package org.apache.streampark.flink.kubernetes.watcher

import org.apache.streampark.common.util.Logger
import org.apache.streampark.flink.kubernetes.{ChangeEventBus, FlinkK8sWatchController, MetricWatcherConfig}
import org.apache.streampark.flink.kubernetes.event.FlinkClusterMetricChangeEvent
import org.apache.streampark.flink.kubernetes.model.{ClusterKey, FlinkMetricCV, TrackId}

import org.apache.flink.configuration.{JobManagerOptions, MemorySize, TaskManagerOptions}
import org.apache.hc.client5.http.fluent.Request
import org.json4s.{DefaultFormats, JArray}
import org.json4s.jackson.JsonMethods.parse

import javax.annotation.concurrent.ThreadSafe

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration.DurationLong
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

@ThreadSafe
class FlinkMetricWatcher(conf: MetricWatcherConfig = MetricWatcherConfig.defaultConf)(
    implicit val watchController: FlinkK8sWatchController,
    implicit val eventBus: ChangeEventBus)
  extends Logger
  with FlinkWatcher {

  implicit private val trackTaskExecutor: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(watchExecutor)

  private var timerSchedule: ScheduledFuture[_] = _

  /** start watcher process */
  override def doStart(): Unit = {
    timerSchedule = watchExecutor.scheduleAtFixedRate(
      () => doWatch(),
      0,
      conf.requestIntervalSec,
      TimeUnit.SECONDS)
    logInfo("[flink-k8s] FlinkMetricWatcher started.")
  }

  /** stop watcher process */
  override def doStop(): Unit = {
    if (!timerSchedule.isCancelled) {
      timerSchedule.cancel(true)
    }
    logInfo("[flink-k8s] FlinkMetricWatcher stopped.")
  }

  /** closes resource, relinquishing any underlying resources. */
  override def doClose(): Unit = {
    if (Option(timerSchedule).isDefined && !timerSchedule.isCancelled) {
      timerSchedule.cancel(true)
    }
    logInfo("[flink-k8s] FlinkMetricWatcher closed.")
  }

  /** single flink metrics tracking task */
  override def doWatch(): Unit = {
    // get all legal tracking cluster key
    val trackIds: Set[TrackId] = Try(watchController.getActiveWatchingIds())
      .filter(_.nonEmpty)
      .getOrElse(return
      )
    // retrieve flink metrics in thread pool
    val futures: Set[Future[Option[FlinkMetricCV]]] =
      trackIds.map(
        id => {
          val future = Future(collectMetrics(id))
          future.onComplete(_.getOrElse(None) match {
            case Some(metric) =>
              val clusterKey = id.toClusterKey
              val isMetricChanged = {
                val preMetric = watchController.flinkMetrics.get(clusterKey)
                preMetric == null || !preMetric.equalsPayload(metric)
              }
              if (isMetricChanged) {
                eventBus.postAsync(FlinkClusterMetricChangeEvent(id, metric))
                // update current flink cluster metrics on cache
                watchController.flinkMetrics.put(clusterKey, metric)
              }
            case _ =>
          })
          future
        })
    // blocking until all future are completed or timeout is reached
    Try(Await.ready(Future.sequence(futures), conf.requestTimeoutSec seconds)).failed.map {
      _ =>
        logError(
          s"[FlinkMetricWatcher] tracking flink metrics on kubernetes mode timeout," +
            s" limitSeconds=${conf.requestTimeoutSec}," +
            s" trackingClusterKeys=${trackIds.mkString(",")}")
    }
  }

  /**
   * Collect flink runtime metrics from kubernetes-native cluster. Returns None when the
   * flink-cluster-client request fails (or in case of the relevant flink rest api require failure).
   *
   * This method can be called directly from outside, without affecting the current cachePool
   * result.
   */
  private def collectMetrics(id: TrackId): Option[FlinkMetricCV] = {
    // get flink rest api
    val clusterKey: ClusterKey = ClusterKey.of(id)
    val flinkJmRestUrl =
      watchController.getClusterRestUrl(clusterKey).filter(_.nonEmpty).getOrElse(return None)

    // call flink rest overview api
    val flinkOverview: FlinkRestOverview = FlinkRestOverview
      .as(
        Request
          .get(s"$flinkJmRestUrl/overview")
          .connectTimeout(FLINK_REST_AWAIT_TIMEOUT_SEC)
          .responseTimeout(FLINK_CLIENT_TIMEOUT_SEC)
          .execute
          .returnContent
          .asString(StandardCharsets.UTF_8))
      .getOrElse(return None)

    // call flink rest jm config api
    val flinkJmConfigs = Try(
      FlinkRestJmConfigItem
        .as(
          Request
            .get(s"$flinkJmRestUrl/jobmanager/config")
            .connectTimeout(FLINK_REST_AWAIT_TIMEOUT_SEC)
            .responseTimeout(FLINK_CLIENT_TIMEOUT_SEC)
            .execute
            .returnContent
            .asString(StandardCharsets.UTF_8))
        .map(e => (e.key, e.value))
        .toMap).getOrElse(return None)

    val ackTime = System.currentTimeMillis
    val flinkMetricCV = {
      val tmMemStr = flinkJmConfigs.getOrElse(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key, "0b")
      val jmMemStr = flinkJmConfigs.getOrElse(JobManagerOptions.TOTAL_PROCESS_MEMORY.key, "0b")
      FlinkMetricCV(
        groupId = id.groupId,
        totalJmMemory = MemorySize.parse(jmMemStr).getMebiBytes,
        totalTmMemory = MemorySize.parse(tmMemStr).getMebiBytes * flinkOverview.taskManagers,
        totalTm = flinkOverview.taskManagers,
        totalSlot = flinkOverview.slotsTotal,
        availableSlot = flinkOverview.slotsAvailable,
        runningJob = flinkOverview.jobsRunning,
        finishedJob = flinkOverview.jobsFinished,
        cancelledJob = flinkOverview.jobsCancelled,
        failedJob = flinkOverview.jobsFailed,
        pollAckTime = ackTime
      )
    }
    Some(flinkMetricCV)
  }

}

/** bean for response message of flink-rest/overview */
private[kubernetes] case class FlinkRestOverview(
    taskManagers: Integer,
    slotsTotal: Integer,
    slotsAvailable: Integer,
    jobsRunning: Integer,
    jobsFinished: Integer,
    jobsCancelled: Integer,
    jobsFailed: Integer,
    flinkVersion: String)

object FlinkRestOverview {

  @transient
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def as(json: String): Option[FlinkRestOverview] = {
    Try(parse(json)) match {
      case Success(ok) =>
        val overview = FlinkRestOverview(
          (ok \ "taskmanagers").extractOpt[Integer].getOrElse(0),
          (ok \ "slots-total").extractOpt[Integer].getOrElse(0),
          (ok \ "slots-available").extractOpt[Integer].getOrElse(0),
          (ok \ "jobs-running").extractOpt[Integer].getOrElse(0),
          (ok \ "jobs-finished").extractOpt[Integer].getOrElse(0),
          (ok \ "jobs-cancelled").extractOpt[Integer].getOrElse(0),
          (ok \ "jobs-failed").extractOpt[Integer].getOrElse(0),
          (ok \ "flink-version").extractOpt[String].orNull
        )
        Some(overview)
      case Failure(_) => None
    }
  }

}

/** bean for response message of flink-rest/jobmanager/config */
private[kubernetes] case class FlinkRestJmConfigItem(key: String, value: String)

private[kubernetes] object FlinkRestJmConfigItem {

  @transient
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def as(json: String): List[FlinkRestJmConfigItem] = {
    Try(parse(json)) match {
      case Success(ok) =>
        ok match {
          case JArray(arr) =>
            arr.map(
              x => {
                FlinkRestJmConfigItem(
                  (x \ "key").extractOpt[String].orNull,
                  (x \ "value").extractOpt[String].orNull)
              })
          case _ => null
        }
      case Failure(_) => null
    }
  }

}
