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
import org.apache.streampark.flink.kubernetes.{ChangeEventBus, FlinkK8sWatchController, JobStatusWatcherConfig, KubernetesRetriever}
import org.apache.streampark.flink.kubernetes.enums.{FlinkJobState, FlinkK8sExecuteMode}
import org.apache.streampark.flink.kubernetes.enums.FlinkK8sExecuteMode.{APPLICATION, SESSION}
import org.apache.streampark.flink.kubernetes.event.FlinkJobStatusChangeEvent
import org.apache.streampark.flink.kubernetes.helper.KubernetesDeploymentHelper
import org.apache.streampark.flink.kubernetes.model._

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.flink.configuration.JobManagerOptions
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.history.FsJobArchivist
import org.apache.hc.client5.http.fluent.Request
import org.json4s.{DefaultFormats, JNothing, JNull}
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.parse

import javax.annotation.Nonnull
import javax.annotation.concurrent.ThreadSafe

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration.DurationLong
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Success, Try}

/**
 * Watcher for continuously monitor flink job status on kubernetes-mode, the traced flink
 * identifiers from FlinkTrackCachePool.trackIds, the traced result of flink jobs status would
 * written to FlinkTrackCachePool.jobStatuses.
 */
@ThreadSafe
class FlinkJobStatusWatcher(conf: JobStatusWatcherConfig = JobStatusWatcherConfig.defaultConf)(
    implicit val watchController: FlinkK8sWatchController,
    implicit val eventBus: ChangeEventBus)
  extends Logger
  with FlinkWatcher {

  implicit private val trackTaskExecutor: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(watchExecutor)

  private var timerSchedule: ScheduledFuture[_] = _

  /** stop watcher process */
  override def doStart(): Unit = {
    timerSchedule = watchExecutor.scheduleAtFixedRate(
      () => doWatch(),
      0,
      conf.requestIntervalSec,
      TimeUnit.SECONDS)
    logInfo("[flink-k8s] FlinkJobStatusWatcher started.")
  }

  /** stop watcher process */
  override def doStop(): Unit = {
    // interrupt all running threads
    if (!timerSchedule.isCancelled) {
      timerSchedule.cancel(true)
    }
    logInfo("[flink-k8s] FlinkJobStatusWatcher stopped.")
  }

  /** closes resource, relinquishing any underlying resources. */
  override def doClose(): Unit = {
    trackTaskExecutor.shutdownNow()
    logInfo("[flink-k8s] FlinkJobStatusWatcher closed.")
  }

  /** single flink job status tracking task */
  override def doWatch(): Unit = {
    this.synchronized {
      // get all legal tracking ids
      val trackIds = Try(watchController.getAllWatchingIds())
        .filter(_.nonEmpty)
        .getOrElse(return
        )

      // 1) k8s application mode
      val appFuture = trackIds.filter(_.executeMode == FlinkK8sExecuteMode.APPLICATION).map {
        id =>
          Future {
            Try(touchApplicationJob(id)) match {
              case Success(Some(jobState)) =>
                updateState(id.copy(jobId = jobState.jobId), jobState)
              case _ =>
            }
          }
      }

      // 2) k8s session mode
      val sessionIds = trackIds.filter(_.executeMode == FlinkK8sExecuteMode.SESSION)
      val sessionCluster = sessionIds.groupBy(_.toClusterKey.toString).flatMap(_._2).toSet
      val sessionFuture = sessionCluster.map {
        trackId =>
          Future {
            val result = Try(touchSessionAllJob(trackId))
            result match {
              case Success(map) =>
                map.find(_._1.jobId == trackId.jobId) match {
                  case Some(job) =>
                    updateState(job._1.copy(appId = trackId.appId), job._2)
                  case _ =>
                    touchSessionJob(trackId) match {
                      case Some(state) =>
                        if (FlinkJobState.isEndState(state.jobState)) {
                          // can't find that job in the k8s cluster.
                          watchController.unWatching(trackId)
                        }
                        eventBus.postAsync(FlinkJobStatusChangeEvent(trackId, state))
                      case _ =>
                    }
                }
              case _ =>
            }
          }
      }

      // blocking until all future are completed or timeout is reached
      Try(Await.ready(Future.sequence(appFuture), conf.requestTimeoutSec seconds)).failed.map {
        _ =>
          logWarn(
            s"[FlinkJobStatusWatcher] tracking flink job status on kubernetes native application mode timeout," +
              s" limitSeconds=${conf.requestTimeoutSec}," +
              s" trackIds=${trackIds.mkString(",")}")
      }

      Try(Await.ready(Future.sequence(sessionFuture), conf.requestTimeoutSec seconds)).failed.map {
        _ =>
          logWarn(
            s"[FlinkJobStatusWatcher] tracking flink job status on kubernetes native session mode timeout," +
              s" limitSeconds=${conf.requestTimeoutSec}," +
              s" trackIds=${trackIds.mkString(",")}")
      }
    }
  }

  /**
   * Get flink status information from kubernetes-native-session cluster. When the
   * flink-cluster-client request fails, the job state would be LOST or SILENT.
   *
   * This method can be called directly from outside, without affecting the current cachePool
   * result.
   */
  def touchSessionJob(@Nonnull trackId: TrackId): Option[JobStatusCV] = {
    touchSessionAllJob(trackId)
      .find(id => id._1.jobId == trackId.jobId && id._2.jobState != FlinkJobState.SILENT)
      .map(_._2)
      .orElse(inferState(trackId))
  }

  /**
   * Get all flink job status information from kubernetes-native-session cluster. The empty array
   * will returned when the k8s-client or flink-cluster-client request fails.
   *
   * This method can be called directly from outside, without affecting the current cachePool
   * result.
   */
  private def touchSessionAllJob(trackId: TrackId): Map[TrackId, JobStatusCV] = {
    val pollEmitTime = System.currentTimeMillis
    val jobDetails = listJobsDetails(ClusterKey.of(trackId))
    jobDetails match {
      case Some(details) if details.jobs.nonEmpty =>
        details.jobs.map {
          d =>
            val jobStatus = d.toJobStatusCV(pollEmitTime, System.currentTimeMillis)
            val trackItem = trackId.copy(jobId = d.jid, appId = null)
            trackItem -> jobStatus
        }.toMap
      case None => Map.empty[TrackId, JobStatusCV]
    }
  }

  /**
   * Get flink status information from kubernetes-native-application cluster. When the
   * flink-cluster-client request fails, will infer the job statue from k8s events.
   *
   * This method can be called directly from outside, without affecting the current cachePool
   * result.
   */
  def touchApplicationJob(@Nonnull trackId: TrackId): Option[JobStatusCV] = {
    implicit val pollEmitTime: Long = System.currentTimeMillis
    val jobDetails = listJobsDetails(ClusterKey.of(trackId))
    if (jobDetails.isEmpty || jobDetails.get.jobs.isEmpty) {
      inferStateFromK8sEvent(trackId)
    } else {
      Some(jobDetails.get.jobs.head.toJobStatusCV(pollEmitTime, System.currentTimeMillis))
    }
  }

  private[this] def updateState(trackId: TrackId, jobState: JobStatusCV): Unit = {
    val latest: JobStatusCV = watchController.jobStatuses.get(trackId)
    if (jobState.diff(latest)) {
      // put job status to cache
      watchController.jobStatuses.put(trackId, jobState)
      // set jobId to trackIds
      watchController.trackIds.update(trackId)

      eventBus.postAsync(FlinkJobStatusChangeEvent(trackId, jobState))
    }

    if (FlinkJobState.isEndState(jobState.jobState)) {
      trackId.executeMode match {
        case APPLICATION =>
          val deployExists = KubernetesRetriever.isDeploymentExists(
            trackId.namespace,
            trackId.clusterId
          )
          if (!deployExists) {
            watchController.endpoints.invalidate(trackId.toClusterKey)
            watchController.unWatching(trackId)
          }
        case SESSION =>
          watchController.unWatching(trackId)
      }
    }
  }

  private[this] def inferState(id: TrackId): Option[JobStatusCV] = {
    lazy val pollEmitTime = System.currentTimeMillis
    val preCache = watchController.jobStatuses.get(id)
    val state = inferFromPreCache(preCache)
    val nonFirstSilent = state == FlinkJobState.SILENT &&
      preCache != null &&
      preCache.jobState == FlinkJobState.SILENT
    val jobState = if (nonFirstSilent) {
      JobStatusCV(
        jobState = state,
        jobId = id.jobId,
        pollEmitTime = preCache.pollEmitTime,
        pollAckTime = preCache.pollAckTime)
    } else {
      JobStatusCV(
        jobState = state,
        jobId = id.jobId,
        pollEmitTime = pollEmitTime,
        pollAckTime = System.currentTimeMillis)
    }
    Option(jobState)
  }

  /** list flink jobs details */
  private def listJobsDetails(clusterKey: ClusterKey): Option[JobDetails] = {
    // get flink rest api
    Try {
      val clusterRestUrl =
        watchController.getClusterRestUrl(clusterKey).filter(_.nonEmpty).getOrElse(return None)
      // list flink jobs from rest api
      callJobsOverviewsApi(clusterRestUrl)
    }.getOrElse {
      logger.warn(
        s"Failed to visit ${clusterKey.clusterId} remote flink jobs on kubernetes-native-mode cluster, and the retry access logic is performed.")
      val clusterRestUrl = watchController.refreshClusterRestUrl(clusterKey).getOrElse(return None)
      Try(callJobsOverviewsApi(clusterRestUrl)) match {
        case Success(s) =>
          logger.info(s"The retry ${clusterKey.clusterId} is successful.")
          s
        case Failure(e) =>
          logger.warn(
            s"The retry fetch ${clusterKey.clusterId} failed, final status failed, errorStack=${e.getMessage}.")
          None
      }
    }
  }

  /** list flink jobs details from rest api */
  private def callJobsOverviewsApi(restUrl: String): Option[JobDetails] = {
    JobDetails.as(
      Request
        .get(s"$restUrl/jobs/overview")
        .connectTimeout(FLINK_REST_AWAIT_TIMEOUT_SEC)
        .responseTimeout(FLINK_CLIENT_TIMEOUT_SEC)
        .execute
        .returnContent()
        .asString(StandardCharsets.UTF_8)
    )
  }

  /**
   * Infer the current flink state from the last relevant k8s events. This method is only used for
   * application-mode job inference in case of a failed JM rest request.
   */
  private def inferStateFromK8sEvent(@Nonnull trackId: TrackId)(implicit
      pollEmitTime: Long): Option[JobStatusCV] = {
    // infer from k8s deployment and event
    val latest: JobStatusCV = watchController.jobStatuses.get(trackId)
    // whether deployment exists on kubernetes cluster
    val deployExists = KubernetesRetriever.isDeploymentExists(
      trackId.namespace,
      trackId.clusterId
    )
    val jobState = trackId match {
      case id
          if watchController.canceling.has(id) || Option(latest).exists(
            _.jobState == FlinkJobState.CANCELLING) =>
        logger.info(s"trackId ${trackId.toString} is canceling")
        if (deployExists) FlinkJobState.CANCELLING else FlinkJobState.CANCELED
      case _ =>
        val isConnection = KubernetesDeploymentHelper.checkConnection()

        if (deployExists) {
          if (isConnection) {
            logger.info(s"${trackId.clusterId} Enter the task starting process.")
            KubernetesDeploymentHelper.watchDeploymentLog(
              trackId.namespace,
              trackId.clusterId,
              trackId.jobId)
            FlinkJobState.STARTING
          } else {
            inferFromPreCache(latest)
          }
        } else if (isConnection) {
          logger.info(
            s"${trackId.clusterId} deployment is deleted and enters the task failure process.")
          FlinkJobState.of(FlinkHistoryArchives.getJobStateFromArchiveFile(trackId))
        } else {
          inferFromPreCache(latest)
        }
    }

    val jobStatusCV = JobStatusCV(
      jobState = jobState,
      jobId = trackId.jobId,
      pollEmitTime = pollEmitTime,
      pollAckTime = System.currentTimeMillis)

    if (
      jobState == FlinkJobState.SILENT && latest != null && latest.jobState == FlinkJobState.SILENT
    ) {
      Some(jobStatusCV.copy(pollEmitTime = latest.pollEmitTime, pollAckTime = latest.pollAckTime))
    } else {
      Some(jobStatusCV)
    }
  }

  private[this] def inferFromPreCache(preCache: JobStatusCV) = preCache match {
    case preCache if preCache == null => FlinkJobState.SILENT
    case preCache
        if preCache.jobState == FlinkJobState.SILENT &&
          System
            .currentTimeMillis() - preCache.pollAckTime >= conf.silentStateJobKeepTrackingSec * 1000 =>
      FlinkJobState.LOST
    case _ => FlinkJobState.SILENT
  }

}

object FlinkJobStatusWatcher {

  /**
   * infer flink job state before persistence.
   *
   * @param current
   *   current flink job state
   * @param previous
   *   previous flink job state from persistent storage
   */
  def inferFlinkJobStateFromPersist(
      current: FlinkJobState.Value,
      previous: FlinkJobState.Value): FlinkJobState.Value = {
    current match {
      case FlinkJobState.POS_TERMINATED | FlinkJobState.TERMINATED =>
        previous match {
          case FlinkJobState.CANCELLING => FlinkJobState.CANCELED
          case FlinkJobState.FAILING => FlinkJobState.FAILED
          case _ =>
            current match {
              case FlinkJobState.POS_TERMINATED => FlinkJobState.FINISHED
              case _ => FlinkJobState.TERMINATED
            }
        }
      case _ => current
    }
  }

}

private[kubernetes] case class JobDetails(jobs: Array[JobDetail] = Array())

private[kubernetes] case class JobDetail(
    jid: String,
    name: String,
    state: String,
    startTime: Long,
    endTime: Long,
    duration: Long,
    lastModification: Long,
    tasks: JobTask) {
  def toJobStatusCV(pollEmitTime: Long, pollAckTime: Long): JobStatusCV = {
    JobStatusCV(
      jobState = FlinkJobState.of(state),
      jobId = jid,
      jobName = name,
      jobStartTime = startTime,
      jobEndTime = endTime,
      duration = duration,
      taskTotal = tasks.total,
      pollEmitTime = pollEmitTime,
      pollAckTime = pollAckTime
    )
  }
}

private[kubernetes] case class JobTask(
    total: Int,
    created: Int,
    scheduled: Int,
    deploying: Int,
    running: Int,
    finished: Int,
    canceling: Int,
    canceled: Int,
    failed: Int,
    reconciling: Int,
    initializing: Int)

private[kubernetes] object JobDetails {

  @transient
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def as(json: String): Option[JobDetails] = {

    Try(parse(json)) match {
      case Success(ok) =>
        ok \ "jobs" match {
          case JNothing | JNull => None
          case JArray(arr) =>
            val details = arr
              .map(
                x => {
                  val task = x \ "tasks"
                  JobDetail(
                    (x \ "jid").extractOpt[String].orNull,
                    (x \ "name").extractOpt[String].orNull,
                    (x \ "state").extractOpt[String].orNull,
                    (x \ "start-time").extractOpt[Long].getOrElse(0),
                    (x \ "end-time").extractOpt[Long].getOrElse(0),
                    (x \ "duration").extractOpt[Long].getOrElse(0),
                    (x \ "last-modification").extractOpt[Long].getOrElse(0),
                    JobTask(
                      (task \ "total").extractOpt[Int].getOrElse(0),
                      (task \ "created").extractOpt[Int].getOrElse(0),
                      (task \ "scheduled").extractOpt[Int].getOrElse(0),
                      (task \ "deploying").extractOpt[Int].getOrElse(0),
                      (task \ "running").extractOpt[Int].getOrElse(0),
                      (task \ "finished").extractOpt[Int].getOrElse(0),
                      (task \ "canceling").extractOpt[Int].getOrElse(0),
                      (task \ "canceled").extractOpt[Int].getOrElse(0),
                      (task \ "failed").extractOpt[Int].getOrElse(0),
                      (task \ "reconciling").extractOpt[Int].getOrElse(0),
                      (task \ "initializing").extractOpt[Int].getOrElse(0)
                    )
                  )
                })
              .toArray
            Some(JobDetails(details))
          case _ => None
        }
      case Failure(_) => None
    }

  }

}

private[kubernetes] object FlinkHistoryArchives extends Logger {

  @transient
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

  private[this] val FAILED_STATE = "FAILED"

  def getJobStateFromArchiveFile(trackId: TrackId): String = {

    require(trackId.jobId != null, "[StreamPark] getJobStateFromArchiveFile: JobId cannot be null.")
    val archiveDir = trackId.properties.getProperty(JobManagerOptions.ARCHIVE_DIR.key)
    if (archiveDir == null) {
      FAILED_STATE
    } else {
      val archivePath = new Path(archiveDir, trackId.jobId)
      Try(FsJobArchivist.getArchivedJsons(archivePath)) match {
        case Failure(e) =>
          throw new RuntimeException(
            s"[StreamPark] getJobStateFromArchiveFile: Failed to get archived jsons, error: ${e.getMessage}")
        case Success(r) =>
          if (r.isEmpty) {
            FAILED_STATE
          } else {
            r.foreach {
              a =>
                if (a.getPath == "/jobs/overview") {
                  Try(parse(a.getJson)) match {
                    case Success(ok) =>
                      ok \ "jobs" match {
                        case JNothing | JNull =>
                        case JArray(arr) =>
                          arr.foreach(
                            x => {
                              val jid = (x \ "jid").extractOpt[String].orNull
                              if (jid == trackId.jobId) {
                                return (x \ "state").extractOpt[String].orNull
                              }
                            })
                        case _ =>
                      }
                    case Failure(_) =>
                  }
                }
                if (a.getPath == s"/jobs/${trackId.jobId}/exceptions") {
                  Try(parse(a.getJson)) match {
                    case Success(ok) =>
                      val log = (ok \ "root-exception").extractOpt[String].orNull
                      if (log != null) {
                        val path = KubernetesDeploymentHelper.getJobErrorLog(trackId.jobId)
                        val file = new File(path)
                        Files.asCharSink(file, Charsets.UTF_8).write(log)
                        println(" error path: " + path)
                      }
                    case _ =>
                  }
                }
            }
            FAILED_STATE
          }
      }
    }
  }

}
