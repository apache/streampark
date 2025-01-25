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
import org.apache.streampark.flink.kubernetes.event.FlinkJobCheckpointChangeEvent
import org.apache.streampark.flink.kubernetes.model.{CheckpointCV, ClusterKey, TrackId}

import org.apache.hc.client5.http.fluent.Request
import org.json4s.{DefaultFormats, JNull}
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods.parse

import javax.annotation.concurrent.ThreadSafe

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration.DurationLong
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

@ThreadSafe
class FlinkCheckpointWatcher(conf: MetricWatcherConfig = MetricWatcherConfig.defaultConf)(
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
    logInfo("[flink-k8s] FlinkCheckpointWatcher started.")
  }

  /** stop watcher process */
  override def doStop(): Unit = {
    if (!timerSchedule.isCancelled) {
      timerSchedule.cancel(true)
    }
    logInfo("[flink-k8s] FlinkCheckpointWatcher stopped.")
  }

  /** closes resource, relinquishing any underlying resources. */
  override def doClose(): Unit = {
    if (Option(timerSchedule).isDefined && !timerSchedule.isCancelled) {
      timerSchedule.cancel(true)
    }
    logInfo("[flink-k8s] FlinkCheckpointWatcher closed.")
  }

  /** single flink metrics tracking task */
  override def doWatch(): Unit = {
    // get all legal tracking cluster key
    val trackIds: Set[TrackId] = Try(watchController.getActiveWatchingIds())
      .filter(_.nonEmpty)
      .getOrElse(return
      )
    // retrieve flink metrics in thread pool
    val futures: Set[Future[Option[CheckpointCV]]] =
      trackIds.map(
        id => {
          val future = Future(collect(id))
          future.onComplete(_.getOrElse(None) match {
            case Some(cp) => eventBus.postAsync(FlinkJobCheckpointChangeEvent(id, cp))
            case _ =>
          })
          future
        })

    // blocking until all future are completed or timeout is reached
    Try(Await.ready(Future.sequence(futures), conf.requestTimeoutSec seconds)).failed.map {
      _ =>
        logError(
          s"[FlinkCheckpointWatcher] tracking flink-job checkpoint on kubernetes mode timeout," +
            s" limitSeconds=${conf.requestTimeoutSec}," +
            s" trackingClusterKeys=${trackIds.mkString(",")}")
    }
  }

  /**
   * Collect flink-job checkpoint from kubernetes-native cluster. Returns None when the
   * flink-cluster-client request fails (or in case of the relevant flink rest api require failure).
   */
  def collect(trackId: TrackId): Option[CheckpointCV] = {
    if (trackId.jobId != null) {
      val flinkJmRestUrl = watchController
        .getClusterRestUrl(ClusterKey.of(trackId))
        .filter(_.nonEmpty)
        .getOrElse(return None)
      // call flink rest overview api
      val checkpoint: Checkpoint = Try(
        Checkpoint.as(
          Request
            .get(s"$flinkJmRestUrl/jobs/${trackId.jobId}/checkpoints")
            .connectTimeout(FLINK_REST_AWAIT_TIMEOUT_SEC)
            .responseTimeout(FLINK_CLIENT_TIMEOUT_SEC)
            .execute
            .returnContent
            .asString(StandardCharsets.UTF_8)) match {
          case Some(c) => c
          case _ => return None
        }).getOrElse(return None)

      val checkpointCV = CheckpointCV(
        id = checkpoint.id,
        externalPath = checkpoint.externalPath,
        isSavepoint = checkpoint.isSavepoint,
        checkpointType = checkpoint.checkpointType,
        status = checkpoint.status,
        triggerTimestamp = checkpoint.triggerTimestamp
      )
      Some(checkpointCV)
    } else None
  }

}

private[kubernetes] case class Checkpoint(
    id: Long,
    status: String,
    externalPath: String,
    isSavepoint: Boolean,
    checkpointType: String,
    triggerTimestamp: Long)

object Checkpoint {

  @transient
  implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

  def as(json: String): Option[Checkpoint] = {
    Try(parse(json)) match {
      case Success(ok) =>
        val completed = ok \ "latest" \ "completed"
        completed match {
          case JNull | JNothing => None
          case _ =>
            val cp = Checkpoint(
              id = (completed \ "id").extractOpt[Long].getOrElse(0L),
              status = (completed \ "status").extractOpt[String].orNull,
              externalPath = (completed \ "external_path").extractOpt[String].orNull,
              isSavepoint = (completed \ "is_savepoint").extractOpt[Boolean].getOrElse(false),
              checkpointType = (completed \ "checkpoint_type").extractOpt[String].orNull,
              triggerTimestamp = (completed \ "trigger_timestamp").extractOpt[Long].getOrElse(0L)
            )
            Some(cp)
        }
      case Failure(_) => None
    }
  }

}
