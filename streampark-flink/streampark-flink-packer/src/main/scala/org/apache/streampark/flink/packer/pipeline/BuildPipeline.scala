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

package org.apache.streampark.flink.packer.pipeline

import org.apache.streampark.common.util.{CpuUtils, Logger, ThreadUtils}
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.flink.packer.pipeline.BuildPipeline.executor

import java.util.concurrent.{Callable, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.{Failure, Success, Try}

/** Behavior that BuildPipeline subclasses must inherit to implement. */
trait BuildPipelineProcess {

  /** the type of pipeline */
  def pipeType: PipelineTypeEnum

  /**
   * the actual build process. the effective steps progress should be implemented in multiple
   * BuildPipeline.execStep() functions.
   */
  @throws[Throwable]
  protected def buildProcess(): BuildResult

  /** the build params of build process */
  protected def offerBuildParam: BuildParam
}

/** Callable methods exposed by BuildPipeline to the outside. */
trait BuildPipelineExpose {

  /** get current state of the pipeline instance */
  def getPipeStatus: PipelineStatusEnum

  /** get error of pipeline instance */
  def getError: PipeError

  /** get all of the steps status StepSeq -> (PipeStepStatus -> status update timestamp) */
  def getStepsStatus: Map[Int, (PipelineStepStatusEnum, Long)]

  /** get current build step index */
  def getCurStep: Int

  /** get count of all build steps */
  def allSteps: Int

  /** launch the pipeline instance */
  def launch(): BuildResult

  def as[T <: BuildPipeline](implicit clz: Class[T]): T = this.asInstanceOf[T]
}

/** Building pipeline trait. */
trait BuildPipeline extends BuildPipelineProcess with BuildPipelineExpose with Logger {

  protected var pipeStatus: PipelineStatusEnum = PipelineStatusEnum.pending

  protected var error: PipeError = PipeError.empty()

  protected var curStep: Int = 0

  protected val stepsStatus: mutable.Map[Int, (PipelineStepStatusEnum, Long)] =
    mutable.Map(
      pipeType.getSteps
        .map(e => e._1.toInt -> (PipelineStepStatusEnum.waiting -> System.currentTimeMillis))
        .toSeq: _*)

  /** use to identify the log record that belongs to which pipeline instance */
  private val logSuffix: String = s"appName=${offerBuildParam.appName}"

  protected var watcher: PipeWatcher = new SilentPipeWatcher

  def registerWatcher(watcher: PipeWatcher): BuildPipeline = {
    this.watcher = watcher
    this
  }

  protected def execStep[R](seq: Int)(process: => R): Option[R] = {
    Try {
      curStep = seq
      stepsStatus(seq) = PipelineStepStatusEnum.running -> System.currentTimeMillis
      logInfo(s"Building pipeline step[$seq/$allSteps] running => ${pipeType.getSteps.get(seq)}")
      watcher.onStepStateChange(snapshot)
      process
    } match {
      case Success(result) =>
        stepsStatus(seq) = PipelineStepStatusEnum.success -> System.currentTimeMillis
        logInfo(s"Building pipeline step[$seq/$allSteps] success")
        watcher.onStepStateChange(snapshot)
        Some(result)
      case Failure(cause) =>
        stepsStatus(seq) = PipelineStepStatusEnum.failure -> System.currentTimeMillis
        pipeStatus = PipelineStatusEnum.failure
        error = PipeError.of(cause.getMessage, cause)
        logInfo(s"Building pipeline step[$seq/$allSteps] failure => ${pipeType.getSteps
            .get(seq)}")
        watcher.onStepStateChange(snapshot)
        None
    }
  }

  protected def skipStep(step: Int): Unit = {
    curStep = step
    stepsStatus(step) = PipelineStepStatusEnum.skipped -> System.currentTimeMillis
    logInfo(s"Building pipeline step[$step/$allSteps] skipped => ${pipeType.getSteps.get(step)}")
    watcher.onStepStateChange(snapshot)
  }

  /** Launch the building pipeline. */
  override def launch(): BuildResult = {
    pipeStatus = PipelineStatusEnum.running
    Try {
      watcher.onStart(snapshot)
      logInfo(s"Building pipeline is launching, params=${offerBuildParam.toString}")
      executor
        .submit(new Callable[BuildResult] {
          override def call(): BuildResult = buildProcess()
        })
        .get(20, TimeUnit.MINUTES)
    } match {
      case Success(result) =>
        pipeStatus = PipelineStatusEnum.success
        logInfo(s"Building pipeline has finished successfully.")
        watcher.onFinish(snapshot, result)
        result
      case Failure(cause) =>
        pipeStatus = PipelineStatusEnum.failure
        error = PipeError.of(cause.getMessage, cause)
        // log and print error trace stack
        logError(s"Building pipeline has failed.", cause)
        val result = ErrorResult()
        watcher.onFinish(snapshot, result)
        result
    }
  }

  override def getPipeStatus: PipelineStatusEnum = pipeStatus

  override def getError: PipeError = error.copy()

  override def getStepsStatus: Map[Int, (PipelineStepStatusEnum, Long)] =
    stepsStatus.toMap

  override def getCurStep: Int = curStep

  override def allSteps: Int = pipeType.getSteps.size

  override def logInfo(msg: => String): Unit =
    super.logInfo(s"[streampark-packer] $msg | $logSuffix")

  override def logError(msg: => String): Unit =
    super.logError(s"[streampark-packer] $msg | $logSuffix")

  override def logError(msg: => String, throwable: Throwable): Unit =
    super.logError(s"[streampark-packer] $msg | $logSuffix", throwable)

  /** intercept snapshot */
  def snapshot: PipelineSnapshot = PipelineSnapshot(
    offerBuildParam.appName,
    pipeType,
    getPipeStatus,
    getCurStep,
    allSteps,
    getStepsStatus,
    getError,
    System.currentTimeMillis)

}

object BuildPipeline {

  private val CPU_NUM = CpuUtils.getCpuCores

  val execPool = new ThreadPoolExecutor(
    CPU_NUM * 2,
    300,
    60L,
    TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable](2048),
    ThreadUtils.threadFactory("streampark-pipeline-watcher-executor"),
    new ThreadPoolExecutor.AbortPolicy)

  implicit val executor: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(execPool)

}
