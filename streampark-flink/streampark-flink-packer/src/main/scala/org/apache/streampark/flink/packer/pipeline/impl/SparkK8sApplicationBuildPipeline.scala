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

package org.apache.streampark.flink.packer.pipeline.impl

import org.apache.streampark.common.fs.LfsOperator
import org.apache.streampark.common.util.{CpuUtils, ThreadUtils}
import org.apache.streampark.flink.kubernetes.PodTemplateTool
import org.apache.streampark.flink.packer.docker._
import org.apache.streampark.flink.packer.pipeline._
import org.apache.streampark.flink.packer.pipeline.BuildPipeline.executor

import com.github.dockerjava.api.command.PushImageCmd
import com.github.dockerjava.core.command.{HackBuildImageCmd, HackPullImageCmd, HackPushImageCmd}
import com.google.common.collect.Sets
import org.apache.commons.lang3.StringUtils

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}

/** Building pipeline for Spark kubernetes-native application mode */
class SparkK8sApplicationBuildPipeline(request: SparkK8sApplicationBuildRequest)
  extends BuildPipeline {

  override def pipeType: PipelineTypeEnum =
    PipelineTypeEnum.SPARK_NATIVE_K8S_APPLICATION

  private var dockerProcessWatcher: DockerProgressWatcher =
    new SilentDockerProgressWatcher

  // non-thread-safe
  private val dockerProcess = new DockerResolveProgress(
    DockerPullProgress.empty(),
    DockerBuildProgress.empty(),
    DockerPushProgress.empty())

  override protected def offerBuildParam: SparkK8sApplicationBuildRequest =
    request

  def registerDockerProgressWatcher(watcher: DockerProgressWatcher): Unit = {
    dockerProcessWatcher = watcher
  }

  @throws[Throwable]
  override protected def buildProcess(): DockerImageBuildResponse = {

    // Step-1: init build workspace of spark job
    // the sub workspace dir like: APP_WORKSPACE/k8s-clusterId@k8s-namespace/
    val buildWorkspace =
      execStep(1) {
        val buildWorkspace =
          s"${request.workspace}/${request.k8sNamespace}"
        LfsOperator.mkCleanDirs(buildWorkspace)
        logInfo(s"Recreate building workspace: $buildWorkspace")
        buildWorkspace
      }.getOrElse(throw getError.exception)

    // Step-2: export k8s pod template files
    val podTemplatePaths = request.sparkPodTemplate match {
      case podTemplate if podTemplate.isEmpty =>
        skipStep(2)
        Map[String, String]()
      case podTemplate =>
        execStep(2) {
          val podTemplateFiles =
            PodTemplateTool
              .preparePodTemplateFiles(buildWorkspace, podTemplate)
              .tmplFiles
          logInfo(s"Export spark podTemplates: ${podTemplateFiles.values.mkString(",")}")
          podTemplateFiles
        }.getOrElse(throw getError.exception)
    }

    // Step-3: prepare spark job jar
    val (mainJarPath, extJarLibs) =
      execStep(3) {
        val mainJarName = Paths.get(request.mainJar).getFileName
        val mainJarPath = s"$buildWorkspace/$mainJarName"
        LfsOperator.copy(request.mainJar, mainJarPath)
        logInfo(s"Prepared spark job jar: $mainJarPath")
        mainJarPath -> Set[String]()
      }.getOrElse(throw getError.exception)

    // Step-4: generate and Export spark image dockerfiles
    val (dockerfile, dockerFileTemplate) =
      execStep(4) {
        val dockerFileTemplate = {
          if (request.integrateWithHadoop) {
            SparkHadoopDockerfileTemplate.fromSystemHadoopConf(
              buildWorkspace,
              request.sparkBaseImage,
              mainJarPath,
              extJarLibs)
          } else {
            SparkDockerfileTemplate(
              buildWorkspace,
              request.sparkBaseImage,
              mainJarPath,
              extJarLibs)
          }
        }
        val dockerFile = dockerFileTemplate.writeDockerfile
        logInfo(
          s"Output spark dockerfile: ${dockerFile.getAbsolutePath}, content: \n${dockerFileTemplate.offerDockerfileContent}")
        dockerFile -> dockerFileTemplate
      }.getOrElse(throw getError.exception)

    val dockerConf = request.dockerConfig
    val baseImageTag = request.sparkBaseImage.trim
    val pushImageTag = {
      if (request.k8sNamespace.isEmpty || request.appName.isEmpty) {
        throw new IllegalArgumentException("k8sNamespace or appName cannot be empty")
      }
      val expectedImageTag =
        s"streampark-sparkjob-${request.k8sNamespace}-${request.appName}"
      compileTag(expectedImageTag, dockerConf.registerAddress, dockerConf.imageNamespace)
    }

    // Step-5: pull spark base image
    execStep(5) {
      usingDockerClient {
        dockerClient =>
          val pullImageCmd = {
            // when the register address prefix is explicitly identified on base image tag,
            // the user's pre-saved docker register auth info would be used.
            val pullImageCmdState =
              dockerConf.registerAddress != null && !baseImageTag.startsWith(
                dockerConf.registerAddress)
            if (pullImageCmdState) {
              dockerClient.pullImageCmd(baseImageTag)
            } else {
              dockerClient
                .pullImageCmd(baseImageTag)
                .withAuthConfig(dockerConf.toAuthConf)
            }
          }
          val pullCmdCallback = pullImageCmd
            .asInstanceOf[HackPullImageCmd]
            .start(watchDockerPullProcess {
              pullRsp =>
                dockerProcess.pull.update(pullRsp)
                Future(dockerProcessWatcher.onDockerPullProgressChange(dockerProcess.pull.snapshot))
            })
          pullCmdCallback.awaitCompletion
          logInfo(s"Already pulled docker image from remote register, imageTag=$baseImageTag")
      }(err => throw new Exception(s"Pull docker image failed, imageTag=$baseImageTag", err))
    }.getOrElse(throw getError.exception)

    // Step-6: build spark image
    execStep(6) {
      usingDockerClient {
        dockerClient =>
          val buildImageCmd = dockerClient
            .buildImageCmd()
            .withBaseDirectory(new File(buildWorkspace))
            .withDockerfile(dockerfile)
            .withTags(Sets.newHashSet(pushImageTag))

          val buildCmdCallback = buildImageCmd
            .asInstanceOf[HackBuildImageCmd]
            .start(watchDockerBuildStep {
              buildStep =>
                dockerProcess.build.update(buildStep)
                Future(
                  dockerProcessWatcher.onDockerBuildProgressChange(dockerProcess.build.snapshot))
            })
          val imageId = buildCmdCallback.awaitImageId
          logInfo(s"Built docker image, imageId=$imageId, imageTag=$pushImageTag")
      }(err => throw new Exception(s"Build docker image failed. tag=$pushImageTag", err))
    }.getOrElse(throw getError.exception)

    // Step-7: push spark image
    execStep(7) {
      usingDockerClient {
        dockerClient =>
          val pushCmd: PushImageCmd = dockerClient
            .pushImageCmd(pushImageTag)
            .withAuthConfig(dockerConf.toAuthConf)

          val pushCmdCallback = pushCmd
            .asInstanceOf[HackPushImageCmd]
            .start(watchDockerPushProcess {
              pushRsp =>
                dockerProcess.push.update(pushRsp)
                Future(dockerProcessWatcher.onDockerPushProgressChange(dockerProcess.push.snapshot))
            })
          pushCmdCallback.awaitCompletion
          logInfo(s"Already pushed docker image, imageTag=$pushImageTag")
      }(err => throw new Exception(s"Push docker image failed. tag=$pushImageTag", err))
    }.getOrElse(throw getError.exception)

    DockerImageBuildResponse(
      buildWorkspace,
      pushImageTag,
      podTemplatePaths,
      dockerFileTemplate.innerMainJarPath)
  }

  /** compile image tag with namespace and remote address. */
  private[this] def compileTag(
      tag: String,
      registerAddress: String,
      imageNamespace: String): String = {
    var tagName = if (tag.contains("/")) tag else s"$imageNamespace/$tag"
    val addRegisterAddressState =
      StringUtils.isNotBlank(registerAddress) && !tagName.startsWith(registerAddress)
    if (addRegisterAddressState) {
      tagName = s"$registerAddress/$tagName"
    }
    tagName.toLowerCase
  }

}

object SparkK8sApplicationBuildPipeline {

  private val CPU_NUM = CpuUtils.getCpuCores

  val execPool = new ThreadPoolExecutor(
    CPU_NUM * 5,
    CPU_NUM * 10,
    60L,
    TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable](2048),
    ThreadUtils.threadFactory("streampark-docker-progress-watcher-executor"),
    new ThreadPoolExecutor.DiscardOldestPolicy)

  implicit val executor: ExecutionContext =
    ExecutionContext.fromExecutorService(execPool)

  def of(request: SparkK8sApplicationBuildRequest): SparkK8sApplicationBuildPipeline =
    new SparkK8sApplicationBuildPipeline(request)

}
