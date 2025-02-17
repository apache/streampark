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
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.common.util.ThreadUtils
import org.apache.streampark.flink.kubernetes.PodTemplateTool
import org.apache.streampark.flink.kubernetes.ingress.IngressController
import org.apache.streampark.flink.packer.docker._
import org.apache.streampark.flink.packer.maven.MavenTool
import org.apache.streampark.flink.packer.pipeline._
import org.apache.streampark.flink.packer.pipeline.BuildPipeline.executor

import com.github.dockerjava.api.command.PushImageCmd
import com.github.dockerjava.core.command.{HackBuildImageCmd, HackPullImageCmd, HackPushImageCmd}
import com.google.common.collect.Sets
import org.apache.commons.lang3.StringUtils

import java.io.File
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}

/** Building pipeline for flink kubernetes-native application mode */
class FlinkK8sApplicationBuildPipeline(request: FlinkK8sApplicationBuildRequest)
  extends BuildPipeline {

  override def pipeType: PipelineTypeEnum =
    PipelineTypeEnum.FLINK_NATIVE_K8S_APPLICATION

  private var dockerProcessWatcher: DockerProgressWatcher =
    new SilentDockerProgressWatcher

  // non-thread-safe
  private val dockerProcess = new DockerResolveProgress(
    DockerPullProgress.empty(),
    DockerBuildProgress.empty(),
    DockerPushProgress.empty())

  override protected def offerBuildParam: FlinkK8sApplicationBuildRequest =
    request

  def registerDockerProgressWatcher(watcher: DockerProgressWatcher): Unit = {
    dockerProcessWatcher = watcher
  }

  @throws[Throwable]
  override protected def buildProcess(): DockerImageBuildResponse = {

    // Step-1: init build workspace of flink job
    // the sub workspace dir like: APP_WORKSPACE/k8s-clusterId@k8s-namespace/
    val buildWorkspace =
      execStep(1) {
        val buildWorkspace =
          s"${request.workspace}/${request.clusterId}@${request.k8sNamespace}"
        LfsOperator.mkCleanDirs(buildWorkspace)
        logInfo(s"Recreate building workspace: $buildWorkspace")
        buildWorkspace
      }.getOrElse(throw getError.exception)

    // Step-2: export k8s pod template files
    val podTemplatePaths = request.flinkPodTemplate match {
      case podTemplate if podTemplate.isEmpty =>
        skipStep(2)
        Map[String, String]()
      case podTemplate =>
        execStep(2) {
          val podTemplateFiles =
            PodTemplateTool
              .preparePodTemplateFiles(buildWorkspace, podTemplate)
              .tmplFiles
          logInfo(s"Export flink podTemplates: ${podTemplateFiles.values.mkString(",")}")
          podTemplateFiles
        }.getOrElse(throw getError.exception)
    }

    // Step-3: build shaded flink job jar and handle extra jars
    // the output shaded jar file name like: streampark-flinkjob_myjob-test.jar
    val (shadedJar, extJarLibs) =
      execStep(3) {
        val shadedJarOutputPath = request.getShadedJarPath(buildWorkspace)
        val shadedJar =
          MavenTool.buildFatJar(request.mainClass, request.providedLibs, shadedJarOutputPath)
        logInfo(s"Output shaded flink job jar: ${shadedJar.getAbsolutePath}")
        shadedJar -> request.dependencyInfo.extJarLibs
      }.getOrElse(throw getError.exception)

    // Step-4: generate and Export flink image dockerfiles
    val (dockerfile, dockerFileTemplate) =
      execStep(4) {
        val dockerFileTemplate = {
          if (request.integrateWithHadoop) {
            FlinkHadoopDockerfileTemplate.fromSystemHadoopConf(
              buildWorkspace,
              request.flinkBaseImage,
              shadedJar.getAbsolutePath,
              extJarLibs)
          } else {
            FlinkDockerfileTemplate(
              buildWorkspace,
              request.flinkBaseImage,
              shadedJar.getAbsolutePath,
              extJarLibs)
          }
        }
        val dockerFile = dockerFileTemplate.writeDockerfile
        logInfo(
          s"Output flink dockerfile: ${dockerFile.getAbsolutePath}, content: \n${dockerFileTemplate.offerDockerfileContent}")
        dockerFile -> dockerFileTemplate
      }.getOrElse(throw getError.exception)

    val dockerConf = request.dockerConfig
    val baseImageTag = request.flinkBaseImage.trim
    val pushImageTag = {
      if (request.k8sNamespace.isEmpty || request.clusterId.isEmpty) {
        throw new IllegalArgumentException("k8sNamespace or clusterId cannot be empty")
      }
      val expectedImageTag =
        s"streampark-flinkjob-${request.k8sNamespace}-${request.clusterId}"
      compileTag(expectedImageTag, dockerConf.registerAddress, dockerConf.imageNamespace)
    }

    // Step-5: pull flink base image
    execStep(5) {
      usingDockerClient {
        dockerClient =>
          val imgExists = dockerClient.listImagesCmd().exec().exists(_.getRepoTags.exists(_.contains(baseImageTag)))
          if (imgExists) {
            logInfo(s"found local docker image $baseImageTag, no need to pull from remote.")
          } else {
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
          }
      }(err => throw new Exception(s"Pull docker image failed, imageTag=$baseImageTag", err))
    }.getOrElse(throw getError.exception)

    // Step-6: build flink image
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

    // Step-7: push flink image
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

    // Step-8:  init build workspace of ingress
    val ingressOutputPath = request.ingressTemplate match {
      case ingress if StringUtils.isBlank(ingress) =>
        skipStep(8)
        ""
      case _ =>
        execStep(8) {
          val ingressOutputPath =
            IngressController.prepareIngressTemplateFiles(buildWorkspace, request.ingressTemplate)
          logInfo(s"Export flink ingress: $ingressOutputPath")
          ingressOutputPath
        }.getOrElse(throw getError.exception)
    }

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

object FlinkK8sApplicationBuildPipeline {

  val execPool = new ThreadPoolExecutor(
    Runtime.getRuntime.availableProcessors * 5,
    Runtime.getRuntime.availableProcessors() * 10,
    60L,
    TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable](2048),
    ThreadUtils.threadFactory("streampark-docker-progress-watcher-executor"),
    new ThreadPoolExecutor.DiscardOldestPolicy)

  implicit val executor: ExecutionContext =
    ExecutionContext.fromExecutorService(execPool)

  def of(request: FlinkK8sApplicationBuildRequest): FlinkK8sApplicationBuildPipeline =
    new FlinkK8sApplicationBuildPipeline(request)

}
