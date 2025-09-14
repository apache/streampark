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

package org.apache.streampark.flink.client.`trait`

import org.apache.streampark.common.conf.ConfigConst._
import org.apache.streampark.common.enums.{ApplicationType, DevelopmentMode, ExecutionMode}
import org.apache.streampark.common.util.{DeflaterUtils, Logger, PropertiesUtils, Utils}
import org.apache.streampark.flink.client.bean._
import org.apache.streampark.flink.core.conf.FlinkRunOption
import org.apache.streampark.flink.deployment.FlinkClusterClient

import com.google.common.collect.Lists
import org.apache.commons.cli.{CommandLine, Options}
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.JobID
import org.apache.flink.client.cli._
import org.apache.flink.client.cli.CliFrontend.loadCustomCommandLines
import org.apache.flink.client.deployment.application.ApplicationConfiguration
import org.apache.flink.client.program.{ClusterClient, PackagedProgram, PackagedProgramUtils}
import org.apache.flink.configuration._
import org.apache.flink.runtime.jobgraph.{JobGraph, SavepointConfigOptions}
import org.apache.flink.util.Preconditions.checkNotNull

import java.io.File
import java.util.{Collections, List => JavaList, Map => JavaMap}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait FlinkClientTrait extends Logger {

  private[client] lazy val PARAM_KEY_FLINK_CONF = KEY_FLINK_CONF("--")
  private[client] lazy val PARAM_KEY_FLINK_SQL = KEY_FLINK_SQL("--")
  private[client] lazy val PARAM_KEY_APP_CONF = KEY_APP_CONF("--")
  private[client] lazy val PARAM_KEY_APP_NAME = KEY_APP_NAME("--")
  private[client] lazy val PARAM_KEY_FLINK_PARALLELISM = KEY_FLINK_PARALLELISM("--")

  @throws[Exception]
  def submit(submitRequest: SubmitRequest): SubmitResponse = {
    logInfo(
      s"""
         |--------------------------------------- flink job start ---------------------------------------
         |    userFlinkHome    : ${submitRequest.flinkVersion.flinkHome}
         |    flinkVersion     : ${submitRequest.flinkVersion.version}
         |    appName          : ${submitRequest.appName}
         |    devMode          : ${submitRequest.developmentMode.name()}
         |    execMode         : ${submitRequest.executionMode.name()}
         |    k8sNamespace     : ${submitRequest.kubernetesNamespace}
         |    flinkExposedType : ${submitRequest.flinkRestExposedType}
         |    clusterId        : ${submitRequest.clusterId}
         |    applicationType  : ${submitRequest.applicationType.getName}
         |    savePoint        : ${submitRequest.savePoint}
         |    properties       : ${submitRequest.properties.mkString(" ")}
         |    args             : ${submitRequest.args}
         |    appConf          : ${submitRequest.appConf}
         |    flinkBuildResult : ${submitRequest.buildResult}
         |-------------------------------------------------------------------------------------------
         |""".stripMargin)

    val flinkConfig = prepareConfig(submitRequest)
    flinkConfig.toMap.foreach(c => logInfo(s"flinkConfig:  ${c._1}: ${c._2}"))
    setConfig(submitRequest, flinkConfig)

    Try(doSubmit(submitRequest, flinkConfig)) match {
      case Success(resp) => resp
      case Failure(e) =>
        logError(
          s"flink job ${submitRequest.appName} start failed, " +
            s"executionMode: ${submitRequest.executionMode.getName}, " +
            s"detail: ${Utils.stringifyException(e)}")
        throw e
    }
  }

  private[this] def prepareConfig(submitRequest: SubmitRequest): Configuration = {

    val (commandLine, flinkConfig) = getCommandLineAndFlinkConfig(submitRequest)

    if (submitRequest.userJarFile != null) {
      val uri = PackagedProgramUtils.resolveURI(submitRequest.userJarFile.getAbsolutePath)
      val programOptions = ProgramOptions.create(commandLine)
      val executionParameters = ExecutionConfigAccessor.fromProgramOptions(
        programOptions,
        Collections.singletonList(uri.toString))
      executionParameters.applyToConfiguration(flinkConfig)
    }

    // 1) set common parameter
    flinkConfig
      .safeSet(PipelineOptions.NAME, submitRequest.effectiveAppName)
      .safeSet(DeploymentOptions.TARGET, submitRequest.executionMode.getName)
      .safeSet(SavepointConfigOptions.SAVEPOINT_PATH, submitRequest.savePoint)
      .safeSet(ApplicationConfiguration.APPLICATION_MAIN_CLASS, submitRequest.appMain)
      .safeSet(ApplicationConfiguration.APPLICATION_ARGS, extractProgramArgs(submitRequest))
      .safeSet(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, submitRequest.jobId)

    if (!submitRequest.hasProp(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key())) {
      // state.checkpoints.num-retained
      val retainedOption = CheckpointingOptions.MAX_RETAINED_CHECKPOINTS
      flinkConfig.safeSet(
        retainedOption,
        submitRequest.flinkDefaultConfiguration.get(retainedOption))
    }

    // 2) set savepoint parameter
    if (StringUtils.isNotBlank(submitRequest.savePoint)) {
      flinkConfig.safeSet(SavepointConfigOptions.SAVEPOINT_PATH, submitRequest.savePoint)
      flinkConfig.setBoolean(
        SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE,
        submitRequest.allowNonRestoredState)
    }

    // 4) set env.xx.opts parameter
    if (MapUtils.isNotEmpty(submitRequest.properties)) {
      // file.encoding...
      if (submitRequest.hasProp(CoreOptions.FLINK_JVM_OPTIONS.key())) {
        val jvmOpt = submitRequest.getProp(CoreOptions.FLINK_JVM_OPTIONS.key()).toString
        if (!jvmOpt.contains("-Dfile.encoding=")) {
          // set default file.encoding
          val opt = s"-Dfile.encoding=UTF-8 $jvmOpt"
          submitRequest.properties.put(CoreOptions.FLINK_JVM_OPTIONS.key(), opt)
        }
      }

      submitRequest.properties
        .filter(_._1.startsWith("env."))
        .foreach(
          x => {
            logInfo(s"env opts:  ${x._1}: ${x._2}")
            flinkConfig.setString(x._1, x._2.toString)
          })
    }

    flinkConfig
  }

  def setConfig(submitRequest: SubmitRequest, flinkConf: Configuration): Unit

  @throws[Exception]
  def triggerSavepoint(savepointRequest: TriggerSavepointRequest): SavepointResponse = {
    logInfo(
      s"""
         |----------------------------------------- flink job trigger savepoint ---------------------
         |     userFlinkHome  : ${savepointRequest.flinkVersion.flinkHome}
         |     flinkVersion   : ${savepointRequest.flinkVersion.version}
         |     clusterId      : ${savepointRequest.clusterId}
         |     savePointPath  : ${savepointRequest.savepointPath}
         |     k8sNamespace   : ${savepointRequest.kubernetesNamespace}
         |     appId          : ${savepointRequest.clusterId}
         |     jobId          : ${savepointRequest.jobId}
         |-------------------------------------------------------------------------------------------
         |""".stripMargin)
    val flinkConf = new Configuration()
    doTriggerSavepoint(savepointRequest, flinkConf)
  }

  @throws[Exception]
  def cancel(cancelRequest: CancelRequest): CancelResponse = {
    logInfo(
      s"""
         |----------------------------------------- flink job cancel --------------------------------
         |     userFlinkHome     : ${cancelRequest.flinkVersion.flinkHome}
         |     flinkVersion      : ${cancelRequest.flinkVersion.version}
         |     clusterId         : ${cancelRequest.clusterId}
         |     withSavePoint     : ${cancelRequest.withSavepoint}
         |     savePointPath     : ${cancelRequest.savepointPath}
         |     withDrain         : ${cancelRequest.withDrain}
         |     k8sNamespace      : ${cancelRequest.kubernetesNamespace}
         |     appId             : ${cancelRequest.clusterId}
         |     jobId             : ${cancelRequest.jobId}
         |-------------------------------------------------------------------------------------------
         |""".stripMargin)
    val flinkConf = new Configuration()
    doCancel(cancelRequest, flinkConf)
  }

  @throws[Exception]
  def doSubmit(submitRequest: SubmitRequest, flinkConf: Configuration): SubmitResponse

  @throws[Exception]
  def doTriggerSavepoint(
      request: TriggerSavepointRequest,
      flinkConf: Configuration): SavepointResponse

  @throws[Exception]
  def doCancel(cancelRequest: CancelRequest, flinkConf: Configuration): CancelResponse

  def trySubmit(submitRequest: SubmitRequest, flinkConfig: Configuration, jarFile: File)(
      jobGraphFunc: (SubmitRequest, Configuration, File) => SubmitResponse,
      restApiFunc: (SubmitRequest, Configuration, File) => SubmitResponse): SubmitResponse = {
    // Prioritize using JobGraph submit plan while using Rest API submit plan as backup
    Try {
      logInfo(s"[flink-submit] Submit job with JobGraph Plan.")
      jobGraphFunc(submitRequest, flinkConfig, jarFile)
    } match {
      case Failure(e) =>
        Try(restApiFunc(submitRequest, flinkConfig, jarFile)) match {
          case Success(r) => r
          case Failure(e1) =>
            throw new RuntimeException(
              s"""\n
                 |[flink-submit] Both JobGraph submit plan and Rest API submit plan all failed!
                 |JobGraph Submit plan failed detail:
                 |------------------------------------------------------------------
                 |${Utils.stringifyException(e)}
                 |------------------------------------------------------------------
                 |
                 | RestAPI Submit plan failed detail:
                 | ------------------------------------------------------------------
                 |${Utils.stringifyException(e1)}
                 |------------------------------------------------------------------
                 |""".stripMargin)
        }
      case Success(v) => v
    }
  }

  private[client] def getJobGraph(
      flinkConfig: Configuration,
      submitRequest: SubmitRequest,
      jarFile: File): (PackagedProgram, JobGraph) = {

    val packageProgram = PackagedProgram.newBuilder
      .setJarFile(jarFile)
      .setEntryPointClassName(
        flinkConfig.getOptional(ApplicationConfiguration.APPLICATION_MAIN_CLASS).get())
      .setSavepointRestoreSettings(submitRequest.savepointRestoreSettings)
      .setArguments(
        flinkConfig
          .getOptional(ApplicationConfiguration.APPLICATION_ARGS)
          .orElse(Lists.newArrayList()): _*)
      .build()

    val jobGraph = PackagedProgramUtils.createJobGraph(
      packageProgram,
      flinkConfig,
      getParallelism(submitRequest),
      null,
      false)
    packageProgram -> jobGraph
  }

  private[client] def getJobID(jobId: String) = Try(JobID.fromHexString(jobId)) match {
    case Success(id) => id
    case Failure(e) => throw new CliArgsException(e.getMessage)
  }

  // ----------Public Method end ------------------

  private[client] def validateAndGetActiveCommandLine(
      customCommandLines: JavaList[CustomCommandLine],
      commandLine: CommandLine): CustomCommandLine = {
    val line = checkNotNull(commandLine)
    logInfo(s"Custom commandline: $customCommandLines")
    for (cli <- customCommandLines) {
      val isActive = cli.isActive(line)
      logInfo(s"Checking custom commandline $cli, isActive: $isActive")
      if (isActive) return cli
    }
    throw new IllegalStateException("No valid command-line found.")
  }

  private[this] def getCustomCommandLines(flinkHome: String): JavaList[CustomCommandLine] = {
    // 1. find the configuration directory
    val configurationDirectory = s"$flinkHome/conf"
    // 2. load the custom command lines
    val flinkConfig =
      Try(FlinkGlobalConfiguration.loadConfiguration(s"$flinkHome/conf"))
        .getOrElse(new Configuration())
    loadCustomCommandLines(flinkConfig, configurationDirectory)
  }

  private[client] def getParallelism(submitRequest: SubmitRequest): Integer = {
    if (submitRequest.properties.containsKey(KEY_FLINK_PARALLELISM())) {
      Integer.valueOf(submitRequest.properties.get(KEY_FLINK_PARALLELISM()).toString)
    } else {
      submitRequest.flinkDefaultConfiguration
        .getInteger(CoreOptions.DEFAULT_PARALLELISM, CoreOptions.DEFAULT_PARALLELISM.defaultValue())
    }
  }

  private[this] def getCommandLineAndFlinkConfig(
      submitRequest: SubmitRequest): (CommandLine, Configuration) = {

    val commandLineOptions = getCommandLineOptions(submitRequest.flinkVersion.flinkHome)

    // read and verify user config...
    val cliArgs = {
      val optionMap = new mutable.HashMap[String, Any]()
      submitRequest.appOption
        .filter {
          opt =>
            val verify = commandLineOptions.hasOption(opt._1)
            if (!verify) {
              logWarn(s"param:${opt._1} is error,skip it.")
            }
            verify
        }
        .foreach {
          opt =>
            val option = commandLineOptions.getOption(opt._1.trim).getOpt
            Try(opt._2.toBoolean).getOrElse(opt._2) match {
              case b if b.isInstanceOf[Boolean] =>
                if (b.asInstanceOf[Boolean]) {
                  optionMap += s"-$option" -> true
                }
              case v => optionMap += s"-$option" -> v
            }
        }

      // fromSavePoint
      if (submitRequest.savePoint != null) {
        optionMap += s"-${FlinkRunOption.SAVEPOINT_PATH_OPTION.getOpt}" -> submitRequest.savePoint
      }

      Seq("-e", "--executor", "-t", "--target").foreach(optionMap.remove)
      if (submitRequest.executionMode != null) {
        optionMap += "-t" -> submitRequest.executionMode.getName
      }

      val array = new ArrayBuffer[String]()
      optionMap.foreach {
        opt =>
          array += opt._1
          if (opt._2.isInstanceOf[String]) {
            array += opt._2.toString
          }
      }

      // app properties
      if (MapUtils.isNotEmpty(submitRequest.properties)) {
        submitRequest.properties.foreach {
          key =>
            if (!key._1.startsWith(CoreOptions.FLINK_JVM_OPTIONS.key())) {
              logInfo(s"submit application dynamicProperties:  ${key._1} :${key._2}")
              array += s"-D${key._1}=${key._2}"
            }
        }
      }
      array.toArray
    }

    logger.info(s"cliArgs: ${cliArgs.mkString(" ")}")

    val commandLine = FlinkRunOption.parse(commandLineOptions, cliArgs, true)

    val activeCommandLine = {
      val customCommandLines: JavaList[CustomCommandLine] = {
        // 1. find the configuration directory
        val configurationDirectory = s"${submitRequest.flinkVersion.flinkHome}/conf"
        // 2. load the custom command lines
        loadCustomCommandLines(submitRequest.flinkDefaultConfiguration, configurationDirectory)
      }
      validateAndGetActiveCommandLine(customCommandLines, commandLine)
    }

    val configuration = new Configuration(submitRequest.flinkDefaultConfiguration)
    configuration.addAll(activeCommandLine.toConfiguration(commandLine))

    commandLine -> configuration
  }

  private[client] def getCommandLineOptions(flinkHome: String) = {
    val customCommandLines = getCustomCommandLines(flinkHome)
    val customCommandLineOptions = new Options
    for (customCommandLine <- customCommandLines) {
      customCommandLine.addGeneralOptions(customCommandLineOptions)
      customCommandLine.addRunOptions(customCommandLineOptions)
    }
    FlinkRunOption.mergeOptions(CliFrontendParser.getRunCommandOptions, customCommandLineOptions)
  }

  private[client] def extractConfiguration(
      flinkHome: String,
      properties: JavaMap[String, Any]): Configuration = {
    val commandLine = {
      val commandLineOptions = getCommandLineOptions(flinkHome)
      // read and verify user config...
      val cliArgs = {
        val array = new ArrayBuffer[String]()
        // The priority of the parameters defined on the page is greater than the app conf file, property parameters etc.
        if (MapUtils.isNotEmpty(properties)) {
          properties.foreach(
            x => {
              array += s"-D${x._1}=${x._2.toString.trim}"
              logInfo(s"deploy cluster dynamicProperties:  ${x._1} :${x._2}")
            })
        }
        array.toArray
      }
      FlinkRunOption.parse(commandLineOptions, cliArgs, true)
    }

    val activeCommandLine =
      validateAndGetActiveCommandLine(getCustomCommandLines(flinkHome), commandLine)

    val flinkDefaultConfiguration =
      Try(FlinkGlobalConfiguration.loadConfiguration(s"$flinkHome/conf"))
        .getOrElse(new Configuration())

    val configuration = new Configuration(flinkDefaultConfiguration)
    configuration.addAll(activeCommandLine.toConfiguration(commandLine))
    configuration
  }

  private[this] def extractProgramArgs(submitRequest: SubmitRequest): JavaList[String] = {
    val programArgs = new ArrayBuffer[String]()
    programArgs ++= PropertiesUtils.extractArguments(submitRequest.args)

    if (submitRequest.applicationType == ApplicationType.STREAMPARK_FLINK) {
      programArgs += PARAM_KEY_FLINK_CONF += submitRequest.flinkYaml
      programArgs += PARAM_KEY_APP_NAME += DeflaterUtils.zipString(submitRequest.effectiveAppName)
      programArgs += PARAM_KEY_FLINK_PARALLELISM += getParallelism(submitRequest).toString

      submitRequest.developmentMode match {
        case DevelopmentMode.FLINK_SQL =>
          programArgs += PARAM_KEY_FLINK_SQL += submitRequest.flinkSQL
          if (submitRequest.appConf != null) {
            programArgs += PARAM_KEY_APP_CONF += submitRequest.appConf
          }
        case _ if Try(!submitRequest.appConf.startsWith("json:")).getOrElse(true) =>
          programArgs += PARAM_KEY_APP_CONF += submitRequest.appConf
      }
    }
    Lists.newArrayList(programArgs: _*)
  }

  implicit private[client] class EnhanceFlinkConfiguration(flinkConfig: Configuration) {
    def safeSet[T](option: ConfigOption[T], value: T): Configuration = {
      flinkConfig match {
        case conf if value != null && value.toString.nonEmpty => conf.set(option, value)
        case conf => conf
      }
    }
  }

  implicit private[client] class EnhanceConfiguration(request: SubmitRequest) {
    lazy val flinkDefaultConfiguration: Configuration = {
      Try(
        FlinkGlobalConfiguration.loadConfiguration(
          s"${request.flinkVersion.flinkHome}/conf")) match {
        case Success(value) =>
          request.executionMode match {
            case ExecutionMode.YARN_SESSION | ExecutionMode.KUBERNETES_NATIVE_SESSION |
                ExecutionMode.REMOTE =>
              value
            case _ =>
              value
                .keySet()
                .foreach(
                  k => {
                    val v = value.getString(k, null)
                    if (v != null) {
                      val result = v
                        .replaceAll(
                          "\\$\\{job(Name|name)}|\\$job(Name|name)",
                          request.effectiveAppName)
                        .replaceAll("\\$\\{job(Id|id)}|\\$job(Id|id)", request.id.toString)
                      value.setString(k, result)
                    }
                  })
              value
          }
        case _ => new Configuration()
      }
    }

  }

  private[client] def cancelJob(
      cancelRequest: CancelRequest,
      jobID: JobID,
      client: ClusterClient[_]): String = {

    val clientWrapper = new FlinkClusterClient(client)
    val withSavepoint = Try(cancelRequest.withSavepoint).getOrElse(false)
    val withDrain = Try(cancelRequest.withDrain).getOrElse(false)

    (withSavepoint, withDrain) match {
      case (false, false) =>
        client.cancel(jobID).get()
        null
      case (true, false) =>
        clientWrapper
          .cancelWithSavepoint(jobID, cancelRequest.savepointPath)
          .get()
      case (_, _) =>
        clientWrapper
          .stopWithSavepoint(jobID, cancelRequest.withDrain, cancelRequest.savepointPath)
          .get()
    }
  }

  private[client] def triggerSavepoint(
      savepointRequest: TriggerSavepointRequest,
      jobID: JobID,
      client: ClusterClient[_]): String = {
    val clientWrapper = new FlinkClusterClient(client)
    clientWrapper.triggerSavepoint(jobID, savepointRequest.savepointPath).get()
  }

  def closeSubmit(submitRequest: SubmitRequest, close: AutoCloseable*): Unit = {
    close.foreach(
      x => {
        if (x.isInstanceOf[PackagedProgram]) {
          if (submitRequest.safePackageProgram) {
            Utils.close(x)
          }
        } else {
          Utils.close(x)
        }
      })
  }

}
