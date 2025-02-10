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

import org.apache.streampark.common.conf.ConfigKeys._
import org.apache.streampark.common.conf.Workspace
import org.apache.streampark.common.constants.Constants
import org.apache.streampark.common.enums._
import org.apache.streampark.common.fs.FsOperator
import org.apache.streampark.common.util._
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.flink.client.bean._
import org.apache.streampark.flink.core.FlinkClusterClient
import org.apache.streampark.flink.core.conf.FlinkRunOption

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
import org.apache.flink.python.PythonOptions
import org.apache.flink.runtime.jobgraph.{JobGraph, SavepointConfigOptions}
import org.apache.flink.util.Preconditions.checkNotNull

import java.io.File
import java.util.Collections

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

trait FlinkClientTrait extends Logger {

  private[client] lazy val PARAM_KEY_FLINK_CONF = KEY_FLINK_CONF(PARAM_PREFIX)
  private[client] lazy val PARAM_KEY_FLINK_SQL = KEY_FLINK_SQL(PARAM_PREFIX)
  private[client] lazy val PARAM_KEY_APP_CONF = KEY_APP_CONF(PARAM_PREFIX)
  private[client] lazy val PARAM_KEY_APP_NAME = KEY_APP_NAME(PARAM_PREFIX)
  private[client] lazy val PARAM_KEY_FLINK_PARALLELISM = KEY_FLINK_PARALLELISM(PARAM_PREFIX)

  @throws[Exception]
  def submit(submitRequest: SubmitRequest): SubmitResponse = {
    logInfo(
      s"""
         |--------------------------------------- flink job start ---------------------------------------
         |    userFlinkHome    : ${submitRequest.flinkVersion.flinkHome}
         |    flinkVersion     : ${submitRequest.flinkVersion.version}
         |    appName          : ${submitRequest.effectiveAppName}
         |    jobType          : ${submitRequest.jobType.name()}
         |    deployMode       : ${submitRequest.deployMode.name()}
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

    // prepare flink config
    val flinkConfig = prepareConfig(submitRequest)

    setConfig(submitRequest, flinkConfig)

    Try(doSubmit(submitRequest, flinkConfig)) match {
      case Success(resp) => resp
      case Failure(e) =>
        logError(
          s"flink job ${submitRequest.appName} start failed, " +
            s"deployMode: ${submitRequest.deployMode.getName}, " +
            s"detail: ${ExceptionUtils.stringifyException(e)}")
        throw e
    }
  }

  private[this] def prepareConfig(submitRequest: SubmitRequest): Configuration = {

    val (commandLine, flinkConfig) = getCommandLineAndFlinkConfig(submitRequest)

    submitRequest.jobType match {
      case FlinkJobType.PYFLINK =>
        val pythonVenv: String = Workspace.local.APP_PYTHON_VENV
        AssertUtils.required(FsOperator.lfs.exists(pythonVenv), s"$pythonVenv File does not exist")

        flinkConfig
          // python.archives
          .safeSet(PythonOptions.PYTHON_ARCHIVES, pythonVenv)
          // python.client.executable
          .safeSet(PythonOptions.PYTHON_CLIENT_EXECUTABLE, Constants.PYTHON_EXECUTABLE)
          // python.executable
          .safeSet(PythonOptions.PYTHON_EXECUTABLE, Constants.PYTHON_EXECUTABLE)

        val flinkOptPath: String =
          System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR)
        if (StringUtils.isBlank(flinkOptPath)) {
          logWarn(s"Get environment variable ${ConfigConstants.ENV_FLINK_OPT_DIR} fail")
          val flinkHome = submitRequest.flinkVersion.flinkHome
          SystemPropertyUtils.setEnv(ConfigConstants.ENV_FLINK_OPT_DIR, s"$flinkHome/opt")
          logInfo(
            s"Set temporary environment variables ${ConfigConstants.ENV_FLINK_OPT_DIR} = $flinkHome/opt")
        }
      case _ =>
        if (submitRequest.userJarFile != null) {
          val uri = PackagedProgramUtils.resolveURI(submitRequest.userJarFile.getAbsolutePath)
          val programOptions = ProgramOptions.create(commandLine)
          val executionParameters = ExecutionConfigAccessor.fromProgramOptions(
            programOptions,
            Collections.singletonList(uri.toString))
          executionParameters.applyToConfiguration(flinkConfig)
        }
    }

    // 1) set common parameter
    flinkConfig
      .safeSet(PipelineOptions.NAME, submitRequest.effectiveAppName)
      .safeSet(DeploymentOptions.TARGET, submitRequest.deployMode.getName)
      .safeSet(SavepointConfigOptions.SAVEPOINT_PATH, submitRequest.savePoint)
      .safeSet(ApplicationConfiguration.APPLICATION_MAIN_CLASS, submitRequest.appMain)
      .safeSet(ApplicationConfiguration.APPLICATION_ARGS, extractProgramArgs(submitRequest))
      .safeSet(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, submitRequest.jobId)

    if (!submitRequest.hasProp(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key())) {
      val flinkDefaultConfiguration = getFlinkDefaultConfiguration(
        submitRequest.flinkVersion.flinkHome)
      // state.checkpoints.num-retained
      val retainedOption = CheckpointingOptions.MAX_RETAINED_CHECKPOINTS
      flinkConfig.safeSet(retainedOption, flinkDefaultConfiguration.get(retainedOption))
    }

    // 2) set savepoint parameter
    if (StringUtils.isNotBlank(submitRequest.savePoint)) {
      flinkConfig.safeSet(SavepointConfigOptions.SAVEPOINT_PATH, submitRequest.savePoint)
      flinkConfig.setBoolean(
        SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE,
        submitRequest.allowNonRestoredState)
      val enableRestoreMode =
        submitRequest.restoreMode != null && submitRequest.flinkVersion
          .checkVersion(FlinkRestoreMode.SINCE_FLINK_VERSION)
      if (enableRestoreMode) {
        flinkConfig.setString(FlinkRestoreMode.RESTORE_MODE, submitRequest.restoreMode.getName);
      }
    }

    // 4) set env.xx.opts parameter
    if (MapUtils.isNotEmpty(submitRequest.properties)) {
      // file.encoding...
      if (submitRequest.hasProp(CoreOptions.FLINK_JVM_OPTIONS.key())) {
        val jvmOpt =
          submitRequest.getProp(CoreOptions.FLINK_JVM_OPTIONS.key()).toString
        if (!jvmOpt.contains("-Dfile.encoding=")) {
          // set default file.encoding
          val opt = s"-Dfile.encoding=UTF-8 $jvmOpt"
          submitRequest.properties.put(CoreOptions.FLINK_JVM_OPTIONS.key(), opt)
        }
      }

      submitRequest.properties
        .filter(_._1.startsWith("env."))
        .foreach(x => {
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
         |     nativeFormat   : ${savepointRequest.nativeFormat}
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
         |     nativeFormat      : ${cancelRequest.nativeFormat}
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
              s"""
                 |[flink-submit] Both JobGraph submit plan and Rest API submit plan all failed!
                 |JobGraph Submit plan failed detail:
                 |------------------------------------------------------------------
                 |${ExceptionUtils.stringifyException(e)}
                 |------------------------------------------------------------------
                 |
                 | RestAPI Submit plan failed detail:
                 | ------------------------------------------------------------------
                 |${ExceptionUtils.stringifyException(e1)}
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

    val packagedProgramBuilder = {
      val builder = PackagedProgram.newBuilder
        .setSavepointRestoreSettings(submitRequest.savepointRestoreSettings)
        .setEntryPointClassName(
          flinkConfig
            .getOptional(ApplicationConfiguration.APPLICATION_MAIN_CLASS)
            .get())
        .setArguments(
          flinkConfig
            .getOptional(ApplicationConfiguration.APPLICATION_ARGS)
            .orElse(Lists.newArrayList()): _*)

      submitRequest.jobType match {
        case FlinkJobType.PYFLINK =>
          if (submitRequest.libs.nonEmpty) {
            // BUG: https://github.com/apache/streampark/issues/3761
            // builder.setUserClassPaths(Lists.newArrayList(submitRequest.libs: _*))
          }
        case _ =>
          builder
            .setJarFile(jarFile)
        // BUG: https://github.com/apache/streampark/issues/3761
        // .setUserClassPaths(Lists.newArrayList(submitRequest.classPaths: _*))
      }
      builder
    }

    val packageProgram = packagedProgramBuilder.build()
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

  private[client] def getFlinkDefaultConfiguration(flinkHome: String): Configuration = {
    Try(GlobalConfiguration.loadConfiguration(s"$flinkHome/conf"))
      .getOrElse(new Configuration())
  }

  private[client] def getOptionFromDefaultFlinkConfig[T](
      flinkHome: String,
      option: ConfigOption[T]): T = {
    getFlinkDefaultConfiguration(flinkHome).get(option)
  }

  private[this] def getCustomCommandLines(flinkHome: String): JavaList[CustomCommandLine] = {
    val flinkDefaultConfiguration: Configuration = getFlinkDefaultConfiguration(flinkHome)
    // 1. find the configuration directory
    val confDir = s"$flinkHome/conf"
    // 2. load the custom command lines
    loadCustomCommandLines(flinkDefaultConfiguration, confDir)
  }

  private[client] def getParallelism(submitRequest: SubmitRequest): Integer = {
    if (submitRequest.hasProp(KEY_FLINK_PARALLELISM())) {
      Integer.valueOf(submitRequest.getProp(KEY_FLINK_PARALLELISM()).toString)
    } else {
      getFlinkDefaultConfiguration(submitRequest.flinkVersion.flinkHome)
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
        .foreach {
          opt =>
            val verify = commandLineOptions.hasOption(opt._1)
            if (!verify) {
              logWarn(s"param:${opt._1} is error,skip it.")
            } else {
              val option = commandLineOptions.getOption(opt._1.trim).getOpt
              Try(opt._2.toBoolean).getOrElse(opt._2) match {
                case b if b.isInstanceOf[Boolean] =>
                  if (b.asInstanceOf[Boolean]) {
                    optionMap += s"-$option" -> true
                  }
                case v => optionMap += s"-$option" -> v
              }
            }
        }

      // fromSavePoint
      if (submitRequest.savePoint != null) {
        optionMap += s"-${FlinkRunOption.SAVEPOINT_PATH_OPTION.getOpt}" -> submitRequest.savePoint
      }

      Seq("-e", "--executor", "-t", "--target").foreach(optionMap.remove)
      if (submitRequest.deployMode != null) {
        optionMap += "-t" -> submitRequest.deployMode.getName
      }

      val array = new ArrayBuffer[String]()
      optionMap.foreach(opt => {
        array += opt._1
        opt._2 match {
          case v: String => array += v
          case _ =>
        }
      })

      // app properties
      if (MapUtils.isNotEmpty(submitRequest.properties)) {
        submitRequest.properties.foreach {
          key =>
            if (!key._1.startsWith("env.")) {
              logInfo(s"application dynamicProperties:  ${key._1} :${key._2}")
              array += s"-D${key._1}=${key._2}"
            }
        }
      }
      array.toArray
    }

    logger.info(s"cliArgs: ${cliArgs.mkString(" ")}")

    val commandLine = FlinkRunOption.parse(commandLineOptions, cliArgs, true)

    val activeCommandLine = validateAndGetActiveCommandLine(
      getCustomCommandLines(submitRequest.flinkVersion.flinkHome),
      commandLine)

    val configuration =
      applyConfiguration(submitRequest.flinkVersion.flinkHome, activeCommandLine, commandLine)

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
          properties.foreach(x => array += s"-D${x._1}=${x._2.toString.trim}")
        }
        array.toArray
      }
      FlinkRunOption.parse(commandLineOptions, cliArgs, true)
    }
    val activeCommandLine =
      validateAndGetActiveCommandLine(getCustomCommandLines(flinkHome), commandLine)
    val flinkConfig =
      applyConfiguration(flinkHome, activeCommandLine, commandLine)
    flinkConfig
  }

  private[this] def extractProgramArgs(submitRequest: SubmitRequest): JavaList[String] = {
    val programArgs = new ArrayBuffer[String]()
    programArgs ++= PropertiesUtils.extractArguments(submitRequest.args)

    if (submitRequest.applicationType == ApplicationType.STREAMPARK_FLINK) {

      programArgs += PARAM_KEY_FLINK_CONF += submitRequest.flinkYaml
      programArgs += PARAM_KEY_APP_NAME += DeflaterUtils.zipString(submitRequest.effectiveAppName)
      programArgs += PARAM_KEY_FLINK_PARALLELISM += getParallelism(submitRequest).toString

      submitRequest.jobType match {
        case FlinkJobType.FLINK_SQL | FlinkJobType.FLINK_CDC =>
          programArgs += PARAM_KEY_FLINK_SQL += submitRequest.flinkSQL
          if (submitRequest.appConf != null) {
            programArgs += PARAM_KEY_APP_CONF += submitRequest.appConf
          }
        case _
            if Try(!submitRequest.appConf.startsWith("json:"))
              .getOrElse(true) =>
          programArgs += PARAM_KEY_APP_CONF += submitRequest.appConf
      }
    }

    // execution.runtime-mode
    Try(
      submitRequest
        .properties(ExecutionOptions.RUNTIME_MODE.key())
        .toString) match {
      case Success(runtimeMode) =>
        programArgs += s"--${ExecutionOptions.RUNTIME_MODE.key()}"
        programArgs += runtimeMode
      case _ =>
    }

    if (submitRequest.jobType == FlinkJobType.PYFLINK) {
      // TODO why deployMode is not yarn-application ???
      if (submitRequest.deployMode != FlinkDeployMode.YARN_APPLICATION) {
        // python file
        programArgs.add("-py")
        programArgs.add(submitRequest.userJarFile.getAbsolutePath)
      }
    }

    Lists.newArrayList(programArgs: _*)
  }

  private[this] def applyConfiguration(
      flinkHome: String,
      activeCustomCommandLine: CustomCommandLine,
      commandLine: CommandLine): Configuration = {

    require(activeCustomCommandLine != null, "activeCustomCommandLine must not be null.")
    val configuration = new Configuration()
    val flinkDefaultConfiguration = getFlinkDefaultConfiguration(flinkHome)
    flinkDefaultConfiguration.keySet.foreach(key => {
      flinkDefaultConfiguration.getString(key, null) match {
        case v if v != null => configuration.setString(key, v)
        case _ =>
      }
    })
    configuration.addAll(activeCustomCommandLine.toConfiguration(commandLine))
    configuration
  }

  implicit private[client] class EnhanceFlinkConfiguration(flinkConfig: Configuration) {
    def safeSet[T](option: ConfigOption[T], value: T): Configuration = {
      flinkConfig match {
        case conf if value != null && value.toString.nonEmpty =>
          conf.set(option, value)
        case conf => conf
      }
    }
  }

  private[client] def cancelJob(
      cancelRequest: CancelRequest,
      jobID: JobID,
      client: ClusterClient[_]): String = {

    val savePointDir: String = tryGetSavepointPathIfNeed(cancelRequest)

    val clientWrapper = new FlinkClusterClient(client)
    val withSavepoint = Try(cancelRequest.withSavepoint).getOrElse(false)
    val withDrain = Try(cancelRequest.withDrain).getOrElse(false)

    (withSavepoint, withDrain) match {
      case (false, false) =>
        client.cancel(jobID).get()
        null
      case (_, _) =>
        clientWrapper
          .stopWithSavepoint(
            jobID,
            cancelRequest.withDrain,
            savePointDir,
            cancelRequest.nativeFormat)
          .get()
    }
  }

  private def tryGetSavepointPathIfNeed(request: SavepointRequestTrait): String = {
    if (!request.withSavepoint) null
    else {
      if (StringUtils.isNotBlank(request.savepointPath)) {
        request.savepointPath
      } else {
        val configDir = getOptionFromDefaultFlinkConfig[String](
          request.flinkVersion.flinkHome,
          ConfigOptions
            .key(CheckpointingOptions.SAVEPOINT_DIRECTORY.key())
            .stringType()
            .defaultValue {
              if (request.deployMode == FlinkDeployMode.YARN_APPLICATION) {
                Workspace.remote.APP_SAVEPOINTS
              } else null
            })

        AssertUtils.required(
          StringUtils.isNotBlank(configDir),
          s"[StreamPark] deployMode: ${request.deployMode.getName}, savePoint path is null or invalid.")
        configDir
      }
    }
  }

  private[client] def triggerSavepoint(
      savepointRequest: TriggerSavepointRequest,
      jobID: JobID,
      client: ClusterClient[_]): String = {
    val savepointPath = tryGetSavepointPathIfNeed(savepointRequest)
    val clientWrapper = new FlinkClusterClient(client)
    clientWrapper
      .triggerSavepoint(jobID, savepointPath, savepointRequest.nativeFormat)
      .get()
  }

  def closeSubmit(submitRequest: SubmitRequest, close: AutoCloseable*): Unit = {
    close.foreach(x => {
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
