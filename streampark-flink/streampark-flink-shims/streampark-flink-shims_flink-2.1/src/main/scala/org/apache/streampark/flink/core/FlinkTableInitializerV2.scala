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

package org.apache.streampark.flink.core

import org.apache.streampark.common.conf.ConfigKeys._
import org.apache.streampark.common.enums.{ApiType, PlannerType}
import org.apache.streampark.common.enums.ApiType.ApiType
import org.apache.streampark.common.util.{DeflaterUtils, PropertiesUtils}
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.flink.core.EnhancerImplicit._
import org.apache.streampark.flink.core.conf.FlinkConfiguration

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableConfig, TableEnvironment}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.util.ParameterTool

import java.io.File

import scala.collection.{mutable, Map}
import scala.util.{Failure, Success, Try}

private[flink] object FlinkTableInitializerV2 {

  def initialize(
      args: Array[String],
      config: (TableConfig, ParameterTool) => Unit): (ParameterTool, TableEnvironment) = {
    val flinkInitializer = new FlinkTableInitializerV2(args, ApiType.SCALA)
    flinkInitializer.tableConfFunc = config
    (flinkInitializer.configuration.parameter, flinkInitializer.tableEnv)
  }

  def initialize(args: TableEnvConfig): (ParameterTool, TableEnvironment) = {
    val flinkInitializer = new FlinkTableInitializerV2(args.args, ApiType.JAVA)
    flinkInitializer.javaTableEnvConfFunc = args.conf
    (flinkInitializer.configuration.parameter, flinkInitializer.tableEnv)
  }

  def initialize(
      args: Array[String],
      configStream: (StreamExecutionEnvironment, ParameterTool) => Unit,
      configTable: (TableConfig, ParameterTool) => Unit): (ParameterTool, StreamExecutionEnvironment, StreamTableEnvironment) = {

    val flinkInitializer = new FlinkTableInitializerV2(args, ApiType.SCALA)
    flinkInitializer.streamEnvConfFunc = configStream
    flinkInitializer.tableConfFunc = configTable
    (
      flinkInitializer.configuration.parameter,
      flinkInitializer.streamEnv,
      flinkInitializer.streamTableEnv)
  }

  def initialize(
      args: StreamTableEnvConfig): (ParameterTool, StreamExecutionEnvironment, StreamTableEnvironment) = {
    val flinkInitializer = new FlinkTableInitializerV2(args.args, ApiType.JAVA)
    flinkInitializer.javaStreamEnvConfFunc = args.streamConfig
    flinkInitializer.javaTableEnvConfFunc = args.tableConfig
    (
      flinkInitializer.configuration.parameter,
      flinkInitializer.streamEnv,
      flinkInitializer.streamTableEnv)
  }

}

private[flink] class FlinkTableInitializerV2(args: Array[String], apiType: ApiType)
  extends FlinkStreamingInitializerV2(args, apiType) {

  private[this] lazy val envSettings = {

    val builder = EnvironmentSettings.newInstance()

    Try(PlannerType.withName(parameter.get(KEY_FLINK_TABLE_PLANNER)))
      .getOrElse(PlannerType.BLINK) match {
      case PlannerType.BLINK =>
        val useBlinkPlanner =
          Try(builder.getClass.getDeclaredMethod("useBlinkPlanner"))
            .getOrElse(null)
        if (useBlinkPlanner == null) {
          logWarn("useBlinkPlanner deprecated")
        } else {
          useBlinkPlanner.setAccessible(true)
          useBlinkPlanner.invoke(builder)
          logInfo("blinkPlanner will be used.")
        }
      case PlannerType.OLD =>
        val useOldPlanner = Try(builder.getClass.getDeclaredMethod("useOldPlanner")).getOrElse(null)
        if (useOldPlanner == null) {
          logWarn("useOldPlanner deprecated")
        } else {
          useOldPlanner.setAccessible(true)
          useOldPlanner.invoke(builder)
          logInfo("useOldPlanner will be used.")
        }
      case PlannerType.ANY =>
        val useAnyPlanner = Try(builder.getClass.getDeclaredMethod("useAnyPlanner")).getOrElse(null)
        if (useAnyPlanner == null) {
          logWarn("useAnyPlanner deprecated")
        } else {
          logInfo("useAnyPlanner will be used.")
          useAnyPlanner.setAccessible(true)
          useAnyPlanner.invoke(builder)
        }
    }

    parameter.get(KEY_FLINK_CONF(), null) match {
      case null | "" =>
        throw new ExceptionInInitializerError(
          "[StreamPark] Usage:can't find config,please set \"--flink.conf $conf \" in main arguments")
      case conf => builder.withConfiguration(
          Configuration.fromMap(PropertiesUtils.fromYamlText(DeflaterUtils.unzipString(conf))))
    }
    val buildWith =
      (parameter.get(KEY_FLINK_TABLE_CATALOG), parameter.get(KEY_FLINK_TABLE_DATABASE))
    buildWith match {
      case (x: String, y: String) if x != null && y != null =>
        logInfo(s"with built in catalog: $x")
        logInfo(s"with built in database: $y")
        builder.withBuiltInCatalogName(x)
        builder.withBuiltInDatabaseName(y)
      case (x: String, _) if x != null =>
        logInfo(s"with built in catalog: $x")
        builder.withBuiltInCatalogName(x)
      case (_, y: String) if y != null =>
        logInfo(s"with built in database: $y")
        builder.withBuiltInDatabaseName(y)
      case _ =>
    }
    builder
  }

  lazy val tableEnv: TableEnvironment = {
    logInfo(s"job working in batch mode")
    envSettings.inBatchMode()
    val tableEnv = TableEnvironment.create(envSettings.build()).setAppName
    apiType match {
      case ApiType.JAVA if javaTableEnvConfFunc != null =>
        javaTableEnvConfFunc.configuration(tableEnv.getConfig, parameter)
      case ApiType.SCALA if tableConfFunc != null =>
        tableConfFunc(tableEnv.getConfig, parameter)
      case _ =>
    }
    tableEnv
  }

  lazy val streamTableEnv: StreamTableEnvironment = {
    logInfo(s"components should work in streaming mode")
    envSettings.inStreamingMode()
    val setting = envSettings.build()

    if (streamEnvConfFunc != null) {
      streamEnvConfFunc(streamEnv, parameter)
    }
    if (javaStreamEnvConfFunc != null) {
      javaStreamEnvConfFunc.configuration(streamEnv, parameter)
    }
    val streamTableEnv =
      StreamTableEnvironment.create(streamEnv, setting).setAppName
    apiType match {
      case ApiType.JAVA if javaTableEnvConfFunc != null =>
        javaTableEnvConfFunc.configuration(streamTableEnv.getConfig, parameter)
      case ApiType.SCALA if tableConfFunc != null =>
        tableConfFunc(streamTableEnv.getConfig, parameter)
      case _ =>
    }
    streamTableEnv
  }

  /** In case of table SQL, the parameter conf is not required, it depends on the developer. */

  override def initParameter(): FlinkConfiguration = {
    val configuration = {
      val argsMap = ParameterTool.fromArgs(args)
      argsMap.get(KEY_APP_CONF(), null) match {
        case null | "" =>
          logWarn("Usage:can't find config,you can set \"--conf $path \" in main arguments")
          val parameter =
            ParameterTool.fromSystemProperties().mergeWith(argsMap)
          FlinkConfiguration(parameter, new Configuration(), new Configuration())
        case file =>
          val configMap = parseConfig(file)
          // set sql..
          val sqlConf = mutable.Map[String, String]()
          configMap.foreach(x => {
            if (x._1.startsWith(KEY_SQL_PREFIX)) {
              sqlConf += x._1.drop(KEY_SQL_PREFIX.length) -> x._2
            }
          })

          // config priority: explicitly specified priority > project profiles > system profiles
          val properConf =
            extractConfigByPrefix(configMap, KEY_FLINK_PROPERTY_PREFIX)
          val appConf = extractConfigByPrefix(configMap, KEY_APP_PREFIX)
          val tableConf =
            extractConfigByPrefix(configMap, KEY_FLINK_TABLE_PREFIX)

          val tableConfig = Configuration.fromMap(tableConf)
          val envConfig = Configuration.fromMap(properConf)

          val parameter = ParameterTool
            .fromSystemProperties()
            .mergeWith(ParameterTool.fromMap(properConf))
            .mergeWith(ParameterTool.fromMap(tableConf))
            .mergeWith(ParameterTool.fromMap(appConf))
            .mergeWith(ParameterTool.fromMap(sqlConf))
            .mergeWith(argsMap)

          FlinkConfiguration(parameter, envConfig, tableConfig)
      }
    }

    configuration.parameter.get(KEY_FLINK_SQL()) match {
      case null => configuration
      case param =>
        // for streampark-console
        Try(DeflaterUtils.unzipString(param)) match {
          case Success(value) =>
            configuration.copy(parameter = configuration.parameter.mergeWith(
              ParameterTool.fromMap(Map(KEY_FLINK_SQL() -> value))))
          case Failure(_) =>
            val sqlFile = new File(param)
            Try(PropertiesUtils.fromYamlFile(sqlFile.getAbsolutePath)) match {
              case Success(value) =>
                configuration.copy(parameter =
                  configuration.parameter.mergeWith(ParameterTool.fromMap(value)))
              case Failure(e) =>
                new IllegalArgumentException(s"[StreamPark] init sql error.$e")
                configuration
            }
        }
    }
  }

}
