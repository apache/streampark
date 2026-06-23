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

import org.apache.streampark.common.conf.ConfigKeys.{KEY_APP_NAME, KEY_FLINK_APP_NAME}
import org.apache.streampark.common.util.DeflaterUtils

import org.apache.flink.configuration.PipelineOptions
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.util.ParameterTool

import scala.util.Try

object EnhancerImplicit {

  implicit class EnhanceParameterTool(parameterTool: ParameterTool) {

    private[flink] def getAppName(name: String = null, required: Boolean = false): String = {
      val appName = name match {
        case null =>
          Try(DeflaterUtils.unzipString(parameterTool.get(KEY_APP_NAME(), null)))
            .getOrElse(parameterTool.get(KEY_FLINK_APP_NAME, null))
        case x => x
      }
      if (required) {
        require(appName != null, "[StreamPark] Application name cannot be null")
      }
      appName
    }

  }

  implicit class EnhanceTableEnvironment(env: TableEnvironment) {

    private[flink] def setAppName(implicit parameter: ParameterTool): TableEnvironment = {
      val appName = parameter.getAppName()
      if (appName != null) {
        env.getConfig.getConfiguration.setString(PipelineOptions.NAME.key, appName)
      }
      env
    }

  }

  implicit class EnhanceStreamTableEnvironment(env: StreamTableEnvironment) {

    private[flink] def setAppName(implicit parameter: ParameterTool): StreamTableEnvironment = {
      val appName = parameter.getAppName()
      if (appName != null) {
        env.getConfig.getConfiguration.setString(PipelineOptions.NAME.key, appName)
      }
      env
    }
  }

}
