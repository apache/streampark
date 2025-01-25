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

package org.apache.streampark.flink.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.util.TimeUtils

import java.io.File
import java.time.Duration
import java.util

object FlinkUtils {

  def getUnionListState[R: TypeInformation](
      context: FunctionInitializationContext,
      execConfig: ExecutionConfig,
      descriptorName: String): ListState[R] = {

    context.getOperatorStateStore.getUnionListState(
      new ListStateDescriptor(
        descriptorName,
        new KryoSerializer[R](implicitly[TypeInformation[R]].getTypeClass, execConfig))
    )
  }

  def getFlinkDistJar(flinkHome: String): String = {
    new File(s"$flinkHome/lib").list().filter(_.matches("flink-dist.*\\.jar")) match {
      case Array() =>
        throw new IllegalArgumentException(
          s"[StreamPark] can no found flink-dist jar in $flinkHome/lib")
      case array if array.length == 1 => s"$flinkHome/lib/${array.head}"
      case more =>
        throw new IllegalArgumentException(
          s"[StreamPark] found multiple flink-dist jar in $flinkHome/lib,[${more.mkString(",")}]")
    }
  }

  def isCheckpointEnabled(map: util.Map[String, String]): Boolean = {
    val checkpointInterval: Duration = TimeUtils.parseDuration(
      map.getOrDefault(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL.key, "0ms"))
    checkpointInterval.toMillis > 0
  }

}
