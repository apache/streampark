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

package org.apache.streampark.flink.connector.jdbc.internal

import org.apache.streampark.common.enums.ApiType
import org.apache.streampark.common.enums.ApiType.ApiType
import org.apache.streampark.common.util.{JdbcUtils, Logger}
import org.apache.streampark.flink.connector.function.{RunningFunction, SQLQueryFunction, SQLResultFunction}
import org.apache.streampark.flink.util.FlinkUtils

import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{CheckpointListener, FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.lang
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.util.{Success, Try}

class JdbcSourceFunction[R: TypeInformation](apiType: ApiType = ApiType.scala, jdbc: Properties)
  extends RichSourceFunction[R]
  with CheckpointedFunction
  with CheckpointListener
  with Logger {

  @volatile private[this] var running = true
  private[this] var scalaRunningFunc: Unit => Boolean = (_) => true
  private[this] var javaRunningFunc: RunningFunction = new RunningFunction {
    override def running(): lang.Boolean = true
  }

  private[this] var scalaSqlFunc: R => String = _
  private[this] var scalaResultFunc: Function[Iterable[Map[String, _]], Iterable[R]] = _
  private[this] var javaSqlFunc: SQLQueryFunction[R] = _
  private[this] var javaResultFunc: SQLResultFunction[R] = _
  @transient private var unionOffsetStates: ListState[R] = null

  private val OFFSETS_STATE_NAME: String = "jdbc-source-query-states"
  private[this] var last: R = _

  // for Scala
  def this(
      jdbc: Properties,
      sqlFunc: R => String,
      resultFunc: Iterable[Map[String, _]] => Iterable[R],
      runningFunc: Unit => Boolean) = {

    this(ApiType.scala, jdbc)
    this.scalaSqlFunc = sqlFunc
    this.scalaResultFunc = resultFunc
    if (runningFunc != null) {
      this.scalaRunningFunc = runningFunc
    }
  }

  // for JAVA
  def this(
      jdbc: Properties,
      javaSqlFunc: SQLQueryFunction[R],
      javaResultFunc: SQLResultFunction[R],
      runningFunc: RunningFunction) {

    this(ApiType.java, jdbc)
    this.javaSqlFunc = javaSqlFunc
    this.javaResultFunc = javaResultFunc
    if (runningFunc != null) {
      this.javaRunningFunc = runningFunc
    }
  }

  @throws[Exception]
  override def run(ctx: SourceContext[R]): Unit = {
    while (this.running) {
      apiType match {
        case ApiType.scala =>
          if (scalaRunningFunc()) {
            ctx.getCheckpointLock.synchronized {
              val sql = scalaSqlFunc(last)
              val result: List[Map[String, _]] = JdbcUtils.select(sql)(jdbc)
              scalaResultFunc(result).foreach(
                x => {
                  last = x
                  ctx.collectWithTimestamp(last, System.currentTimeMillis())
                })
            }
          }
        case ApiType.java =>
          if (javaRunningFunc.running()) {
            ctx.getCheckpointLock.synchronized {
              val sql = javaSqlFunc.query(last)
              val result: List[Map[String, _]] = JdbcUtils.select(sql)(jdbc)
              javaResultFunc
                .result(result.map(_.asJava))
                .foreach(
                  x => {
                    last = x
                    ctx.collectWithTimestamp(last, System.currentTimeMillis())
                  })
            }
          }
      }
    }
  }

  override def cancel(): Unit = this.running = false

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    if (running) {
      unionOffsetStates.clear()
      if (last != null) {
        unionOffsetStates.add(last)
      }
    } else {
      logError("JdbcSource snapshotState called on closed source")
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    logInfo("JdbcSource snapshotState initialize")
    unionOffsetStates = FlinkUtils
      .getUnionListState[R](context, getRuntimeContext.getExecutionConfig, OFFSETS_STATE_NAME)
    Try(unionOffsetStates.get.head) match {
      case Success(q) => last = q
      case _ =>
    }
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    logInfo(s"JdbcSource checkpointComplete: $checkpointId")
  }

}
