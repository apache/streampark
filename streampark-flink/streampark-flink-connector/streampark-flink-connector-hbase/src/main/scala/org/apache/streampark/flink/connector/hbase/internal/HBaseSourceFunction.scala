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

package org.apache.streampark.flink.connector.hbase.internal

import org.apache.streampark.common.enums.ApiType
import org.apache.streampark.common.enums.ApiType.ApiType
import org.apache.streampark.common.util.Logger
import org.apache.streampark.flink.connector.function.FilterFunction
import org.apache.streampark.flink.connector.hbase.bean.HBaseQuery
import org.apache.streampark.flink.connector.hbase.function.{HBaseQueryFunction, HBaseResultFunction}
import org.apache.streampark.flink.util.FlinkUtils

import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{CheckpointListener, FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.hadoop.hbase.client.{Result, Table}

import java.lang
import java.util.Properties

import scala.collection.JavaConversions._
import scala.util.{Success, Try}

class HBaseSourceFunction[R: TypeInformation](apiType: ApiType = ApiType.scala, prop: Properties)
  extends RichSourceFunction[R]
  with CheckpointedFunction
  with CheckpointListener
  with Logger {

  @volatile private[this] var running = true
  private[this] var scalaFilterFunc: R => Boolean = (_: R) => true
  private[this] var javaFilterFunc: FilterFunction[R] = new FilterFunction[R] {

    /** filter function */
    override def filter(t: R): lang.Boolean = true
  }

  @transient private[this] var table: Table = _

  @volatile var query: HBaseQuery = _

  private[this] var scalaQueryFunc: R => HBaseQuery = _
  private[this] var scalaResultFunc: Result => R = _

  private[this] var javaQueryFunc: HBaseQueryFunction[R] = _
  private[this] var javaResultFunc: HBaseResultFunction[R] = _

  @transient private var state: ListState[R] = _
  private val OFFSETS_STATE_NAME: String = "hbase-source-query-states"
  private[this] var last: R = _

  // for Scala
  def this(
      prop: Properties,
      queryFunc: R => HBaseQuery,
      resultFunc: Result => R,
      filter: R => Boolean) = {

    this(ApiType.scala, prop)
    this.scalaQueryFunc = queryFunc
    this.scalaResultFunc = resultFunc
    if (filter != null) {
      this.scalaFilterFunc = filter
    }
  }

  // for JAVA
  def this(
      prop: Properties,
      queryFunc: HBaseQueryFunction[R],
      resultFunc: HBaseResultFunction[R],
      filter: FilterFunction[R]) {

    this(ApiType.java, prop)
    this.javaQueryFunc = queryFunc
    this.javaResultFunc = resultFunc
    if (filter != null) {
      this.javaFilterFunc = filter
    }
  }

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def run(ctx: SourceContext[R]): Unit = {
    while (this.running) {
      apiType match {
        case ApiType.scala =>
          ctx.getCheckpointLock.synchronized {
            // Returns the query object of the last (or recovered from checkpoint) query to the user, and the user constructs the conditions for the next query based on this.
            query = scalaQueryFunc(last)
            require(
              query != null && query.getTable != null,
              "[StreamPark] HBaseSource query and query's param table must not be null ")
            table = query.getTable(prop)
            table
              .getScanner(query)
              .foreach(
                x => {
                  val r = scalaResultFunc(x)
                  if (scalaFilterFunc(r)) {
                    last = r
                    ctx.collectWithTimestamp(r, System.currentTimeMillis())
                  }
                })
          }
        case ApiType.java =>
          ctx.getCheckpointLock.synchronized {
            // Returns the query object of the last (or recovered from checkpoint) query to the user, and the user constructs the conditions for the next query based on this.
            query = javaQueryFunc.query(last)
            require(
              query != null && query.getTable != null,
              "[StreamPark] HBaseSource query and query's param table must not be null ")
            table = query.getTable(prop)
            table
              .getScanner(query)
              .foreach(
                x => {
                  val r = javaResultFunc.result(x)
                  if (javaFilterFunc.filter(r)) {
                    last = r
                    ctx.collectWithTimestamp(r, System.currentTimeMillis())
                  }
                })
          }
      }
    }
  }

  override def cancel(): Unit = this.running = false

  override def close(): Unit = {
    super.close()
    if (table != null) {
      table.close()
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    if (running) {
      state.clear()
      if (last != null) {
        state.add(last)
      }
    } else {
      logError("HBaseSource snapshotState called on closed source")
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    // Recover from checkpoint...
    logInfo("HBaseSource snapshotState initialize")
    state = FlinkUtils
      .getUnionListState[R](context, getRuntimeContext.getExecutionConfig, OFFSETS_STATE_NAME)
    Try(state.get.head) match {
      case Success(q) => last = q
      case _ =>
    }
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    logInfo(s"HBaseSource checkpointComplete: $checkpointId")
  }

}
