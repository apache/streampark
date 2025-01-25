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

package org.apache.streampark.flink.connector.mongo.internal

import org.apache.streampark.common.enums.ApiType
import org.apache.streampark.common.enums.ApiType.ApiType
import org.apache.streampark.common.util.{Logger, MongoConfig}
import org.apache.streampark.flink.connector.function.RunningFunction
import org.apache.streampark.flink.connector.mongo.function.{MongoQueryFunction, MongoResultFunction}
import org.apache.streampark.flink.util.FlinkUtils

import com.mongodb.MongoClient
import com.mongodb.client.{FindIterable, MongoCollection, MongoCursor}
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{CheckpointListener, FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.bson.Document

import java.lang
import java.util.Properties

import scala.collection.JavaConversions._
import scala.util.{Success, Try}

class MongoSourceFunction[R: TypeInformation](
    apiType: ApiType,
    prop: Properties = new Properties(),
    collection: String)
  extends RichSourceFunction[R]
  with CheckpointedFunction
  with CheckpointListener
  with Logger {

  @volatile private[this] var running = true
  private[this] var scalaRunningFunc: Unit => Boolean = (_) => true
  private[this] var javaRunningFunc: RunningFunction = new RunningFunction {
    override def running(): lang.Boolean = true
  }

  var client: MongoClient = _
  private var mongoCollection: MongoCollection[Document] = _

  private[this] var scalaQueryFunc: (R, MongoCollection[Document]) => FindIterable[Document] = _
  private[this] var scalaResultFunc: MongoCursor[Document] => List[R] = _

  private[this] var javaQueryFunc: MongoQueryFunction[R] = _
  private[this] var javaResultFunc: MongoResultFunction[R] = _

  @transient private var state: ListState[R] = _
  private val OFFSETS_STATE_NAME: String = "mongo-source-query-states"
  private[this] var last: R = _

  // for Scala
  def this(
      collectionName: String,
      prop: Properties,
      scalaQueryFunc: (R, MongoCollection[Document]) => FindIterable[Document],
      scalaResultFunc: MongoCursor[Document] => List[R],
      runningFunc: Unit => Boolean) = {

    this(ApiType.scala, prop, collectionName)
    this.scalaQueryFunc = scalaQueryFunc
    this.scalaResultFunc = scalaResultFunc
    if (runningFunc != null) {
      this.scalaRunningFunc = runningFunc
    }
  }

  // for JAVA
  def this(
      collectionName: String,
      prop: Properties,
      queryFunc: MongoQueryFunction[R],
      resultFunc: MongoResultFunction[R],
      runningFunc: RunningFunction) {

    this(ApiType.java, prop, collectionName)
    this.javaQueryFunc = queryFunc
    this.javaResultFunc = resultFunc
    if (runningFunc != null) {
      this.javaRunningFunc = runningFunc
    }
  }

  override def cancel(): Unit = this.running = false

  override def open(parameters: Configuration): Unit = {
    client = MongoConfig.getClient(prop)
    val db = MongoConfig.getProperty(prop, MongoConfig.database)
    val database = client.getDatabase(db)
    mongoCollection = database.getCollection(collection)
  }

  @throws[Exception]
  override def run(ctx: SourceContext[R]): Unit = {
    while (this.running) {
      apiType match {
        case ApiType.scala =>
          if (scalaRunningFunc()) {
            ctx.getCheckpointLock.synchronized {
              val find = scalaQueryFunc(last, mongoCollection)
              if (find != null) {
                scalaResultFunc(find.iterator).foreach(
                  x => {
                    last = x
                    ctx.collectWithTimestamp(last, System.currentTimeMillis())
                  })
              }
            }
          }
        case ApiType.java =>
          if (javaRunningFunc.running()) {
            ctx.getCheckpointLock.synchronized {
              val find = javaQueryFunc.query(last, mongoCollection)
              if (find != null) {
                javaResultFunc
                  .result(find.iterator)
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
  }

  override def close(): Unit = {
    this.running = false
    client.close()
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    if (running) {
      state.clear()
      if (last != null) {
        state.add(last)
      }
    } else {
      logError("MongoSource snapshotState called on closed source")
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    // restore from checkpoint
    logInfo("MongoSource snapshotState initialize")
    state = FlinkUtils
      .getUnionListState[R](context, getRuntimeContext.getExecutionConfig, OFFSETS_STATE_NAME)
    Try(state.get().head) match {
      case Success(q) => last = q
      case _ =>
    }
  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    logInfo(s"MongoSource checkpointComplete: $checkpointId")
  }

}
