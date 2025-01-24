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

package org.apache.streampark.flink.connector.jdbc.sink

import org.apache.streampark.common.conf.ConfigConst._
import org.apache.streampark.common.enums.Semantic
import org.apache.streampark.common.util.{ConfigUtils, Logger}
import org.apache.streampark.flink.connector.jdbc.internal.{Jdbc2PCSinkFunction, JdbcSinkFunction}
import org.apache.streampark.flink.connector.sink.Sink
import org.apache.streampark.flink.core.scala.StreamingContext

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream

import java.util.Properties

import scala.annotation.meta.param

object JdbcSink {

  /**
   * @param ctx
   *   : StreamingContext
   * @param alias
   *   : Instance alias (used to distinguish between multiple different database instances...)
   * @return
   */
  def apply(
      @(transient @param)
      parallelism: Int = 0,
      alias: String = "",
      name: String = null,
      uid: String = null)(implicit ctx: StreamingContext): JdbcSink =
    new JdbcSink(ctx, parallelism, alias, name, uid)

}

class JdbcSink(
    @(transient @param) ctx: StreamingContext,
    parallelism: Int = 0,
    alias: String = "",
    name: String = null,
    uid: String = null)
  extends Sink
  with Logger {

  /**
   * @param stream
   *   : DataStream
   * @param toSQLFn
   *   : The function converted to SQL is provided by the user.
   * @tparam T
   *   : The data type of the stream in the DataStream
   * @return
   */
  def sink[T](stream: DataStream[T])(implicit toSQLFn: T => String): DataStreamSink[T] = {
    val prop = ConfigUtils.getJdbcProperties(ctx.parameter.toMap, alias)
    val semantic = Semantic.of(prop.getProperty(KEY_SEMANTIC, Semantic.NONE.name()))
    val sink = semantic match {
      case Semantic.EXACTLY_ONCE =>
        val sinkFun = new Jdbc2PCSinkFunction[T](prop, toSQLFn)
        if (parallelism > 1) {
          logWarn(s"parallelism:$parallelism, Jdbc Semantic EXACTLY_ONCE,parallelism bust be 1.")
        }
        stream.addSink(sinkFun)
      case _ =>
        val sinkFun = new JdbcSinkFunction[T](prop, toSQLFn)
        stream.addSink(sinkFun)
    }
    afterSink(sink, parallelism, name, uid)
  }
}

class JdbcOutputFormat[T: TypeInformation](implicit prop: Properties, toSQlFun: T => String)
  extends RichOutputFormat[T]
  with Logger {

  val sinkFunction = new JdbcSinkFunction[T](prop, toSQlFun)

  var configuration: Configuration = _

  override def configure(configuration: Configuration): Unit = this.configuration = configuration

  override def open(taskNumber: Int, numTasks: Int): Unit = sinkFunction.open(this.configuration)

  override def writeRecord(record: T): Unit = sinkFunction.invoke(record, null)

  override def close(): Unit = sinkFunction.close()
}

//-------------Jdbc2PCSinkFunction exactly-once support ---------------------------------------------------------------------------------------

class Jdbc2PCOutputFormat[T: TypeInformation](implicit prop: Properties, toSQlFun: T => String)
  extends RichOutputFormat[T]
  with Logger {

  private val sinkFunction = new Jdbc2PCSinkFunction[T](prop, toSQlFun)

  private var configuration: Configuration = _

  override def configure(configuration: Configuration): Unit = this.configuration = configuration

  override def open(taskNumber: Int, numTasks: Int): Unit = sinkFunction.open(this.configuration)

  override def writeRecord(record: T): Unit = sinkFunction.invoke(record, null)

  override def close(): Unit = sinkFunction.close()
}
