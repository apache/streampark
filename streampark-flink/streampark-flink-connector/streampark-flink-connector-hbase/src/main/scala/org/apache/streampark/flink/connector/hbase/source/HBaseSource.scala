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

package org.apache.streampark.flink.connector.hbase.source

import org.apache.streampark.common.util.ConfigUtils
import org.apache.streampark.flink.connector.hbase.bean.HBaseQuery
import org.apache.streampark.flink.connector.hbase.internal.HBaseSourceFunction
import org.apache.streampark.flink.core.scala.StreamingContext

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.hadoop.hbase.client._

import java.util.Properties

import scala.annotation.meta.param

object HBaseSource {

  def apply(@(transient @param) alias: String = "", properties: Properties = new Properties())(
      implicit ctx: StreamingContext): HBaseSource = new HBaseSource(ctx, alias, properties)

}

/**
 * Support end-to-end exactly once and replay
 *
 * @param ctx
 * @param property
 */
class HBaseSource(
    @(transient @param) val ctx: StreamingContext,
    alias: String,
    property: Properties) {

  def getDataStream[R: TypeInformation](
      query: R => HBaseQuery,
      func: Result => R,
      running: Unit => Boolean): DataStream[R] = {

    if (query == null) {
      throw new NullPointerException("getDataStream error, SQLQueryFunction must not be null")
    }
    if (func == null) {
      throw new NullPointerException("getDataStream error, SQLResultFunction must not be null")
    }
    val jdbc = ConfigUtils.getHBaseConfig(ctx.parameter.toMap)
    if (property != null) {
      jdbc.putAll(property)
    }
    val hBaseFunc = new HBaseSourceFunction[R](jdbc, query, func, running)
    ctx.addSource(hBaseFunc)
  }

}
