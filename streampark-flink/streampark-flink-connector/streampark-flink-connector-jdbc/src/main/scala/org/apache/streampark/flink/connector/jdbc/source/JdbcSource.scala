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

package org.apache.streampark.flink.connector.jdbc.source

import org.apache.streampark.common.util.ConfigUtils
import org.apache.streampark.flink.connector.jdbc.internal.JdbcSourceFunction
import org.apache.streampark.flink.core.scala.StreamingContext

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

import java.util.Properties

import scala.annotation.meta.param
import scala.collection.Map

object JdbcSource {

  def apply(alias: String = "", properties: Properties = new Properties())(implicit
      ctx: StreamingContext): JdbcSource = new JdbcSource(ctx, alias, properties) {}
}

class JdbcSource(
    @(transient @param) val ctx: StreamingContext,
    alias: String,
    property: Properties) {

  /**
   * @param sqlFun
   * @param fun
   * @param jdbc
   * @tparam R
   * @return
   */
  def getDataStream[R: TypeInformation](
      sqlFun: R => String,
      func: Iterable[Map[String, _]] => Iterable[R],
      filter: R => Boolean): DataStream[R] = {
    val jdbc = ConfigUtils.getJdbcProperties(ctx.parameter.toMap, alias)
    if (property != null) {
      jdbc.putAll(property)
    }
    val mysqlFun = new JdbcSourceFunction[R](jdbc, sqlFun, func, filter)
    ctx.addSource(mysqlFun)
  }

}
