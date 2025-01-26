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

package org.apache.streampark.flink.connector.mongo.source

import org.apache.streampark.common.util.Utils
import org.apache.streampark.flink.connector.mongo.internal.MongoSourceFunction
import org.apache.streampark.flink.core.scala.StreamingContext

import com.mongodb.client.{FindIterable, MongoCollection, MongoCursor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.bson.Document

import java.util.Properties

import scala.annotation.meta.param

object MongoSource {

  def apply(@(transient @param) property: Properties = new Properties())(implicit
      ctx: StreamingContext): MongoSource = new MongoSource(ctx, property)

}

class MongoSource(
    @(transient @param) val ctx: StreamingContext,
    property: Properties = new Properties()) {

  def getDataStream[R: TypeInformation](
      collection: String,
      queryFun: (R, MongoCollection[Document]) => FindIterable[Document],
      resultFun: MongoCursor[Document] => List[R],
      filter: R => Boolean = null)(implicit prop: Properties = new Properties()): DataStream[R] = {

    Utils.copyProperties(property, prop)
    val mongoFun = new MongoSourceFunction[R](collection, prop, queryFun, resultFun, filter)
    ctx.addSource(mongoFun)
  }

}
