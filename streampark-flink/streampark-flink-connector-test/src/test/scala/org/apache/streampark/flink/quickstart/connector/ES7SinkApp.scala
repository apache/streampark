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
package org.apache.streampark.flink.quickstart.connector

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.streampark.flink.connector.elasticsearch7.sink.ES7Sink
import org.apache.streampark.flink.connector.elasticsearch7.util.ElasticsearchUtils
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.streampark.flink.quickstart.connector.bean.Entity
import org.elasticsearch.action.index.IndexRequest
import org.json4s.jackson.JsonMethods

object ES7SinkApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Entity] = TypeInformation.of(classOf[Entity])

  override def handle(): Unit = {
    val source: DataStream[Entity] = context.addSource(new MyDataSource)

    implicit def indexedSeq(x: Entity): IndexRequest = ElasticsearchUtils.indexRequest(
        "flink_order",
        s"${x.orderId}_${x.timestamp}",
        JsonMethods.mapper.writeValueAsString(x)
    )

    ES7Sink().sink[Entity](source)
  }
}
