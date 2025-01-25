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

import com.mongodb.BasicDBObject
import org.apache.streampark.common.util.{DateUtils, JsonUtils}
import org.apache.streampark.flink.connector.mongo.source.MongoSource
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util.Properties
import scala.collection.JavaConversions._

object MongoSourceApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[String] = TypeInformation.of(classOf[String])

  override def handle(): Unit = {
    implicit val prop: Properties = context.parameter.getProperties
    val source = MongoSource()
    source.getDataStream[String](
      "shop",
      (a, d) => {
        Thread.sleep(1000)
        /**
         * 从上一条记录提前offset数据,作为下一条数据查询的条件,如果offset为Null,则表明是第一次查询,需要指定默认offset
         */
        val offset = if (a == null) "2019-09-27 00:00:00" else {
          JsonUtils.read[Map[String, _]](a).get("updateTime").toString
        }
        val cond = new BasicDBObject().append("updateTime", new BasicDBObject("$gte", DateUtils.parse(offset)))
        d.find(cond)
      },
      _.toList.map(_.toJson()), null
    ).print()
  }

}
