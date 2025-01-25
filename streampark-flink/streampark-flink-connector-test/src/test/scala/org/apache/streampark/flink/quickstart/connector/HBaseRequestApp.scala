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

import org.apache.streampark.common.util.ConfigUtils
import org.apache.streampark.flink.connector.hbase.bean.HBaseQuery
import org.apache.streampark.flink.connector.hbase.request.HBaseRequest
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.hadoop.hbase.client.Get

object HBaseRequestApp extends FlinkStreaming {

  implicit val stringType: TypeInformation[String] = TypeInformation.of(classOf[String])

  implicit val reqType: TypeInformation[(String, Boolean)] = TypeInformation.of(classOf[(String, Boolean)])

  override def handle(): Unit = {

    implicit val conf = ConfigUtils.getHBaseConfig(context.parameter.toMap)
    //one topic
    val source = context.fromCollection(Seq("123456", "1111", "222"))

    source.print("source:>>>")

    HBaseRequest(source).requestOrdered[(String, Boolean)](x => {
      new HBaseQuery("person", new Get(x.getBytes()))
    }, timeout = 5000, resultFunc = (a, r) => {
      a -> !r.isEmpty
    }).print(" check.... ")


  }

}
