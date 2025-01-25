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

import org.apache.streampark.flink.connector.http.sink.HttpSink
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.streampark.flink.quickstart.connector.bean.Entity
import org.apache.flink.api.common.typeinfo.TypeInformation

object HttpSinkApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Entity] = TypeInformation.of(classOf[Entity])
  implicit val stringType: TypeInformation[String] = TypeInformation.of(classOf[String])

  override def handle(): Unit = {

    /**
     * source
     */
    val source = context.addSource(new MyDataSource)
      .map(x => s"http://www.qq.com?id=${x.userId}")

    // sink
    new HttpSink(context).get(source)

  }

}
