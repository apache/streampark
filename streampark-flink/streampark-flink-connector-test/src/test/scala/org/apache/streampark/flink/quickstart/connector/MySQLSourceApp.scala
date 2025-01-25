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

import org.apache.streampark.flink.connector.jdbc.source.JdbcSource
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation

object MySQLSourceApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Order] = TypeInformation.of(classOf[Order])

  override def handle(): Unit = {

    JdbcSource().getDataStream[Order](lastOne => {
      val laseOffset = if (lastOne == null) "2020-10-10 23:00:00" else lastOne.timestamp
      s"select * from t_order where timestamp > '$laseOffset' order by timestamp asc "
    },
      _.map(x => new Order(x("market_id").toString, x("timestamp").toString)), null
    ).print()

  }

}

class Order(val marketId: String, val timestamp: String) extends Serializable
