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
import org.apache.streampark.flink.connector.hbase.sink.{HBaseOutputFormat, HBaseSink}
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.streampark.flink.quickstart.connector.bean.Entity
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.util.Bytes

import java.util.{Collections, Random}
import scala.language.implicitConversions

object HBaseSinkApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Entity] = TypeInformation.of(classOf[Entity])
  implicit val stringType: TypeInformation[String] = TypeInformation.of(classOf[String])

  override def handle(): Unit = {
    val source = context.addSource(new MyDataSource)
    val random = new Random()

    //定义转换规则...
    implicit def entry2Put(entity: Entity): java.lang.Iterable[Mutation] = {
      val put = new Put(Bytes.toBytes(System.nanoTime() + random.nextInt(1000000)), entity.timestamp)
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cid"), Bytes.toBytes(entity.cityId))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("oid"), Bytes.toBytes(entity.orderId))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("os"), Bytes.toBytes(entity.orderStatus))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("oq"), Bytes.toBytes(entity.quantity))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sid"), Bytes.toBytes(entity.siteId))
      Collections.singleton(put)
    }
    //source ===> trans ===> sink

    //1）插入方式1
    HBaseSink().sink[Entity](source, "order")

    //2) 插入方式2
    //1.指定HBase 配置文件
    val prop = ConfigUtils.getHBaseConfig(context.parameter.toMap)
    //2.插入...
    source.writeUsingOutputFormat(new HBaseOutputFormat[Entity]("order", prop))


  }

}
