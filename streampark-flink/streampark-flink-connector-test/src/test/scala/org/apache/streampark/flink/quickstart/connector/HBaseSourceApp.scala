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
import org.apache.streampark.flink.connector.hbase.source.HBaseSource
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Get, Scan}
import org.apache.hadoop.hbase.util.Bytes

import java.util

object HBaseSourceApp extends FlinkStreaming {

  implicit val stringType: TypeInformation[String] = TypeInformation.of(classOf[String])

  override def handle(): Unit = {

    implicit val conf = ConfigUtils.getHBaseConfig(context.parameter.toMap)

    val id = HBaseSource().getDataStream[String](query => {
      Thread.sleep(10)
      if (query == null) {
        new HBaseQuery("person", new Scan())
      } else {
        //TODO 从上一条记录中获取便宜量,决定下次查询的条件...
        new HBaseQuery("person", new Scan())
      }
    }, r => new String(r.getRow), null)

    HBaseRequest(id).requestOrdered(x => {
      new HBaseQuery("person", new Get(x.getBytes()))
    }, (a, r) => {
      val map = new util.HashMap[String, String]()
      val cellScanner = r.cellScanner()
      while (cellScanner.advance()) {
        val cell = cellScanner.current()
        val q = Bytes.toString(CellUtil.cloneQualifier(cell))
        val (name, v) = q.split("_") match {
          case Array(_type, name) =>
            _type match {
              case "i" => name -> Bytes.toInt(CellUtil.cloneValue(cell))
              case "s" => name -> Bytes.toString(CellUtil.cloneValue(cell))
              case "d" => name -> Bytes.toDouble(CellUtil.cloneValue(cell))
              case "f" => name -> Bytes.toFloat(CellUtil.cloneValue(cell))
            }
          case _ =>
        }
        map.put(name.toString, v.toString)
      }
      map.toString
    }).print("Async")
  }

}
