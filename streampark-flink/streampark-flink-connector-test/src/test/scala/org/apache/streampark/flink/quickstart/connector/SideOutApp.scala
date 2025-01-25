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

import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 侧输出流
 */
object SideOutApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[SideEntry] = TypeInformation.of(classOf[SideEntry])

  override def handle(): Unit = {
    val source = context.addSource(new SideSource())

    /**
     * 侧输出流。。。
     * 官方写法:设置侧输出流
     */
    val side1 = source.process(new ProcessFunction[SideEntry, SideEntry] {
      val tag = new OutputTag[SideEntry]("flink")

      override def processElement(value: SideEntry, ctx: ProcessFunction[SideEntry, SideEntry]#Context, out: Collector[SideEntry]): Unit = {
        if (value.userId < 100) {
          ctx.output(tag, value)
        } else {
          out.collect(value)
        }
      }
    })

    //官方写法,获取侧输出流
    side1.getSideOutput(new OutputTag[SideEntry]("flink")).print("flink:========>")

  }

}

/**
 *
 * @param userId      : 用户Id
 * @param orderId     : 订单ID
 * @param siteId      : 站点ID
 * @param cityId      : 城市Id
 * @param orderStatus : 订单状态(1:下单,0:退单)
 * @param isNewOrder  : 是否是首单
 * @param price       : 单价
 * @param quantity    : 订单数量
 * @param timestamp   : 下单时间
 */
case class SideEntry(userId: Long,
                     orderId: Long,
                     siteId: Long,
                     cityId: Long,
                     orderStatus: Int,
                     isNewOrder: Int,
                     price: Double,
                     quantity: Int,
                     timestamp: Long)

class SideSource extends SourceFunction[SideEntry] {

  private[this] var isRunning = true

  override def cancel(): Unit = this.isRunning = false

  val random = new Random()

  override def run(ctx: SourceFunction.SourceContext[SideEntry]): Unit = {
    while (isRunning) {
      val userId = random.nextInt(1000)
      val orderId = random.nextInt(100)
      val status = random.nextInt(1)
      val isNew = random.nextInt(1)
      val price = random.nextDouble()
      val quantity = new Random(10).nextInt()
      val order = SideEntry(userId, orderId, siteId = 1, cityId = 1, status, isNew, price, quantity, System.currentTimeMillis)
      ctx.collect(order)
    }
  }

}

