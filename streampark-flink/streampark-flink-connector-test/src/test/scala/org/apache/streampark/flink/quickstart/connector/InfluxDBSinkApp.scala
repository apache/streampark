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

import org.apache.streampark.flink.connector.influx.bean.InfluxEntity
import org.apache.streampark.flink.connector.influx.sink.InfluxSink
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * 侧输出流
 */
object InfluxDBSinkApp extends FlinkStreaming {

  implicit val entityType: TypeInformation[Weather] = TypeInformation.of(classOf[Weather])

  override def handle(): Unit = {
    val source = context.addSource(new WeatherSource())

    //weather,altitude=1000,area=北 temperature=11,humidity=-4

    implicit val entity: InfluxEntity[Weather] = new InfluxEntity[Weather](
      "mydb",
      "test",
      "autogen",
      (x: Weather) => Map("altitude" -> x.altitude.toString, "area" -> x.area),
      (x: Weather) => Map("temperature" -> x.temperature.asInstanceOf[Object], "humidity" -> x.humidity.asInstanceOf[Object])
    )

    InfluxSink().sink(source, "mydb")

  }

}

/**
 *
 * 温度 temperature
 * 湿度 humidity
 * 地区 area
 * 海拔 altitude
 */
case class Weather(temperature: Long,
                   humidity: Long,
                   area: String,
                   altitude: Long)

class WeatherSource extends SourceFunction[Weather] {

  private[this] var isRunning = true

  override def cancel(): Unit = this.isRunning = false

  val random = new Random()

  override def run(ctx: SourceFunction.SourceContext[Weather]): Unit = {
    while (isRunning) {
      val temperature = random.nextInt(100)
      val humidity = random.nextInt(30)
      val area = List("北", "上", "广", "深")(random.nextInt(4))
      val altitude = random.nextInt(10000)
      val order = Weather(temperature, humidity, area, altitude)
      ctx.collect(order)
    }
  }

}

