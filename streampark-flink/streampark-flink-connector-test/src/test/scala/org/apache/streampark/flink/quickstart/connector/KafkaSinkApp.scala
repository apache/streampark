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

import org.apache.streampark.flink.connector.kafka.sink.KafkaSink
import org.apache.streampark.flink.core.scala.FlinkStreaming
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random


object KafkaSinkApp extends FlinkStreaming {

  implicit val stringType: TypeInformation[String] = TypeInformation.of(classOf[String])
  implicit val entityType: TypeInformation[Behavior] = TypeInformation.of(classOf[Behavior])

  override def handle(): Unit = {
    val source = new BehaviorSource()
    val ds = context.addSource[Behavior](source).map(_.toString)
    ds.print()
    KafkaSink().sink(ds)
  }

}


case class Behavior(user_id: String,
                    item_id: Long,
                    category_id: Long,
                    behavior: String,
                    ts: Long) {
  override def toString: String = {
    s"""
       |{
       |user_id:$user_id,
       |item_id:$item_id,
       |category_id:$category_id,
       |behavior:$behavior,
       |ts:$ts
       |}
       |""".stripMargin
  }
}


class BehaviorSource extends SourceFunction[Behavior] {
  private[this] var isRunning = true

  override def cancel(): Unit = this.isRunning = false

  val random = new Random()
  var index = 0

  override def run(ctx: SourceFunction.SourceContext[Behavior]): Unit = {
    val seq = Seq("view", "click", "search", "buy", "share")
    while (isRunning && index <= 10000) {
      index += 1
      val user_id = random.nextInt(1000)
      val item_id = random.nextInt(100)
      val category_id = random.nextInt(20)
      val behavior = seq(random.nextInt(5))
      val order = Behavior(user_id.toString, item_id, category_id, behavior, System.currentTimeMillis())
      ctx.collect(order)
    }
  }

}


