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
package org.apache.streampark.flink.quickstart.connector;

import org.apache.streampark.flink.connector.function.QueryFunction;
import org.apache.streampark.flink.connector.function.ResultFunction;
import org.apache.streampark.flink.connector.jdbc.source.JdbcJavaSource;
import org.apache.streampark.flink.core.StreamEnvConfig;
import org.apache.streampark.flink.core.scala.StreamingContext;
import org.apache.streampark.flink.quickstart.connector.bean.Order;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MySQLJavaApp {

  public static void main(String[] args) {

    StreamEnvConfig envConfig = new StreamEnvConfig(args, null);

    StreamingContext context = new StreamingContext(envConfig);

    // 读取MySQL数据源
    new JdbcJavaSource<>(context, Order.class)
        .getDataStream(
            (QueryFunction<Order>)
                lastOne -> {
                  // 5秒抽取一次
                  Thread.sleep(3000);
                  Serializable lastOffset =
                      lastOne == null ? "2020-10-10 23:00:00" : lastOne.getTimestamp();
                  return String.format(
                      "select * from t_order "
                          + "where timestamp > '%s' "
                          + "order by timestamp asc ",
                      lastOffset);
                },
            (ResultFunction<Order>)
                map -> {
                  List<Order> result = new ArrayList<>();
                  map.forEach(
                      item -> {
                        Order order = new Order();
                        order.setOrderId(item.get("order_id").toString());
                        order.setMarketId(item.get("market_id").toString());
                        order.setTimestamp(Long.parseLong(item.get("timestamp").toString()));
                        result.add(order);
                      });
                  return result;
                })
        .returns(TypeInformation.of(Order.class))
        .print("jdbc source: >>>>>");

    context.start();
  }
}
