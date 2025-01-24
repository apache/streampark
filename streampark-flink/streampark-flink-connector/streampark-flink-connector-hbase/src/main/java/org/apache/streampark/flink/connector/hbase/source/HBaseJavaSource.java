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

package org.apache.streampark.flink.connector.hbase.source;

import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.flink.connector.function.RunningFunction;
import org.apache.streampark.flink.connector.hbase.function.HBaseQueryFunction;
import org.apache.streampark.flink.connector.hbase.function.HBaseResultFunction;
import org.apache.streampark.flink.connector.hbase.internal.HBaseSourceFunction;
import org.apache.streampark.flink.core.scala.StreamingContext;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.Properties;

public class HBaseJavaSource<T> {
  private final StreamingContext context;
  private final Properties property;

  public HBaseJavaSource(StreamingContext context, Properties property) {
    this.context = context;
    this.property = property;
  }

  public DataStreamSource<T> getDataStream(
      HBaseQueryFunction<T> queryFunction,
      HBaseResultFunction<T> resultFunction,
      RunningFunction runningFunc) {

    AssertUtils.notNull(queryFunction, "queryFunction must not be null");
    AssertUtils.notNull(resultFunction, "resultFunction must not be null");
    HBaseSourceFunction<T> sourceFunction =
        new HBaseSourceFunction<>(property, queryFunction, resultFunction, runningFunc, null);
    return context.getJavaEnv().addSource(sourceFunction);
  }
}
