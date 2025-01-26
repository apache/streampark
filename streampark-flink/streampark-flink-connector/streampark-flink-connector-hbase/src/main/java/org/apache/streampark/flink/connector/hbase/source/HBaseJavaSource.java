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

import org.apache.streampark.common.util.ConfigUtils;
import org.apache.streampark.flink.connector.function.FilterFunction;
import org.apache.streampark.flink.connector.hbase.function.HBaseQueryFunction;
import org.apache.streampark.flink.connector.hbase.function.HBaseResultFunction;
import org.apache.streampark.flink.connector.hbase.internal.HBaseSourceFunction;
import org.apache.streampark.flink.core.scala.StreamingContext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.Properties;

public class HBaseJavaSource<T> {
  private final StreamingContext context;
  private Properties property;
  private String alias;
  private final TypeInformation<T> typeInformation;

  public HBaseJavaSource(StreamingContext context, Class<T> typeInfo) {
    if (context == null) {
      throw new NullPointerException("context must not be null");
    }
    if (typeInfo == null) {
      throw new NullPointerException("typeInfo must not be null");
    }
    this.context = context;
    this.typeInformation = TypeInformation.of(typeInfo);
  }

  public HBaseJavaSource<T> property(Properties property) {
    this.property = property;
    return this;
  }

  public HBaseJavaSource<T> alias(String alias) {
    this.alias = alias;
    return this;
  }

  public DataStreamSource<T> getDataStream(
      HBaseQueryFunction<T> queryFunction, HBaseResultFunction<T> resultFunction) {
    return getDataStream(queryFunction, resultFunction, null);
  }

  public DataStreamSource<T> getDataStream(
      HBaseQueryFunction<T> queryFunction,
      HBaseResultFunction<T> resultFunction,
      FilterFunction<T> filterFunction) {

    if (queryFunction == null) {
      throw new NullPointerException("HBaseJavaSource error: query function cannot be null");
    }
    if (resultFunction == null) {
      throw new NullPointerException("HBaseJavaSource error: result function cannot be null");
    }

    if (this.property == null) {
      this.property = ConfigUtils.getHBaseConfig(context.parameter().toMap(), alias);
    }

    HBaseSourceFunction<T> sourceFunction =
        new HBaseSourceFunction<>(
            property, queryFunction, resultFunction, filterFunction, typeInformation);
    return context.getJavaEnv().addSource(sourceFunction);
  }
}
