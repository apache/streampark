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

package org.apache.streampark.flink.connector.jdbc.source;

import org.apache.streampark.common.util.ConfigUtils;
import org.apache.streampark.flink.connector.function.FilterFunction;
import org.apache.streampark.flink.connector.function.QueryFunction;
import org.apache.streampark.flink.connector.function.ResultFunction;
import org.apache.streampark.flink.connector.jdbc.internal.JdbcSourceFunction;
import org.apache.streampark.flink.core.scala.StreamingContext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.Properties;

public class JdbcJavaSource<T> {

  private final StreamingContext context;
  private final TypeInformation<T> typeInformation;

  private Properties jdbc;
  private String alias = null;

  public JdbcJavaSource(StreamingContext context, Class<T> typeInfo) {
    if (typeInfo == null) {
      throw new NullPointerException("typeInfo must not be null");
    }
    if (context == null) {
      throw new NullPointerException("context must not be null");
    }
    this.context = context;
    this.typeInformation = TypeInformation.of(typeInfo);
  }

  public JdbcJavaSource<T> jdbc(Properties jdbc) {
    this.jdbc = jdbc;
    return this;
  }

  public JdbcJavaSource<T> alias(String alias) {
    this.alias = alias;
    return this;
  }

  public DataStreamSource<T> getDataStream(
      QueryFunction<T> queryFunction, ResultFunction<T> resultFunction) {
    return getDataStream(queryFunction, resultFunction, null);
  }

  public DataStreamSource<T> getDataStream(
      QueryFunction<T> queryFunction, ResultFunction<T> resultFunction, FilterFunction<T> filter) {

    if (queryFunction == null) {
      throw new NullPointerException(
          "JdbcJavaSource getDataStream error: QueryFunction must not be null");
    }
    if (resultFunction == null) {
      throw new NullPointerException(
          "JdbcJavaSource getDataStream error: ResultFunction must not be null");
    }

    if (this.jdbc == null) {
      this.jdbc = ConfigUtils.getJdbcProperties(context.parameter().toMap(), alias);
    }

    JdbcSourceFunction<T> sourceFunction =
        new JdbcSourceFunction<T>(jdbc, queryFunction, resultFunction, filter, typeInformation);
    return context.getJavaEnv().addSource(sourceFunction);
  }
}
