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

package org.apache.streampark.flink.connector.mongo.source;

import org.apache.streampark.flink.connector.function.FilterFunction;
import org.apache.streampark.flink.connector.mongo.function.MongoQueryFunction;
import org.apache.streampark.flink.connector.mongo.function.MongoResultFunction;
import org.apache.streampark.flink.connector.mongo.internal.MongoSourceFunction;
import org.apache.streampark.flink.core.scala.StreamingContext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.Properties;

public class MongoJavaSource<T> {
  private final StreamingContext context;
  private Properties property;
  private String alias;
  private final TypeInformation<T> typeInformation;

  public MongoJavaSource(StreamingContext context, Class<T> typeInfo) {
    this.context = context;
    this.typeInformation = TypeInformation.of(typeInfo);
  }

  public MongoJavaSource<T> property(Properties property) {
    this.property = property;
    return this;
  }

  public MongoJavaSource<T> alias(String alias) {
    this.alias = alias;
    return this;
  }

  public DataStreamSource<T> getDataStream(
      String collectionName,
      MongoQueryFunction<T> queryFunction,
      MongoResultFunction<T> resultFunction) {
    return getDataStream(collectionName, queryFunction, resultFunction, null);
  }

  public DataStreamSource<T> getDataStream(
      String collectionName,
      MongoQueryFunction<T> queryFunction,
      MongoResultFunction<T> resultFunction,
      FilterFunction runningFunc) {

    if (collectionName == null) {
      throw new NullPointerException("MongoJavaSource error: collectionName must not be null");
    }

    if (queryFunction == null) {
      throw new NullPointerException("MongoJavaSource error: mongoQueryFunction must not be null");
    }

    if (resultFunction == null) {
      throw new NullPointerException("MongoJavaSource error: mongoResultFunction must not be null");
    }

    MongoSourceFunction<T> sourceFunction =
        new MongoSourceFunction<>(
            collectionName, property, queryFunction, resultFunction, runningFunc, typeInformation);
    return context.getJavaEnv().addSource(sourceFunction);
  }
}
