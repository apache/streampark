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

package org.apache.streampark.flink.core.javaapi;

import org.apache.streampark.flink.core.FlinkSqlExecutor$;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.TableEnvironment;

import java.util.function.Consumer;

import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/**
 * Shared, package-private helpers for {@link FlinkTableJob} and {@link FlinkStreamTableJob}.
 * Pulled out to avoid duplicating the {@code app.name} lookup and the {@code FlinkSqlExecutor}
 * Java/Scala callback bridge across both classes.
 */
final class FlinkJobSupport {

    private FlinkJobSupport() {
    }

    /**
     * Runs a single SQL statement via the (still-Scala) {@code FlinkSqlExecutor}, bridging a
     * plain Java {@link Consumer} to the Scala {@code String => Unit} callback it expects so
     * neither caller needs to touch Scala types directly.
     *
     * <p>{@code FlinkSqlExecutor.executeSql} is {@code private[streampark]}, so Scala does not
     * generate a public static forwarder for it — it must be called via the module instance
     * ({@code FlinkSqlExecutor$.MODULE$}) rather than {@code FlinkSqlExecutor.executeSql(...)}.
     */
    static void executeSql(
                           String sql, ParameterTool parameter, TableEnvironment tableEnv,
                           Consumer<String> callback) {
        Function1<String, BoxedUnit> scalaCallback =
            callback == null
                ? null
                : new AbstractFunction1<String, BoxedUnit>() {

                    @Override
                    public BoxedUnit apply(String result) {
                        callback.accept(result);
                        return BoxedUnit.UNIT;
                    }
                };
        FlinkSqlExecutor$.MODULE$.executeSql(sql, parameter, tableEnv, scalaCallback);
    }

    /**
     * Returns the required {@code app.name} parameter, or throws if it's missing.
     *
     * <p>TODO: mirror {@code EnhancerImplicit#getAppName(required = true)} once ported to Java
     * (Phase 1.3), and delegate to it from here instead.
     */
    static String requireAppName(ParameterTool parameter) {
        String appName = parameter.get("app.name");
        if (appName == null || appName.isEmpty()) {
            throw new IllegalArgumentException("[StreamPark] \"app.name\" is required");
        }
        return appName;
    }
}
