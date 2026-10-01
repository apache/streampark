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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.TableEnvironment;

import java.util.function.Consumer;

/**
 * Package-private shared base for {@link FlinkTableJob} and {@link FlinkStreamTableJob} — holds
 * the common {@code ready → handle → execute → destroy} lifecycle, the SQL bridge, and the
 * {@code app.name} lookup, so neither subclass has to repeat them.
 *
 * <p>Not part of the public API: callers only ever see {@link FlinkTableJob} or {@link
 * FlinkStreamTableJob}, never this type directly.
 */
abstract class AbstractFlinkJob {

    protected final ParameterTool parameter;

    protected final TableEnvironment tableEnv;

    protected AbstractFlinkJob(ParameterTool parameter, TableEnvironment tableEnv) {
        this.parameter = parameter;
        this.tableEnv = tableEnv;
    }

    /** Recommended entry point to start the job. */
    public final JobExecutionResult start() throws Exception {
        ready();
        handle();
        JobExecutionResult result = execute(FlinkJobSupport.requireAppName(parameter));
        destroy();
        return result;
    }

    /** Hook called before {@link #handle()}. Override for pre-job setup. */
    protected void ready() {
    }

    /** User job logic goes here — build and register the pipeline. */
    protected abstract void handle() throws Exception;

    /** Runs the job under the given name; subclasses decide what "running" means for them. */
    protected abstract JobExecutionResult execute(String jobName) throws Exception;

    /** Hook called after {@link #execute(String)}. Override for cleanup. */
    protected void destroy() {
    }

    /**
     * Convenience shortcut for running a single SQL statement, matching legacy {@code sql(...)}.
     * Statement result lines (e.g. from {@code SHOW TABLES}, {@code EXPLAIN}) are logged by
     * default — use {@link #sql(String, Consumer)} to receive them instead.
     */
    public void sql(String sql) {
        sql(sql, null);
    }

    /**
     * Runs a single SQL statement, routing any result lines to the given callback instead of the
     * default log output. See {@link FlinkJobSupport#executeSql} for details on how this bridges
     * to the (still-Scala) {@code FlinkSqlExecutor}.
     */
    public void sql(String sql, Consumer<String> callback) {
        FlinkJobSupport.executeSql(sql, parameter, tableEnv, callback);
    }

    public ParameterTool getParameter() {
        return parameter;
    }
}
