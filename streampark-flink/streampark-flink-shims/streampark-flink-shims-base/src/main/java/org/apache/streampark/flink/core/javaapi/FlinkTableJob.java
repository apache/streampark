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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java lifecycle base class for Flink Table API (batch) jobs.
 *
 * <p>Unlike the legacy Scala {@code FlinkTableTrait}, this class does NOT extend / mimic {@link
 * TableEnvironment}. Instead it holds a {@link TableEnvironment} by composition — call {@link
 * #getTableEnv()} to access the full Table API surface directly. This avoids re-implementing and
 * maintaining dozens of delegate methods every time the Flink Table API changes.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * public class MyTableJob extends FlinkTableJob {
 *
 *   public MyTableJob(ParameterTool parameter, TableEnvironment tableEnv) {
 *     super(parameter, tableEnv);
 *   }
 *
 *   @Override
 *   protected void handle() throws Exception {
 *     getTableEnv().executeSql("SELECT * FROM my_table");
 *   }
 * }
 * }</pre>
 */
public abstract class FlinkTableJob extends AbstractFlinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableJob.class);

    protected FlinkTableJob(ParameterTool parameter, TableEnvironment tableEnv) {
        super(parameter, tableEnv);
    }

    /**
     * Executes the job under the given name. Batch Table API jobs are typically already triggered
     * synchronously by {@code executeSql} / {@code StatementSet#execute()} inside {@link
     * #handle()}, so this returns {@code null} by default — override if a specific job result is
     * needed.
     */
    @Override
    protected JobExecutionResult execute(String jobName) {
        // TODO(#4408): replace with Utils.printLogo(...) once streampark-common Java migration
        // (Phase 1) lands
        LOG.info("[StreamPark] FlinkTable {} Starting...", jobName);
        return null;
    }

    /** Direct access to the underlying Table API — use this instead of delegate methods. */
    public TableEnvironment getTableEnv() {
        return tableEnv;
    }
}
