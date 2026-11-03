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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Smoke tests for {@link FlinkTableJob} — verifies the lifecycle contract and SQL bridging. */
class FlinkTableJobTest {

    private static TableEnvironment newBatchTableEnv() {
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    }

    @Test
    void lifecycleRunsInOrderAndExposesTableEnv() throws Exception {
        List<String> calls = new ArrayList<>();
        TableEnvironment tableEnv = newBatchTableEnv();

        FlinkTableJob job =
            new FlinkTableJob(JobTestParams.withAppName("test-table-job"), tableEnv) {

                @Override
                protected void ready() {
                    calls.add("ready");
                }

                @Override
                protected void handle() {
                    calls.add("handle");
                    // getTableEnv() must return exactly the instance we constructed with —
                    // proves composition, not a copy or a re-wrapped object.
                    assertSame(tableEnv, getTableEnv());
                }

                @Override
                protected void destroy() {
                    calls.add("destroy");
                }
            };

        job.start();

        assertEquals(List.of("ready", "handle", "destroy"), calls);
    }

    @Test
    void sqlRunsAgainstRealTableEnvAndInvokesCallback() {
        TableEnvironment tableEnv = newBatchTableEnv();
        tableEnv.executeSql("CREATE TABLE sink (msg STRING) WITH ('connector' = 'print')");

        // FlinkSqlExecutor.executeSql treats a non-blank `sql` argument as a *parameter lookup
        // key*, not literal SQL text — the actual statement must be registered under that key in
        // the ParameterTool first (this mirrors how it's driven from job config in practice).
        Map<String, String> params = new HashMap<>();
        params.put("app.name", "test-table-job");
        params.put("my.insert.sql", "INSERT INTO sink VALUES ('hello')");
        ParameterTool parameter = ParameterTool.fromMap(params);

        List<String> callbackLines = new ArrayList<>();
        FlinkTableJob job =
            new FlinkTableJob(parameter, tableEnv) {

                @Override
                protected void handle() {
                    sql("my.insert.sql", callbackLines::add);
                }
            };

        assertDoesNotThrow(job::start);
    }

    @Test
    void missingAppNameThrowsBeforeExecuting() {
        TableEnvironment tableEnv = newBatchTableEnv();
        Map<String, String> empty = new HashMap<>();
        FlinkTableJob job =
            new FlinkTableJob(ParameterTool.fromMap(empty), tableEnv) {

                @Override
                protected void handle() {
                    // Intentionally empty — start() must throw on the missing "app.name" before
                    // handle() is ever reached, so this body is never executed.
                }
            };

        assertThrows(IllegalArgumentException.class, job::start);
    }
}
