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

package org.apache.streampark.flink.core;

import org.apache.streampark.common.util.StreamParkLoggerFactory;
import org.apache.streampark.flink.core.lineage.CompiledPlanLineageParser;
import org.apache.streampark.flink.core.lineage.LineagePipeline;
import org.apache.streampark.flink.core.lineage.SqlWithOptionsParser;

import org.apache.streampark.shaded.org.slf4j.Logger;

import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 2.x counterpart of {@code streampark-flink-shims-base}'s class of the same name — see that
 * one's javadoc for the full rationale (classloader/connector-classpath precondition, why this
 * never throws, why {@code compilePlan()} is safe to call without ever calling {@code execute()},
 * and why the environment is always a streaming one regardless of the job's declared runtime mode).
 *
 * <p>Duplicated rather than shared because {@code SqlCommand} is itself duplicated per-Flink-major
 * version in this codebase (see this module's own {@code SqlCommand.java}) — same pattern this
 * module's {@code FlinkSqlExecutor} already follows for the same reason: the two variants' SQL
 * command regexes are declared independently even where they happen to coincide today.
 */
public final class FlinkSqlLineageExtractor {

    private static final Logger LOG =
        StreamParkLoggerFactory.loggerFactory().getLogger(FlinkSqlLineageExtractor.class.getName());

    private FlinkSqlLineageExtractor() {
    }

    public static List<LineagePipeline> extractLineage(String sql) {
        try {
            return doExtract(sql);
        } catch (Exception e) {
            LOG.warn("[lineage] failed to extract lineage, submission proceeds without lineage for it", e);
            return new ArrayList<>();
        }
    }

    /**
     * Visible for testing: {@link #extractLineage} swallows every failure by design, so only this
     * method can tell "the plan compiled and yielded nothing resolvable" apart from "the plan failed
     * to compile at all".
     */
    static List<LineagePipeline> doExtract(String sql) {
        List<SqlCommandCall> calls = SqlCommandParser.parseSQL(sql, null);
        if (calls == null || calls.isEmpty()) {
            return new ArrayList<>();
        }

        TableEnvironment tableEnv =
            TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        StatementSet statementSet = tableEnv.createStatementSet();
        Map<String, SqlWithOptionsParser.WithOptions> tempTables = new LinkedHashMap<>();
        Map<String, String> catalogTypes = new LinkedHashMap<>();
        boolean hasInsert = false;

        for (SqlCommandCall call : calls) {
            switch (call.command) {
                case INSERT:
                    statementSet.addInsertSql(call.originSql);
                    hasInsert = true;
                    break;
                case SET:
                    applySet(call, tableEnv);
                    break;
                case SELECT:
                case SHOW_CATALOGS:
                case SHOW_CURRENT_CATALOG:
                case SHOW_DATABASES:
                case SHOW_CURRENT_DATABASE:
                case SHOW_TABLES:
                case SHOW_CREATE_TABLE:
                case SHOW_COLUMNS:
                case SHOW_VIEWS:
                case SHOW_CREATE_VIEW:
                case SHOW_FUNCTIONS:
                case SHOW_MODULES:
                case DESC:
                case DESCRIBE:
                case EXPLAIN:
                case DELETE:
                case UPDATE:
                case RESET:
                case RESET_ALL:
                case BEGIN_STATEMENT_SET:
                case END_STATEMENT_SET:
                    // Irrelevant to schema registration or lineage; skip rather than risk a
                    // side effect (e.g. EXPLAIN executing for real) in a throwaway environment.
                    break;
                default:
                    if (call.command == SqlCommand.CREATE_TABLE) {
                        SqlWithOptionsParser.WithOptions options = SqlWithOptionsParser.parse(call.originSql);
                        if (options != null) {
                            tempTables.put(options.name(), options);
                        }
                    } else if (call.command == SqlCommand.CREATE_CATALOG) {
                        SqlWithOptionsParser.rememberCatalogType(call.originSql, catalogTypes);
                    }
                    tableEnv.executeSql(call.originSql);
            }
        }

        if (!hasInsert) {
            return new ArrayList<>();
        }

        CompiledPlan plan = statementSet.compilePlan();
        return new ArrayList<>(CompiledPlanLineageParser.parse(plan.asJsonString(), tempTables, catalogTypes));
    }

    /**
     * Drops the job's {@code execution.runtime-mode}: it can only be chosen when the {@link
     * TableEnvironment} is instantiated (Flink rejects any later change, even to the same value),
     * and {@link #doExtract} always instantiates a streaming one.
     */
    private static void applySet(SqlCommandCall call, TableEnvironment tableEnv) {
        if (call.operands == null || call.operands.length < 2) {
            return;
        }
        if (ExecutionOptions.RUNTIME_MODE.key().equals(call.operands[0])) {
            return;
        }
        tableEnv.getConfig().getConfiguration().setString(call.operands[0], call.operands[1]);
    }
}
