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
 * Extracts table-level lineage from a Flink SQL job's source text, without submitting it.
 *
 * <p>Called from the Console submission path via {@code FlinkShimsProxy.proxy(...)} — the same
 * per-Flink-version, {@code ChildFirstClassLoader}-isolated mechanism other shims features use.
 * That classloader is built from the registered Flink Home's entire {@code lib/} directory (see
 * {@code FlinkShimsProxy.getFlinkShimsClassLoader}), so a connector-backed {@code CREATE TABLE}
 * (mysql-cdc, doris, ...) resolves here exactly when its factory jar is present there — the same
 * requirement that already applies for that connector to run for real. If the operator's Flink
 * Home lib/ has drifted from the cluster's, this degrades to no lineage for that job (see {@link
 * #extractLineage}), not a submission failure.
 *
 * <p>Builds a throwaway {@link TableEnvironment} to run every non-{@code INSERT} statement (DDL:
 * {@code CREATE TABLE/CATALOG/DATABASE/VIEW}, {@code USE}, ...) for schema registration, collects
 * {@code INSERT} statements into a {@link StatementSet}, and calls {@code compilePlan()} — a
 * planning-only operation that builds the job graph and validates types without starting any task
 * or touching a connector's actual I/O (that only happens on {@code execute()}, which this class
 * never calls).
 *
 * <p>That environment is always a <em>streaming</em> one, whatever {@code execution.runtime-mode}
 * the job itself declares: {@code compilePlan()} is implemented only by Flink's stream planner —
 * its batch planner throws {@code UnsupportedOperationException("The compiled plan feature is not
 * supported in batch mode.")} (verified in both 1.20 and 2.2). Which tables feed which is a
 * property of the query, not of the runtime mode, so planning a batch job's statements as
 * streaming yields the same source/sink topology. A statement that genuinely cannot be planned as
 * streaming degrades to no lineage for that job, like any other extraction failure.
 */
public final class FlinkSqlLineageExtractor {

    private static final Logger LOG =
        StreamParkLoggerFactory.loggerFactory().getLogger(FlinkSqlLineageExtractor.class.getName());

    private FlinkSqlLineageExtractor() {
    }

    /**
     * Never throws — this runs on the job submission path, where a lineage gap must never fail the
     * submission. Returns an empty list on any failure (logged), including: the SQL has no INSERT,
     * a DDL statement fails (e.g. a missing connector factory), or the plan fails to compile.
     */
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
