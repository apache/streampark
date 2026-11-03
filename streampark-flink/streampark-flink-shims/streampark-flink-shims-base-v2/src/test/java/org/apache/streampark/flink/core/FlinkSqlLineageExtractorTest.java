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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The Flink 2.x counterpart of the shims-base test of the same name. The two extractors are
 * separate classes because the {@code TableEnvironment} bootstrap differs between Flink 1.x and
 * 2.x, so each needs its own coverage — a regression fixed in one does not protect the other.
 */
class FlinkSqlLineageExtractorTest {

    /**
     * A job declaring {@code execution.runtime-mode = BATCH} must still get a compiled plan. Flink
     * implements {@code compilePlan()} only in its stream planner — the batch one throws
     * {@code UnsupportedOperationException("The compiled plan feature is not supported in batch
     * mode.")} — so honouring that SET when building the throwaway extraction environment loses
     * lineage for every batch job.
     *
     * <p>Asserted through {@code doExtract} rather than {@link
     * FlinkSqlLineageExtractor#extractLineage}: the latter is fail-open and would return the same
     * empty list whether the plan compiled or blew up, hiding exactly this regression.
     */
    @Test
    void batchRuntimeModeDeclarationStillCompilesAPlan() {
        assertThatCode(() -> FlinkSqlLineageExtractor.doExtract(batchDeclaringSql()))
            .doesNotThrowAnyException();
    }

    /**
     * Same SQL through the public entry point: the connectors it uses have no dataset-identity rule,
     * so the outcome is no lineage — reported as an empty list, never an exception, because this
     * runs on the job submission path.
     */
    @Test
    void unknownConnectorsYieldNoLineageRatherThanFailing() {
        assertThat(FlinkSqlLineageExtractor.extractLineage(batchDeclaringSql())).isEmpty();
    }

    @Test
    void sqlWithoutInsertYieldsNoLineage() {
        String sql =
            "CREATE TABLE probe_src (id BIGINT) WITH ("
                + "'connector' = 'datagen',"
                + "'number-of-rows' = '1')";

        assertThat(FlinkSqlLineageExtractor.extractLineage(sql)).isEmpty();
    }

    /** datagen -> blackhole: the only source/sink pair guaranteed present in a bare Flink install. */
    private static String batchDeclaringSql() {
        return "SET 'execution.runtime-mode' = 'BATCH';\n"
            + "CREATE TABLE probe_src (id BIGINT) WITH ("
            + "'connector' = 'datagen',"
            + "'number-of-rows' = '1');\n"
            + "CREATE TABLE probe_sink (id BIGINT) WITH ("
            + "'connector' = 'blackhole');\n"
            + "INSERT INTO probe_sink SELECT id FROM probe_src;";
    }
}
