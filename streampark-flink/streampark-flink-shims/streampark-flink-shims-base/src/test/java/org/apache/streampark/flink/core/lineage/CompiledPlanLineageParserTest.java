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

package org.apache.streampark.flink.core.lineage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan JSON shapes below mirror real Flink {@code CompiledPlan.asJsonString()} output (confirmed
 * against a real Flink SQL job's plan in the reference implementation this parser was ported from
 * — see {@code datasophon-lineage-emitter}'s {@code DatasetResolverTest}), trimmed to only the
 * fields this parser reads.
 */
class CompiledPlanLineageParserTest {

    /**
     * What {@code CREATE CATALOG paimon_s3 WITH ('type' = 'paimon', ...)} in the job's own SQL
     * contributes: the plan identifies a table's catalog by name only, so the catalog's declared
     * type is the only thing that says how its datasets should be named.
     */
    private static final Map<String, String> PAIMON_CATALOG = Map.of("paimon_s3", "paimon");

    private static SqlWithOptionsParser.WithOptions tempTable(String name, Map<String, String> options) {
        SqlWithOptionsParser.WithOptions parsed =
            SqlWithOptionsParser.parse(
                "CREATE TABLE `" + name + "` WITH (" + toWithClause(options) + ")");
        assertThat(parsed).isNotNull();
        return parsed;
    }

    private static String toWithClause(Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        options.forEach((k, v) -> sb.append("'").append(k).append("'='").append(v).append("',"));
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    @Test
    void resolvesMysqlCdcSourceAndPaimonCatalogSink() {
        String plan =
            "{\"nodes\":["
                + "{\"id\":1,\"scanTableSource\":{\"table\":{\"identifier\":\"`paimon_s3`.`lineage_flink_verify`.`mysql_pat_surgery`\"}}},"
                + "{\"id\":2,\"dynamicTableSink\":{\"table\":{\"identifier\":\"`paimon_s3`.`lineage_flink_verify`.`ods_pat_surgery`\"}}}"
                + "],\"edges\":[{\"source\":1,\"target\":2}]}";
        SqlWithOptionsParser.WithOptions source =
            tempTable(
                "mysql_pat_surgery",
                Map.of(
                    "connector", "mysql-cdc",
                    "hostname", "192.168.10.131",
                    "port", "3306",
                    "database-name", "lineage_flink_verify",
                    "table-name", "pat_surgery"));

        List<LineagePipeline> pipelines =
            CompiledPlanLineageParser.parse(plan, Map.of(source.name(), source), PAIMON_CATALOG);

        assertThat(pipelines).hasSize(1);
        LineagePipeline pipeline = pipelines.get(0);
        assertThat(pipeline.output())
            .isEqualTo(new LineageDataset("paimon://paimon_s3/lineage_flink_verify", "ods_pat_surgery"));
        assertThat(pipeline.inputs())
            .containsExactly(
                new LineageDataset("mysql-cdc://192.168.10.131:3306", "lineage_flink_verify.pat_surgery"));
    }

    @Test
    void resolvesLookupJoinTemporalTableAsInput() {
        // stream-exec-lookup-join nodes don't have a scanTableSource — the temporal table they
        // read is nested under temporalTable.lookupTableSource instead.
        String plan =
            "{\"nodes\":["
                + "{\"id\":1,\"scanTableSource\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`fact`\"}}},"
                + "{\"id\":2,\"temporalTable\":{\"lookupTableSource\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`dim`\"}}}},"
                + "{\"id\":3,\"dynamicTableSink\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`sink`\"}}}"
                + "],\"edges\":[{\"source\":1,\"target\":2},{\"source\":2,\"target\":3}]}";

        List<LineagePipeline> pipelines = CompiledPlanLineageParser.parse(plan, Map.of(), PAIMON_CATALOG);

        assertThat(pipelines).hasSize(1);
        assertThat(pipelines.get(0).inputs())
            .containsExactlyInAnyOrder(
                new LineageDataset("paimon://paimon_s3/db", "fact"),
                new LineageDataset("paimon://paimon_s3/db", "dim"));
    }

    @Test
    void pairsEachSinkWithOnlyItsOwnInputsInAStatementSet() {
        // T15-style finding (see LineagePipeline javadoc): a STATEMENT SET compiles into disjoint
        // connected components, one per INSERT. Flattening across the whole plan would report
        // every input as feeding every output — this plan mirrors that shape with two independent
        // two-node pipelines that must not cross-contaminate each other's inputs.
        String plan =
            "{\"nodes\":["
                + "{\"id\":1,\"scanTableSource\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`src_a`\"}}},"
                + "{\"id\":2,\"dynamicTableSink\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`sink_a`\"}}},"
                + "{\"id\":3,\"scanTableSource\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`src_b`\"}}},"
                + "{\"id\":4,\"dynamicTableSink\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`sink_b`\"}}}"
                + "],\"edges\":[{\"source\":1,\"target\":2},{\"source\":3,\"target\":4}]}";

        List<LineagePipeline> pipelines = CompiledPlanLineageParser.parse(plan, Map.of(), PAIMON_CATALOG);

        assertThat(pipelines).hasSize(2);
        for (LineagePipeline pipeline : pipelines) {
            assertThat(pipeline.inputs()).hasSize(1);
            String inputName = pipeline.inputs().iterator().next().name();
            if (pipeline.output().name().equals("sink_a")) {
                assertThat(inputName).isEqualTo("src_a");
            } else {
                assertThat(inputName).isEqualTo("src_b");
            }
        }
    }

    @Test
    void dropsUnresolvableInputInsteadOfFailingWholePipeline() {
        // The sink resolves fine (Paimon catalog table); the source is a per-job table declared
        // with an unrecognized connector. Fail-open: the pipeline is still returned, just missing
        // that one input — never an exception on the submission path.
        String plan =
            "{\"nodes\":["
                + "{\"id\":1,\"scanTableSource\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`kafka_src`\"}}},"
                + "{\"id\":2,\"dynamicTableSink\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`sink`\"}}}"
                + "],\"edges\":[{\"source\":1,\"target\":2}]}";
        SqlWithOptionsParser.WithOptions source = tempTable("kafka_src", Map.of("connector", "kafka"));

        List<LineagePipeline> pipelines =
            CompiledPlanLineageParser.parse(plan, Map.of(source.name(), source), PAIMON_CATALOG);

        assertThat(pipelines).hasSize(1);
        assertThat(pipelines.get(0).inputs()).isEmpty();
    }

    @Test
    void namespaceSchemeFollowsTheCatalogsDeclaredType() {
        // Nothing here is Paimon-specific: the scheme is whatever `CREATE CATALOG ... WITH
        // ('type' = ...)` declared, so a Hive catalog's datasets must be named hive://, not
        // silently reported under some other engine's namespace.
        String plan =
            "{\"nodes\":["
                + "{\"id\":1,\"scanTableSource\":{\"table\":{\"identifier\":\"`hive_prod`.`db`.`src`\"}}},"
                + "{\"id\":2,\"dynamicTableSink\":{\"table\":{\"identifier\":\"`hive_prod`.`db`.`sink`\"}}}"
                + "],\"edges\":[{\"source\":1,\"target\":2}]}";

        List<LineagePipeline> pipelines =
            CompiledPlanLineageParser.parse(plan, Map.of(), Map.of("hive_prod", "hive"));

        assertThat(pipelines).hasSize(1);
        assertThat(pipelines.get(0).output()).isEqualTo(new LineageDataset("hive://hive_prod/db", "sink"));
        assertThat(pipelines.get(0).inputs())
            .containsExactly(new LineageDataset("hive://hive_prod/db", "src"));
    }

    @Test
    void skipsCatalogTableWhoseCatalogTypeIsUnknown() {
        // A catalog attached outside this job's SQL leaves its type unknowable here. Guessing a
        // scheme would silently split one physical table into two graph nodes (dataset identity is
        // deduplicated by exact string), which is worse than reporting nothing.
        String plan =
            "{\"nodes\":["
                + "{\"id\":1,\"scanTableSource\":{\"table\":{\"identifier\":\"`unknown_cat`.`db`.`src`\"}}},"
                + "{\"id\":2,\"dynamicTableSink\":{\"table\":{\"identifier\":\"`unknown_cat`.`db`.`sink`\"}}}"
                + "],\"edges\":[{\"source\":1,\"target\":2}]}";

        assertThat(CompiledPlanLineageParser.parse(plan, Map.of(), Map.of())).isEmpty();
    }

    @Test
    void ignoresEdgesReferencingNodesAbsentFromThePlan() {
        String plan =
            "{\"nodes\":["
                + "{\"id\":2,\"dynamicTableSink\":{\"table\":{\"identifier\":\"`paimon_s3`.`db`.`sink`\"}}}"
                + "],\"edges\":[{\"source\":99,\"target\":2}]}";

        List<LineagePipeline> pipelines = CompiledPlanLineageParser.parse(plan, Map.of(), PAIMON_CATALOG);

        assertThat(pipelines).hasSize(1);
        assertThat(pipelines.get(0).inputs()).isEmpty();
    }

    @Test
    void throwsOnMalformedJsonRatherThanSilentlyReturningEmpty() {
        // Malformed CompiledPlan JSON is a structural/version-mismatch problem worth surfacing to
        // the caller's own try/catch, not a per-dataset resolution gap to silently absorb here.
        assertThatThrownBy(() -> CompiledPlanLineageParser.parse("not json", Map.of(), Map.of()))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void plansWithNoSinksYieldNoPipelines() {
        String plan = "{\"nodes\":[],\"edges\":[]}";

        assertThat(CompiledPlanLineageParser.parse(plan, Map.of(), PAIMON_CATALOG)).isEmpty();
    }
}
