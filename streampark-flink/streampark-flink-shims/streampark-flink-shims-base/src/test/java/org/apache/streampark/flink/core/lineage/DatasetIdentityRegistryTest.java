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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DatasetIdentityRegistryTest {

    @Test
    void resolvesMysqlCdc() {
        LineageDataset dataset =
            DatasetIdentityRegistry.resolve(
                "src",
                Map.of(
                    "connector", "mysql-cdc",
                    "hostname", "192.168.10.131",
                    "port", "3306",
                    "database-name", "lineage_flink_verify",
                    "table-name", "pat_surgery"));

        assertThat(dataset).isNotNull();
        assertThat(dataset.namespace()).isEqualTo("mysql-cdc://192.168.10.131:3306");
        assertThat(dataset.name()).isEqualTo("lineage_flink_verify.pat_surgery");
    }

    @Test
    void resolvesDoris() {
        LineageDataset dataset =
            DatasetIdentityRegistry.resolve(
                "sink",
                Map.of(
                    "connector", "doris",
                    "fenodes", "192.168.10.131:8030",
                    "table.identifier", "db.ods_table"));

        assertThat(dataset).isNotNull();
        assertThat(dataset.namespace()).isEqualTo("doris://192.168.10.131:8030");
        assertThat(dataset.name()).isEqualTo("db.ods_table");
    }

    @Test
    void returnsNullForUnknownConnectorInsteadOfThrowing() {
        assertThat(DatasetIdentityRegistry.resolve("t", Map.of("connector", "kafka"))).isNull();
    }

    @Test
    void returnsNullWhenConnectorOptionMissing() {
        assertThat(DatasetIdentityRegistry.resolve("t", Map.of())).isNull();
    }

    @Test
    void returnsNullWhenRequiredOptionMissing() {
        assertThat(DatasetIdentityRegistry.resolve("t", Map.of("connector", "mysql-cdc", "hostname", "h")))
            .isNull();
    }
}
