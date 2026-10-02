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

import static org.assertj.core.api.Assertions.assertThat;

class SqlWithOptionsParserTest {

    @Test
    void parsesPlainCreateTableWithOptions() {
        // Deliberately NOT "CREATE TEMPORARY TABLE" — StreamPark SQL jobs commonly omit TEMPORARY,
        // and this parser must not silently lose lineage for that common case.
        String sql =
            "CREATE TABLE mysql_pat_surgery (id BIGINT, name STRING) WITH ("
                + "'connector' = 'mysql-cdc',"
                + "'hostname' = '192.168.10.131',"
                + "'port' = '3306',"
                + "'database-name' = 'lineage_flink_verify',"
                + "'table-name' = 'pat_surgery')";

        SqlWithOptionsParser.WithOptions result = SqlWithOptionsParser.parse(sql);

        assertThat(result).isNotNull();
        assertThat(result.name()).isEqualTo("mysql_pat_surgery");
        assertThat(result.options())
            .containsEntry("connector", "mysql-cdc")
            .containsEntry("hostname", "192.168.10.131")
            .containsEntry("port", "3306")
            .containsEntry("database-name", "lineage_flink_verify")
            .containsEntry("table-name", "pat_surgery");
    }

    @Test
    void parsesCreateTemporaryTableWithOptions() {
        String sql =
            "CREATE TEMPORARY TABLE IF NOT EXISTS `doris_sink` (id BIGINT) WITH ("
                + "'connector' = 'doris',"
                + "'fenodes' = '192.168.10.131:8030',"
                + "'table.identifier' = 'db.ods_table')";

        SqlWithOptionsParser.WithOptions result = SqlWithOptionsParser.parse(sql);

        assertThat(result).isNotNull();
        assertThat(result.name()).isEqualTo("doris_sink");
        assertThat(result.options()).containsEntry("connector", "doris");
    }

    @Test
    void parsesLowercaseAndQualifiedTableNames() {
        SqlWithOptionsParser.WithOptions result =
            SqlWithOptionsParser.parse("create table mydb.mytable (id BIGINT) with ('connector' = 'doris')");

        assertThat(result).isNotNull();
        assertThat(result.name()).isEqualTo("mytable");
        assertThat(result.options()).containsEntry("connector", "doris");
    }

    @Test
    void parsesLongOptionValueWithoutOverflowingTheStack() {
        // A greedily-repeated literal body recurses once per character, so a long value (a
        // certificate, a serialized properties blob) used to throw StackOverflowError on the
        // submission path rather than yielding lineage.
        StringBuilder longValue = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            longValue.append('a');
        }
        String sql =
            "CREATE TABLE t (id BIGINT) WITH ('connector' = 'mysql-cdc','password' = '" + longValue + "')";

        SqlWithOptionsParser.WithOptions result = SqlWithOptionsParser.parse(sql);

        assertThat(result).isNotNull();
        assertThat(result.options().get("password")).hasSize(20000);
    }

    @Test
    void unescapesDoubledSingleQuotesInsideOptionValues() {
        String sql =
            "CREATE TABLE t (id BIGINT) WITH ("
                + "'connector' = 'mysql-cdc',"
                + "'password' = 'a''b')";

        SqlWithOptionsParser.WithOptions result = SqlWithOptionsParser.parse(sql);

        assertThat(result).isNotNull();
        assertThat(result.options()).containsEntry("password", "a'b");
    }

    @Test
    void reducesQualifiedTableNameToItsLocalName() {
        // A CompiledPlan identifier always reports the table under its own name only, so keying
        // these options by the qualified form would never match and the table would be treated as
        // catalog-backed instead of connector-backed.
        String sql =
            "CREATE TABLE mydb.mytable (id BIGINT) WITH ('connector' = 'doris',"
                + "'fenodes' = '192.168.10.131:8030','table.identifier' = 'db.t')";

        SqlWithOptionsParser.WithOptions result = SqlWithOptionsParser.parse(sql);

        assertThat(result).isNotNull();
        assertThat(result.name()).isEqualTo("mytable");
        assertThat(result.options()).containsEntry("connector", "doris");
    }

    @Test
    void ignoresAWithClauseThatOnlyAppearsInsideAStringLiteral() {
        String sql =
            "CREATE TABLE t (id BIGINT COMMENT 'joined WITH (other)') WITH ("
                + "'connector' = 'mysql-cdc','hostname' = 'h')";

        SqlWithOptionsParser.WithOptions result = SqlWithOptionsParser.parse(sql);

        assertThat(result).isNotNull();
        assertThat(result.options())
            .containsEntry("connector", "mysql-cdc")
            .containsEntry("hostname", "h");
    }

    @Test
    void parsesCreateCatalogType() {
        String sql =
            "CREATE CATALOG paimon_s3 WITH ('type' = 'paimon','warehouse' = 's3://bucket/warehouse')";

        SqlWithOptionsParser.WithOptions result = SqlWithOptionsParser.parseCatalog(sql);

        assertThat(result).isNotNull();
        assertThat(result.name()).isEqualTo("paimon_s3");
        assertThat(result.options()).containsEntry("type", "paimon");
    }

    @Test
    void returnsNullWhenStatementHasNoWithClause() {
        String sql = "CREATE TABLE t LIKE other_table";

        assertThat(SqlWithOptionsParser.parse(sql)).isNull();
    }

    @Test
    void returnsNullForNonCreateTableStatement() {
        assertThat(SqlWithOptionsParser.parse("INSERT INTO sink SELECT * FROM src")).isNull();
    }
}
