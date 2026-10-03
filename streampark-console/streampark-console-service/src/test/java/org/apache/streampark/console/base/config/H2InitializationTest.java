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

package org.apache.streampark.console.base.config;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

class H2InitializationTest {

    @Test
    void initializesQuickstartProject() throws Exception {
        try (
            Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:project-seed;MODE=MySQL;DATABASE_TO_LOWER=TRUE", "sa", "")) {
            ScriptUtils.executeSqlScript(connection, new ClassPathResource("db/schema-h2.sql"));
            ScriptUtils.executeSqlScript(connection, new ClassPathResource("db/data-h2.sql"));

            try (
                Statement statement = connection.createStatement();
                ResultSet project = statement.executeQuery("select * from t_flink_project")) {
                assertThat(project.next()).isTrue();
                assertThat(project.getLong("id")).isEqualTo(100000L);
                assertThat(project.getLong("team_id")).isEqualTo(100000L);
                assertThat(project.getString("name")).isEqualTo("streampark-quickstart");
                assertThat(project.getString("url"))
                    .isEqualTo("https://github.com/apache/streampark-quickstart");
                assertThat(project.getString("refs")).isEqualTo("dev");
                assertThat(project.getInt("type")).isEqualTo(1);
                assertThat(project.getInt("repository")).isEqualTo(1);
                assertThat(project.getInt("build_state")).isEqualTo(-1);
                assertThat(project.getTimestamp("create_time")).isNotNull();
                assertThat(project.getTimestamp("modify_time")).isNotNull();
                assertThat(project.next()).isFalse();
            }
        }
    }
}
