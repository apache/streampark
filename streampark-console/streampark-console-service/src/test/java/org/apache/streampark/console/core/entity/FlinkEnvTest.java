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

package org.apache.streampark.console.core.entity;

import org.apache.streampark.common.util.DeflaterUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkEnvTest {

    @TempDir
    private Path tempDir;

    @Test
    void unzipFlinkConfShouldRecoverWhenStoredValueIsFlinkHome() throws IOException {
        FlinkEnv flinkEnv = flinkEnvWithStoredFlinkHome();

        flinkEnv.unzipFlinkConf();

        assertThat(flinkEnv.getFlinkConf())
            .contains("execution.checkpointing.savepoint-dir: hdfs:///savepoints");
    }

    @Test
    void getFlinkConfigShouldRecoverWhenStoredValueIsFlinkHome() throws IOException {
        FlinkEnv flinkEnv = flinkEnvWithStoredFlinkHome();

        Properties flinkConfig = flinkEnv.getFlinkConfig();

        assertThat(flinkConfig)
            .containsEntry("execution.checkpointing.savepoint-dir", "hdfs:///savepoints");
        assertThat(DeflaterUtils.unzipString(flinkEnv.getFlinkConf()))
            .contains("execution.checkpointing.savepoint-dir: hdfs:///savepoints");
    }

    @Test
    void getFlinkConfigShouldRecoverWhenStoredValueHasTrailingSlash() throws IOException {
        FlinkEnv flinkEnv = flinkEnvWithStoredFlinkHome();
        flinkEnv.setFlinkConf(flinkEnv.getFlinkHome() + "/");

        Properties flinkConfig = flinkEnv.getFlinkConfig();

        assertThat(flinkConfig)
            .containsEntry("execution.checkpointing.savepoint-dir", "hdfs:///savepoints");
    }

    private FlinkEnv flinkEnvWithStoredFlinkHome() throws IOException {
        Path flinkHome = tempDir.resolve("flink-1.16.3");
        Path confDir = flinkHome.resolve("conf");
        Files.createDirectories(confDir);
        Files.write(
            confDir.resolve("flink-conf.yaml"),
            "execution.checkpointing.savepoint-dir: hdfs:///savepoints\n"
                .getBytes(StandardCharsets.UTF_8));

        FlinkEnv flinkEnv = new FlinkEnv();
        flinkEnv.setFlinkHome(flinkHome.toString());
        flinkEnv.setFlinkConf(flinkHome.toString());
        flinkEnv.setVersion("1.16.3");
        return flinkEnv;
    }
}
