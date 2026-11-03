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

package org.apache.streampark.console.core.service.application.impl;

import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;
import org.apache.streampark.flink.packer.pipeline.SimpleBuildResponse;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkApplicationBuildResultTest {

    @Test
    void submitBuiltYarnSqlJar() {
        FlinkApplication application = application(FlinkJobType.FLINK_SQL);
        ShadedBuildResponse built = new ShadedBuildResponse("/app", "/app/sql-fat.jar");
        Object resolved = ReflectionTestUtils.invokeMethod(
            new FlinkApplicationActionServiceImpl(), "resolveBuildResultForSubmit",
            application, built, "/client/sql-client.jar");
        assertThat(resolved).isSameAs(built);
    }

    @Test
    void preserveYarnJarArtifact() {
        Object resolved = ReflectionTestUtils.invokeMethod(
            new FlinkApplicationActionServiceImpl(), "resolveBuildResultForSubmit",
            application(FlinkJobType.FLINK_JAR), new SimpleBuildResponse(), "/app/user.jar");
        assertThat(((ShadedBuildResponse) resolved).shadedJarPath()).isEqualTo("/app/user.jar");
    }

    private FlinkApplication application(FlinkJobType jobType) {
        FlinkApplication application = new FlinkApplication();
        application.setDeployMode(FlinkDeployMode.YARN_APPLICATION.getMode());
        application.setJobType(jobType.getMode());
        return application;
    }
}
