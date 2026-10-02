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

import org.apache.streampark.console.core.bean.LineageConfig;
import org.apache.streampark.console.core.entity.SparkApplication;
import org.apache.streampark.console.core.service.SettingService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SparkApplicationActionServiceImplTest {

    @Mock
    private SettingService settingService;

    @InjectMocks
    private SparkApplicationActionServiceImpl service;

    private LineageConfig enabledLineageConfig() {
        LineageConfig config = new LineageConfig();
        config.setGravitinoAddress("http://192.168.10.132:8090");
        config.setGravitinoToken("test-token");
        config.setGravitinoNamespace("streampark");
        return config;
    }

    @Test
    void injectsLineageConfigWhenAppSwitchAndGlobalAddressAreBothSet() {
        SparkApplication application = new SparkApplication();
        application.setLineageEnable(true);
        lenient().when(settingService.getLineageConfig()).thenReturn(enabledLineageConfig());

        Map<String, String> sparkProperties = new HashMap<>();
        service.applyLineageConfig(application, sparkProperties);

        assertThat(sparkProperties)
            .containsEntry("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
            .containsEntry("spark.openlineage.transport.type", "http")
            .containsEntry("spark.openlineage.transport.url", "http://192.168.10.132:8090")
            .containsEntry("spark.openlineage.transport.endpoint", "/api/lineage")
            .containsEntry("spark.openlineage.transport.auth.type", "api_key")
            .containsEntry("spark.openlineage.transport.auth.apiKey", "test-token")
            .containsEntry("spark.openlineage.namespace", "streampark")
            .containsEntry("spark.openlineage.columnLineage.datasetLineageEnabled", "true");
    }

    @Test
    void doesNotInjectWhenAppSwitchIsOff() {
        SparkApplication application = new SparkApplication();
        application.setLineageEnable(false);

        Map<String, String> sparkProperties = new HashMap<>();
        service.applyLineageConfig(application, sparkProperties);

        assertThat(sparkProperties).isEmpty();
    }

    @Test
    void doesNotInjectWhenAppSwitchIsNull() {
        SparkApplication application = new SparkApplication();

        Map<String, String> sparkProperties = new HashMap<>();
        service.applyLineageConfig(application, sparkProperties);

        assertThat(sparkProperties).isEmpty();
    }

    @Test
    void doesNotInjectWhenGlobalGravitinoAddressIsBlank() {
        SparkApplication application = new SparkApplication();
        application.setLineageEnable(true);
        lenient().when(settingService.getLineageConfig()).thenReturn(new LineageConfig());

        Map<String, String> sparkProperties = new HashMap<>();
        service.applyLineageConfig(application, sparkProperties);

        assertThat(sparkProperties).isEmpty();
    }

    @Test
    void doesNotOverrideUserSuppliedProperties() {
        SparkApplication application = new SparkApplication();
        application.setLineageEnable(true);
        lenient().when(settingService.getLineageConfig()).thenReturn(enabledLineageConfig());

        Map<String, String> sparkProperties = new HashMap<>();
        sparkProperties.put("spark.extraListeners", "com.example.MyOwnListener");
        service.applyLineageConfig(application, sparkProperties);

        // Other lineage keys the user did not set are still injected.
        assertThat(sparkProperties)
            .containsEntry("spark.extraListeners", "com.example.MyOwnListener")
            .containsEntry("spark.openlineage.transport.type", "http");
    }
}
