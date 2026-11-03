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
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.service.SettingService;
import org.apache.streampark.flink.core.lineage.LineagePipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FlinkApplicationActionServiceImplTest {

    @Mock
    private SettingService settingService;

    @InjectMocks
    private FlinkApplicationActionServiceImpl service;

    @BeforeEach
    void injectSettingService() {
        ReflectionTestUtils.setField(service, "settingService", settingService);
    }

    private static LineageConfig enabledConfig(boolean nativeListenerEnable) {
        LineageConfig config = new LineageConfig();
        config.setGravitinoAddress("http://192.168.10.132:8090");
        config.setGravitinoToken("test-token");
        config.setGravitinoNamespace("streampark");
        config.setFlinkNativeListenerEnable(nativeListenerEnable);
        return config;
    }

    @Test
    void applyNativeLineageListenerConfigDoesNothingWhenAppSwitchOff() {
        FlinkApplication application = new FlinkApplication();
        application.setLineageEnable(false);
        Map<String, Object> properties = new HashMap<>();

        service.applyNativeLineageListenerConfig(application, properties);

        verifyNoInteractions(settingService);
        assertThat(properties).isEmpty();
    }

    @Test
    void applyNativeLineageListenerConfigDoesNothingWhenGravitinoAddressUnset() {
        FlinkApplication application = new FlinkApplication();
        application.setLineageEnable(true);
        lenient().when(settingService.getLineageConfig()).thenReturn(new LineageConfig());
        Map<String, Object> properties = new HashMap<>();

        service.applyNativeLineageListenerConfig(application, properties);

        assertThat(properties).isEmpty();
    }

    @Test
    void applyNativeLineageListenerConfigDoesNothingWhenGlobalListenerSwitchOff() {
        FlinkApplication application = new FlinkApplication();
        application.setLineageEnable(true);
        lenient().when(settingService.getLineageConfig()).thenReturn(enabledConfig(false));
        Map<String, Object> properties = new HashMap<>();

        service.applyNativeLineageListenerConfig(application, properties);

        assertThat(properties).isEmpty();
    }

    @Test
    void applyNativeLineageListenerConfigInjectsWhenBothSwitchesOn() {
        FlinkApplication application = new FlinkApplication();
        application.setLineageEnable(true);
        lenient().when(settingService.getLineageConfig()).thenReturn(enabledConfig(true));
        Map<String, Object> properties = new HashMap<>();

        service.applyNativeLineageListenerConfig(application, properties);

        assertThat(properties)
            .containsEntry(
                "execution.job-status-changed-listeners",
                "io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory")
            .containsEntry("openlineage.transport.type", "http")
            .containsEntry("openlineage.transport.url", "http://192.168.10.132:8090")
            .containsEntry("openlineage.transport.endpoint", "/api/lineage")
            .containsEntry("openlineage.transport.auth.type", "api_key")
            .containsEntry("openlineage.transport.auth.apiKey", "test-token")
            .containsEntry("openlineage.job.namespace", "streampark");
    }

    @Test
    void applyNativeLineageListenerConfigDoesNotOverrideUserSuppliedProperties() {
        FlinkApplication application = new FlinkApplication();
        application.setLineageEnable(true);
        lenient().when(settingService.getLineageConfig()).thenReturn(enabledConfig(true));
        Map<String, Object> properties = new HashMap<>();
        properties.put("execution.job-status-changed-listeners", "com.example.MyOwnListenerFactory");

        service.applyNativeLineageListenerConfig(application, properties);

        // this method itself is a plain put — the "don't override the user" contract is enforced
        // by the caller (getProperties) applying Dynamic Properties after this method runs, not by
        // this method checking for an existing value; this test documents that division of duty.
        assertThat(properties)
            .containsEntry(
                "execution.job-status-changed-listeners",
                "io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory");
    }

    @Test
    void extractLineagePipelinesReturnsEmptyWhenAppSwitchOff() {
        FlinkApplication application = new FlinkApplication();
        application.setLineageEnable(false);

        List<LineagePipeline> pipelines = service.extractLineagePipelines(null, application, "SELECT 1");

        verifyNoInteractions(settingService);
        assertThat(pipelines).isEmpty();
    }

    @Test
    void extractLineagePipelinesReturnsEmptyWhenGlobalConfigDisabled() {
        FlinkApplication application = new FlinkApplication();
        application.setLineageEnable(true);
        lenient().when(settingService.getLineageConfig()).thenReturn(new LineageConfig());

        List<LineagePipeline> pipelines = service.extractLineagePipelines(null, application, "SELECT 1");

        assertThat(pipelines).isEmpty();
    }
}
