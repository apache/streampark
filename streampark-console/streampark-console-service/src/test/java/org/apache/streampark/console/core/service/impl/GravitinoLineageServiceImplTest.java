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

package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.console.core.bean.LineageConfig;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.service.SettingService;
import org.apache.streampark.flink.core.lineage.LineageDataset;
import org.apache.streampark.flink.core.lineage.LineagePipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GravitinoLineageServiceImplTest {

    @Mock
    private SettingService settingService;

    @InjectMocks
    private GravitinoLineageServiceImpl service;

    private static LineagePipeline pipeline() {
        LineageDataset output = new LineageDataset("paimon://catalog/db", "sink_table");
        LineageDataset input = new LineageDataset("mysql-cdc://host:3306", "db.source_table");
        return new LineagePipeline(output, Set.of(input));
    }

    // Unreachable on purpose — exercises the fail-open path without a real Gravitino server.
    private static LineageConfig unreachableEnabledConfig() {
        LineageConfig config = new LineageConfig();
        config.setGravitinoAddress("http://127.0.0.1:1");
        config.setGravitinoNamespace("streampark");
        return config;
    }

    @Test
    void runIdForIsDeterministicForTheSameJobAndDataset() {
        LineageDataset output = new LineageDataset("paimon://catalog/db", "sink_table");

        java.util.UUID first = GravitinoLineageServiceImpl.runIdFor("abc123", output);
        java.util.UUID second = GravitinoLineageServiceImpl.runIdFor("abc123", output);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void runIdForDiffersAcrossDifferentJobsOrDatasets() {
        LineageDataset output = new LineageDataset("paimon://catalog/db", "sink_table");
        LineageDataset otherOutput = new LineageDataset("paimon://catalog/db", "other_sink");

        java.util.UUID a = GravitinoLineageServiceImpl.runIdFor("job-1", output);
        java.util.UUID b = GravitinoLineageServiceImpl.runIdFor("job-2", output);
        java.util.UUID c = GravitinoLineageServiceImpl.runIdFor("job-1", otherOutput);

        assertThat(a).isNotEqualTo(b).isNotEqualTo(c);
    }

    @Test
    void runIdForMatchesTheNameUuidOfTheDocumentedKeyFormat() {
        LineageDataset output = new LineageDataset("paimon://catalog/db", "sink_table");
        String expectedKey = "flink-job:abc123:paimon://catalog/db/sink_table";

        java.util.UUID actual = GravitinoLineageServiceImpl.runIdFor("abc123", output);

        assertThat(actual)
            .isEqualTo(java.util.UUID.nameUUIDFromBytes(expectedKey.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @Test
    void trackAndEmitStartNoOpsOnEmptyPipelines() {
        FlinkApplication application = new FlinkApplication();
        application.setId(1L);

        service.trackAndEmitStart(application, "job-1", List.of());

        verifyNoInteractions(settingService);
        assertThat(service.hasPendingRun(1L)).isFalse();
    }

    @Test
    void trackAndEmitStartNoOpsWhenLineageDisabled() {
        FlinkApplication application = new FlinkApplication();
        application.setId(1L);
        lenient().when(settingService.getLineageConfig()).thenReturn(new LineageConfig());

        service.trackAndEmitStart(application, "job-1", List.of(pipeline()));

        assertThat(service.hasPendingRun(1L)).isFalse();
    }

    @Test
    void trackAndEmitStartTracksThePendingRunEvenWhenGravitinoIsUnreachable() {
        FlinkApplication application = new FlinkApplication();
        application.setId(1L);
        application.setJobName("test-job");
        lenient().when(settingService.getLineageConfig()).thenReturn(unreachableEnabledConfig());

        service.trackAndEmitStart(application, "job-1", List.of(pipeline()));

        assertThat(service.hasPendingRun(1L)).isTrue();
    }

    @Test
    void emitTerminalNoOpsWhenNoPendingRunIsTracked() {
        // Must not throw even though nothing was ever tracked for this appId.
        service.emitTerminal(999L, true);

        verifyNoInteractions(settingService);
        assertThat(service.hasPendingRun(999L)).isFalse();
    }

    @Test
    void emitTerminalConsumesThePendingRunExactlyOnce() {
        FlinkApplication application = new FlinkApplication();
        application.setId(1L);
        application.setJobName("test-job");
        lenient().when(settingService.getLineageConfig()).thenReturn(unreachableEnabledConfig());
        service.trackAndEmitStart(application, "job-1", List.of(pipeline()));
        assertThat(service.hasPendingRun(1L)).isTrue();

        service.emitTerminal(1L, true);

        assertThat(service.hasPendingRun(1L)).isFalse();
        // second call for the same appId is a no-op, not an error
        service.emitTerminal(1L, true);
    }
}
