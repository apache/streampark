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
import org.apache.streampark.console.core.service.GravitinoLineageService;
import org.apache.streampark.console.core.service.SettingService;
import org.apache.streampark.flink.core.lineage.LineageDataset;
import org.apache.streampark.flink.core.lineage.LineagePipeline;

import org.apache.commons.lang3.StringUtils;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ApiKeyTokenProvider;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class GravitinoLineageServiceImpl implements GravitinoLineageService {

    /**
     * Same run-facet path Gravitino's {@code JdbcLineageStorage.runningAppId(...)} already parses
     * on the receiving end (field name is engine-agnostic on the wire despite the "spark" prefix —
     * that convention predates this Flink integration and is kept here for cross-emitter
     * consistency, not reinvented).
     */
    private static final String RUN_FACET_APP_ID_KEY = "spark_properties";

    private static final String RUN_FACET_APP_ID_PROPERTY = "spark.app.id";

    private static final URI PRODUCER = URI.create("https://streampark.apache.org/");

    /**
     * How long a started run stays eligible for its terminal event. A run whose application is
     * deleted, or whose terminal state is never observed (Console restarted, watcher stopped
     * tracking it), would otherwise pin its entry forever — this map lives for the whole Console
     * process, so an unbounded one is a slow leak. The bound is time, not size: a legitimate
     * streaming job may run for weeks before its COMPLETE, and evicting it because newer jobs
     * started would lose the terminal event for the longest-running jobs first, which is exactly
     * backwards.
     */
    private static final Duration PENDING_RUN_TTL = Duration.ofDays(30);

    private final SettingService settingService;

    public GravitinoLineageServiceImpl(SettingService settingService) {
        this.settingService = settingService;
    }

    /** In-memory only — see class contract in {@link GravitinoLineageService}. */
    private final ConcurrentMap<Long, PendingRun> pendingRuns =
        Caffeine.newBuilder().expireAfterWrite(PENDING_RUN_TTL).<Long, PendingRun>build().asMap();

    @Override
    public void trackAndEmitStart(
                                  FlinkApplication application, String flinkJobIdHex,
                                  List<LineagePipeline> pipelines) {
        if (pipelines == null || pipelines.isEmpty()) {
            return;
        }
        LineageConfig config;
        try {
            config = settingService.getLineageConfig();
        } catch (Exception e) {
            log.warn("[lineage] failed to read lineage config, skipping START for application id={}",
                application.getId(), e);
            return;
        }
        if (!config.enabled()) {
            return;
        }
        PendingRun run =
            new PendingRun(flinkJobIdHex, config.namespaceOrDefault(), application.getJobName(), pipelines);
        emitRunEvents(config, EventType.START, application.getId(), run);
        // Tracked regardless of individual emit failures above: a later terminal call is itself
        // independently fail-open (see emitTerminal), so there is no harm in attempting it even for
        // a pipeline whose START never reached Gravitino — only a missed opportunity to close out
        // the ones that did.
        pendingRuns.put(application.getId(), run);
    }

    @Override
    public void emitTerminal(Long appId, boolean success) {
        PendingRun run = pendingRuns.remove(appId);
        if (run == null) {
            return;
        }
        LineageConfig config;
        try {
            config = settingService.getLineageConfig();
        } catch (Exception e) {
            log.warn("[lineage] failed to read lineage config, skipping terminal event for application id={}", appId,
                e);
            return;
        }
        if (!config.enabled()) {
            return;
        }
        emitRunEvents(config, success ? EventType.COMPLETE : EventType.FAIL, appId, run);
    }

    /**
     * Emits one event per pipeline of a run over a single client. Fail-open at both levels: an
     * unusable client costs the run its events, but one pipeline's failed emit must not cost the
     * remaining pipelines of the same job theirs.
     */
    private void emitRunEvents(LineageConfig config, EventType eventType, Long appId, PendingRun run) {
        try (OpenLineageClient client = buildClient(config)) {
            OpenLineage openLineage = new OpenLineage(PRODUCER);
            for (LineagePipeline pipeline : run.pipelines) {
                emitOne(client, openLineage, eventType, appId, run, pipeline);
            }
        } catch (Exception e) {
            log.warn("[lineage] failed to build Gravitino client for application id={}", appId, e);
        }
    }

    private void emitOne(
                         OpenLineageClient client,
                         OpenLineage openLineage,
                         EventType eventType,
                         Long appId,
                         PendingRun run,
                         LineagePipeline pipeline) {
        try {
            UUID runId = runIdFor(run.jobIdHex, pipeline.output());
            // The Flink JobID travels as a run facet on START only — a terminal event is matched to
            // its run by runId, and re-sending the facet would only restate what START established.
            String startFacetJobIdHex = eventType == EventType.START ? run.jobIdHex : null;
            client.emit(
                buildEvent(
                    openLineage, eventType, runId, run.jobNamespace, run.jobName, startFacetJobIdHex, pipeline));
        } catch (Exception e) {
            log.warn(
                "[lineage] failed to emit {} for application id={}, sink={}",
                eventType,
                appId,
                pipeline.output(),
                e);
        }
    }

    /**
     * Deterministic OpenLineage runId for one (Flink JobID, sink dataset) pair, stable across a
     * pipeline's START/COMPLETE/FAIL. Must stay byte-identical to the same algorithm used elsewhere
     * against this Gravitino deployment — this is what lets independently-emitted events for the
     * same run agree on its identity without any shared state.
     */
    static UUID runIdFor(String flinkJobIdHex, LineageDataset output) {
        String key = "flink-job:" + flinkJobIdHex + ":" + output.namespace() + "/" + output.name();
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Test-only observation hook into the in-memory pending-run tracking. */
    boolean hasPendingRun(Long appId) {
        return pendingRuns.containsKey(appId);
    }

    private OpenLineage.RunEvent buildEvent(
                                            OpenLineage openLineage,
                                            EventType eventType,
                                            UUID runId,
                                            String jobNamespace,
                                            String jobName,
                                            String startFacetJobIdHex,
                                            LineagePipeline pipeline) {
        OpenLineage.RunFacetsBuilder facetsBuilder = openLineage.newRunFacetsBuilder();
        if (startFacetJobIdHex != null) {
            OpenLineage.DefaultRunFacet appIdFacet = new OpenLineage.DefaultRunFacet(PRODUCER);
            appIdFacet
                .getAdditionalProperties()
                .put("properties", Map.of(RUN_FACET_APP_ID_PROPERTY, startFacetJobIdHex));
            facetsBuilder.put(RUN_FACET_APP_ID_KEY, appIdFacet);
        }
        OpenLineage.Run run = openLineage.newRun(runId, facetsBuilder.build());
        OpenLineage.Job job = openLineage.newJob(jobNamespace, jobName, openLineage.newJobFacetsBuilder().build());

        List<OpenLineage.InputDataset> inputs = pipeline.inputs().stream()
            .map(input -> openLineage.newInputDataset(input.namespace(), input.name(), null, null))
            .collect(Collectors.toList());
        List<OpenLineage.OutputDataset> outputs =
            Collections.singletonList(
                openLineage.newOutputDataset(
                    pipeline.output().namespace(), pipeline.output().name(), null, null));

        return openLineage.newRunEvent(ZonedDateTime.now(ZoneOffset.UTC), eventType, run, job, inputs, outputs);
    }

    private OpenLineageClient buildClient(LineageConfig config) {
        HttpConfig httpConfig = new HttpConfig();
        httpConfig.setUrl(URI.create(config.getGravitinoAddress()));
        httpConfig.setEndpoint(LineageConfig.LINEAGE_ENDPOINT_PATH);
        if (StringUtils.isNotBlank(config.getGravitinoToken())) {
            ApiKeyTokenProvider tokenProvider = new ApiKeyTokenProvider();
            tokenProvider.setApiKey(config.getGravitinoToken());
            httpConfig.setAuth(tokenProvider);
        }
        return OpenLineageClient.builder().transport(new HttpTransport(httpConfig)).build();
    }

    private static final class PendingRun {

        private final String jobIdHex;
        private final String jobNamespace;
        private final String jobName;
        private final List<LineagePipeline> pipelines;

        private PendingRun(String jobIdHex, String jobNamespace, String jobName, List<LineagePipeline> pipelines) {
            this.jobIdHex = jobIdHex;
            this.jobNamespace = jobNamespace;
            this.jobName = jobName;
            this.pipelines = pipelines;
        }
    }
}
