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

package org.apache.streampark.console.core.service;

import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.flink.core.lineage.LineagePipeline;

import java.util.List;

/**
 * Reports Flink job table-level lineage to Gravitino's {@code POST /api/lineage} as OpenLineage
 * {@code RunEvent}s.
 *
 * <p>Every method here is fail-open by contract: a disabled switch, an unconfigured Gravitino
 * address, or any failure while talking to Gravitino is logged and swallowed, never thrown. This
 * runs on the job submission and state-watching paths, where a lineage gap must never affect the
 * job itself.
 *
 * <p>{@link #trackAndEmitStart} and {@link #emitTerminal} are a pair: a successful start call
 * remembers the run in memory so the later terminal call (driven by {@code FlinkAppHttpWatcher}'s
 * state polling) knows what to close out, without needing the caller to thread pipeline data
 * through the whole state-watching path. This tracking is in-memory only — it does not survive a
 * Console restart, so a run whose job finishes while Console is down never gets its COMPLETE/FAIL
 * event. That is a deliberate, bounded scope decision (see the implementation), not an oversight.
 */
public interface GravitinoLineageService {

    /**
     * Called once, right after a Flink SQL job's submission succeeds. No-ops if {@code pipelines}
     * is empty (extraction found nothing, or lineage is disabled for this application/globally).
     *
     * @param application the just-started application (its id keys the in-memory pending-run
     *     tracking consumed by {@link #emitTerminal})
     * @param flinkJobIdHex the Flink JobID this run was submitted with
     * @param pipelines the pipelines resolved from the job's SQL; safe to pass an empty list
     */
    void trackAndEmitStart(FlinkApplication application, String flinkJobIdHex, List<LineagePipeline> pipelines);

    /**
     * Called when {@code FlinkAppHttpWatcher} observes an application transition into a terminal
     * state. No-ops if no pending run is tracked for {@code appId} (lineage was never started for
     * this run, or it was already closed out).
     *
     * @param appId the application id
     * @param success {@code true} to emit COMPLETE, {@code false} to emit FAIL
     */
    void emitTerminal(Long appId, boolean success);
}
