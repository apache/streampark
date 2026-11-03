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

package org.apache.streampark.console.core.bean;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.Setter;

/**
 * Gravitino lineage configuration. {@link #enabled()} is the single gate every lineage call site
 * checks before doing any work: it is only true when an operator has actually filled in a
 * Gravitino address, so a fresh install with these settings left blank injects nothing and emits
 * nothing.
 */
@Getter
@Setter
public class LineageConfig {

    /**
     * Namespace used when an operator has not set one. It ends up inside emitted event payloads and
     * inside injected job configuration, so both paths must agree on it — hence one definition.
     */
    public static final String DEFAULT_NAMESPACE = "streampark";

    /**
     * Gravitino's lineage-ingest path, appended to {@link #gravitinoAddress}. Written into the
     * Flink and Spark listener configuration as well as used by the Console's own emitter, and all
     * three must address the same endpoint — hence one definition.
     */
    public static final String LINEAGE_ENDPOINT_PATH = "/api/lineage";

    /** Gravitino base URL, e.g. {@code http://192.168.10.132:8090}. */
    private String gravitinoAddress;

    /** Bearer token forwarded as-is to Gravitino's {@code /api/lineage}; required once oauth is enabled there. */
    private String gravitinoToken;

    /** OpenLineage job/dataset namespace StreamPark reports under. */
    private String gravitinoNamespace;

    /** Whether to also inject the official {@code openlineage-flink} job-status-changed-listener config. */
    private boolean flinkNativeListenerEnable;

    public boolean enabled() {
        return StringUtils.isNotBlank(gravitinoAddress);
    }

    /** The configured namespace, or {@link #DEFAULT_NAMESPACE} when none was set. */
    public String namespaceOrDefault() {
        return StringUtils.defaultIfBlank(gravitinoNamespace, DEFAULT_NAMESPACE);
    }
}
