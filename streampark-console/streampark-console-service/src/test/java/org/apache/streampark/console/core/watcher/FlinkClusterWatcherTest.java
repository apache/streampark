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

package org.apache.streampark.console.core.watcher;

import org.apache.streampark.common.enums.ClusterState;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.console.core.entity.FlinkCluster;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkClusterWatcherTest {

    private HttpServer server;
    private String address;

    @BeforeEach
    void startRestEndpoint() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/overview", exchange -> {
            byte[] response = "{\"taskmanagers\":1,\"slots-total\":2,\"slots-available\":2}"
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (java.io.OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });
        server.start();
        address = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopRestEndpoint() {
        server.stop(0);
    }

    @Test
    void verifyKubernetesSessionRestEndpoint() {
        FlinkCluster cluster = cluster(FlinkDeployMode.KUBERNETES_NATIVE_SESSION);
        cluster.setJobManagerUrl(address);
        FlinkClusterWatcher watcher = new FlinkClusterWatcher();
        assertThat(watcher.verifyClusterConnection(cluster)).isTrue();
        assertThat(watcher.getClusterState(cluster)).isEqualTo(ClusterState.RUNNING);
    }

    @Test
    void preserveRemoteRestHealthCheck() {
        assertThat(new FlinkClusterWatcher().verifyClusterConnection(cluster(FlinkDeployMode.REMOTE)))
            .isTrue();
    }

    @Test
    void rejectUnreachableKubernetesSession() {
        FlinkCluster cluster = cluster(FlinkDeployMode.KUBERNETES_NATIVE_SESSION);
        server.stop(0);
        assertThat(new FlinkClusterWatcher().verifyClusterConnection(cluster)).isFalse();
    }

    private FlinkCluster cluster(FlinkDeployMode mode) {
        FlinkCluster cluster = new FlinkCluster();
        cluster.setId(4508L);
        cluster.setDeployMode(mode.getMode());
        cluster.setAddress(address);
        return cluster;
    }
}
