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

package org.apache.streampark.e2e.cases;

import org.apache.streampark.e2e.core.StreamPark;
import org.apache.streampark.e2e.pages.LoginPage;
import org.apache.streampark.e2e.pages.common.Constants;
import org.apache.streampark.e2e.pages.flink.ApacheFlinkPage;
import org.apache.streampark.e2e.pages.flink.FlinkHomePage;
import org.apache.streampark.e2e.pages.flink.clusters.ClusterDetailForm;
import org.apache.streampark.e2e.pages.flink.clusters.FlinkClustersPage;
import org.apache.streampark.e2e.pages.flink.clusters.RemoteForm;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;

@StreamPark(composeFiles = "docker/flink-1.20-on-remote/docker-compose.yaml")
public class Flink120OnRemoteClusterDeployTest {

    public static RemoteWebDriver browser;

    private static final String flinkName = "flink-1.20.1";

    private static final String flinkHome = "/opt/flink/";

    private static final String flinkDescription = "description test";

    private static final String flinkClusterName = "flink_1.20.1_cluster_e2e";

    private static final String flinkJobManagerUrl = "http://jobmanager:8081";

    private static final ClusterDetailForm.DeployMode deployMode = ClusterDetailForm.DeployMode.STANDALONE;

    @BeforeAll
    public static void setUp() {
        FlinkHomePage flinkHomePage = new LoginPage(browser)
            .login()
            .goToNav(ApacheFlinkPage.class)
            .goToTab(FlinkHomePage.class);

        flinkHomePage.createFlinkHome(flinkName, flinkHome, flinkDescription);

        flinkHomePage.goToNav(ApacheFlinkPage.class)
            .goToTab(FlinkClustersPage.class);
    }

    @Test
    @Order(1)
    public void testCreateFlinkCluster() {
        FlinkClustersPage flinkClustersPage = new FlinkClustersPage(browser);

        flinkClustersPage.createFlinkCluster()
            .<RemoteForm>addCluster(deployMode)
            .jobManagerURL(flinkJobManagerUrl)
            .clusterName(flinkClusterName)
            .flinkVersion(flinkName)
            .submit();

        Awaitility.await()
            .untilAsserted(
                () -> assertThat(flinkClustersPage.flinkClusterList)
                    .as("Flink clusters list should contain newly-created application")
                    .extracting(WebElement::getText)
                    .anyMatch(it -> it.contains(flinkClusterName)));
    }

    @Test
    @Order(5)
    public void testDeleteFlinkCluster() {
        final FlinkClustersPage flinkClustersPage = new FlinkClustersPage(browser);

        flinkClustersPage.deleteFlinkCluster(flinkClusterName);

        Awaitility.await()
            .untilAsserted(
                () -> {
                    browser.navigate().refresh();
                    Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
                    assertThat(flinkClustersPage.flinkClusterList)
                        .noneMatch(it -> it.getText().contains(flinkClusterName));
                });
    }
}
