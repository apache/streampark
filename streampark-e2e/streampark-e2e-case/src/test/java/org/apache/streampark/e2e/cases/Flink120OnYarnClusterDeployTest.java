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
import org.apache.streampark.e2e.pages.flink.clusters.YarnSessionForm;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;

@StreamPark(composeFiles = "docker/flink-1.20-on-yarn/docker-compose.yaml")
public class Flink120OnYarnClusterDeployTest {

    public static RemoteWebDriver browser;

    private static final String flinkName = "flink-1.20.1";

    private static final String flinkHome = "/flink-1.20.1";

    private static final String flinkDescription = "description test";

    private static final String flinkClusterName = "flink_1.20.1_cluster_e2e";

    private static final String flinkClusterNameEdited = "flink_1.20.1_cluster_e2e_edited";

    private static final ClusterDetailForm.DeployMode deployMode = ClusterDetailForm.DeployMode.YARN_SESSION;

    @BeforeAll
    public static void setup() {
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
        final FlinkClustersPage flinkClustersPage = new FlinkClustersPage(browser);

        flinkClustersPage.createFlinkCluster()
            .<YarnSessionForm>addCluster(deployMode)
            .resolveOrder(YarnSessionForm.ResolveOrder.CHILD_FIRST)
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
    @Order(2)
    public void testEditFlinkCluster() {
        final FlinkClustersPage flinkClustersPage = new FlinkClustersPage(browser);

        flinkClustersPage.editFlinkCluster(flinkClusterName)
            .<YarnSessionForm>addCluster(deployMode)
            .clusterName(flinkClusterNameEdited)
            .submit();

        Awaitility.await()
            .untilAsserted(
                () -> assertThat(flinkClustersPage.flinkClusterList)
                    .as("Flink clusters list should contain edited application")
                    .extracting(WebElement::getText)
                    .anyMatch(it -> it.contains(flinkClusterNameEdited)));
    }

    @Test
    @Order(3)
    public void testStartFlinkCluster() {
        final FlinkClustersPage flinkClustersPage = new FlinkClustersPage(browser);

        flinkClustersPage.startFlinkCluster(flinkClusterNameEdited);

        Awaitility.await()
            .untilAsserted(
                () -> assertThat(flinkClustersPage.flinkClusterList)
                    .as("Flink clusters list should contain running application")
                    .extracting(WebElement::getText)
                    .anyMatch(it -> it.contains("RUNNING")));
    }

    @Test
    @Order(4)
    public void testStopFlinkCluster() {
        final FlinkClustersPage flinkClustersPage = new FlinkClustersPage(browser);

        flinkClustersPage.stopFlinkCluster(flinkClusterNameEdited);

        Awaitility.await()
            .untilAsserted(
                () -> assertThat(flinkClustersPage.flinkClusterList)
                    .as("Flink clusters list should contain canceled application")
                    .extracting(WebElement::getText)
                    .anyMatch(it -> it.contains("CANCELED")));
    }

    @Test
    @Order(5)
    public void testDeleteFlinkCluster() {
        final FlinkClustersPage flinkClustersPage = new FlinkClustersPage(browser);

        flinkClustersPage.deleteFlinkCluster(flinkClusterNameEdited);

        Awaitility.await()
            .untilAsserted(
                () -> {
                    browser.navigate().refresh();
                    Thread.sleep(Constants.DEFAULT_SLEEP_MILLISECONDS);
                    assertThat(flinkClustersPage.flinkClusterList)
                        .noneMatch(it -> it.getText().contains(flinkClusterNameEdited));
                });
    }
}
