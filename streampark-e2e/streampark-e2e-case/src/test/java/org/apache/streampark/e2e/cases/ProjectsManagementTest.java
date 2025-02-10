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
import org.apache.streampark.e2e.pages.resource.ProjectsPage;
import org.apache.streampark.e2e.pages.resource.ResourcePage;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@StreamPark(composeFiles = "docker/basic/docker-compose.yaml")
public class ProjectsManagementTest {

    private final Duration PROJECT_BUILD_TIMEOUT_MINUTES = Duration.ofMinutes(5);

    public static RemoteWebDriver browser;

    private static final String projectName = "e2e_test_project";

    private static final String editedProjectName = "e2e_test_project_edited";

    private static final String url = "https://github.com/apache/streampark-quickstart";

    private static final String branch = "dev";

    private static final String buildArgument =
        "-pl quickstart-flink/quickstart-apacheflink/apacheflinksql_1.16 -am -Dmaven.test.skip=true";

    private static final String description = "e2e test project description";

    @BeforeAll
    public static void setup() {
        new LoginPage(browser)
            .login()
            .goToNav(ResourcePage.class)
            .goToTab(ProjectsPage.class);
    }

    @Test
    @Order(1)
    void testCreateProject() {
        final ProjectsPage projectsPage = new ProjectsPage(browser);

        projectsPage.createProject(projectName, url, branch, buildArgument, description);

        Awaitility.await()
            .untilAsserted(
                () -> assertThat(projectsPage.projectList)
                    .as("Projects list should contain newly-created project")
                    .extracting(WebElement::getText)
                    .anyMatch(it -> it.contains(projectName)));
    }

    @Test
    @Order(2)
    void testEditProject() {
        final ProjectsPage projectsPage = new ProjectsPage(browser);
        projectsPage.editProject(projectName, editedProjectName);
        Awaitility.await()
            .untilAsserted(
                () -> assertThat(projectsPage.projectList)
                    .as("Projects list should contain edited project")
                    .extracting(WebElement::getText)
                    .anyMatch(it -> it.contains(editedProjectName)));
    }

    @Test
    @Order(3)
    void testBuildProject() {
        ProjectsPage projectsPage = new ProjectsPage(browser);

        projectsPage.buildProject(editedProjectName);

        Awaitility.await().timeout(PROJECT_BUILD_TIMEOUT_MINUTES)
            .untilAsserted(
                () -> assertThat(projectsPage.projectList)
                    .as("Projects list should contain build success project")
                    .extracting(WebElement::getText)
                    .anyMatch(it -> it.contains("SUCCESSFUL")));

    }

    @Test
    @Order(4)
    void testDeleteProject() {
        final ProjectsPage projectsPage = new ProjectsPage(browser);
        projectsPage.deleteProject(editedProjectName);
        Awaitility.await()
            .untilAsserted(
                () -> {
                    browser.navigate().refresh();
                    assertThat(projectsPage.projectList)
                        .noneMatch(it -> it.getText().contains(editedProjectName));
                });
    }
}
