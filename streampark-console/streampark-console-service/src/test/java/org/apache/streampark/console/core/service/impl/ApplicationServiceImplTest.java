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

import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.service.ProjectService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ApplicationServiceImplTest {

  @Mock private ProjectService projectService;

  private final ApplicationServiceImpl applicationService = new ApplicationServiceImpl();

  @TempDir private Path tempDir;

  @BeforeEach
  void setUp() {
    ReflectionTestUtils.setField(applicationService, "projectService", projectService);
  }

  @Test
  void getReadableConfFileShouldAllowConfigUnderProjectModuleConf() throws Exception {
    Path distHome = Files.createDirectory(tempDir.resolve("dist"));
    Path configFile =
        Files.createDirectories(distHome.resolve("app").resolve("conf"))
            .resolve("application.yaml");
    Files.write(configFile, "key: value".getBytes());

    when(projectService.getById(1L)).thenReturn(project(1L, 10L, distHome));

    Application application = application(1L, 10L, "app.tar.gz", configFile);

    assertThat(applicationService.getReadableConfFile(application))
        .isEqualTo(configFile.toFile().getCanonicalFile());
  }

  @Test
  void getReadableConfFileShouldRejectConfigOutsideProjectModuleConf() throws Exception {
    Path distHome = Files.createDirectory(tempDir.resolve("dist"));
    Files.createDirectories(distHome.resolve("app").resolve("conf"));
    Path secretFile = Files.write(tempDir.resolve("secret.txt"), "secret".getBytes());

    when(projectService.getById(1L)).thenReturn(project(1L, 10L, distHome));

    Application application = application(1L, 10L, "app.tar.gz", secretFile);

    assertThatThrownBy(() -> applicationService.getReadableConfFile(application))
        .isInstanceOf(ApiAlertException.class)
        .hasMessage("Invalid config.");
  }

  @Test
  void getReadableConfFileShouldRejectModuleTraversal() throws Exception {
    Path distHome = Files.createDirectory(tempDir.resolve("dist"));
    Path outsideModule = Files.createDirectories(tempDir.resolve("outside").resolve("conf"));
    Path configFile =
        Files.write(outsideModule.resolve("application.yaml"), "key: value".getBytes());

    Application application = application(1L, 10L, "../outside.tar.gz", configFile);

    assertThatThrownBy(() -> applicationService.getReadableConfFile(application))
        .isInstanceOf(ApiAlertException.class)
        .hasMessage("Invalid module.");
  }

  @Test
  void getReadableConfFileShouldRejectModulePathAlias() throws Exception {
    Path distHome = Files.createDirectory(tempDir.resolve("dist"));
    Path configFile =
        Files.createDirectories(distHome.resolve("app").resolve("conf"))
            .resolve("application.yaml");
    Files.write(configFile, "key: value".getBytes());

    Application application = application(1L, 10L, "app/conf/..", configFile);

    assertThatThrownBy(() -> applicationService.getReadableConfFile(application))
        .isInstanceOf(ApiAlertException.class)
        .hasMessage("Invalid module.");
  }

  @Test
  void getReadableConfFileShouldRejectProjectFromOtherTeam() throws Exception {
    Path distHome = Files.createDirectory(tempDir.resolve("dist"));
    Path configFile =
        Files.createDirectories(distHome.resolve("app").resolve("conf"))
            .resolve("application.yaml");
    Files.write(configFile, "key: value".getBytes());

    when(projectService.getById(1L)).thenReturn(project(1L, 20L, distHome));

    Application application = application(1L, 10L, "app.tar.gz", configFile);

    assertThatThrownBy(() -> applicationService.getReadableConfFile(application))
        .isInstanceOf(ApiAlertException.class)
        .hasMessage("Invalid project.");
  }

  @Test
  void getYarnNameShouldReadOnlyValidatedConfig() throws Exception {
    Path distHome = Files.createDirectory(tempDir.resolve("dist"));
    Path configFile =
        Files.createDirectories(distHome.resolve("app").resolve("conf"))
            .resolve("application.properties");
    Files.write(configFile, "flink.property.pipeline.name=test-app".getBytes());

    when(projectService.getById(1L)).thenReturn(project(1L, 10L, distHome));

    Application application = application(1L, 10L, "app.tar.gz", configFile);

    assertThat(applicationService.getYarnName(application)).isEqualTo("test-app");
  }

  private Application application(Long projectId, Long teamId, String module, Path configFile) {
    Application application = new Application();
    application.setProjectId(projectId);
    application.setTeamId(teamId);
    application.setModule(module);
    application.setConfig(configFile.toString());
    return application;
  }

  private Project project(Long id, Long teamId, Path distHome) {
    return new Project() {
      {
        setId(id);
        setTeamId(teamId);
      }

      @Override
      public File getDistHome() {
        return distHome.toFile();
      }
    };
  }
}
