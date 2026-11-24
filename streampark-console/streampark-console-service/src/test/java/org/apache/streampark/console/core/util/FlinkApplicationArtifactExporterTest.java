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

package org.apache.streampark.console.core.util;

import org.apache.streampark.common.enums.ApplicationType;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.flink.packer.pipeline.BuildResult;
import org.apache.streampark.flink.packer.pipeline.DockerImageBuildResponse;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;
import org.apache.streampark.flink.packer.pipeline.SimpleBuildResponse;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies archive layout and source-boundary failures for job artifact downloads. */
class FlinkApplicationArtifactExporterTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void shouldWriteArchiveWithDependencies() throws Exception {
        Path workspace = createWorkspace();
        FlinkApplication application = application(workspace);
        ShadedBuildResponse result = new ShadedBuildResponse(workspace.toString(), "main.jar");
        Path archive = temporaryDirectory.resolve("artifact.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application, result, null, archive, "app-42-dependencies");

        try (ZipFile zip = new ZipFile(archive.toFile())) {
            List<String> names = new ArrayList<>();
            for (ZipEntry entry : java.util.Collections.list(zip.entries())) {
                names.add(entry.getName());
            }
            assertThat(names)
                .containsExactly(
                    "app-42-dependencies/",
                    "app-42-dependencies/main.jar",
                    "app-42-dependencies/lib/dep.jar");
            assertThat(
                new String(
                    zip.getInputStream(zip.getEntry("app-42-dependencies/main.jar")).readAllBytes(),
                    StandardCharsets.UTF_8))
                        .isEqualTo("main");
            assertThat(
                new String(
                    zip.getInputStream(zip.getEntry("app-42-dependencies/lib/dep.jar"))
                        .readAllBytes(),
                    StandardCharsets.UTF_8))
                        .isEqualTo("dependency");
        }
    }

    @Test
    void shouldRejectSymlinkBeforeResponse() throws Exception {
        Path workspace = createWorkspace();
        Path outside = temporaryDirectory.resolve("outside.jar");
        Files.writeString(outside, "outside");
        try {
            Files.createSymbolicLink(workspace.resolve("lib/bad.jar"), outside);
        } catch (UnsupportedOperationException | IOException e) {
            Assumptions.assumeTrue(false, "symbolic links are unavailable");
        }
        FlinkApplication application = application(workspace);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.write(
                application,
                new ShadedBuildResponse(workspace.toString(), "main.jar"),
                null,
                response,
                () -> {
                }))
                    .isInstanceOf(ApiAlertException.class)
                    .hasMessage("No downloadable job artifact is available.");
        assertThat(response.getHeader("Content-Disposition")).isNull();
        assertThat(response.getContentAsByteArray()).isEmpty();
    }

    @Test
    void shouldRejectSymlinkLibBeforeResponse() throws Exception {
        Path workspace = createWorkspace();
        Path outsideLib = Files.createDirectories(temporaryDirectory.resolve("outside-lib"));
        Files.writeString(outsideLib.resolve("dep.jar"), "dependency");
        Files.delete(workspace.resolve("lib/dep.jar"));
        Files.delete(workspace.resolve("lib"));
        try {
            Files.createSymbolicLink(workspace.resolve("lib"), outsideLib);
        } catch (UnsupportedOperationException | IOException e) {
            Assumptions.assumeTrue(false, "symbolic links are unavailable");
        }
        FlinkApplication application = application(workspace);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.write(
                application,
                new ShadedBuildResponse(workspace.toString(), "main.jar"),
                null,
                response,
                () -> {
                }))
                    .isInstanceOf(ApiAlertException.class);
        assertThat(response.getHeader("Content-Disposition")).isNull();
        assertThat(response.getContentAsByteArray()).isEmpty();
    }

    @Test
    void shouldRejectFileLibBeforeResponse() throws Exception {
        Path workspace = createWorkspace();
        Files.delete(workspace.resolve("lib/dep.jar"));
        Files.delete(workspace.resolve("lib"));
        Files.writeString(workspace.resolve("lib"), "not a directory");
        FlinkApplication application = application(workspace);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.write(
                application,
                new ShadedBuildResponse(workspace.toString(), "main.jar"),
                null,
                response,
                () -> {
                }))
                    .isInstanceOf(ApiAlertException.class)
                    .hasMessage("No downloadable job artifact is available.");
        assertThat(response.getHeader("Content-Disposition")).isNull();
        assertThat(response.getContentAsByteArray()).isEmpty();
    }

    @Test
    void shouldRejectFailedBuild() throws Exception {
        Path workspace = createWorkspace();
        FlinkApplication application = application(workspace);
        Path archive = temporaryDirectory.resolve("failed.zip");

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.createArchive(
                application,
                new ShadedBuildResponse(workspace.toString(), "main.jar", false),
                null,
                archive,
                "app-42-dependencies"))
                    .isInstanceOf(ApiAlertException.class);
        assertThat(archive).doesNotExist();
    }

    @Test
    void shouldWriteMainOnly() throws Exception {
        Path workspace = Files.createDirectories(temporaryDirectory.toRealPath().resolve("main-only"));
        Files.writeString(workspace.resolve("main.jar"), "main");
        FlinkApplication application = application(workspace);
        Path archive = temporaryDirectory.resolve("main-only.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new ShadedBuildResponse(workspace.toString(), "main.jar"),
            null,
            archive,
            "app-42-dependencies");

        try (ZipFile zip = new ZipFile(archive.toFile())) {
            assertThat(Collections.list(zip.entries()).stream().map(ZipEntry::getName))
                .containsExactly("app-42-dependencies/", "app-42-dependencies/main.jar");
        }
    }

    @Test
    void shouldRejectMissingRequiredLib() throws Exception {
        Path workspace = Files.createDirectories(temporaryDirectory.toRealPath().resolve("main-only"));
        Files.writeString(workspace.resolve("main.jar"), "main");
        for (FlinkDeployMode deployMode : List.of(
            FlinkDeployMode.REMOTE,
            FlinkDeployMode.YARN_SESSION,
            FlinkDeployMode.YARN_PER_JOB,
            FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION)) {
            FlinkApplication application = application(workspace);
            when(application.getDeployModeEnum()).thenReturn(deployMode);
            when(application.isFlinkSqlJob()).thenReturn(true);
            MockHttpServletResponse response = new MockHttpServletResponse();
            BuildResult result = deployMode == FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION
                ? new DockerImageBuildResponse(
                    workspace.toString(), "flink:test", Collections.emptyMap(), "local:///main.jar")
                : new ShadedBuildResponse(workspace.toString(), "main.jar");

            assertThatThrownBy(
                () -> FlinkApplicationArtifactExporter.write(application, result, null, response, () -> {
                }))
                    .as("missing lib for %s", deployMode)
                    .isInstanceOf(ApiAlertException.class);
            assertThat(response.getHeader("Content-Disposition")).isNull();
            assertThat(response.getContentAsByteArray()).isEmpty();
        }
    }

    @Test
    void shouldWriteK8sSessionSqlWithoutLib() throws Exception {
        Path workspace = Files.createDirectories(temporaryDirectory.toRealPath().resolve("session-sql"));
        Files.writeString(workspace.resolve("main.jar"), "shaded");
        FlinkApplication application = application(workspace);
        when(application.getDeployModeEnum()).thenReturn(FlinkDeployMode.KUBERNETES_NATIVE_SESSION);
        when(application.isFlinkSqlJob()).thenReturn(true);
        Path archive = temporaryDirectory.resolve("session-sql.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new ShadedBuildResponse(workspace.toString(), "main.jar"),
            null,
            archive,
            "app-42-dependencies");

        assertThat(zipNames(archive))
            .containsExactly("app-42-dependencies/", "app-42-dependencies/main.jar");

        Path lib = Files.createDirectory(workspace.resolve("lib"));
        Files.writeString(lib.resolve("unrelated.jar"), "not part of the session build");
        FlinkApplicationArtifactExporter.createArchive(
            application,
            new ShadedBuildResponse(workspace.toString(), "main.jar"),
            null,
            archive,
            "app-42-dependencies");

        assertThat(zipNames(archive))
            .containsExactly("app-42-dependencies/", "app-42-dependencies/main.jar");
    }

    @Test
    void shouldKeepStreamParkMainInLocalLib() throws Exception {
        Path workspace = Files.createDirectories(temporaryDirectory.toRealPath().resolve("streampark-lib"));
        Path lib = Files.createDirectories(workspace.resolve("lib"));
        Files.writeString(lib.resolve("module.jar"), "main");
        Files.writeString(lib.resolve("dep.jar"), "dependency");
        FlinkApplication application = application(workspace);
        when(application.isStreamParkType()).thenReturn(true);
        Path archive = temporaryDirectory.resolve("streampark-local.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new ShadedBuildResponse(workspace.toString(), lib.resolve("module.jar").toString()),
            null,
            archive,
            "app-42-dependencies");

        assertThat(zipNames(archive))
            .containsExactly(
                "app-42-dependencies/",
                "app-42-dependencies/module.jar",
                "app-42-dependencies/lib/dep.jar");
    }

    @Test
    void shouldKeepStreamParkMainOnlyInLocalLib() throws Exception {
        Path workspace = Files.createDirectories(temporaryDirectory.toRealPath().resolve("streampark-main-only"));
        Path lib = Files.createDirectories(workspace.resolve("lib"));
        Files.writeString(lib.resolve("module.jar"), "main");
        FlinkApplication application = application(workspace);
        when(application.isStreamParkType()).thenReturn(true);
        Path archive = temporaryDirectory.resolve("streampark-main-only.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new ShadedBuildResponse(workspace.toString(), lib.resolve("module.jar").toString()),
            null,
            archive,
            "app-42-dependencies");

        assertThat(zipNames(archive))
            .containsExactly("app-42-dependencies/", "app-42-dependencies/module.jar");
    }

    @Test
    void shouldWriteDockerJarAndDependencies() throws Exception {
        Path workspace = createWorkspace();
        FlinkApplication application = application(workspace);
        Path archive = temporaryDirectory.resolve("docker.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new DockerImageBuildResponse(
                workspace.toString(),
                "flink:test",
                Collections.emptyMap(),
                "local:///opt/flink/usrlib/main.jar"),
            null,
            archive,
            "app-42-dependencies");

        assertThat(zipNames(archive))
            .containsExactly(
                "app-42-dependencies/",
                "app-42-dependencies/main.jar",
                "app-42-dependencies/lib/dep.jar");
    }

    @Test
    void shouldKeepSameNamedYarnDependency() throws Exception {
        String appHome = "/hdfs/apps/43";
        TestFsOperator fs = new TestFsOperator()
            .directory(appHome)
            .directory(appHome + "/lib")
            .file(appHome + "/main.jar", "main")
            .file(appHome + "/lib/main.jar", "dependency")
            .file(appHome + "/lib/dep.jar", "dependency");
        FlinkApplication application = mock(FlinkApplication.class);
        when(application.getId()).thenReturn(43L);
        when(application.getDeployModeEnum()).thenReturn(FlinkDeployMode.YARN_APPLICATION);
        when(application.isFlinkJar()).thenReturn(true);
        when(application.isFlinkSqlJob()).thenReturn(false);
        when(application.isStreamParkType()).thenReturn(false);
        when(application.getApplicationType()).thenReturn(ApplicationType.APACHE_FLINK);
        when(application.getJar()).thenReturn("main.jar");
        when(application.getAppHome()).thenReturn(appHome);
        when(application.getAppLib()).thenReturn(appHome + "/lib");
        Path archive = temporaryDirectory.resolve("yarn-same-name.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new SimpleBuildResponse("unused", true),
            null,
            archive,
            "app-43-dependencies",
            fs,
            fs);

        assertThat(zipNames(archive))
            .containsExactly(
                "app-43-dependencies/",
                "app-43-dependencies/main.jar",
                "app-43-dependencies/lib/dep.jar",
                "app-43-dependencies/lib/main.jar");
    }

    @Test
    void shouldRejectWorkspaceOutsideApp() throws Exception {
        Path appRoot = Files.createDirectories(temporaryDirectory.toRealPath().resolve("app-root"));
        Path outside = Files.createDirectories(temporaryDirectory.toRealPath().resolve("outside"));
        Files.writeString(outside.resolve("main.jar"), "outside");
        FlinkApplication application = application(appRoot);

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.createArchive(
                application,
                new ShadedBuildResponse(outside.toString(), "main.jar"),
                null,
                temporaryDirectory.resolve("outside.zip"),
                "app-42-dependencies"))
                    .isInstanceOf(ApiAlertException.class);
    }

    @Test
    void shouldRejectReadFailureBeforeResponse() throws Exception {
        Path workspace = createWorkspace();
        TestFsOperator fs = new TestFsOperator()
            .file(workspace.resolve("main.jar").toString(), "main")
            .file(workspace.resolve("lib/dep.jar").toString(), "dependency")
            .failOnOpen(workspace.resolve("lib/dep.jar").toString());
        FlinkApplication application = application(workspace);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.write(
                application,
                new ShadedBuildResponse(workspace.toString(), "main.jar"),
                null,
                response,
                () -> {
                },
                fs,
                fs))
                    .isInstanceOf(ApiAlertException.class)
                    .hasMessage("The job artifact could not be read.")
                    .hasCauseInstanceOf(IOException.class);
        assertThat(response.getHeader("Content-Disposition")).isNull();
        assertThat(response.getContentAsByteArray()).isEmpty();
    }

    @Test
    void shouldWriteYarnSqlDependenciesOnly() throws Exception {
        String appHome = "/hdfs/apps/42";
        TestFsOperator fs = new TestFsOperator()
            .directory(appHome)
            .directory(appHome + "/lib")
            .file(appHome + "/lib/dep.jar", "dependency");
        FlinkApplication application = mock(FlinkApplication.class);
        when(application.getId()).thenReturn(42L);
        when(application.getDeployModeEnum()).thenReturn(FlinkDeployMode.YARN_APPLICATION);
        when(application.isFlinkSqlJob()).thenReturn(true);
        when(application.isFlinkJar()).thenReturn(false);
        when(application.getAppHome()).thenReturn(appHome);
        when(application.getAppLib()).thenReturn(appHome + "/lib");
        Path archive = temporaryDirectory.resolve("yarn-sql.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new SimpleBuildResponse("unused", true),
            null,
            archive,
            "app-42-dependencies",
            fs,
            fs);

        assertThat(zipNames(archive))
            .containsExactly("app-42-dependencies/", "app-42-dependencies/lib/dep.jar");
    }

    @Test
    void shouldUseStreamParkLibJar() throws Exception {
        String appHome = "/hdfs/apps/42";
        TestFsOperator fs = new TestFsOperator()
            .directory(appHome)
            .directory(appHome + "/lib")
            .file(appHome + "/lib/sample.jar", "main")
            .file(appHome + "/lib/dep.jar", "dependency");
        FlinkApplication application = mock(FlinkApplication.class);
        when(application.getId()).thenReturn(42L);
        when(application.getDeployModeEnum()).thenReturn(FlinkDeployMode.YARN_APPLICATION);
        when(application.isFlinkJar()).thenReturn(true);
        when(application.isFlinkSqlJob()).thenReturn(false);
        when(application.isStreamParkType()).thenReturn(true);
        when(application.getModule()).thenReturn("sample");
        when(application.getAppHome()).thenReturn(appHome);
        when(application.getAppLib()).thenReturn(appHome + "/lib");
        Path archive = temporaryDirectory.resolve("streampark.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new SimpleBuildResponse("unused", true),
            null,
            archive,
            "app-42-dependencies",
            fs,
            fs);

        assertThat(zipNames(archive))
            .containsExactly(
                "app-42-dependencies/",
                "app-42-dependencies/sample.jar",
                "app-42-dependencies/lib/dep.jar");
    }

    @Test
    void shouldUseApacheResourceFileName() throws Exception {
        String appHome = "/hdfs/apps/42";
        TestFsOperator fs = new TestFsOperator()
            .directory(appHome)
            .directory(appHome + "/lib")
            .file(appHome + "/physical.jar", "main")
            .file(appHome + "/lib/dep.jar", "dependency");
        Resource resource = new Resource();
        resource.setResourcePath("physical.jar:/hdfs/resources/physical.jar");
        ResourceService resourceService = mock(ResourceService.class);
        when(resourceService.findByResourceName(7L, "display.jar")).thenReturn(resource);
        FlinkApplication application = mock(FlinkApplication.class);
        when(application.getId()).thenReturn(42L);
        when(application.getDeployModeEnum()).thenReturn(FlinkDeployMode.YARN_APPLICATION);
        when(application.isFlinkJar()).thenReturn(true);
        when(application.isFlinkSqlJob()).thenReturn(false);
        when(application.isStreamParkType()).thenReturn(false);
        when(application.getApplicationType()).thenReturn(ApplicationType.APACHE_FLINK);
        when(application.getJar()).thenReturn("display.jar");
        when(application.getTeamId()).thenReturn(7L);
        when(application.getAppHome()).thenReturn(appHome);
        when(application.getAppLib()).thenReturn(appHome + "/lib");
        Path archive = temporaryDirectory.resolve("apache-resource.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new SimpleBuildResponse("unused", true),
            resourceService,
            archive,
            "app-42-dependencies",
            fs,
            fs);

        assertThat(zipNames(archive))
            .containsExactly(
                "app-42-dependencies/",
                "app-42-dependencies/physical.jar",
                "app-42-dependencies/lib/dep.jar");
    }

    @Test
    void shouldUseStagedResourceCopy() throws Exception {
        Path testRoot = temporaryDirectory.toRealPath();
        Path workspace = Files.createDirectory(testRoot.resolve("resource-app"));
        Path resourcePath = testRoot.resolve("uploads/physical.jar");
        Files.writeString(workspace.resolve("physical.jar"), "staged");

        Resource resource = new Resource();
        resource.setResourcePath("display.jar:" + resourcePath);
        resource.setResourceName("display.jar");
        resource.setTeamId(7L);
        ResourceService resourceService = mock(ResourceService.class);
        when(resourceService.findByResourceName(7L, "display.jar")).thenReturn(resource);
        FlinkApplication application = apacheResourceApplication(workspace);
        Path archive = temporaryDirectory.resolve("resource-local.zip");

        FlinkApplicationArtifactExporter.createArchive(
            application,
            new ShadedBuildResponse(workspace.toString(), resourcePath.toString()),
            resourceService,
            archive,
            "app-42-dependencies");

        try (ZipFile zip = new ZipFile(archive.toFile())) {
            ZipEntry entry = zip.getEntry("app-42-dependencies/physical.jar");
            assertThat(entry).isNotNull();
            assertThat(new String(zip.getInputStream(entry).readAllBytes(), StandardCharsets.UTF_8))
                .isEqualTo("staged");
        }
    }

    @Test
    void shouldRejectUnmatchedResourcePath() throws Exception {
        Path testRoot = temporaryDirectory.toRealPath();
        Path workspace = Files.createDirectory(testRoot.resolve("resource-path-app"));
        Path resultPath = testRoot.resolve("uploads/physical.jar");
        Path registeredPath = testRoot.resolve("uploads/other.jar");
        Files.writeString(workspace.resolve("physical.jar"), "staged");

        Resource resource = new Resource();
        resource.setResourcePath("display.jar:" + registeredPath);
        resource.setResourceName("display.jar");
        resource.setTeamId(7L);
        ResourceService resourceService = mock(ResourceService.class);
        when(resourceService.findByResourceName(7L, "display.jar")).thenReturn(resource);
        FlinkApplication application = apacheResourceApplication(workspace);

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.createArchive(
                application,
                new ShadedBuildResponse(workspace.toString(), resultPath.toString()),
                resourceService,
                temporaryDirectory.resolve("resource-path.zip"),
                "app-42-dependencies"))
                    .isInstanceOf(ApiAlertException.class);
    }

    @Test
    void shouldRejectForeignResource() throws Exception {
        Path testRoot = temporaryDirectory.toRealPath();
        Path workspace = Files.createDirectory(testRoot.resolve("foreign-resource-app"));
        Path resourcePath = testRoot.resolve("uploads/physical.jar");
        Files.writeString(workspace.resolve("physical.jar"), "staged");

        Resource resource = new Resource();
        resource.setResourcePath("display.jar:" + resourcePath);
        resource.setResourceName("display.jar");
        resource.setTeamId(8L);
        ResourceService resourceService = mock(ResourceService.class);
        when(resourceService.findByResourceName(7L, "display.jar")).thenReturn(resource);
        FlinkApplication application = apacheResourceApplication(workspace);

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.createArchive(
                application,
                new ShadedBuildResponse(workspace.toString(), resourcePath.toString()),
                resourceService,
                temporaryDirectory.resolve("foreign-resource.zip"),
                "app-42-dependencies"))
                    .isInstanceOf(ApiAlertException.class);
    }

    @Test
    void shouldRejectUnstagedResource() throws Exception {
        Path workspace = Files.createDirectories(temporaryDirectory.toRealPath().resolve("app"));
        Path outside = Files.writeString(temporaryDirectory.toRealPath().resolve("outside.jar"), "outside");
        Resource resource = new Resource();
        resource.setResourcePath("display.jar:" + outside);
        resource.setResourceName("display.jar");
        resource.setTeamId(7L);
        ResourceService resourceService = mock(ResourceService.class);
        when(resourceService.findByResourceName(7L, "display.jar")).thenReturn(resource);
        FlinkApplication application = apacheResourceApplication(workspace);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThatThrownBy(
            () -> FlinkApplicationArtifactExporter.write(
                application,
                new ShadedBuildResponse(workspace.toString(), outside.toString()),
                resourceService,
                response,
                () -> {
                }))
                    .isInstanceOf(ApiAlertException.class);
        assertThat(response.getHeader("Content-Disposition")).isNull();
        assertThat(response.getContentAsByteArray()).isEmpty();
    }

    private FlinkApplication apacheResourceApplication(Path workspace) {
        FlinkApplication application = mock(FlinkApplication.class);
        when(application.getId()).thenReturn(42L);
        when(application.getDeployModeEnum()).thenReturn(FlinkDeployMode.REMOTE);
        when(application.getLocalAppHome()).thenReturn(workspace.toString());
        when(application.isFlinkJar()).thenReturn(true);
        when(application.isUploadResource()).thenReturn(true);
        when(application.getApplicationType()).thenReturn(ApplicationType.APACHE_FLINK);
        when(application.getJar()).thenReturn("display.jar");
        when(application.getTeamId()).thenReturn(7L);
        return application;
    }

    private List<String> zipNames(Path archive) throws IOException {
        try (ZipFile zip = new ZipFile(archive.toFile())) {
            return Collections.list(zip.entries()).stream()
                .map(ZipEntry::getName)
                .collect(Collectors.toList());
        }
    }

    private Path createWorkspace() throws IOException {
        Path workspace = Files.createDirectories(temporaryDirectory.toRealPath().resolve("workspace"));
        Files.writeString(workspace.resolve("main.jar"), "main");
        Path lib = Files.createDirectories(workspace.resolve("lib"));
        Files.writeString(lib.resolve("dep.jar"), "dependency");
        return workspace;
    }

    private FlinkApplication application(Path workspace) {
        FlinkApplication application = mock(FlinkApplication.class);
        when(application.getId()).thenReturn(42L);
        when(application.getDeployModeEnum()).thenReturn(FlinkDeployMode.REMOTE);
        when(application.getLocalAppHome()).thenReturn(workspace.toString());
        return application;
    }

    private static final class TestFsOperator extends FsOperator {

        private final Map<String, byte[]> files = new HashMap<>();
        private final Set<String> directories = new HashSet<>();
        private final Set<String> unreadable = new HashSet<>();

        private TestFsOperator() {
            super();
        }

        private TestFsOperator file(String path, String content) {
            files.put(path, content.getBytes(StandardCharsets.UTF_8));
            return this;
        }

        private TestFsOperator directory(String path) {
            directories.add(path);
            return this;
        }

        private TestFsOperator failOnOpen(String path) {
            unreadable.add(path);
            return this;
        }

        @Override
        public boolean exists(String path) {
            return files.containsKey(path) || directories.contains(path);
        }

        @Override
        public void mkdirs(String path) {
            throw new UnsupportedOperationException("test fixture is read-only");
        }

        @Override
        public void delete(String path) {
            throw new UnsupportedOperationException("test fixture is read-only");
        }

        @Override
        public void mkCleanDirs(String path) {
            throw new UnsupportedOperationException("test fixture is read-only");
        }

        @Override
        public void upload(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            throw new UnsupportedOperationException("test fixture is read-only");
        }

        @Override
        public void copy(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            throw new UnsupportedOperationException("test fixture is read-only");
        }

        @Override
        public void copyDir(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            throw new UnsupportedOperationException("test fixture is read-only");
        }

        @Override
        public void move(String srcPath, String dstPath) {
            throw new UnsupportedOperationException("test fixture is read-only");
        }

        @Override
        public InputStream open(String path) throws IOException {
            if (unreadable.contains(path)) {
                throw new IOException("unreadable");
            }
            byte[] content = files.get(path);
            if (content == null) {
                throw new IOException("missing");
            }
            return new ByteArrayInputStream(content);
        }

        @Override
        public List<String> list(String path) {
            String prefix = path.endsWith("/") ? path : path + "/";
            List<String> entries = new ArrayList<>();
            for (String file : files.keySet()) {
                if (file.startsWith(prefix) && file.indexOf('/', prefix.length()) < 0) {
                    entries.add(file);
                }
            }
            return entries;
        }

        @Override
        public boolean isFile(String path) {
            return files.containsKey(path);
        }

        @Override
        public boolean isDirectory(String path) {
            return directories.contains(path);
        }

        @Override
        public String canonicalPath(String path) throws IOException {
            if (!exists(path)) {
                throw new IOException("missing");
            }
            return path;
        }
    }
}
