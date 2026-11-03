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

package org.apache.streampark.flink.packer.pipeline.impl;

import org.apache.streampark.common.configuration.Configuration;
import org.apache.streampark.common.configuration.GlobalConfiguration;
import org.apache.streampark.common.configuration.Workspace;
import org.apache.streampark.common.configuration.option.MavenOptions;
import org.apache.streampark.common.core.FlinkVersion;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.flink.kubernetes.model.KubernetesPodTemplates;
import org.apache.streampark.flink.packer.docker.DockerConf;
import org.apache.streampark.flink.packer.maven.DependencyInfo;
import org.apache.streampark.flink.packer.pipeline.DockerImageBuildResponse;
import org.apache.streampark.flink.packer.pipeline.FlinkK8sApplicationBuildRequest;
import org.apache.streampark.flink.packer.pipeline.FlinkYarnApplicationBuildRequest;
import org.apache.streampark.flink.packer.pipeline.PipelineTypeEnum;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkSqlApplicationBuildTest {

    @TempDir
    Path workspace;

    private Configuration original;

    @BeforeEach
    void prepareRuntimeRepository() throws Exception {
        original = GlobalConfiguration.current();
        Path repository = workspace.resolve("repository/org/yaml/snakeyaml/2.0");
        Files.createDirectories(repository);
        Files.copy(Path.of(Yaml.class.getProtectionDomain().getCodeSource().getLocation().toURI()),
            repository.resolve("snakeyaml-2.0.jar"));
        Files.writeString(repository.resolve("snakeyaml-2.0.pom"),
            "<project><modelVersion>4.0.0</modelVersion><groupId>org.yaml</groupId>"
                + "<artifactId>snakeyaml</artifactId><version>2.0</version></project>");
        GlobalConfiguration.set(MavenOptions.REPOSITORY_URL,
            workspace.resolve("repository").toUri().toString(), "local test repository");
    }

    @AfterEach
    void restoreConfiguration() {
        GlobalConfiguration.update(original);
    }

    @Test
    void buildSqlArtifactWithRuntimeDependencies() throws Exception {
        Path client = createJar("sql-client.jar", "org/apache/streampark/flink/cli/SqlClient.class");
        Path connector = createJar("connector.jar", "test/Connector.class");
        FlinkYarnApplicationBuildRequest request = new FlinkYarnApplicationBuildRequest(
            "SQL job", "org.apache.streampark.flink.cli.SqlClient",
            workspace.resolve("app").toString(), "/remote/app/lib", FlinkJobType.FLINK_SQL,
            new DependencyInfo(Collections.emptySet(), Collections.singleton(connector.toString())),
            new FlinkVersion(workspace.toString()) {

                @Override
                public String majorVersion() {
                    return "2.2";
                }
            }, client.toString());
        assertThat(request.providedLibs().extJarLibs())
            .contains(Workspace.LOCAL.shims + "/flink-2.2", client.toString());
        Files.createDirectories(Path.of(request.workspace()));
        FlinkYarnApplicationBuildPipeline pipeline = new FlinkYarnApplicationBuildPipeline(request) {

            @Override
            protected <R> R execStep(int seq, Callable<R> process) {
                // Exercise the packaging step without requiring an HDFS cluster.
                if (seq != 2) {
                    return null;
                }
                try {
                    return process.call();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };
        ShadedBuildResponse result = pipeline.buildProcess();
        assertThat(result.pass()).isTrue();
        assertThat(PipelineTypeEnum.FLINK_YARN_APPLICATION.getResultType())
            .isEqualTo(ShadedBuildResponse.class);
        assertThat(result.shadedJarPath()).endsWith("/app/streampark-flinkjob_SQL_job.jar");
        try (JarFile jar = new JarFile(result.shadedJarPath())) {
            assertThat(jar.getJarEntry("org/apache/streampark/flink/cli/SqlClient.class")).isNotNull();
            assertThat(jar.getJarEntry("test/Connector.class")).isNotNull();
            assertThat(jar.getJarEntry("org/yaml/snakeyaml/Yaml.class")).isNotNull();
            assertThat(jar.getManifest().getMainAttributes().getValue("Main-Class"))
                .isEqualTo(request.mainClass());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void buildKubernetesSqlImageContext(boolean hadoop) throws Exception {
        Path client = createJar("sql-client.jar", "org/apache/streampark/flink/cli/SqlClient.class");
        Path connector = createJar("connector.jar", "test/Connector.class");
        DependencyInfo dependencies = new DependencyInfo(Collections.emptySet(),
            Collections.singleton(connector.toString()));
        FlinkK8sApplicationBuildRequest request = new FlinkK8sApplicationBuildRequest(
            "SQL job", workspace.resolve("app").toString(), "org.apache.streampark.flink.cli.SqlClient",
            client.toString(), FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION, FlinkJobType.FLINK_SQL,
            new FlinkVersion(workspace.toString()) {

                @Override
                public String majorVersion() {
                    return "2.2";
                }
            }, dependencies, "sql-job", "default", "flink:2.2.1", KubernetesPodTemplates.empty(),
            hadoop, DockerConf.of("registry.example.org", "streampark", "", ""), "");
        FlinkK8sApplicationBuildPipeline pipeline = new FlinkK8sApplicationBuildPipeline(request) {

            @Override
            protected void runDockerBuildSteps(String buildWorkspace, java.io.File dockerfile,
                                               DockerConf dockerConf, String baseImageTag, String pushImageTag,
                                               boolean checkLocalImageFirst) {
                // Verify the build context without contacting a Docker daemon or registry.
                assertThat(dockerfile).isFile();
                assertThat(Path.of(buildWorkspace, "lib", "snakeyaml-2.0.jar")).isRegularFile();
                assertThat(Path.of(buildWorkspace, "lib", "connector.jar")).isRegularFile();
            }
        };
        DockerImageBuildResponse result = pipeline.buildProcess();
        assertThat(result.pass()).isTrue();
        assertThat(result.dockerInnerMainJarPath())
            .isEqualTo("local:///opt/flink/usrlib/streampark-flinkjob_SQL_job.jar");
        assertThat(dependencies.extJarLibs()).containsExactly(connector.toString());
        assertThat(dependencies.mavenArts()).isEmpty();
        assertThat(Files.readString(Path.of(result.workspacePath(), "Dockerfile")))
            .contains("$FLINK_HOME/lib/", "$FLINK_HOME/usrlib/streampark-flinkjob_SQL_job.jar");
    }

    private Path createJar(String name, String entry) throws Exception {
        Path jar = workspace.resolve(name);
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry(entry));
            output.write(new byte[]{1});
            output.closeEntry();
        }
        return jar;
    }
}
