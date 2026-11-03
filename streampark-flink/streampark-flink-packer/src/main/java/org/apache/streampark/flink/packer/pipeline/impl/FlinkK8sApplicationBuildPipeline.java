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

import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.common.fs.LfsOperator;
import org.apache.streampark.flink.kubernetes.PodTemplateTool;
import org.apache.streampark.flink.kubernetes.ingress.IngressController;
import org.apache.streampark.flink.packer.docker.DockerConf;
import org.apache.streampark.flink.packer.docker.FlinkDockerfileTemplate;
import org.apache.streampark.flink.packer.docker.FlinkDockerfileTemplateTrait;
import org.apache.streampark.flink.packer.docker.FlinkHadoopDockerfileTemplate;
import org.apache.streampark.flink.packer.maven.FlinkSqlDependencySupport;
import org.apache.streampark.flink.packer.maven.MavenTool;
import org.apache.streampark.flink.packer.pipeline.DockerImageBuildResponse;
import org.apache.streampark.flink.packer.pipeline.FlinkK8sApplicationBuildRequest;
import org.apache.streampark.flink.packer.pipeline.K8sDockerBuildSupport;
import org.apache.streampark.flink.packer.pipeline.PipelineTypeEnum;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Building pipeline for flink kubernetes-native application mode */
public class FlinkK8sApplicationBuildPipeline extends AbstractK8sApplicationBuildPipeline {

    private final FlinkK8sApplicationBuildRequest request;

    public FlinkK8sApplicationBuildPipeline(FlinkK8sApplicationBuildRequest request) {
        this.request = request;
    }

    @Override
    public PipelineTypeEnum pipeType() {
        return PipelineTypeEnum.FLINK_NATIVE_K8S_APPLICATION;
    }

    @Override
    public FlinkK8sApplicationBuildRequest offerBuildParam() {
        return request;
    }

    @Override
    public DockerImageBuildResponse buildProcess() {
        String buildWorkspace =
            execStep(
                1,
                () -> {
                    String workspace =
                        request.workspace()
                            + "/"
                            + request.clusterId()
                            + "@"
                            + request.k8sNamespace();
                    LfsOperator.mkCleanDirs(workspace);
                    logInfo("Recreate building workspace: " + workspace);
                    return workspace;
                });

        Map<String, String> podTemplatePaths =
            preparePodTemplateStep(
                buildWorkspace,
                2,
                request.flinkPodTemplate().isEmpty(),
                () -> PodTemplateTool.preparePodTemplateFiles(buildWorkspace, request.flinkPodTemplate())
                    .tmplFiles(),
                "flink");

        final File shadedJar =
            execStep(
                3,
                () -> {
                    String shadedJarOutputPath = request.getShadedJarPath(buildWorkspace);
                    File jar =
                        MavenTool.buildFatJar(
                            request.mainClass(),
                            request.providedLibs(),
                            shadedJarOutputPath);
                    logInfo("Output shaded flink job jar: " + jar.getAbsolutePath());
                    return jar;
                });

        Object[] dockerResult =
            execStep(
                4,
                () -> {
                    Set<String> extJarLibs = new HashSet<>(request.dependencyInfo().extJarLibs());
                    if (request.flinkJobType() == FlinkJobType.FLINK_SQL) {
                        extJarLibs.addAll(FlinkSqlDependencySupport.resolveRuntimeJars());
                    }
                    FlinkDockerfileTemplateTrait template;
                    if (request.integrateWithHadoop()) {
                        template =
                            FlinkHadoopDockerfileTemplate.fromSystemHadoopConf(
                                buildWorkspace,
                                request.flinkBaseImage(),
                                shadedJar.getAbsolutePath(),
                                extJarLibs);
                    } else {
                        template =
                            new FlinkDockerfileTemplate(
                                buildWorkspace,
                                request.flinkBaseImage(),
                                shadedJar.getAbsolutePath(),
                                extJarLibs);
                    }
                    File dockerFile = template.writeDockerfile();
                    logInfo(
                        "Output flink dockerfile: "
                            + dockerFile.getAbsolutePath()
                            + ", content: \n"
                            + template.offerDockerfileContent());
                    return new Object[]{dockerFile, template};
                });
        File dockerfile = (File) dockerResult[0];
        FlinkDockerfileTemplateTrait dockerFileTemplate = (FlinkDockerfileTemplateTrait) dockerResult[1];

        DockerConf dockerConf = request.dockerConfig();
        String baseImageTag = request.flinkBaseImage().trim();
        if (request.k8sNamespace().isEmpty() || request.clusterId().isEmpty()) {
            throw new IllegalArgumentException("k8sNamespace or clusterId cannot be empty");
        }
        String pushImageTag =
            K8sDockerBuildSupport.compileTag(
                "streampark-flinkjob-" + request.k8sNamespace() + "-" + request.clusterId(),
                dockerConf.registerAddress(),
                dockerConf.imageNamespace());

        runDockerBuildSteps(buildWorkspace, dockerfile, dockerConf, baseImageTag, pushImageTag, true);

        if (StringUtils.isBlank(request.ingressTemplate())) {
            skipStep(8);
        } else {
            execStep(
                8,
                () -> {
                    String ingressOutputPath =
                        IngressController.prepareIngressTemplateFiles(
                            buildWorkspace, request.ingressTemplate());
                    logInfo("Export flink ingress: " + ingressOutputPath);
                    return ingressOutputPath;
                });
        }

        return new DockerImageBuildResponse(
            buildWorkspace,
            pushImageTag,
            podTemplatePaths,
            dockerFileTemplate.innerMainJarPath());
    }

    public static FlinkK8sApplicationBuildPipeline of(FlinkK8sApplicationBuildRequest request) {
        return new FlinkK8sApplicationBuildPipeline(request);
    }
}
