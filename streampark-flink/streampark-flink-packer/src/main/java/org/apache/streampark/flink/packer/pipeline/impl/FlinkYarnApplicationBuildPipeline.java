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
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.common.fs.HdfsOperator;
import org.apache.streampark.common.fs.LfsOperator;
import org.apache.streampark.flink.packer.maven.DependencyInfo;
import org.apache.streampark.flink.packer.maven.FlinkSqlDependencySupport;
import org.apache.streampark.flink.packer.maven.MavenTool;
import org.apache.streampark.flink.packer.pipeline.BuildPipeline;
import org.apache.streampark.flink.packer.pipeline.FlinkYarnApplicationBuildRequest;
import org.apache.streampark.flink.packer.pipeline.PipelineTypeEnum;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;
import org.apache.streampark.flink.packer.pipeline.YarnJarUploader;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** Building pipeline for flink yarn application mode */
public class FlinkYarnApplicationBuildPipeline extends BuildPipeline {

    private final FlinkYarnApplicationBuildRequest request;

    public FlinkYarnApplicationBuildPipeline(FlinkYarnApplicationBuildRequest request) {
        this.request = request;
    }

    @Override
    public PipelineTypeEnum pipeType() {
        return PipelineTypeEnum.FLINK_YARN_APPLICATION;
    }

    @Override
    public FlinkYarnApplicationBuildRequest offerBuildParam() {
        return request;
    }

    @Override
    public ShadedBuildResponse buildProcess() {
        if (request.flinkJobType() != FlinkJobType.FLINK_SQL) {
            runYarnSqlBuildSteps(
                request.localWorkspace(),
                request.yarnProvidedPath(),
                request.flinkJobType() == FlinkJobType.PYFLINK,
                request.dependencyInfo());
            return new ShadedBuildResponse(request.workspace(), null);
        }

        execStep(1, () -> {
            LfsOperator.mkCleanDirs(request.workspace());
            HdfsOperator.mkCleanDirs(request.yarnProvidedPath());
            return null;
        });
        File shadedJar = execStep(2, () -> MavenTool.buildFatJar(
            request.mainClass(), request.providedLibs(), request.getShadedJarPath(request.workspace())));
        execStep(3, () -> {
            DependencyInfo dependencies =
                FlinkSqlDependencySupport.withRuntimeDependencies(request.dependencyInfo());
            Set<String> jars = MavenTool.resolveArtifacts(dependencies.mavenArts()).stream()
                .map(File::getAbsolutePath).collect(Collectors.toCollection(HashSet::new));
            jars.addAll(dependencies.extJarLibs());
            for (String jar : jars) {
                YarnJarUploader.uploadJarToHdfsOrLfs(FsOperator.lfs(), jar, request.workspace() + "/lib");
                YarnJarUploader.uploadJarToHdfsOrLfs(FsOperator.hdfs(), jar, request.yarnProvidedPath());
            }
            return null;
        });
        return new ShadedBuildResponse(request.workspace(), shadedJar.getAbsolutePath());
    }

    public static FlinkYarnApplicationBuildPipeline of(FlinkYarnApplicationBuildRequest request) {
        return new FlinkYarnApplicationBuildPipeline(request);
    }
}
