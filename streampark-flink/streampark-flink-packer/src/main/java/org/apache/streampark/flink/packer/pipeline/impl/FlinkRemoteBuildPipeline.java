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
import org.apache.streampark.common.fs.LfsOperator;
import org.apache.streampark.flink.packer.maven.FlinkSqlDependencySupport;
import org.apache.streampark.flink.packer.maven.MavenTool;
import org.apache.streampark.flink.packer.pipeline.BuildPipeline;
import org.apache.streampark.flink.packer.pipeline.FlinkRemotePerJobBuildRequest;
import org.apache.streampark.flink.packer.pipeline.PipelineTypeEnum;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Building pipeline for flink standalone session mode */
public class FlinkRemoteBuildPipeline extends BuildPipeline {

    private final FlinkRemotePerJobBuildRequest request;

    public FlinkRemoteBuildPipeline(FlinkRemotePerJobBuildRequest request) {
        this.request = request;
    }

    @Override
    public PipelineTypeEnum pipeType() {
        return PipelineTypeEnum.FLINK_STANDALONE;
    }

    @Override
    public FlinkRemotePerJobBuildRequest offerBuildParam() {
        return request;
    }

    @Override
    public ShadedBuildResponse buildProcess() {
        if (request.skipBuild()) {
            File userJar = new File(request.customFlinkUserJar());
            if (!userJar.isFile()) {
                logError("User jar not found for skipBuild: " + request.customFlinkUserJar());
                return new ShadedBuildResponse(request.workspace(), request.customFlinkUserJar(), false);
            }
            return new ShadedBuildResponse(request.workspace(), request.customFlinkUserJar());
        }

        execStep(1, () -> {
            LfsOperator.mkCleanDirs(request.workspace());
            logInfo("Recreate building workspace: " + request.workspace());
            return null;
        });

        File shadedJar =
            execStep(
                2,
                () -> {
                    if (request.flinkJobType() == FlinkJobType.FLINK_SQL) {
                        File output =
                            MavenTool.buildFatJar(
                                request.mainClass(),
                                request.providedLibs(),
                                request.getShadedJarPath(request.workspace()));
                        logInfo("output shaded flink job jar: " + output.getAbsolutePath());
                        return output;
                    }
                    return new File(request.customFlinkUserJar());
                });

        List<String> extraLibJars =
            execStep(
                3,
                () -> {
                    if (request.flinkJobType() == FlinkJobType.PYFLINK) {
                        List<File> mavenArts =
                            MavenTool.resolveArtifacts(request.dependencyInfo().mavenArts());
                        List<String> paths =
                            mavenArts.stream()
                                .map(File::getAbsolutePath)
                                .collect(Collectors.toList());
                        paths.addAll(request.dependencyInfo().extJarLibs());
                        return paths;
                    }
                    if (request.flinkJobType() == FlinkJobType.FLINK_SQL) {
                        return new java.util.ArrayList<>(FlinkSqlDependencySupport.resolveRuntimeJars());
                    }
                    return Collections.<String>emptyList();
                });

        execStep(
            4,
            () -> {
                if (request.flinkJobType() == FlinkJobType.PYFLINK
                    || request.flinkJobType() == FlinkJobType.FLINK_SQL) {
                    FsOperator lfs = FsOperator.lfs();
                    String lib = request.workspace().concat("/lib");
                    lfs.mkdirsIfNotExists(lib);
                    for (String jar : extraLibJars) {
                        File originFile = new File(jar);
                        if (originFile.isFile()) {
                            lfs.copy(originFile.getAbsolutePath(), lib);
                        }
                    }
                }
                return null;
            });

        return new ShadedBuildResponse(request.workspace(), shadedJar.getAbsolutePath());
    }

    public static FlinkRemoteBuildPipeline of(FlinkRemotePerJobBuildRequest request) {
        return new FlinkRemoteBuildPipeline(request);
    }
}
