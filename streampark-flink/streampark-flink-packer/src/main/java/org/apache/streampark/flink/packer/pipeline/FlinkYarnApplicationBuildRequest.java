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

package org.apache.streampark.flink.packer.pipeline;

import org.apache.streampark.common.core.FlinkVersion;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.flink.packer.maven.DependencyInfo;

public class FlinkYarnApplicationBuildRequest implements FlinkBuildParam {

    private final String appName;
    private final String mainClass;
    private final String localWorkspace;
    private final String yarnProvidedPath;
    private final FlinkJobType flinkJobType;
    private final DependencyInfo dependencyInfo;
    private final FlinkVersion flinkVersion;
    private final String customFlinkUserJar;

    public FlinkYarnApplicationBuildRequest(
                                            String appName,
                                            String mainClass,
                                            String localWorkspace,
                                            String yarnProvidedPath,
                                            FlinkJobType flinkJobType,
                                            DependencyInfo dependencyInfo,
                                            FlinkVersion flinkVersion,
                                            String customFlinkUserJar) {
        this.appName = appName;
        this.mainClass = mainClass;
        this.localWorkspace = localWorkspace;
        this.yarnProvidedPath = yarnProvidedPath;
        this.flinkJobType = flinkJobType;
        this.dependencyInfo = dependencyInfo;
        this.flinkVersion = flinkVersion;
        this.customFlinkUserJar = customFlinkUserJar;
    }

    @Override
    public String appName() {
        return appName;
    }

    @Override
    public String mainClass() {
        return mainClass;
    }

    public String localWorkspace() {
        return localWorkspace;
    }

    @Override
    public String workspace() {
        return localWorkspace;
    }

    @Override
    public FlinkDeployMode deployMode() {
        return FlinkDeployMode.YARN_APPLICATION;
    }

    @Override
    public FlinkVersion flinkVersion() {
        return flinkVersion;
    }

    @Override
    public String customFlinkUserJar() {
        return customFlinkUserJar;
    }

    public String yarnProvidedPath() {
        return yarnProvidedPath;
    }

    public FlinkJobType flinkJobType() {
        return flinkJobType;
    }

    public DependencyInfo dependencyInfo() {
        return dependencyInfo;
    }
}
