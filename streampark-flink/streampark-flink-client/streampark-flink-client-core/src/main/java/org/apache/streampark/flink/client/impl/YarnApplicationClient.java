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

package org.apache.streampark.flink.client.impl;

import org.apache.streampark.common.configuration.Constants;
import org.apache.streampark.common.configuration.Workspace;
import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.FileUtils;
import org.apache.streampark.common.util.HdfsUtils;
import org.apache.streampark.common.util.Tuple2;
import org.apache.streampark.flink.client.bean.RemoteWorkspace;
import org.apache.streampark.flink.client.bean.ResolvedSubmitRequest;
import org.apache.streampark.flink.client.request.SubmitRequest;
import org.apache.streampark.flink.client.response.SubmitResponse;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Submits Flink jobs to application-scoped YARN clusters. */
public final class YarnApplicationClient extends AbstractYarnClient {

    public static final YarnApplicationClient INSTANCE = new YarnApplicationClient();

    private static final Workspace WORKSPACE = Workspace.REMOTE;

    private YarnApplicationClient() {
    }

    /** Configures remote artifacts and PyFlink resources for YARN application mode. */
    @Override
    protected void setConfig(ResolvedSubmitRequest resolved, Configuration flinkConfig) {
        SubmitRequest submitRequest = resolved.request();
        super.setConfig(resolved, flinkConfig);

        List<String> providedLibs = new ArrayList<>();
        RemoteWorkspace hdfsWorkspace = RemoteWorkspace.resolve(submitRequest.flinkVersion());
        providedLibs.add(hdfsWorkspace.flinkLib());
        providedLibs.add(hdfsWorkspace.flinkPlugins());
        providedLibs.add(hdfsWorkspace.appJars());

        if (submitRequest.jobType() == FlinkJobType.FLINK_SQL) {
            String jobLib = WORKSPACE.workspace + "/" + submitRequest.id() + "/lib";
            try {
                if (HdfsUtils.exists(jobLib)) {
                    providedLibs.add(jobLib);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to check Flink SQL job lib path: " + jobLib, e);
            }
        }

        flinkConfig.set(YarnConfigOptions.PROVIDED_LIB_DIRS, providedLibs);
        flinkConfig.set(YarnConfigOptions.FLINK_DIST_JAR, hdfsWorkspace.flinkDistJar());
        flinkConfig.set(
            PipelineOptions.JARS,
            Collections.singletonList(
                ((ShadedBuildResponse) submitRequest.buildResult()).shadedJarPath()));
        flinkConfig.set(
            YarnConfigOptions.APPLICATION_NAME,
            resolved.getJobName());
        flinkConfig.set(
            YarnConfigOptions.APPLICATION_TYPE,
            submitRequest.applicationType().getName());

        if (submitRequest.jobType() == FlinkJobType.PYFLINK) {
            File userJar = resolved.getUserJarFile();
            AssertUtils.required(userJar != null, "PyFlink job archive is missing");
            String pyVenv = WORKSPACE.pythonVenv;
            AssertUtils.required(FsOperator.hdfs().exists(pyVenv), pyVenv + " File does not exist");

            String localLib = Workspace.LOCAL.workspace + "/" + submitRequest.id() + "/lib";
            if (FileUtils.exists(localLib) && FileUtils.directoryNotBlank(localLib)) {
                flinkConfig.set(PipelineOptions.JARS, Arrays.asList(localLib));
            }

            ArrayList<String> shipFiles = new ArrayList<>();
            shipFiles.add(userJar.getParentFile().getAbsolutePath());

            flinkConfig.set(YarnConfigOptions.SHIP_FILES, shipFiles);
            flinkConfig.set(PythonOptions.PYTHON_FILES, userJar.getParentFile().getName());
            flinkConfig.set(PythonOptions.PYTHON_ARCHIVES, pyVenv);
            flinkConfig.set(PythonOptions.PYTHON_CLIENT_EXECUTABLE, Constants.PYTHON_EXECUTABLE);
            flinkConfig.set(PythonOptions.PYTHON_EXECUTABLE, Constants.PYTHON_EXECUTABLE);

            List<String> args = flinkConfig.get(ApplicationConfiguration.APPLICATION_ARGS);
            ArrayList<String> argsList = new ArrayList<>(args);
            argsList.add("-pym");
            argsList.add(
                userJar.getName()
                    .substring(0, userJar.getName().length() - Constants.PYTHON_SUFFIX.length()));
            flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, argsList);
        }

        logEffectiveSubmitConfiguration(flinkConfig);
    }

    /** Deploys an application cluster and returns its YARN and REST identities. */
    @Override
    protected SubmitResponse doSubmit(
                                      ResolvedSubmitRequest resolved,
                                      Configuration flinkConfig) throws FlinkException {
        SubmitRequest submitRequest = resolved.request();
        return execute(
            () -> {
                Tuple2<ClusterSpecification, YarnClusterDescriptor> deployDescriptor =
                    getYarnClusterDeployDescriptor(flinkConfig, submitRequest.hadoopUser());
                ClusterSpecification clusterSpecification = deployDescriptor._1;
                YarnClusterDescriptor clusterDescriptor = deployDescriptor._2;
                logClusterSpecification(clusterSpecification);

                ClusterClient<ApplicationId> clusterClient = null;
                try {
                    ApplicationConfiguration applicationConfiguration =
                        ApplicationConfiguration.fromConfiguration(flinkConfig);
                    clusterClient =
                        clusterDescriptor
                            .deployApplicationCluster(
                                clusterSpecification, applicationConfiguration)
                            .getClusterClient();
                    ApplicationId applicationId = clusterClient.getClusterId();
                    logYarnJobStarted(applicationId);
                    return new SubmitResponse(
                        applicationId.toString(),
                        flinkConfig.toMap(),
                        "",
                        clusterClient.getWebInterfaceURL());
                } finally {
                    closeSubmissionResources(clusterClient, clusterDescriptor);
                }
            });
    }
}
