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

package org.apache.streampark.console.core.request.flink;

import org.apache.streampark.console.core.annotation.ApiParam;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import java.io.Serializable;

/**
 * Request body for {@code POST /flink/app/create}, aligned with webapp {@code CreateParams}.
 */
@Getter
@Setter
public class FlinkAppCreateRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotNull
    @ApiParam(description = "Team id", required = true)
    private Long teamId;

    @NotNull
    @ApiParam(description = "Job type", required = true)
    private Integer jobType;

    @NotNull
    @ApiParam(description = "Deploy mode", required = true)
    private Integer deployMode;

    @NotNull
    @ApiParam(description = "Flink version id", required = true)
    private Long versionId;

    private String flinkSql;

    private Long sqlId;

    @NotNull
    @ApiParam(description = "Application type", required = true)
    private Integer appType;

    private String config;

    private Integer format;

    @NotBlank
    @ApiParam(description = "Job name", required = true)
    private String jobName;

    private String tags;

    private String args;

    private String dependency;

    private String options;

    private Integer cpMaxFailureInterval;

    private Integer cpFailureRateInterval;

    private Integer cpFailureAction;

    private String dynamicProperties;

    private Integer resolveOrder;

    private Integer restartSize;

    private Long alertId;

    private String description;

    private String k8sNamespace;

    private String clusterId;

    private Long flinkClusterId;

    private String flinkImage;

    private String jar;

    private String mainClass;

    private Long projectId;

    private String module;

    private Integer resourceFrom;

    private Boolean build;

    private String hotParams;

    private Integer k8sRestExposedType;

    private String k8sPodTemplate;

    private String k8sJmPodTemplate;

    private String k8sTmPodTemplate;

    private String ingressTemplate;

    private Boolean k8sHadoopIntegration;

    private String serviceAccount;

    private Boolean lineageEnable;
}
