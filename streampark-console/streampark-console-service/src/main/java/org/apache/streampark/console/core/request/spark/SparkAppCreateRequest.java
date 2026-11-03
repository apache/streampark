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

package org.apache.streampark.console.core.request.spark;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import java.io.Serializable;

/**
 * Request body for {@code POST /spark/app/create}, aligned with webapp {@code SparkApplication}.
 */
@Getter
@Setter
public class SparkAppCreateRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotNull
    private Long teamId;

    @NotNull
    private Integer jobType;

    @NotNull
    private Integer appType;

    @NotNull
    private Long versionId;

    @NotBlank
    private String appName;

    @NotNull
    private Integer deployMode;

    private Integer resourceFrom;

    private Long projectId;

    private String module;

    private String mainClass;

    private String jar;

    private String appProperties;

    private String appArgs;

    private String yarnQueue;

    private String k8sMasterUrl;

    private String k8sContainerImage;

    private Integer k8sImagePullPolicy;

    private String k8sServiceAccount;

    private String k8sNamespace;

    private String k8sDriverPodTemplate;

    private String k8sExecutorPodTemplate;

    private Boolean k8sHadoopIntegration;

    private String hadoopUser;

    private Boolean lineageEnable;

    private Integer restartSize;

    private Long alertId;

    private String description;

    private String tags;

    private String options;

    private Boolean build;

    private String dependency;

    private String teamResource;

    private String sparkSql;

    private String config;

    private Integer format;

    private Long sqlId;
}
