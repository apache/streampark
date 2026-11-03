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

package org.apache.streampark.console.core.response.spark;

import org.apache.streampark.console.core.bean.AppControl;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/**
 * API response for a Spark application, aligned with webapp {@code SparkApplication}.
 */
@Getter
@Setter
@SuppressWarnings("java:S1948")
public class SparkAppResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long id;

    private Long teamId;

    private Integer jobType;

    private Integer appType;

    private Long versionId;

    private String appName;

    private Integer deployMode;

    private Integer resourceFrom;

    private Long projectId;

    private String module;

    private String mainClass;

    private String jar;

    private Long jarCheckSum;

    private String appProperties;

    private String appArgs;

    private String clusterId;

    private String yarnQueue;

    private String yarnQueueName;

    private String yarnQueueLabel;

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

    private Integer restartCount;

    private Integer state;

    private String options;

    private Integer optionState;

    private Date optionTime;

    private Long userId;

    private String description;

    private Integer tracking;

    private Integer release;

    private Boolean build;

    private Long alertId;

    private Date createTime;

    private Date modifyTime;

    private Date startTime;

    private Date endTime;

    private Long duration;

    private String tags;

    private String driverCores;

    private String driverMemory;

    private String executorCores;

    private String executorMemory;

    private String executorMaxNums;

    private Long numTasks;

    private Long numCompletedTasks;

    private Long numStages;

    private Long numCompletedStages;

    private Long usedMemory;

    private Long usedVCores;

    private String teamResource;

    private String dependency;

    private Long sqlId;

    private String sparkSql;

    private Boolean backUp;

    private Boolean restart;

    private String config;

    private Long configId;

    private String sparkVersion;

    private String confPath;

    private Integer format;

    private String backUpDescription;

    private String sparkRestUrl;

    private Integer buildStatus;

    private AppControl appControl;

    private String userName;

    private String nickName;
}
