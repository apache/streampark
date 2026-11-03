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

package org.apache.streampark.console.core.response.flink;

import org.apache.streampark.console.core.bean.AppControl;
import org.apache.streampark.console.core.metrics.flink.JobsOverview;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

/**
 * API response for a Flink application, aligned with webapp {@code AppListRecord}.
 */
@Getter
@Setter
@SuppressWarnings("java:S1948")
public class FlinkAppResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long id;

    private Integer jobType;

    private Long projectId;

    private String tags;

    private Long userId;

    private Long teamId;

    private String jobName;

    private String appId;

    private String jobId;

    private Long versionId;

    private String clusterId;

    private String flinkImage;

    private String k8sNamespace;

    private String serviceAccount;

    private Integer state;

    private Integer release;

    private Boolean build;

    private Integer restartSize;

    private Integer restartCount;

    private Integer optionState;

    private Long alertId;

    private String args;

    private String module;

    private String options;

    private String hotParams;

    private Integer resolveOrder;

    private Integer deployMode;

    private String dynamicProperties;

    private Integer appType;

    private Integer tracking;

    private String jar;

    private Long jarCheckSum;

    private String mainClass;

    private Date startTime;

    private Date endTime;

    private Long duration;

    private Integer cpMaxFailureInterval;

    private Integer cpFailureRateInterval;

    private Integer cpFailureAction;

    private Integer totalTM;

    private Integer totalSlot;

    private Integer availableSlot;

    private Integer jmMemory;

    private Integer tmMemory;

    private Integer totalTask;

    private Long flinkClusterId;

    private String description;

    private Date createTime;

    private Date optionTime;

    private Date modifyTime;

    private Integer resourceFrom;

    private Integer k8sRestExposedType;

    private String k8sPodTemplate;

    private String k8sJmPodTemplate;

    private String k8sTmPodTemplate;

    private String ingressTemplate;

    private String defaultModeIngress;

    private Boolean k8sHadoopIntegration;

    private Boolean lineageEnable;

    private JobsOverview.Task overview;

    private String teamResource;

    private String dependency;

    private Long sqlId;

    private String flinkSql;

    private Integer[] stateArray;

    private Integer[] jobTypeArray;

    private Boolean backUp;

    private Boolean restart;

    private String userName;

    private String nickName;

    private String config;

    private Long configId;

    private String flinkVersion;

    private String confPath;

    private Integer format;

    private String savepointPath;

    private Boolean restoreOrTriggerSavepoint;

    private Boolean drain;

    private Boolean allowNonRestored;

    private Boolean nativeFormat;

    private String socketId;

    private String projectName;

    private String createTimeFrom;

    private String createTimeTo;

    private String backUpDescription;

    private String yarnQueue;

    private String flinkRestUrl;

    private Integer buildStatus;

    private AppControl appControl;

    private String hadoopUser;
}
