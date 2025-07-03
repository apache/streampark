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

package org.apache.streampark.console.core.entity;

import org.apache.streampark.common.conf.ConfigKeys;
import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.constants.Constants;
import org.apache.streampark.common.enums.ApplicationType;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.common.enums.FlinkK8sRestExposedType;
import org.apache.streampark.common.enums.StorageType;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.console.base.mybatis.entity.BaseEntity;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.core.bean.AppControl;
import org.apache.streampark.console.core.bean.Dependency;
import org.apache.streampark.console.core.enums.FlinkAppStateEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.enums.ResourceFromEnum;
import org.apache.streampark.console.core.metrics.flink.JobsOverview;
import org.apache.streampark.console.core.util.YarnQueueLabelExpression;
import org.apache.streampark.flink.kubernetes.model.K8sPodTemplates;
import org.apache.streampark.flink.packer.maven.DependencyInfo;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.annotation.FieldStrategy;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Getter
@Setter
@TableName("t_flink_app")
@Slf4j
public class FlinkApplication extends BaseEntity {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long teamId;

    /**
     * 1) flink jar 2) flink SQL
     */
    private Integer jobType;

    private Long projectId;
    /**
     * creator
     */
    private Long userId;

    /**
     * The name of the frontend and program displayed in yarn
     */
    private String jobName;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String jobId;

    /**
     * The address of the jobmanager, that is, the direct access address of the Flink web UI
     */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String jobManagerUrl;

    /**
     * flink version
     */
    private Long versionId;

    /**
     * 1. yarn application id(on yarn) 2. k8s application id (on k8s application)
     */
    private String clusterId;

    /**
     * flink docker base image
     */
    private String flinkImage;

    /**
     * k8s namespace
     */
    private String k8sNamespace = Constants.DEFAULT;

    /**
     * The exposed type of the rest service of K8s(kubernetes.rest-service.exposed.type)
     */
    private Integer k8sRestExposedType;
    /**
     * flink kubernetes pod template
     */
    private String k8sPodTemplate;

    private String k8sJmPodTemplate;
    private String k8sTmPodTemplate;

    @Getter
    private String ingressTemplate;

    @Setter
    private String defaultModeIngress;

    /**
     * flink-hadoop integration on flink-k8s mode
     */
    private Boolean k8sHadoopIntegration;

    private Integer state;
    /**
     * task release status
     */
    @TableField("`release`")
    private Integer release;

    /**
     * determine if a task needs to be built
     */
    private Boolean build;

    /**
     * max restart retries after job failed
     */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Integer restartSize;

    /**
     * has restart count
     */
    private Integer restartCount;

    private Integer optionState;

    /**
     * alert id
     */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Long alertId;

    private String args;
    /**
     * application module
     */
    private String module;

    private String options;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String hotParams;

    private Integer resolveOrder;

    private Integer deployMode;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String dynamicProperties;

    private Integer appType;

    /**
     * determine if tracking status
     */
    private Integer tracking;

    private String jar;

    /**
     * for upload type tasks, checkSum needs to be recorded whether it needs to be republished after
     * the update and modify.
     */
    private Long jarCheckSum;

    private String mainClass;

    private Date startTime;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Date endTime;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Long duration;

    /**
     * checkpoint max failure interval
     */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Integer cpMaxFailureInterval;

    /**
     * checkpoint failure rate interval
     */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Integer cpFailureRateInterval;

    /**
     * Actions triggered after X minutes failed Y times: 1: send alert 2: restart
     */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Integer cpFailureAction;

    /**
     * overview
     */
    @TableField("TOTAL_TM")
    private Integer totalTM;

    @TableField("HADOOP_USER")
    private String hadoopUser;

    private Integer totalSlot;
    private Integer availableSlot;
    private Integer jmMemory;
    private Integer tmMemory;
    private Integer totalTask;

    /**
     * the cluster id bound to the task in remote mode
     */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Long flinkClusterId;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String description;

    private Date createTime;

    private Date optionTime;

    private Date modifyTime;

    /**
     * 1: build (build from csv) 2: upload (upload local jar job)
     */
    private Integer resourceFrom;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String tags;

    /**
     * running job
     */
    private transient JobsOverview.Task overview;

    private transient String teamResource;
    private transient String dependency;
    private transient Long sqlId;
    private transient String flinkSql;

    private transient Integer[] stateArray;
    private transient Integer[] jobTypeArray;
    private transient Boolean backUp = false;
    private transient Boolean restart = false;
    private transient String userName;
    private transient String nickName;
    private transient String config;
    private transient Long configId;
    private transient String flinkVersion;
    private transient String confPath;
    private transient Integer format;
    private transient String savepointPath;
    private transient Boolean restoreOrTriggerSavepoint = false;
    private transient Boolean drain = false;
    private transient Long savepointTimeout = 60L;
    private transient Boolean allowNonRestored = false;
    private transient Boolean nativeFormat = false;
    private transient Integer restoreMode;
    private transient String socketId;
    private transient String projectName;
    private transient String createTimeFrom;
    private transient String createTimeTo;
    private transient String backUpDescription;
    private transient String yarnQueue;
    private transient String serviceAccount;

    /**
     * Flink Web UI Url
     */
    private transient String flinkRestUrl;

    /**
     * refer to {@link org.apache.streampark.flink.packer.pipeline.BuildPipeline}
     */
    private transient Integer buildStatus;

    private transient AppControl appControl;

    public void setK8sNamespace(String k8sNamespace) {
        this.k8sNamespace = StringUtils.isBlank(k8sNamespace) ? Constants.DEFAULT : k8sNamespace;
    }

    public K8sPodTemplates getK8sPodTemplates() {
        return K8sPodTemplates.of(k8sPodTemplate, k8sJmPodTemplate, k8sTmPodTemplate);
    }

    public void setState(Integer state) {
        this.state = state;
        this.tracking = shouldTracking() ? 1 : 0;
    }

    public void setYarnQueueByHotParams() {
        if (!(FlinkDeployMode.YARN_APPLICATION == this.getDeployModeEnum()
            || FlinkDeployMode.YARN_PER_JOB == this.getDeployModeEnum())) {
            return;
        }

        Map<String, Object> hotParamsMap = this.getHotParamsMap();
        if (MapUtils.isNotEmpty(hotParamsMap)
            && hotParamsMap.containsKey(ConfigKeys.KEY_YARN_APP_QUEUE())) {
            String yarnQueue = hotParamsMap.get(ConfigKeys.KEY_YARN_APP_QUEUE()).toString();
            String labelExpr = Optional.ofNullable(hotParamsMap.get(ConfigKeys.KEY_YARN_APP_NODE_LABEL()))
                .map(Object::toString)
                .orElse(null);
            this.setYarnQueue(YarnQueueLabelExpression.of(yarnQueue, labelExpr).toString());
        }
    }

    /**
     * Determine if a FlinkAppState requires tracking.
     *
     * @return 1: need to be tracked | 0: no need to be tracked.
     */
    public Boolean shouldTracking() {
        switch (getStateEnum()) {
            case ADDED:
            case CREATED:
            case FINISHED:
            case FAILED:
            case CANCELED:
            case TERMINATED:
            case POS_TERMINATED:
                return false;
            default:
                return true;
        }
    }

    /**
     * Determine whether the application can be started to prevent repeated starts.
     *
     * @return true: can start | false: can not start.
     */
    public boolean isCanBeStart() {
        switch (getStateEnum()) {
            case ADDED:
            case CREATED:
            case FAILED:
            case CANCELED:
            case FINISHED:
            case LOST:
            case TERMINATED:
            case SUCCEEDED:
            case KILLED:
            case POS_TERMINATED:
                return true;
            default:
                return false;
        }
    }

    @JsonIgnore
    public ReleaseStateEnum getReleaseState() {
        return ReleaseStateEnum.of(release);
    }

    @JsonIgnore
    public FlinkJobType getJobTypeEnum() {
        return FlinkJobType.of(jobType);
    }

    @JsonIgnore
    public FlinkAppStateEnum getStateEnum() {
        return FlinkAppStateEnum.getState(state);
    }

    @JsonIgnore
    public FlinkK8sRestExposedType getK8sRestExposedTypeEnum() {
        return FlinkK8sRestExposedType.of(this.k8sRestExposedType);
    }

    @JsonIgnore
    public FlinkDeployMode getDeployModeEnum() {
        return FlinkDeployMode.of(deployMode);
    }

    public boolean cpFailedTrigger() {
        return this.cpMaxFailureInterval != null
            && this.cpFailureRateInterval != null
            && this.cpFailureAction != null;
    }

    /**
     * Local compilation and packaging working directory
     */
    @JsonIgnore
    public String getDistHome() {
        String path = String.format("%s/%s/%s", Workspace.APP_LOCAL_DIST(), projectId.toString(), getModule());
        log.info("local distHome:{}", path);
        return path;
    }

    @JsonIgnore
    public String getLocalAppHome() {
        String path = String.format("%s/%s", Workspace.local().APP_WORKSPACE(), id.toString());
        log.info("local appHome:{}", path);
        return path;
    }

    @JsonIgnore
    public String getRemoteAppHome() {
        String path = String.format("%s/%s", Workspace.remote().APP_WORKSPACE(), id.toString());
        log.info("remote appHome:{}", path);
        return path;
    }

    /**
     * Automatically identify remoteAppHome or localAppHome based on app FlinkDeployMode
     */
    @JsonIgnore
    public String getAppHome() {
        switch (this.getDeployModeEnum()) {
            case KUBERNETES_NATIVE_APPLICATION:
            case KUBERNETES_NATIVE_SESSION:
            case YARN_PER_JOB:
            case YARN_SESSION:
            case REMOTE:
            case LOCAL:
                return getLocalAppHome();
            case YARN_APPLICATION:
                return getRemoteAppHome();
            default:
                throw new UnsupportedOperationException(
                    "unsupported deployMode ".concat(getDeployModeEnum().getName()));
        }
    }

    public String getMainClass() {
        FlinkJobType flinkJobType = this.getJobTypeEnum();
        switch (flinkJobType) {
            case FLINK_SQL:
                return Constants.STREAMPARK_FLINKSQL_CLIENT_CLASS;
            case PYFLINK:
                return Constants.PYTHON_FLINK_DRIVER_CLASS_NAME;
            case FLINK_JAR:
                return mainClass;
            case UNKNOWN:
            default:
                return null;
        }
    }

    @JsonIgnore
    public String getAppLib() {
        return getAppHome().concat("/lib");
    }

    @JsonIgnore
    public ApplicationType getApplicationType() {
        return ApplicationType.of(appType);
    }

    @JsonIgnore
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public Map<String, Object> getOptionMap() {
        if (StringUtils.isBlank(this.options)) {
            return new HashMap<>();
        }
        Map<String, Object> optionMap = JacksonUtils.read(this.options, Map.class);
        optionMap.entrySet().removeIf(entry -> entry.getValue() == null);
        return optionMap;
    }

    @JsonIgnore
    public boolean isFlinkSql() {
        return FlinkJobType.FLINK_SQL.getMode().equals(this.getJobType());
    }

    @JsonIgnore
    public boolean isFlinkJar() {
        return FlinkJobType.FLINK_JAR.getMode().equals(this.getJobType());
    }

    @JsonIgnore
    public boolean isFlinkJarOrPyFlink() {
        return FlinkJobType.FLINK_JAR.getMode().equals(this.getJobType())
            || FlinkJobType.PYFLINK.getMode().equals(this.getJobType());
    }

    @JsonIgnore
    public boolean isUploadResource() {
        return isFlinkJarOrPyFlink()
            && ResourceFromEnum.UPLOAD.getValue().equals(this.getResourceFrom());
    }

    @JsonIgnore
    public boolean isBuildResource() {
        return isFlinkJarOrPyFlink()
            && ResourceFromEnum.BUILD.getValue().equals(this.getResourceFrom());
    }

    public boolean isStreamParkType() {
        return this.getAppType() == ApplicationType.STREAMPARK_FLINK.getType();
    }

    @JsonIgnore
    @SneakyThrows
    public Dependency getDependencyObject() {
        return Dependency.toDependency(this.dependency);
    }

    @JsonIgnore
    public DependencyInfo getDependencyInfo() {
        return Dependency.toDependency(getDependency()).toJarPackDeps();
    }

    @JsonIgnore
    public boolean isRunning() {
        return FlinkAppStateEnum.RUNNING.getValue() == this.getState();
    }

    @JsonIgnore
    public boolean isNeedRollback() {
        return ReleaseStateEnum.NEED_ROLLBACK.get() == this.getRelease();
    }

    @JsonIgnore
    public boolean isNeedRestartOnFailed() {
        if (this.restartSize != null && this.restartCount != null) {
            return this.restartSize > 0 && this.restartCount <= this.restartSize;
        }
        return false;
    }

    @JsonIgnore
    public StorageType getStorageType() {
        return getStorageType(getDeployMode());
    }

    public static StorageType getStorageType(Integer deployMode) {
        FlinkDeployMode deployModeEnum = FlinkDeployMode.of(deployMode);
        switch (Objects.requireNonNull(deployModeEnum)) {
            case YARN_APPLICATION:
                return StorageType.HDFS;
            case YARN_PER_JOB:
            case YARN_SESSION:
            case KUBERNETES_NATIVE_SESSION:
            case KUBERNETES_NATIVE_APPLICATION:
            case REMOTE:
                return StorageType.LFS;
            default:
                throw new UnsupportedOperationException("Unsupported ".concat(deployModeEnum.getName()));
        }
    }

    @JsonIgnore
    public FsOperator getFsOperator() {
        return FsOperator.of(getStorageType());
    }

    @JsonIgnore
    public Workspace getWorkspace() {
        return Workspace.of(getStorageType());
    }

    @JsonIgnore
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public Map<String, Object> getHotParamsMap() {
        if (this.hotParams != null) {
            Map<String, Object> map = JacksonUtils.read(this.hotParams, Map.class);
            map.entrySet().removeIf(entry -> entry.getValue() == null);
            return map;
        }
        return Collections.EMPTY_MAP;
    }

    @SneakyThrows
    public void doSetHotParams() {
        updateHotParams(this);
    }

    @SneakyThrows
    public void updateHotParams(FlinkApplication appParam) {
        if (appParam != this) {
            this.hotParams = null;
        }
        FlinkDeployMode deployModeEnum = appParam.getDeployModeEnum();
        Map<String, String> hotParams = new HashMap<>(0);
        if (needFillYarnQueueLabel(deployModeEnum)) {
            hotParams.putAll(YarnQueueLabelExpression.getQueueLabelMap(appParam.getYarnQueue()));
        }
        if (deployModeEnum == FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION) {
            if (StringUtils.isNotBlank(appParam.getServiceAccount())) {
                hotParams.put(ConfigKeys.KEY_KERBEROS_SERVICE_ACCOUNT(), appParam.getServiceAccount());
            }
        }
        if (!hotParams.isEmpty()) {
            this.setHotParams(JacksonUtils.write(hotParams));
        }
    }

    private boolean needFillYarnQueueLabel(FlinkDeployMode mode) {
        return FlinkDeployMode.YARN_PER_JOB == mode || FlinkDeployMode.YARN_APPLICATION == mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return id.equals(((FlinkApplication) o).id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public boolean isKubernetesModeJob() {
        return FlinkDeployMode.isKubernetesMode(this.getDeployModeEnum());
    }

}
