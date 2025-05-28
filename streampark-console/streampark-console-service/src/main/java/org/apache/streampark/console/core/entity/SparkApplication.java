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
import org.apache.streampark.common.enums.SparkDeployMode;
import org.apache.streampark.common.enums.SparkJobType;
import org.apache.streampark.common.enums.StorageType;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.console.base.mybatis.entity.BaseEntity;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.core.bean.AppControl;
import org.apache.streampark.console.core.bean.Dependency;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.enums.ResourceFromEnum;
import org.apache.streampark.console.core.enums.SparkAppStateEnum;
import org.apache.streampark.console.core.metrics.spark.SparkApplicationSummary;
import org.apache.streampark.console.core.util.YarnQueueLabelExpression;
import org.apache.streampark.flink.packer.maven.DependencyInfo;
import org.apache.streampark.spark.kubernetes.model.SparkK8sPodTemplates;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.annotation.FieldStrategy;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Getter
@Setter
@TableName("t_spark_app")
@Slf4j
public class SparkApplication extends BaseEntity {

    @TableId(type = IdType.AUTO)
    private Long id;

    private Long teamId;

    /** 1) spark jar 2) spark SQL 3) pyspark*/
    private Integer jobType;

    /** 1) Apache Spark 2) StreamPark Spark */
    private Integer appType;

    /** spark version */
    private Long versionId;

    /** spark.app.name */
    private String appName;

    private Integer deployMode;

    /** 1: build (build from csv) 2: upload (upload local jar job) */
    private Integer resourceFrom;

    private Long projectId;

    /** application module */
    private String module;

    private String mainClass;

    private String jar;

    /**
     * for upload type tasks, checkSum needs to be recorded whether it needs to be republished after
     * the update and modify.
     */
    private Long jarCheckSum;

    /**
     * Arbitrary Spark configuration property in key=value format
     * e.g. spark.driver.cores=1
     */
    private String appProperties;

    /** Arguments passed to the main method of your main class */
    private String appArgs;

    /**
     * yarn application id for spark on Yarn. e.g. application_1722935916851_0014
     * driver pod name for spark on K8s.(will be supported in the future)
     */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String clusterId;

    private String yarnQueue;

    private transient String yarnQueueName;

    /**
     * spark on yarn can launch am and executors on particular nodes by configuring:
     * "spark.yarn.am.nodeLabelExpression" and "spark.yarn.executor.nodeLabelExpression"
     */
    private transient String yarnQueueLabel;

    /** The api server url of k8s. */
    private String k8sMasterUrl;

    /** spark docker base image */
    private String k8sContainerImage;

    /** k8s image pull policy */
    private int k8sImagePullPolicy;

    /** k8s spark service account */
    private String k8sServiceAccount;

    /** k8s namespace */
    private String k8sNamespace = Constants.DEFAULT;

    /** spark kubernetes pod template */
    private String k8sDriverPodTemplate;

    private String k8sExecutorPodTemplate;

    /** spark-hadoop integration on spark-k8s mode */
    private Boolean k8sHadoopIntegration;

    @TableField("HADOOP_USER")
    private String hadoopUser;

    /** max restart retries after job failed */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Integer restartSize;

    /** has restart count */
    private Integer restartCount;

    private Integer state;

    private String options;

    private Integer optionState;

    private Date optionTime;

    private Long userId;

    private String description;

    /** determine if tracking status */
    private Integer tracking;

    /** task release status */
    @TableField("`release`")
    private Integer release;

    /** determine if a task needs to be built */
    private Boolean build;

    /** alert id */
    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Long alertId;

    private Date createTime;

    private Date modifyTime;

    private Date startTime;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Date endTime;

    private Long duration;

    private String tags;

    /** scheduling */
    private String driverCores;
    private String driverMemory;
    private String executorCores;
    private String executorMemory;
    private String executorMaxNums;

    /** metrics of running job */
    private Long numTasks;
    private Long numCompletedTasks;
    private Long numStages;
    private Long numCompletedStages;
    private Long usedMemory;
    private Long usedVCores;

    private transient String teamResource;
    private transient String dependency;
    private transient Long sqlId;
    private transient String nickName;
    private transient String sparkSql;
    private transient Boolean backUp = false;
    private transient Boolean restart = false;
    private transient String config;
    private transient Long configId;
    private transient String sparkVersion;
    private transient String confPath;
    private transient Integer format;
    private transient String backUpDescription;

    /** spark Web UI Url */
    private transient String sparkRestUrl;

    /** refer to {@link org.apache.streampark.flink.packer.pipeline.BuildPipeline} */
    private transient Integer buildStatus;

    private transient AppControl appControl;

    public void setK8sNamespace(String k8sNamespace) {
        this.k8sNamespace = StringUtils.isBlank(k8sNamespace) ? Constants.DEFAULT : k8sNamespace;
    }

    public void setState(Integer state) {
        this.state = state;
        this.tracking = shouldTracking() ? 1 : 0;
    }

    public void resolveYarnQueue() {
        if (!(SparkDeployMode.YARN_CLIENT == this.getDeployModeEnum()
            || SparkDeployMode.YARN_CLUSTER == this.getDeployModeEnum())) {
            return;
        }
        if (StringUtils.isBlank(this.yarnQueue)) {
            this.yarnQueue = "default";
        }
        Map<String, String> queueLabelMap = YarnQueueLabelExpression.getQueueLabelMap(this.yarnQueue);
        this.setYarnQueueName(queueLabelMap.getOrDefault(ConfigKeys.KEY_YARN_APP_QUEUE(), "default"));
        this.setYarnQueueLabel(queueLabelMap.getOrDefault(ConfigKeys.KEY_YARN_APP_NODE_LABEL(), null));
    }

    /**
     * Resolve the scheduling configuration of the Spark application.
     * About executorMaxNums:
     * 1) if dynamic allocation is disabled, it depends on "spark.executor.instances".
     * 2) if dynamic allocation is enabled and "spark.dynamicAllocation.maxExecutors" is set, it depends on it.
     * 3) if dynamic allocation is enabled and "spark.dynamicAllocation.maxExecutors" is not set,
     *    the number of executors can up to infinity.
     *
     * @param map The configuration map integrated with default configurations,
     *            configuration template and custom configurations.
     */
    public void resolveScheduleConf(Map<String, String> map) {
        this.setDriverCores(map.get(ConfigKeys.KEY_SPARK_DRIVER_CORES()));
        this.setDriverMemory(map.get(ConfigKeys.KEY_SPARK_DRIVER_MEMORY()));
        this.setExecutorCores(map.get(ConfigKeys.KEY_SPARK_EXECUTOR_CORES()));
        this.setExecutorMemory(map.get(ConfigKeys.KEY_SPARK_EXECUTOR_MEMORY()));
        boolean isDynamicAllocationEnabled =
            Boolean.parseBoolean(map.get(ConfigKeys.KEY_SPARK_DYNAMIC_ALLOCATION_ENABLED()));
        if (isDynamicAllocationEnabled) {
            this.setExecutorMaxNums(map.getOrDefault(ConfigKeys.KEY_SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS(), "inf"));
        } else {
            this.setExecutorMaxNums(map.get(ConfigKeys.KEY_SPARK_EXECUTOR_INSTANCES()));
        }
    }

    /**
     * Determine if a SparkAppState requires tracking.
     *
     * @return 1: need to be tracked | 0: no need to be tracked.
     */
    public Boolean shouldTracking() {
        switch (getStateEnum()) {
            case ADDED:
            case FINISHED:
            case FAILED:
            case KILLED:
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
            case FAILED:
            case FINISHED:
            case LOST:
            case SUCCEEDED:
            case KILLED:
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
    public SparkJobType getJobTypeEnum() {
        return SparkJobType.valueOf(jobType);
    }

    @JsonIgnore
    public SparkAppStateEnum getStateEnum() {
        return SparkAppStateEnum.of(state);
    }

    @JsonIgnore
    public SparkDeployMode getDeployModeEnum() {
        return SparkDeployMode.of(deployMode);
    }

    /** Local compilation and packaging working directory */
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

    /** Automatically identify remoteAppHome or localAppHome based on app SparkDeployMode */
    @JsonIgnore
    public String getAppHome() {
        switch (this.getDeployModeEnum()) {
            case REMOTE:
            case LOCAL:
            case KUBERNETES_NATIVE_CLIENT:
            case KUBERNETES_NATIVE_CLUSTER:
                return getLocalAppHome();
            case YARN_CLIENT:
            case YARN_CLUSTER:
                return getRemoteAppHome();
            default:
                throw new UnsupportedOperationException(
                    "unsupported deployMode ".concat(getDeployModeEnum().getName()));
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

    public SparkK8sPodTemplates getK8sPodTemplates() {
        return SparkK8sPodTemplates.of(k8sDriverPodTemplate, k8sExecutorPodTemplate);
    }

    @JsonIgnore
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public Map<String, String> getOptionMap() {
        if (StringUtils.isBlank(this.options)) {
            return new HashMap<>();
        }
        Map<String, String> optionMap = JacksonUtils.read(this.options, Map.class);
        optionMap.entrySet().removeIf(entry -> entry.getValue() == null);
        return optionMap;
    }

    @JsonIgnore
    public boolean isSparkOnYarnJob() {
        return SparkDeployMode.YARN_CLUSTER.getMode() == (this.getDeployMode())
            || SparkDeployMode.YARN_CLIENT.getMode() == (this.getDeployMode());
    }

    @JsonIgnore
    public boolean isSparkSqlJob() {
        return SparkJobType.SPARK_SQL.getMode().equals(this.getJobType());
    }

    @JsonIgnore
    public boolean isSparkJarJob() {
        return SparkJobType.SPARK_JAR.getMode().equals(this.getJobType());
    }

    @JsonIgnore
    public boolean isSparkJarOrPySparkJob() {
        return SparkJobType.SPARK_JAR.getMode().equals(this.getJobType())
            || SparkJobType.PYSPARK.getMode().equals(this.getJobType());
    }

    @JsonIgnore
    public boolean isFromUploadJob() {
        return isSparkJarOrPySparkJob()
            && ResourceFromEnum.UPLOAD.getValue().equals(this.getResourceFrom());
    }

    @JsonIgnore
    public boolean isFromBuildJob() {
        return isSparkJarOrPySparkJob()
            && ResourceFromEnum.BUILD.getValue().equals(this.getResourceFrom());
    }

    public boolean isStreamParkJob() {
        return this.getAppType() == ApplicationType.STREAMPARK_SPARK.getType();
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
        return SparkAppStateEnum.RUNNING.getValue() == this.getState();
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
        SparkDeployMode deployModeEnum = SparkDeployMode.of(deployMode);
        switch (Objects.requireNonNull(deployModeEnum)) {
            case YARN_CLUSTER:
            case YARN_CLIENT:
                return StorageType.HDFS;
            case KUBERNETES_NATIVE_CLUSTER:
            case KUBERNETES_NATIVE_CLIENT:
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

    public void fillRunningMetrics(SparkApplicationSummary summary) {
        this.setNumTasks(summary.getNumTasks());
        this.setNumCompletedTasks(summary.getNumCompletedTasks());
        this.setNumStages(summary.getNumStages());
        this.setNumCompletedStages(summary.getNumCompletedStages());
        this.setUsedMemory(summary.getUsedMemory());
        this.setUsedVCores(summary.getUsedVCores());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return id.equals(((SparkApplication) o).id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public static class SFunc {

        public static final SFunction<SparkApplication, Long> ID = SparkApplication::getId;
        public static final SFunction<SparkApplication, String> APP_ID = SparkApplication::getClusterId;
        public static final SFunction<SparkApplication, Date> START_TIME = SparkApplication::getStartTime;
        public static final SFunction<SparkApplication, Date> END_TIME = SparkApplication::getEndTime;
        public static final SFunction<SparkApplication, Long> DURATION = SparkApplication::getDuration;
        public static final SFunction<SparkApplication, Integer> STATE = SparkApplication::getState;
        public static final SFunction<SparkApplication, String> OPTIONS = SparkApplication::getOptions;
        public static final SFunction<SparkApplication, Integer> EXECUTION_MODE = SparkApplication::getDeployMode;
    }
}
