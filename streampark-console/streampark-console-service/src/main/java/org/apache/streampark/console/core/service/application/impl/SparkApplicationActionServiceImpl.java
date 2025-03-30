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

package org.apache.streampark.console.core.service.application.impl;

import org.apache.streampark.common.conf.ConfigKeys;
import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.constants.Constants;
import org.apache.streampark.common.enums.ApplicationType;
import org.apache.streampark.common.enums.SparkDeployMode;
import org.apache.streampark.common.enums.SparkJobType;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.common.util.ExceptionUtils;
import org.apache.streampark.common.util.HadoopUtils;
import org.apache.streampark.common.util.PropertiesUtils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.core.entity.ApplicationBuildPipeline;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.entity.SparkApplication;
import org.apache.streampark.console.core.entity.SparkApplicationConfig;
import org.apache.streampark.console.core.entity.SparkEnv;
import org.apache.streampark.console.core.entity.SparkSql;
import org.apache.streampark.console.core.enums.ConfigFileTypeEnum;
import org.apache.streampark.console.core.enums.DistributedTaskEnum;
import org.apache.streampark.console.core.enums.EngineTypeEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.enums.SparkAppStateEnum;
import org.apache.streampark.console.core.enums.SparkOperationEnum;
import org.apache.streampark.console.core.enums.SparkOptionStateEnum;
import org.apache.streampark.console.core.mapper.SparkApplicationMapper;
import org.apache.streampark.console.core.service.DistributedTaskService;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.console.core.service.SparkEnvService;
import org.apache.streampark.console.core.service.SparkSqlService;
import org.apache.streampark.console.core.service.VariableService;
import org.apache.streampark.console.core.service.application.ApplicationLogService;
import org.apache.streampark.console.core.service.application.SparkAplicationBuildPipelineService;
import org.apache.streampark.console.core.service.application.SparkApplicationActionService;
import org.apache.streampark.console.core.service.application.SparkApplicationConfigService;
import org.apache.streampark.console.core.service.application.SparkApplicationInfoService;
import org.apache.streampark.console.core.util.ServiceHelper;
import org.apache.streampark.console.core.watcher.SparkAppHttpWatcher;
import org.apache.streampark.flink.packer.pipeline.BuildResult;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;
import org.apache.streampark.spark.client.SparkClient;
import org.apache.streampark.spark.client.bean.CancelRequest;
import org.apache.streampark.spark.client.bean.CancelResponse;
import org.apache.streampark.spark.client.bean.SubmitRequest;
import org.apache.streampark.spark.client.bean.SubmitResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.apache.hadoop.service.Service.STATE.STARTED;

@Slf4j
@Service
public class SparkApplicationActionServiceImpl
    extends
        ServiceImpl<SparkApplicationMapper, SparkApplication>
    implements
        SparkApplicationActionService {

    @Qualifier("streamparkDeployExecutor")
    @Autowired
    private Executor executorService;

    @Autowired
    private SparkApplicationInfoService applicationInfoService;

    @Autowired
    private SparkApplicationConfigService configService;

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private SparkEnvService sparkEnvService;

    @Autowired
    private SparkSqlService sparkSqlService;

    @Autowired
    private SparkAplicationBuildPipelineService appBuildPipeService;

    @Autowired
    private VariableService variableService;

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private DistributedTaskService distributedTaskService;

    private final Map<Long, CompletableFuture<SubmitResponse>> startJobFutureMap = new ConcurrentHashMap<>();

    private final Map<Long, CompletableFuture<CancelResponse>> cancelJobFutureMap = new ConcurrentHashMap<>();

    @Override
    public void revoke(Long appId) throws ApplicationException {
        SparkApplication application = getById(appId);
        // For HA purposes, if the task is not processed locally, save the Distribution task and return
        if (!distributedTaskService.isLocalProcessing(appId)) {
            distributedTaskService.saveDistributedTask(application, false, DistributedTaskEnum.REVOKE);
            return;
        }
        ApiAlertException.throwIfNull(
            application, String.format("The application id=%s not found, revoke failed.", appId));

        // 1) delete files that have been published to workspace
        application.getFsOperator().delete(application.getAppHome());

        // 2) rollback the files to the workspace
        // backUpService.revoke(application);

        // 3) restore related status
        LambdaUpdateWrapper<SparkApplication> updateWrapper = Wrappers.lambdaUpdate();
        updateWrapper.eq(SparkApplication::getId, application.getId());
        if (application.isSparkSqlJob()) {
            updateWrapper.set(SparkApplication::getRelease, ReleaseStateEnum.FAILED.get());
        } else {
            updateWrapper.set(SparkApplication::getRelease, ReleaseStateEnum.NEED_RELEASE.get());
        }
        if (!application.isRunning()) {
            updateWrapper.set(SparkApplication::getState, SparkAppStateEnum.REVOKED.getValue());
        }
        baseMapper.update(null, updateWrapper);
    }

    @Override
    public void restart(SparkApplication appParam) throws Exception {
        // For HA purposes, if the task is not processed locally, save the Distribution task and return
        if (!distributedTaskService.isLocalProcessing(appParam.getId())) {
            distributedTaskService.saveDistributedTask(appParam, false, DistributedTaskEnum.RESTART);
            return;
        }
        this.cancel(appParam);
        this.start(appParam, false);
    }

    @Override
    public void forcedStop(Long id) {
        SparkApplication application = this.baseMapper.selectApp(id);
        // For HA purposes, if the task is not processed locally, save the Distribution task and return
        if (!distributedTaskService.isLocalProcessing(id)) {
            distributedTaskService.saveDistributedTask(application, false, DistributedTaskEnum.FORCED_STOP);
            return;
        }
        CompletableFuture<SubmitResponse> startFuture = startJobFutureMap.remove(id);
        CompletableFuture<CancelResponse> stopFuture = cancelJobFutureMap.remove(id);
        if (startFuture != null) {
            startFuture.cancel(true);
        }
        if (stopFuture != null) {
            stopFuture.cancel(true);
        }
        if (startFuture == null && stopFuture == null) {
            this.doStopped(id);
        }
    }

    @Override
    public void cancel(SparkApplication appParam) throws Exception {
        // For HA purposes, if the task is not processed locally, save the Distribution task and return
        if (!distributedTaskService.isLocalProcessing(appParam.getId())) {
            distributedTaskService.saveDistributedTask(appParam, false, DistributedTaskEnum.CANCEL);
            return;
        }
        SparkAppHttpWatcher.setOptionState(appParam.getId(), SparkOptionStateEnum.STOPPING);
        SparkApplication application = getById(appParam.getId());
        application.setState(SparkAppStateEnum.STOPPING.getValue());

        ApplicationLog applicationLog = new ApplicationLog();
        applicationLog.setJobType(EngineTypeEnum.SPARK.getCode());
        applicationLog.setOptionName(SparkOperationEnum.CANCEL.getValue());
        applicationLog.setAppId(application.getId());
        applicationLog.setCreateTime(new Date());
        applicationLog.setClusterId(application.getClusterId());
        applicationLog.setUserId(ServiceHelper.getUserId());
        application.setOptionTime(new Date());
        this.baseMapper.updateById(application);

        Long userId = ServiceHelper.getUserId();
        if (!application.getUserId().equals(userId)) {
            SparkAppHttpWatcher.addCanceledApp(application.getId(), userId);
        }

        SparkEnv sparkEnv = sparkEnvService.getById(application.getVersionId());

        Map<String, String> stopProper = new HashMap<>();

        CancelRequest stopRequest =
            new CancelRequest(
                application.getId(),
                sparkEnv.getSparkVersion(),
                SparkDeployMode.of(application.getDeployMode()),
                stopProper,
                application.getClusterId());

        CompletableFuture<CancelResponse> stopFuture =
            CompletableFuture.supplyAsync(() -> SparkClient.cancel(stopRequest), executorService);

        cancelJobFutureMap.put(application.getId(), stopFuture);
        stopFuture.whenComplete(
            (cancelResponse, throwable) -> {
                cancelJobFutureMap.remove(application.getId());
                if (throwable != null) {
                    String exception = ExceptionUtils.stringifyException(throwable);
                    applicationLog.setException(exception);
                    applicationLog.setSuccess(false);
                    applicationLogService.save(applicationLog);

                    if (throwable instanceof CancellationException) {
                        doStopped(application.getId());
                    } else {
                        log.error("stop spark job failed.", throwable);
                        application.setOptionState(SparkOptionStateEnum.NONE.getValue());
                        application.setState(SparkAppStateEnum.FAILED.getValue());
                        updateById(application);
                        SparkAppHttpWatcher.unWatching(application.getId());
                    }
                    return;
                }
                applicationLog.setSuccess(true);
                // save log...
                applicationLogService.save(applicationLog);
            });
    }

    @Override
    public void start(SparkApplication appParam, boolean auto) throws Exception {
        // For HA purposes, if the task is not processed locally, save the Distribution task and return
        if (!distributedTaskService.isLocalProcessing(appParam.getId())) {
            distributedTaskService.saveDistributedTask(appParam, false, DistributedTaskEnum.START);
            return;
        }
        // 1) check application
        final SparkApplication application = getById(appParam.getId());
        AssertUtils.notNull(application);
        ApiAlertException.throwIfTrue(
            !application.isCanBeStart(), "[StreamPark] The application cannot be started repeatedly.");

        SparkEnv sparkEnv = sparkEnvService.getByIdOrDefault(application.getVersionId());
        ApiAlertException.throwIfNull(sparkEnv, "[StreamPark] can no found spark version");

        if (SparkDeployMode.isYarnMode(application.getDeployModeEnum())) {
            checkYarnBeforeStart(application);
        }

        ApplicationBuildPipeline buildPipeline = appBuildPipeService.getById(application.getId());
        AssertUtils.notNull(buildPipeline);

        // if manually started, clear the restart flag
        if (!auto) {
            application.setRestartCount(0);
        } else {
            if (!application.isNeedRestartOnFailed()) {
                return;
            }
            application.setRestartCount(application.getRestartCount() + 1);
        }

        // 2) update app state to starting...
        starting(application);

        ApplicationLog applicationLog = new ApplicationLog();
        applicationLog.setJobType(EngineTypeEnum.SPARK.getCode());
        applicationLog.setOptionName(SparkOperationEnum.START.getValue());
        applicationLog.setAppId(application.getId());
        applicationLog.setCreateTime(new Date());
        applicationLog.setUserId(ServiceHelper.getUserId());

        // set the latest to Effective, (it will only become the current effective at this time)
        // applicationManageService.toEffective(application);

        Map<String, Object> extraParameter = new HashMap<>(0);
        if (application.isSparkSqlJob()) {
            SparkSql sparkSql = sparkSqlService.getEffective(application.getId(), true);
            // Get the sql of the replaced placeholder
            String realSql = variableService.replaceVariable(application.getTeamId(), sparkSql.getSql());
            sparkSql.setSql(DeflaterUtils.zipString(realSql));
            extraParameter.put(ConfigKeys.KEY_SPARK_SQL(null), sparkSql.getSql());
        }

        Tuple2<String, String> userJarAndAppConf = getUserJarAndAppConf(sparkEnv, application);
        String sparkUserJar = userJarAndAppConf.f0;
        String appConf = userJarAndAppConf.f1;

        BuildResult buildResult = buildPipeline.getBuildResult();
        if (SparkDeployMode.isYarnMode(application.getDeployModeEnum())) {
            buildResult = new ShadedBuildResponse(null, sparkUserJar, true);
            if (StringUtils.isNotBlank(application.getYarnQueueName())) {
                extraParameter.put(ConfigKeys.KEY_SPARK_YARN_QUEUE_NAME(), application.getYarnQueueName());
            }
            if (StringUtils.isNotBlank(application.getYarnQueueLabel())) {
                extraParameter.put(ConfigKeys.KEY_SPARK_YARN_QUEUE_LABEL(), application.getYarnQueueLabel());
            }
        }

        // Get the args after placeholder replacement
        String applicationArgs = variableService.replaceVariable(application.getTeamId(), application.getAppArgs());

        SubmitRequest submitRequest = new SubmitRequest(
            sparkEnv.getSparkVersion(),
            SparkDeployMode.of(application.getDeployMode()),
            sparkEnv.getSparkConf(),
            SparkJobType.valueOf(application.getJobType()),
            application.getId(),
            application.getAppName(),
            application.getMainClass(),
            appConf,
            PropertiesUtils.extractSparkPropertiesAsJava(application.getAppProperties()),
            PropertiesUtils.extractSparkArgumentsAsJava(applicationArgs),
            application.getApplicationType(),
            application.getHadoopUser(),
            buildResult,
            extraParameter);

        CompletableFuture<SubmitResponse> future = CompletableFuture
            .supplyAsync(() -> SparkClient.submit(submitRequest), executorService);

        startJobFutureMap.put(application.getId(), future);
        future.whenComplete(
            (response, throwable) -> {
                // 1) remove Future
                startJobFutureMap.remove(application.getId());

                // 2) exception
                if (throwable != null) {
                    String exception = ExceptionUtils.stringifyException(throwable);
                    applicationLog.setException(exception);
                    applicationLog.setSuccess(false);
                    applicationLogService.save(applicationLog);
                    if (throwable instanceof CancellationException) {
                        doStopped(application.getId());
                    } else {
                        SparkApplication app = getById(appParam.getId());
                        app.setState(SparkAppStateEnum.FAILED.getValue());
                        app.setOptionState(SparkOptionStateEnum.NONE.getValue());
                        updateById(app);
                        SparkAppHttpWatcher.unWatching(appParam.getId());
                    }
                    return;
                }

                // 3) success
                applicationLog.setSuccess(true);
                application.resolveScheduleConf(response.sparkProperties());
                if (StringUtils.isNoneEmpty(response.sparkAppId())) {
                    application.setClusterId(response.sparkAppId());
                }
                applicationLog.setClusterId(response.sparkAppId());
                applicationLog.setTrackingUrl(response.trackingUrl());
                application.setStartTime(new Date());
                application.setEndTime(null);

                // if start completed, will be added task to tracking queue
                SparkAppHttpWatcher.setOptionState(appParam.getId(), SparkOptionStateEnum.STARTING);
                SparkAppHttpWatcher.doWatching(application);

                // update app
                updateById(application);
                // save log
                applicationLogService.save(applicationLog);
            });
    }

    /**
     * Check whether a job with the same name is running in the yarn queue
     *
     * @param jobName
     * @return
     */
    private boolean checkAppRepeatInYarn(String jobName) {
        try {
            YarnClient yarnClient = HadoopUtils.yarnClient();
            Set<String> types = Sets.newHashSet(
                ApplicationType.STREAMPARK_FLINK.getName(), ApplicationType.APACHE_FLINK.getName());
            EnumSet<YarnApplicationState> states = EnumSet.of(YarnApplicationState.RUNNING,
                YarnApplicationState.ACCEPTED);
            List<ApplicationReport> applications = yarnClient.getApplications(types, states);
            for (ApplicationReport report : applications) {
                if (report.getName().equals(jobName)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException("The yarn api is abnormal. Ensure that yarn is running properly.");
        }
    }

    private void starting(SparkApplication application) {
        application.setState(SparkAppStateEnum.STARTING.getValue());
        application.setOptionTime(new Date());
        updateById(application);
    }

    private Tuple2<String, String> getUserJarAndAppConf(
                                                        SparkEnv sparkEnv, SparkApplication application) {
        SparkDeployMode deployModeEnum = application.getDeployModeEnum();
        SparkApplicationConfig applicationConfig = configService.getEffective(application.getId());

        ApiAlertException.throwIfNull(
            deployModeEnum, "DeployMode can't be null, start application failed.");

        String sparkUserJar = null;
        String appConf = null;

        switch (application.getJobTypeEnum()) {
            case SPARK_SQL:
                SparkSql sparkSql = sparkSqlService.getEffective(application.getId(), false);
                AssertUtils.notNull(sparkSql);
                // 1) dist_userJar
                String sqlDistJar = ServiceHelper.getSparkSqlClientJar(sparkEnv);
                // 2) appConfig
                appConf = applicationConfig == null
                    ? null
                    : String.format("yaml://%s", applicationConfig.getContent());
                // 3) client
                if (SparkDeployMode.YARN_CLUSTER == deployModeEnum) {
                    String clientPath = Workspace.remote().APP_CLIENT();
                    sparkUserJar = String.format("%s/%s", clientPath, sqlDistJar);
                }
                break;

            case PYSPARK:
                appConf = applicationConfig == null
                    ? null
                    : String.format("yaml://%s", applicationConfig.getContent());
                Resource resource = resourceService.findByResourceName(application.getTeamId(), application.getJar());

                ApiAlertException.throwIfNull(
                    resource, "pyflink file can't be null, start application failed.");

                ApiAlertException.throwIfNull(
                    resource.getFilePath(), "pyflink file can't be null, start application failed.");

                ApiAlertException.throwIfFalse(
                    resource.getFilePath().endsWith(Constants.PYTHON_SUFFIX),
                    "pyflink format error, must be a \".py\" suffix, start application failed.");

                sparkUserJar = resource.getFilePath();
                break;

            case SPARK_JAR:
                if (application.isFromUploadJob()) {
                    appConf = applicationConfig == null
                        ? null
                        : String.format("yaml://%s", applicationConfig.getContent());
                } else {
                    switch (application.getApplicationType()) {
                        case STREAMPARK_SPARK:
                            ConfigFileTypeEnum fileType = ConfigFileTypeEnum.of(applicationConfig.getFormat());
                            if (fileType != null && ConfigFileTypeEnum.UNKNOWN != fileType) {
                                appConf = String.format(
                                    "%s://%s", fileType.getTypeName(), applicationConfig.getContent());
                            } else {
                                throw new IllegalArgumentException(
                                    "application' config type error,must be ( yaml| properties| hocon )");
                            }
                            break;
                        case APACHE_SPARK:
                            appConf = String.format(
                                "json://{\"%s\":\"%s\"}",
                                ConfigKeys.KEY_FLINK_APPLICATION_MAIN_CLASS(), application.getMainClass());
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "[StreamPark] ApplicationType must be (StreamPark spark | Apache spark)... ");
                    }
                }

                if (SparkDeployMode.isYarnMode(deployModeEnum)) {
                    switch (application.getApplicationType()) {
                        case STREAMPARK_SPARK:
                            sparkUserJar = String.format(
                                "%s/%s",
                                application.getAppLib(),
                                application.getModule().concat(Constants.JAR_SUFFIX));
                            break;
                        case APACHE_SPARK:
                            sparkUserJar = String.format("%s/%s", application.getAppHome(), application.getJar());
                            if (!FsOperator.hdfs().exists(sparkUserJar)) {
                                resource = resourceService.findByResourceName(
                                    application.getTeamId(), application.getJar());
                                if (resource != null && StringUtils.isNotBlank(resource.getFilePath())) {
                                    sparkUserJar = String.format(
                                        "%s/%s",
                                        application.getAppHome(),
                                        new File(resource.getFilePath()).getName());
                                }
                            }
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "[StreamPark] ApplicationType must be (StreamPark spark | Apache spark)... ");
                    }
                }
                break;
        }
        return Tuple2.of(sparkUserJar, appConf);
    }

    private void doStopped(Long id) {
        SparkApplication application = getById(id);
        application.setOptionState(SparkOptionStateEnum.NONE.getValue());
        application.setState(SparkAppStateEnum.KILLED.getValue());
        application.setOptionTime(new Date());
        updateById(application);
        SparkAppHttpWatcher.unWatching(application.getId());
        // kill application
        if (SparkDeployMode.isYarnMode(application.getDeployModeEnum())) {
            try {
                List<ApplicationReport> applications = applicationInfoService
                    .getYarnAppReport(application.getAppName());
                if (!applications.isEmpty()) {
                    YarnClient yarnClient = HadoopUtils.yarnClient();
                    yarnClient.killApplication(applications.get(0).getApplicationId());
                }
            } catch (Exception ignored) {
            }
        }
    }

    /* check yarn cluster before job start job */
    private void checkYarnBeforeStart(SparkApplication application) {
        STATE yarnState = HadoopUtils.yarnClient().getServiceState();
        ApiAlertException.throwIfFalse(
            yarnState == STARTED,
            "[StreamPark] The yarn cluster service state is " + yarnState.name() + ", please check it");
        ApiAlertException.throwIfTrue(
            !applicationInfoService.getYarnAppReport(application.getAppName()).isEmpty(),
            "[StreamPark] The same task name is already running in the yarn queue");
    }
}
