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
import org.apache.streampark.common.enums.ClusterState;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.common.enums.FlinkK8sRestExposedType;
import org.apache.streampark.common.enums.FlinkRestoreMode;
import org.apache.streampark.common.enums.ResolveOrder;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.common.util.ExceptionUtils;
import org.apache.streampark.common.util.FlinkConfigurationUtils;
import org.apache.streampark.common.util.HadoopUtils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.base.util.Tuple2;
import org.apache.streampark.console.base.util.Tuple3;
import org.apache.streampark.console.core.entity.ApplicationBuildPipeline;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkApplicationConfig;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSavepoint;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.enums.CheckPointTypeEnum;
import org.apache.streampark.console.core.enums.ConfigFileTypeEnum;
import org.apache.streampark.console.core.enums.EngineTypeEnum;
import org.apache.streampark.console.core.enums.FlinkAppStateEnum;
import org.apache.streampark.console.core.enums.OperationEnum;
import org.apache.streampark.console.core.enums.OptionStateEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.mapper.FlinkApplicationMapper;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.console.core.service.SavepointService;
import org.apache.streampark.console.core.service.SettingService;
import org.apache.streampark.console.core.service.VariableService;
import org.apache.streampark.console.core.service.application.ApplicationLogService;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBackupService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBuildPipelineService;
import org.apache.streampark.console.core.service.application.FlinkApplicationConfigService;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.core.util.ServiceHelper;
import org.apache.streampark.console.core.watcher.FlinkAppHttpWatcher;
import org.apache.streampark.console.core.watcher.FlinkClusterWatcher;
import org.apache.streampark.console.core.watcher.FlinkK8sWatcherWrapper;
import org.apache.streampark.flink.client.FlinkClient;
import org.apache.streampark.flink.client.bean.CancelRequest;
import org.apache.streampark.flink.client.bean.CancelResponse;
import org.apache.streampark.flink.client.bean.SubmitRequest;
import org.apache.streampark.flink.client.bean.SubmitResponse;
import org.apache.streampark.flink.kubernetes.FlinkK8sWatcher;
import org.apache.streampark.flink.kubernetes.helper.KubernetesDeploymentHelper;
import org.apache.streampark.flink.kubernetes.ingress.IngressController;
import org.apache.streampark.flink.kubernetes.model.TrackId;
import org.apache.streampark.flink.packer.pipeline.BuildResult;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;

import java.io.File;
import java.net.URI;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class FlinkApplicationActionServiceImpl
    extends
        ServiceImpl<FlinkApplicationMapper, FlinkApplication>
    implements
        FlinkApplicationActionService {

    @Qualifier("streamparkDeployExecutor")
    @Autowired
    private Executor executorService;

    @Autowired
    private FlinkApplicationBackupService backUpService;

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Autowired
    private FlinkApplicationInfoService applicationInfoService;

    @Autowired
    private FlinkApplicationConfigService configService;

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private FlinkEnvService flinkEnvService;

    @Autowired
    private FlinkSqlService flinkSqlService;

    @Autowired
    private SavepointService savepointService;

    @Autowired
    private SettingService settingService;

    @Autowired
    private FlinkK8sWatcher k8SFlinkTrackMonitor;

    @Autowired
    private FlinkApplicationBuildPipelineService appBuildPipeService;

    @Autowired
    private FlinkClusterService flinkClusterService;

    @Autowired
    private VariableService variableService;

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private FlinkClusterWatcher flinkClusterWatcher;

    @Autowired
    private FlinkK8sWatcherWrapper k8sWatcherWrapper;

    private final Map<Long, CompletableFuture<SubmitResponse>> startFutureMap =
        new ConcurrentHashMap<>();

    private final Map<Long, CompletableFuture<CancelResponse>> cancelFutureMap =
        new ConcurrentHashMap<>();

    @Override
    public void revoke(Long appId) throws ApplicationException {
        FlinkApplication application = getById(appId);
        ApiAlertException.throwIfNull(
            application, String.format("The application id=%s not found, revoke failed.", appId));

        // 1) delete files that have been published to workspace
        application.getFsOperator().delete(application.getAppHome());

        // 2) rollback the files to the workspace
        backUpService.revoke(application);

        // 3) restore related status
        LambdaUpdateWrapper<FlinkApplication> updateWrapper = Wrappers.lambdaUpdate();
        updateWrapper.eq(FlinkApplication::getId, application.getId());
        if (application.isJobTypeFlinkSqlOrCDC()) {
            updateWrapper.set(FlinkApplication::getRelease, ReleaseStateEnum.FAILED.get());
        } else {
            updateWrapper.set(FlinkApplication::getRelease, ReleaseStateEnum.NEED_RELEASE.get());
        }
        if (!application.isRunning()) {
            updateWrapper.set(FlinkApplication::getState, FlinkAppStateEnum.REVOKED.getValue());
        }
        baseMapper.update(null, updateWrapper);
    }

    @Override
    public void restart(FlinkApplication appParam) throws Exception {
        this.cancel(appParam);
        this.start(appParam, false);
    }

    @Override
    public void abort(Long id) {
        FlinkApplication application = this.baseMapper.selectApp(id);
        CompletableFuture<SubmitResponse> startFuture = startFutureMap.remove(id);
        CompletableFuture<CancelResponse> cancelFuture = cancelFutureMap.remove(id);
        if (application.isKubernetesModeJob()) {
            KubernetesDeploymentHelper.watchPodTerminatedLog(
                application.getK8sNamespace(), application.getJobName(), application.getJobId());
        }
        if (startFuture != null) {
            startFuture.cancel(true);
        }
        if (cancelFuture != null) {
            cancelFuture.cancel(true);
        }
        if (startFuture == null && cancelFuture == null) {
            this.doAbort(id);
        }
    }

    @Override
    public void cancel(FlinkApplication appParam) throws Exception {
        FlinkAppHttpWatcher.setOptionState(appParam.getId(), OptionStateEnum.CANCELLING);
        FlinkApplication application = getById(appParam.getId());
        application.setState(FlinkAppStateEnum.CANCELLING.getValue());

        ApplicationLog applicationLog = new ApplicationLog();
        applicationLog.setJobType(EngineTypeEnum.FLINK.getCode());
        applicationLog.setOptionName(OperationEnum.CANCEL.getValue());
        applicationLog.setAppId(application.getId());
        applicationLog.setTrackingUrl(application.getJobManagerUrl());
        applicationLog.setCreateTime(new Date());
        applicationLog.setClusterId(application.getClusterId());
        applicationLog.setUserId(ServiceHelper.getUserId());

        if (appParam.getRestoreOrTriggerSavepoint()) {
            FlinkAppHttpWatcher.addSavepoint(application.getId());
            application.setOptionState(OptionStateEnum.SAVEPOINTING.getValue());
        } else {
            application.setOptionState(OptionStateEnum.CANCELLING.getValue());
        }

        application.setOptionTime(new Date());
        this.baseMapper.updateById(application);

        Long userId = ServiceHelper.getUserId();
        if (!application.getUserId().equals(userId)) {
            FlinkAppHttpWatcher.addCanceledApp(application.getId(), userId);
        }

        FlinkEnv flinkEnv = flinkEnvService.getById(application.getVersionId());

        // infer savepoint
        String customSavepoint = null;
        if (appParam.getRestoreOrTriggerSavepoint()) {
            customSavepoint = appParam.getSavepointPath();
            if (StringUtils.isBlank(customSavepoint)) {
                customSavepoint = savepointService.getSavePointPath(appParam);
            }
        }

        Map<String, Object> properties = new HashMap<>();

        if (FlinkDeployMode.isRemoteMode(application.getDeployModeEnum())) {
            FlinkCluster cluster = flinkClusterService.getById(application.getFlinkClusterId());
            ApiAlertException.throwIfNull(
                cluster,
                String.format(
                    "The clusterId=%s cannot be find, maybe the clusterId is wrong or "
                        + "the cluster has been deleted. Please contact the Admin.",
                    application.getFlinkClusterId()));
            URI activeAddress = cluster.getRemoteURI();
            properties.put(RestOptions.ADDRESS.key(), activeAddress.getHost());
            properties.put(RestOptions.PORT.key(), activeAddress.getPort());
        }

        Tuple3<String, String, FlinkK8sRestExposedType> clusterIdNamespace =
            getNamespaceClusterId(application);
        String namespace = clusterIdNamespace.t1;
        String clusterId = clusterIdNamespace.t2;

        CancelRequest cancelRequest =
            new CancelRequest(
                application.getId(),
                flinkEnv.getFlinkVersion(),
                FlinkDeployMode.of(application.getDeployMode()),
                properties,
                clusterId,
                application.getJobId(),
                appParam.getRestoreOrTriggerSavepoint(),
                appParam.getDrain(),
                customSavepoint,
                appParam.getNativeFormat(),
                namespace);

        final Date triggerTime = new Date();
        CompletableFuture<CancelResponse> cancelFuture =
            CompletableFuture.supplyAsync(() -> FlinkClient.cancel(cancelRequest), executorService);

        cancelFutureMap.put(application.getId(), cancelFuture);

        cancelFuture.whenCompleteAsync(
            (cancelResponse, throwable) -> {
                cancelFutureMap.remove(application.getId());

                if (throwable != null) {
                    String exception = ExceptionUtils.stringifyException(throwable);
                    applicationLog.setException(exception);
                    applicationLog.setSuccess(false);
                    applicationLogService.save(applicationLog);

                    if (throwable instanceof CancellationException) {
                        doAbort(application.getId());
                    } else {
                        log.error("stop flink job failed.", throwable);
                        application.setOptionState(OptionStateEnum.NONE.getValue());
                        application.setState(FlinkAppStateEnum.FAILED.getValue());
                        updateById(application);

                        if (appParam.getRestoreOrTriggerSavepoint()) {
                            savepointService.expire(application.getId());
                        }
                        // re-tracking flink job on kubernetes and logging exception
                        if (application.isKubernetesModeJob()) {
                            TrackId id = k8sWatcherWrapper.toTrackId(application);
                            k8SFlinkTrackMonitor.unWatching(id);
                            k8SFlinkTrackMonitor.doWatching(id);
                        } else {
                            FlinkAppHttpWatcher.unWatching(application.getId());
                        }
                    }
                    return;
                }

                applicationLog.setSuccess(true);
                // save log...
                applicationLogService.save(applicationLog);

                if (cancelResponse != null && cancelResponse.savepointDir() != null) {
                    String savepointDir = cancelResponse.savepointDir();
                    log.info("savepoint path: {}", savepointDir);
                    FlinkSavepoint savepoint = new FlinkSavepoint();
                    savepoint.setPath(savepointDir);
                    savepoint.setAppId(application.getId());
                    savepoint.setLatest(true);
                    savepoint.setType(CheckPointTypeEnum.SAVEPOINT.get());
                    savepoint.setCreateTime(new Date());
                    savepoint.setTriggerTime(triggerTime);
                    savepointService.save(savepoint);
                }

                if (application.isKubernetesModeJob()) {
                    k8SFlinkTrackMonitor.unWatching(k8sWatcherWrapper.toTrackId(application));
                }
            });
    }

    @Override
    public void start(FlinkApplication appParam, boolean auto) throws Exception {
        // 1) check application
        final FlinkApplication application = getById(appParam.getId());
        AssertUtils.notNull(application);
        ApiAlertException.throwIfTrue(
            !application.isCanBeStart(), "[StreamPark] The application cannot be started repeatedly.");

        if (FlinkDeployMode.isRemoteMode(application.getDeployModeEnum())
            || FlinkDeployMode.isSessionMode(application.getDeployModeEnum())) {
            checkBeforeStart(application);
        }

        if (FlinkDeployMode.isYarnMode(application.getDeployModeEnum())) {
            ApiAlertException.throwIfTrue(
                !applicationInfoService.getYarnAppReport(application.getJobName()).isEmpty(),
                "[StreamPark] The same task name is already running in the yarn queue");
        }

        ApplicationBuildPipeline buildPipeline = appBuildPipeService.getById(application.getId());
        AssertUtils.notNull(buildPipeline);

        FlinkEnv flinkEnv = flinkEnvService.getByIdOrDefault(application.getVersionId());
        ApiAlertException.throwIfNull(flinkEnv, "[StreamPark] can no found flink version");

        // if manually started, clear the restart flag
        if (!auto) {
            application.setRestartCount(0);
        } else {
            if (!application.isNeedRestartOnFailed()) {
                return;
            }
            appParam.setRestoreOrTriggerSavepoint(true);
            application.setRestartCount(application.getRestartCount() + 1);
        }
        // 2) update app state to starting...
        starting(application);
        ApplicationLog applicationLog = constructAppLog(application);
        // set the latest to Effective, (it will only become the current effective at this time)
        applicationManageService.toEffective(application);

        Map<String, Object> extraParameter = new HashMap<>(0);
        if (application.isJobTypeFlinkSqlOrCDC()) {
            FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), true);
            // Get the sql of the replaced placeholder
            String realSql = variableService.replaceVariable(application.getTeamId(), flinkSql.getSql());
            flinkSql.setSql(DeflaterUtils.zipString(realSql));
            extraParameter.put(ConfigKeys.KEY_FLINK_SQL(null), flinkSql.getSql());
        }

        Tuple2<String, String> userJarAndAppConf = getUserJarAndAppConf(flinkEnv, application);
        String flinkUserJar = userJarAndAppConf.t1;
        String appConf = userJarAndAppConf.t2;

        BuildResult buildResult = buildPipeline.getBuildResult();
        if (FlinkDeployMode.YARN_APPLICATION == application.getDeployModeEnum()) {
            buildResult = new ShadedBuildResponse(null, flinkUserJar, true);
        }

        // Get the args after placeholder replacement
        String args =
            StringUtils.isBlank(appParam.getArgs()) ? application.getArgs() : appParam.getArgs();
        String applicationArgs = variableService.replaceVariable(application.getTeamId(), args);

        Tuple3<String, String, FlinkK8sRestExposedType> clusterIdNamespace =
            getNamespaceClusterId(application);
        String k8sNamespace = clusterIdNamespace.t1;
        String k8sClusterId = clusterIdNamespace.t2;
        FlinkK8sRestExposedType exposedType = clusterIdNamespace.t3;

        String dynamicProperties =
            StringUtils.isBlank(appParam.getDynamicProperties())
                ? application.getDynamicProperties()
                : appParam.getDynamicProperties();

        SubmitRequest submitRequest =
            new SubmitRequest(
                flinkEnv.getFlinkVersion(),
                FlinkDeployMode.of(application.getDeployMode()),
                getProperties(application, dynamicProperties),
                flinkEnv.getFlinkConf(),
                FlinkJobType.of(application.getJobType()),
                application.getId(),
                new JobID().toHexString(),
                application.getJobName(),
                appConf,
                application.getApplicationType(),
                getSavepointPath(appParam),
                FlinkRestoreMode.of(appParam.getRestoreMode()),
                applicationArgs,
                k8sClusterId,
                application.getHadoopUser(),
                buildResult,
                extraParameter,
                k8sNamespace,
                exposedType);

        CompletableFuture<SubmitResponse> future =
            CompletableFuture.supplyAsync(() -> FlinkClient.submit(submitRequest), executorService);

        startFutureMap.put(application.getId(), future);

        future.whenCompleteAsync(
            (response, throwable) -> {
                // 1) remove Future
                startFutureMap.remove(application.getId());
                // 2) exception
                if (throwable != null) {
                    processForException(appParam, throwable, applicationLog, application);
                    return;
                }
                // 3) success
                processForSuccess(appParam, response, applicationLog, application);
            });
    }

    @Nonnull
    private ApplicationLog constructAppLog(FlinkApplication application) {
        ApplicationLog applicationLog = new ApplicationLog();
        applicationLog.setJobType(EngineTypeEnum.FLINK.getCode());
        applicationLog.setOptionName(OperationEnum.START.getValue());
        applicationLog.setAppId(application.getId());
        applicationLog.setCreateTime(new Date());
        applicationLog.setUserId(ServiceHelper.getUserId());
        return applicationLog;
    }

    private void processForSuccess(
                                   FlinkApplication appParam,
                                   SubmitResponse response,
                                   ApplicationLog applicationLog,
                                   FlinkApplication flinkApplication) {
        applicationLog.setSuccess(true);
        if (response.flinkConfig() != null) {
            String jmMemory = response.flinkConfig().get(ConfigKeys.KEY_FLINK_JM_PROCESS_MEMORY());
            if (jmMemory != null) {
                flinkApplication.setJmMemory(MemorySize.parse(jmMemory).getMebiBytes());
            }
            String tmMemory = response.flinkConfig().get(ConfigKeys.KEY_FLINK_TM_PROCESS_MEMORY());
            if (tmMemory != null) {
                flinkApplication.setTmMemory(MemorySize.parse(tmMemory).getMebiBytes());
            }
        }
        if (StringUtils.isNoneEmpty(response.jobId())) {
            flinkApplication.setJobId(response.jobId());
        }

        if (FlinkDeployMode.isYarnMode(flinkApplication.getDeployMode())) {
            flinkApplication.setClusterId(response.clusterId());
            applicationLog.setClusterId(response.clusterId());
        }

        if (StringUtils.isNoneEmpty(response.jobManagerUrl())) {
            flinkApplication.setJobManagerUrl(response.jobManagerUrl());
            applicationLog.setTrackingUrl(response.jobManagerUrl());
        }
        applicationLog.setClusterId(response.clusterId());
        flinkApplication.setStartTime(new Date());
        flinkApplication.setEndTime(null);

        // if start completed, will be added task to tracking queue
        if (flinkApplication.isKubernetesModeJob()) {
            processForK8sApp(flinkApplication, applicationLog);
        } else {
            FlinkAppHttpWatcher.setOptionState(appParam.getId(), OptionStateEnum.STARTING);
            FlinkAppHttpWatcher.doWatching(flinkApplication);
        }
        // update app
        updateById(flinkApplication);
        // save log
        applicationLogService.save(applicationLog);
    }

    private void processForK8sApp(FlinkApplication application, ApplicationLog applicationLog) {
        application.setRelease(ReleaseStateEnum.DONE.get());
        k8SFlinkTrackMonitor.doWatching(k8sWatcherWrapper.toTrackId(application));
        if (!FlinkDeployMode.isKubernetesApplicationMode(application.getDeployMode())) {
            return;
        }
        String domainName = settingService.getIngressModeDefault();
        if (StringUtils.isNotBlank(domainName)) {
            try {
                IngressController.configureIngress(
                    domainName, application.getClusterId(), application.getK8sNamespace());
            } catch (KubernetesClientException e) {
                log.info("Failed to create ingress, stack info:{}", e.getMessage());
                applicationLog.setException(e.getMessage());
                applicationLog.setSuccess(false);
                applicationLogService.save(applicationLog);
                application.setState(FlinkAppStateEnum.FAILED.getValue());
                application.setOptionState(OptionStateEnum.NONE.getValue());
            }
        }
    }

    private void processForException(
                                     FlinkApplication appParam,
                                     Throwable throwable,
                                     ApplicationLog applicationLog,
                                     FlinkApplication application) {
        String exception = ExceptionUtils.stringifyException(throwable);
        applicationLog.setException(exception);
        applicationLog.setSuccess(false);
        applicationLogService.save(applicationLog);
        if (throwable instanceof CancellationException) {
            doAbort(application.getId());
        } else {
            FlinkApplication app = getById(appParam.getId());
            app.setState(FlinkAppStateEnum.FAILED.getValue());
            app.setOptionState(OptionStateEnum.NONE.getValue());
            updateById(app);
            if (app.isKubernetesModeJob()) {
                k8SFlinkTrackMonitor.unWatching(k8sWatcherWrapper.toTrackId(app));
            } else {
                FlinkAppHttpWatcher.unWatching(appParam.getId());
            }
        }
    }

    /**
     * Check whether a job with the same name is running in the yarn queue
     *
     * @param jobName job name
     * @return true if the job is running, false otherwise
     */
    private boolean checkAppRepeatInYarn(String jobName) {
        try {
            YarnClient yarnClient = HadoopUtils.yarnClient();
            Set<String> types =
                Sets.newHashSet(
                    ApplicationType.STREAMPARK_FLINK.getName(), ApplicationType.APACHE_FLINK.getName());
            EnumSet<YarnApplicationState> states =
                EnumSet.of(YarnApplicationState.RUNNING, YarnApplicationState.ACCEPTED);
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

    private void starting(FlinkApplication application) {
        application.setState(FlinkAppStateEnum.STARTING.getValue());
        application.setOptionTime(new Date());
        updateById(application);
    }

    private Tuple2<String, String> getUserJarAndAppConf(
                                                        FlinkEnv flinkEnv, FlinkApplication application) {
        FlinkDeployMode deployModeEnum = application.getDeployModeEnum();
        FlinkApplicationConfig applicationConfig = configService.getEffective(application.getId());

        ApiAlertException.throwIfNull(
            deployModeEnum, "DeployMode can't be null, start application failed.");

        String flinkUserJar = null;
        String appConf = null;

        switch (application.getJobTypeEnum()) {
            case FLINK_SQL:
                FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), false);
                AssertUtils.notNull(flinkSql);
                // 1) dist_userJar
                String sqlDistJar = ServiceHelper.getFlinkSqlClientJar(flinkEnv);
                // 2) appConfig
                appConf =
                    applicationConfig == null
                        ? null
                        : String.format("yaml://%s", applicationConfig.getContent());
                // 3) client
                if (FlinkDeployMode.YARN_APPLICATION == deployModeEnum) {
                    String clientPath = Workspace.remote().APP_CLIENT();
                    flinkUserJar = String.format("%s/%s", clientPath, sqlDistJar);
                }
                break;

            case FLINK_CDC:
                log.info("the current job id: {}", application.getId());
                FlinkSql flinkCDC = flinkSqlService.getEffective(application.getId(), false);
                AssertUtils.notNull(flinkCDC);
                // 1) dist_userJar
                String cdcDistJar = ServiceHelper.getFlinkCDCClientJar(flinkEnv);
                // 2) appConfig
                appConf =
                    applicationConfig == null
                        ? null
                        : String.format("yaml://%s", applicationConfig.getContent());
                // 3) client
                if (FlinkDeployMode.YARN_APPLICATION == deployModeEnum) {
                    String clientPath = Workspace.remote().APP_CLIENT();
                    flinkUserJar = String.format("%s/%s", clientPath, cdcDistJar);
                }
                break;

            case PYFLINK:
                Resource resource =
                    resourceService.findByResourceName(application.getTeamId(), application.getJar());

                ApiAlertException.throwIfNull(
                    resource, "pyflink file can't be null, start application failed.");

                ApiAlertException.throwIfNull(
                    resource.getFilePath(), "pyflink file can't be null, start application failed.");

                ApiAlertException.throwIfFalse(
                    resource.getFilePath().endsWith(Constants.PYTHON_SUFFIX),
                    "pyflink format error, must be a \".py\" suffix, start application failed.");

                flinkUserJar = resource.getFilePath();
                break;

            case FLINK_JAR:
                if (application.isResourceFromUpload()) {
                    appConf =
                        String.format(
                            "json://{\"%s\":\"%s\"}",
                            ConfigKeys.KEY_FLINK_APPLICATION_MAIN_CLASS(), application.getMainClass());
                } else {
                    switch (application.getApplicationType()) {
                        case STREAMPARK_FLINK:
                            ConfigFileTypeEnum fileType = ConfigFileTypeEnum.of(applicationConfig.getFormat());
                            if (fileType != null && ConfigFileTypeEnum.UNKNOWN != fileType) {
                                appConf =
                                    String.format(
                                        "%s://%s", fileType.getTypeName(), applicationConfig.getContent());
                            } else {
                                throw new IllegalArgumentException(
                                    "application' config type error,must be ( yaml| properties| hocon )");
                            }
                            break;
                        case APACHE_FLINK:
                            appConf =
                                String.format(
                                    "json://{\"%s\":\"%s\"}",
                                    ConfigKeys.KEY_FLINK_APPLICATION_MAIN_CLASS(), application.getMainClass());
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "[StreamPark] ApplicationType must be (StreamPark flink | Apache flink)... ");
                    }
                }

                if (FlinkDeployMode.YARN_APPLICATION == deployModeEnum) {
                    switch (application.getApplicationType()) {
                        case STREAMPARK_FLINK:
                            flinkUserJar =
                                String.format(
                                    "%s/%s",
                                    application.getAppLib(),
                                    application.getModule().concat(Constants.JAR_SUFFIX));
                            break;
                        case APACHE_FLINK:
                            flinkUserJar = String.format("%s/%s", application.getAppHome(), application.getJar());
                            if (!FsOperator.hdfs().exists(flinkUserJar)) {
                                resource =
                                    resourceService.findByResourceName(
                                        application.getTeamId(), application.getJar());
                                if (resource != null && StringUtils.isNotBlank(resource.getFilePath())) {
                                    flinkUserJar =
                                        String.format(
                                            "%s/%s",
                                            application.getAppHome(), new File(resource.getFilePath()).getName());
                                }
                            }
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "[StreamPark] ApplicationType must be (StreamPark flink | Apache flink)... ");
                    }
                }
                break;
        }
        return Tuple2.of(flinkUserJar, appConf);
    }

    private Map<String, Object> getProperties(
                                              FlinkApplication application, String runtimeProperties) {
        Map<String, Object> properties = new HashMap<>(application.getOptionMap());
        if (FlinkDeployMode.isRemoteMode(application.getDeployModeEnum())) {
            FlinkCluster cluster = flinkClusterService.getById(application.getFlinkClusterId());
            ApiAlertException.throwIfNull(
                cluster,
                String.format(
                    "The clusterId=%s can't be find, maybe the clusterId is wrong or "
                        + "the cluster has been deleted. Please contact the Admin.",
                    application.getFlinkClusterId()));
            URI activeAddress = cluster.getRemoteURI();
            properties.put(RestOptions.ADDRESS.key(), activeAddress.getHost());
            properties.put(RestOptions.PORT.key(), activeAddress.getPort());
        } else if (FlinkDeployMode.isYarnMode(application.getDeployModeEnum())) {
            if (FlinkDeployMode.YARN_SESSION == application.getDeployModeEnum()) {
                FlinkCluster cluster = flinkClusterService.getById(application.getFlinkClusterId());
                ApiAlertException.throwIfNull(
                    cluster,
                    String.format(
                        "The yarn session clusterId=%s cannot be find, maybe the clusterId is wrong or "
                            + "the cluster has been deleted. Please contact the Admin.",
                        application.getFlinkClusterId()));
                properties.put(ConfigKeys.KEY_YARN_APP_ID(), cluster.getClusterId());
            } else {
                String yarnQueue =
                    (String) application.getHotParamsMap().get(ConfigKeys.KEY_YARN_APP_QUEUE());
                String yarnLabelExpr =
                    (String) application.getHotParamsMap().get(ConfigKeys.KEY_YARN_APP_NODE_LABEL());
                Optional.ofNullable(yarnQueue)
                    .ifPresent(yq -> properties.put(ConfigKeys.KEY_YARN_APP_QUEUE(), yq));
                Optional.ofNullable(yarnLabelExpr)
                    .ifPresent(yLabel -> properties.put(ConfigKeys.KEY_YARN_APP_NODE_LABEL(), yLabel));
            }
        } else if (FlinkDeployMode.isKubernetesMode(application.getDeployModeEnum())) {
            properties.put(ConfigKeys.KEY_K8S_IMAGE_PULL_POLICY(), "Always");
        }

        if (FlinkDeployMode.isKubernetesApplicationMode(application.getDeployMode())) {
            properties.put(JobManagerOptions.ARCHIVE_DIR.key(), Workspace.ARCHIVES_FILE_PATH());
        }

        if (application.getAllowNonRestored()) {
            properties.put(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key(), true);
        }

        Map<String, String> dynamicProperties =
            FlinkConfigurationUtils.extractDynamicPropertiesAsJava(runtimeProperties);
        properties.putAll(dynamicProperties);
        ResolveOrder resolveOrder = ResolveOrder.of(application.getResolveOrder());
        if (resolveOrder != null) {
            properties.put(CoreOptions.CLASSLOADER_RESOLVE_ORDER.key(), resolveOrder.getName());
        }

        return properties;
    }

    private void doAbort(Long id) {
        FlinkApplication application = getById(id);
        application.setOptionState(OptionStateEnum.NONE.getValue());
        application.setState(FlinkAppStateEnum.CANCELED.getValue());
        application.setOptionTime(new Date());
        updateById(application);
        savepointService.expire(application.getId());
        // re-tracking flink job on kubernetes and logging exception
        if (application.isKubernetesModeJob()) {
            TrackId trackId = k8sWatcherWrapper.toTrackId(application);
            k8SFlinkTrackMonitor.unWatching(trackId);
            k8SFlinkTrackMonitor.doWatching(trackId);
        } else {
            FlinkAppHttpWatcher.unWatching(application.getId());
        }
        // kill application
        if (FlinkDeployMode.isYarnMode(application.getDeployModeEnum())) {
            try {
                List<ApplicationReport> applications =
                    applicationInfoService.getYarnAppReport(application.getJobName());
                if (!applications.isEmpty()) {
                    YarnClient yarnClient = HadoopUtils.yarnClient();
                    yarnClient.killApplication(applications.get(0).getApplicationId());
                }
            } catch (Exception exception) {
                log.error("Kill yarn application failed.", exception);
            }
        }
    }

    private String getSavepointPath(FlinkApplication appParam) {
        if (appParam.getRestoreOrTriggerSavepoint()) {
            if (StringUtils.isBlank(appParam.getSavepointPath())) {
                FlinkSavepoint savepoint = savepointService.getLatest(appParam.getId());
                if (savepoint != null) {
                    return savepoint.getPath();
                }
            } else {
                return appParam.getSavepointPath();
            }
        }
        return null;
    }

    /* check flink cluster before job start job */
    private void checkBeforeStart(FlinkApplication application) {
        FlinkEnv flinkEnv = flinkEnvService.getByAppId(application.getId());
        ApiAlertException.throwIfNull(flinkEnv, "[StreamPark] can no found flink version");

        ApiAlertException.throwIfFalse(
            flinkClusterService.existsByFlinkEnvId(flinkEnv.getId()),
            "[StreamPark] The flink cluster don't exist, please check it");

        FlinkCluster flinkCluster = flinkClusterService.getById(application.getFlinkClusterId());
        ApiAlertException.throwIfFalse(
            flinkClusterWatcher.getClusterState(flinkCluster) == ClusterState.RUNNING,
            "[StreamPark] The flink cluster not running, please start it");
    }

    private Tuple3<String, String, FlinkK8sRestExposedType> getNamespaceClusterId(
                                                                                  FlinkApplication application) {
        String clusterId = null;
        String k8sNamespace = null;
        FlinkK8sRestExposedType exposedType = null;
        switch (application.getDeployModeEnum()) {
            case YARN_APPLICATION:
            case YARN_PER_JOB:
            case YARN_SESSION:
                clusterId = application.getClusterId();
                break;
            case KUBERNETES_NATIVE_APPLICATION:
                clusterId = application.getJobName();
                k8sNamespace = application.getK8sNamespace();
                exposedType = application.getK8sRestExposedTypeEnum();
                break;
            case KUBERNETES_NATIVE_SESSION:
                FlinkCluster cluster = flinkClusterService.getById(application.getFlinkClusterId());
                ApiAlertException.throwIfNull(
                    cluster,
                    String.format(
                        "The Kubernetes session clusterId=%s can't found, maybe the clusterId is wrong or the cluster has been deleted. Please contact the Admin.",
                        application.getFlinkClusterId()));
                clusterId = cluster.getClusterId();
                k8sNamespace = cluster.getK8sNamespace();
                exposedType = cluster.getK8sRestExposedTypeEnum();
                break;
            default:
                break;
        }
        return Tuple3.of(k8sNamespace, clusterId, exposedType);
    }
}
