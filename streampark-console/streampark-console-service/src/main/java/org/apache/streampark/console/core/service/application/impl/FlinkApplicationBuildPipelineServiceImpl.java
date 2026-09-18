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

import org.apache.streampark.common.configuration.Constants;
import org.apache.streampark.common.configuration.Workspace;
import org.apache.streampark.common.core.FlinkVersion;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.FileUtils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.bean.DockerConfig;
import org.apache.streampark.console.core.entity.ApplicationBuildPipeline;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkApplicationConfig;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.enums.CandidateTypeEnum;
import org.apache.streampark.console.core.enums.OptionStateEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.core.service.MessageService;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.console.core.service.SettingService;
import org.apache.streampark.console.core.service.application.ApplicationLogService;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBackupService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBuildPipelineService;
import org.apache.streampark.console.core.service.application.FlinkApplicationConfigService;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.core.util.ApplicationBuildPipelineUtils;
import org.apache.streampark.console.core.util.FlinkApplicationConfigUtils;
import org.apache.streampark.console.core.util.FlinkEnvUtils;
import org.apache.streampark.console.core.util.ServiceHelper;
import org.apache.streampark.console.core.watcher.FlinkAppHttpWatcher;
import org.apache.streampark.flink.packer.docker.DockerConf;
import org.apache.streampark.flink.packer.maven.DependencyInfo;
import org.apache.streampark.flink.packer.pipeline.BuildPipeline;
import org.apache.streampark.flink.packer.pipeline.BuildResult;
import org.apache.streampark.flink.packer.pipeline.DockerBuildSnapshot;
import org.apache.streampark.flink.packer.pipeline.DockerProgressWatcher;
import org.apache.streampark.flink.packer.pipeline.DockerPullSnapshot;
import org.apache.streampark.flink.packer.pipeline.DockerPushSnapshot;
import org.apache.streampark.flink.packer.pipeline.DockerResolvedSnapshot;
import org.apache.streampark.flink.packer.pipeline.FlinkK8sApplicationBuildRequest;
import org.apache.streampark.flink.packer.pipeline.FlinkK8sSessionBuildRequest;
import org.apache.streampark.flink.packer.pipeline.FlinkRemotePerJobBuildRequest;
import org.apache.streampark.flink.packer.pipeline.FlinkYarnApplicationBuildRequest;
import org.apache.streampark.flink.packer.pipeline.PipeWatcher;
import org.apache.streampark.flink.packer.pipeline.PipelineSnapshot;
import org.apache.streampark.flink.packer.pipeline.PipelineTypeEnum;
import org.apache.streampark.flink.packer.pipeline.impl.FlinkK8sApplicationBuildPipeline;
import org.apache.streampark.flink.packer.pipeline.impl.FlinkK8sSessionBuildPipeline;
import org.apache.streampark.flink.packer.pipeline.impl.FlinkRemoteBuildPipeline;
import org.apache.streampark.flink.packer.pipeline.impl.FlinkYarnApplicationBuildPipeline;

import org.apache.commons.lang3.StringUtils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.streampark.common.enums.ApplicationType.APACHE_FLINK;

@Service
@Slf4j
@Transactional(propagation = Propagation.SUPPORTS, rollbackFor = Exception.class)
public class FlinkApplicationBuildPipelineServiceImpl
    extends
        AbstractApplicationBuildPipelineService
    implements
        FlinkApplicationBuildPipelineService {

    @Autowired
    private FlinkEnvService flinkEnvService;

    @Autowired
    private FlinkSqlService flinkSqlService;

    @Autowired
    private FlinkApplicationBackupService backUpService;

    @Autowired
    private SettingService settingService;

    @Autowired
    private MessageService messageService;

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Autowired
    private FlinkApplicationInfoService applicationInfoService;

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private FlinkAppHttpWatcher flinkAppHttpWatcher;

    @Autowired
    private FlinkApplicationConfigService applicationConfigService;

    @Autowired
    private ResourceService resourceService;

    @Qualifier("streamparkBuildPipelineExecutor")
    @Autowired
    private ExecutorService executorService;

    private static final Cache<Long, DockerPullSnapshot> DOCKER_PULL_PG_SNAPSHOTS = Caffeine.newBuilder()
        .expireAfterWrite(30, TimeUnit.DAYS).build();

    private static final Cache<Long, DockerBuildSnapshot> DOCKER_BUILD_PG_SNAPSHOTS = Caffeine.newBuilder()
        .expireAfterWrite(30, TimeUnit.DAYS).build();

    private static final Cache<Long, DockerPushSnapshot> DOCKER_PUSH_PG_SNAPSHOTS = Caffeine.newBuilder()
        .expireAfterWrite(30, TimeUnit.DAYS).build();

    /**
     * Build application. This is an async call method.
     *
     * @param appId      application id
     * @param forceBuild forced start pipeline or not
     * @return Whether the pipeline was successfully started
     */
    @Override
    public boolean buildApplication(@Nonnull Long appId, boolean forceBuild) {
        // check the build environment
        checkBuildEnv(appId, forceBuild);

        FlinkApplication app = applicationManageService.getById(appId);
        ApplicationLog applicationLog = ApplicationBuildPipelineUtils.createReleaseLog(app.getId(), null);

        // check if you need to go through the build process (if the jar and pom have changed,
        // you need to go through the build process, if other common parameters are modified,
        // you don't need to go through the build process)
        boolean needBuild = applicationManageService.checkBuildAndUpdate(app);
        if (!needBuild) {
            applicationLog.setSuccess(true);
            applicationLogService.save(applicationLog);
            return true;
        }
        // rollback
        if (app.isNeedRollback() && app.isFlinkSqlJob()) {
            flinkSqlService.rollback(app);
        }

        // 1) flink sql setDependency
        FlinkSql newFlinkSql = flinkSqlService.getCandidate(app.getId(), CandidateTypeEnum.NEW);
        FlinkSql effectiveFlinkSql = flinkSqlService.getEffective(app.getId(), false);
        FlinkJobType jobType = app.getJobTypeEnum();
        if (jobType == FlinkJobType.FLINK_SQL || jobType == FlinkJobType.PYFLINK) {
            FlinkSql flinkSql = newFlinkSql == null ? effectiveFlinkSql : newFlinkSql;
            AssertUtils.notNull(flinkSql);
            app.setDependency(flinkSql.getDependency());
            app.setTeamResource(flinkSql.getTeamResource());
        }

        // create pipeline instance
        BuildPipeline pipeline = createPipelineInstance(app);

        // clear history
        removeByAppId(app.getId());
        // register pipeline progress event watcher.
        // save snapshot of pipeline to db when status of pipeline was changed.
        pipeline.registerWatcher(
            new PipeWatcher() {

                @Override
                public void onStart(PipelineSnapshot snapshot) {
                    saveEntity(ApplicationBuildPipelineUtils.fromSnapshot(snapshot, app.getId()));

                    app.setRelease(ReleaseStateEnum.RELEASING.get());
                    applicationManageService.updateRelease(app);

                    if (flinkAppHttpWatcher.isWatchingApp(app.getId())) {
                        flinkAppHttpWatcher.init();
                    }

                    applicationInfoService.checkEnv(app);

                    String appUploads = app.getWorkspace().uploads;
                    ApplicationBuildPipelineUtils.prepareBuildResources(
                        app.isFlinkJarOrPyFlink(),
                        () -> ApplicationBuildPipelineUtils.prepareJarJobHome(
                            app.getTeamId(),
                            app.getJar(),
                            app.getAppHome(),
                            app.getAppLib(),
                            app.isUploadResource() ? null : app.getDistHome(),
                            app.getFsOperator(),
                            appUploads,
                            app.isUploadResource(),
                            app.getApplicationType(),
                            resourceService,
                            true),
                        app.getDependencyObject(),
                        resourceService);
                }

                @Override
                public void onStepStateChange(PipelineSnapshot snapshot) {
                    saveEntity(ApplicationBuildPipelineUtils.fromSnapshot(snapshot, app.getId()));
                }

                @Override
                public void onFinish(PipelineSnapshot snapshot, BuildResult result) {
                    saveEntity(ApplicationBuildPipelineUtils.finishedSnapshot(snapshot, result, app.getId()));
                    if (result.pass()) {
                        ApplicationBuildPipelineUtils.applySuccessfulRelease(
                            app,
                            () -> {
                                if (app.isFlinkSqlJob()) {
                                    applicationManageService.toEffective(app);
                                } else if (app.isStreamParkType()) {
                                    FlinkApplicationConfig config =
                                        applicationConfigService.getLatest(app.getId());
                                    if (config != null) {
                                        FlinkApplicationConfigUtils.applyTo(config, app);
                                        applicationConfigService.toEffective(
                                            app.getId(), app.getConfigId());
                                    }
                                }
                            });
                        if (!app.isNeedRollback()) {
                            if (app.isFlinkSqlJob() && newFlinkSql != null) {
                                backUpService.backup(app, newFlinkSql);
                            } else {
                                backUpService.backup(app, null);
                            }
                        }
                        applicationLog.setSuccess(true);
                    } else {
                        ApplicationBuildPipelineUtils.recordReleaseFailure(
                            app.getId(), app.getJobName(), snapshot, applicationLog, messageService);
                        app.setRelease(ReleaseStateEnum.FAILED.get());
                        app.setOptionState(OptionStateEnum.NONE.getValue());
                        app.setBuild(true);
                    }
                    ApplicationBuildPipelineUtils.finalizeRelease(
                        () -> applicationManageService.updateRelease(app),
                        applicationLog,
                        applicationLogService,
                        () -> {
                            if (flinkAppHttpWatcher.isWatchingApp(app.getId())) {
                                flinkAppHttpWatcher.init();
                            }
                        });
                }
            });
        // save docker resolve progress detail to cache, only for flink-k8s application mode.
        if (PipelineTypeEnum.FLINK_NATIVE_K8S_APPLICATION == pipeline.pipeType()) {
            registerDockerProgressWatcher(pipeline, app);
        }
        // save pipeline instance snapshot to db before release it.
        ApplicationBuildPipeline buildPipeline =
            ApplicationBuildPipeline.initFromPipeline(pipeline).setAppId(app.getId());
        boolean saved = saveEntity(buildPipeline);
        DOCKER_PULL_PG_SNAPSHOTS.invalidate(app.getId());
        DOCKER_BUILD_PG_SNAPSHOTS.invalidate(app.getId());
        DOCKER_PUSH_PG_SNAPSHOTS.invalidate(app.getId());
        // async release pipeline
        executorService.submit((Runnable) pipeline::launch);
        return saved;
    }

    private void registerDockerProgressWatcher(BuildPipeline pipeline, FlinkApplication app) {
        pipeline
            .as(FlinkK8sApplicationBuildPipeline.class)
            .registerDockerProgressWatcher(
                new DockerProgressWatcher() {

                    @Override
                    public void onDockerPullProgressChange(DockerPullSnapshot snapshot) {
                        DOCKER_PULL_PG_SNAPSHOTS.put(app.getId(), snapshot);
                    }

                    @Override
                    public void onDockerBuildProgressChange(DockerBuildSnapshot snapshot) {
                        DOCKER_BUILD_PG_SNAPSHOTS.put(app.getId(), snapshot);
                    }

                    @Override
                    public void onDockerPushProgressChange(DockerPushSnapshot snapshot) {
                        DOCKER_PUSH_PG_SNAPSHOTS.put(app.getId(), snapshot);
                    }
                });
    }

    /**
     * check the build environment
     *
     * @param appId      application id
     * @param forceBuild forced start pipeline or not
     */
    private void checkBuildEnv(Long appId, boolean forceBuild) {
        FlinkApplication app = applicationManageService.getById(appId);

        // 1) check flink version
        String checkEnvErrorMessage = "Check flink env failed, please check the flink version of this job";
        FlinkEnv env = flinkEnvService.getByIdOrDefault(app.getVersionId());
        ApiAlertException.throwIfNull(env, checkEnvErrorMessage);
        FlinkVersion flinkVersion = FlinkEnvUtils.version(env);
        boolean checkVersion = flinkVersion.checkVersion(false);
        ApiAlertException.throwIfFalse(
            checkVersion, "Unsupported flink version:" + flinkVersion.version());

        // 2) check env
        boolean envOk = applicationInfoService.checkEnv(app);
        ApiAlertException.throwIfFalse(envOk, checkEnvErrorMessage);

        // 3) Whether the application can currently start a new building progress
        ApiAlertException.throwIfTrue(
            !forceBuild && !allowToBuildNow(appId),
            "The job is invalid, or the job cannot be built while it is running");
    }

    /**
     * create building pipeline instance
     */
    private BuildPipeline createPipelineInstance(@Nonnull FlinkApplication app) {
        FlinkEnv flinkEnv = flinkEnvService.getByIdOrDefault(app.getVersionId());
        String flinkUserJar = retrieveFlinkUserJar(flinkEnv, app);

        if (!FileUtils.exists(flinkUserJar)) {
            Resource resource = resourceService.findByResourceName(app.getTeamId(), app.getJar());
            if (resource != null && StringUtils.isNotBlank(resource.getFilePath())) {
                flinkUserJar = resource.getFilePath();
            }
        }

        FlinkDeployMode deployModeEnum = app.getDeployModeEnum();
        String mainClass = Constants.STREAMPARK_FLINKSQL_CLIENT_CLASS;
        switch (deployModeEnum) {
            case YARN_APPLICATION:
                String yarnProvidedPath = app.getAppLib();
                String localWorkspace = app.getLocalAppHome().concat("/lib");
                if (FlinkJobType.FLINK_JAR == app.getJobTypeEnum()
                    && APACHE_FLINK == app.getApplicationType()) {
                    yarnProvidedPath = app.getAppHome();
                    localWorkspace = app.getLocalAppHome();
                }
                FlinkYarnApplicationBuildRequest yarnAppRequest = buildFlinkYarnApplicationBuildRequest(app, mainClass,
                    localWorkspace, yarnProvidedPath, flinkEnv, flinkUserJar);
                log.info("Submit params to building pipeline : {}", yarnAppRequest);
                return FlinkYarnApplicationBuildPipeline.of(yarnAppRequest);
            case YARN_PER_JOB:
            case YARN_SESSION:
            case REMOTE:
                FlinkRemotePerJobBuildRequest buildRequest = buildFlinkRemotePerJobBuildRequest(app, mainClass,
                    flinkUserJar, flinkEnv);
                log.info("Submit params to building pipeline : {}", buildRequest);
                return FlinkRemoteBuildPipeline.of(buildRequest);
            case KUBERNETES_NATIVE_SESSION:
                FlinkK8sSessionBuildRequest k8sSessionBuildRequest = buildFlinkK8sSessionBuildRequest(app, mainClass,
                    flinkUserJar, flinkEnv);
                log.info("Submit params to building pipeline : {}", k8sSessionBuildRequest);
                return FlinkK8sSessionBuildPipeline.of(k8sSessionBuildRequest);
            case KUBERNETES_NATIVE_APPLICATION:
                DockerConfig dockerConfig = settingService.getDockerConfig();
                FlinkK8sApplicationBuildRequest k8sApplicationBuildRequest = buildFlinkK8sApplicationBuildRequest(
                    app, mainClass, flinkUserJar, flinkEnv, dockerConfig);
                log.info("Submit params to building pipeline : {}", k8sApplicationBuildRequest);
                return FlinkK8sApplicationBuildPipeline.of(k8sApplicationBuildRequest);
            default:
                throw new UnsupportedOperationException(
                    "Unsupported Building Application for DeployMode: " + app.getDeployModeEnum());
        }
    }

    @Nonnull
    private FlinkYarnApplicationBuildRequest buildFlinkYarnApplicationBuildRequest(
                                                                                   @Nonnull FlinkApplication app,
                                                                                   String mainClass,
                                                                                   String localWorkspace,
                                                                                   String yarnProvidedPath,
                                                                                   FlinkEnv flinkEnv,
                                                                                   String flinkUserJar) {
        return new FlinkYarnApplicationBuildRequest(
            app.getJobName(),
            mainClass,
            app.isFlinkSqlJob() ? app.getLocalAppHome() : localWorkspace,
            yarnProvidedPath,
            app.getJobTypeEnum(),
            getMergedDependencyInfo(app),
            FlinkEnvUtils.version(flinkEnv),
            flinkUserJar);
    }

    @Nonnull
    private FlinkK8sApplicationBuildRequest buildFlinkK8sApplicationBuildRequest(
                                                                                 @Nonnull FlinkApplication app,
                                                                                 String mainClass,
                                                                                 String flinkUserJar,
                                                                                 FlinkEnv flinkEnv,
                                                                                 DockerConfig dockerConfig) {
        FlinkK8sApplicationBuildRequest k8sApplicationBuildRequest = new FlinkK8sApplicationBuildRequest(
            app.getJobName(),
            app.getLocalAppHome(),
            mainClass,
            flinkUserJar,
            app.getDeployModeEnum(),
            app.getJobTypeEnum(),
            FlinkEnvUtils.version(flinkEnv),
            getMergedDependencyInfo(app),
            app.getJobName(),
            app.getK8sNamespace(),
            app.getFlinkImage(),
            app.getK8sPodTemplates(),
            app.getK8sHadoopIntegration() != null ? app.getK8sHadoopIntegration() : false,
            DockerConf.of(
                dockerConfig.getAddress(),
                dockerConfig.getNamespace(),
                dockerConfig.getUsername(),
                dockerConfig.getPassword()),
            app.getIngressTemplate());
        return k8sApplicationBuildRequest;
    }

    @Nonnull
    private FlinkK8sSessionBuildRequest buildFlinkK8sSessionBuildRequest(
                                                                         @Nonnull FlinkApplication app,
                                                                         String mainClass,
                                                                         String flinkUserJar, FlinkEnv flinkEnv) {
        FlinkK8sSessionBuildRequest k8sSessionBuildRequest = new FlinkK8sSessionBuildRequest(
            app.getJobName(),
            app.getLocalAppHome(),
            mainClass,
            flinkUserJar,
            app.getDeployModeEnum(),
            app.getJobTypeEnum(),
            FlinkEnvUtils.version(flinkEnv),
            getMergedDependencyInfo(app),
            app.getClusterId(),
            app.getK8sNamespace());
        return k8sSessionBuildRequest;
    }

    @Nonnull
    private FlinkRemotePerJobBuildRequest buildFlinkRemotePerJobBuildRequest(
                                                                             @Nonnull FlinkApplication app,
                                                                             String mainClass,
                                                                             String flinkUserJar, FlinkEnv flinkEnv) {
        return new FlinkRemotePerJobBuildRequest(
            app.getJobName(),
            app.getLocalAppHome(),
            mainClass,
            flinkUserJar,
            app.isFlinkJar(),
            app.getDeployModeEnum(),
            app.getJobTypeEnum(),
            FlinkEnvUtils.version(flinkEnv),
            getMergedDependencyInfo(app));
    }

    /**
     * copy from {@link FlinkApplicationActionService#start(FlinkApplication, boolean)}
     */
    private String retrieveFlinkUserJar(FlinkEnv flinkEnv, FlinkApplication app) {
        switch (app.getJobTypeEnum()) {
            case FLINK_JAR:
                switch (app.getApplicationType()) {
                    case STREAMPARK_FLINK:
                        return String.format(
                            "%s/%s", app.getAppLib(), app.getModule().concat(Constants.JAR_SUFFIX));
                    case APACHE_FLINK:
                        return String.format("%s/%s", app.getAppHome(), app.getJar());
                    default:
                        throw new IllegalArgumentException(
                            "[StreamPark] unsupported ApplicationType of FlinkJar: "
                                + app.getApplicationType());
                }
            case PYFLINK:
                return String.format("%s/%s", app.getAppHome(), app.getJar());
            case FLINK_SQL:
                String sqlDistJar = ServiceHelper.getFlinkSqlClientJar(flinkEnv);
                if (app.getDeployModeEnum() == FlinkDeployMode.YARN_APPLICATION) {
                    String clientPath = Workspace.REMOTE.client;
                    return String.format("%s/%s", clientPath, sqlDistJar);
                }
                return Workspace.LOCAL.client.concat("/").concat(sqlDistJar);
            default:
                throw new UnsupportedOperationException(
                    "[StreamPark] unsupported JobType: " + app.getJobTypeEnum());
        }
    }

    @Override
    public DockerResolvedSnapshot getDockerProgressDetailSnapshot(@Nonnull Long appId) {
        return new DockerResolvedSnapshot(
            DOCKER_PULL_PG_SNAPSHOTS.getIfPresent(appId),
            DOCKER_BUILD_PG_SNAPSHOTS.getIfPresent(appId),
            DOCKER_PUSH_PG_SNAPSHOTS.getIfPresent(appId));
    }

    private DependencyInfo getMergedDependencyInfo(FlinkApplication application) {
        return ApplicationBuildPipelineUtils.getMergedDependencyInfo(
            application.getDependencyInfo(),
            application.getTeamResource(),
            application.getTeamId(),
            resourceService,
            log,
            true);
    }
}
