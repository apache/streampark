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

import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.constants.Constants;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.enums.FlinkJobType;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.ExceptionUtils;
import org.apache.streampark.common.util.FileUtils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.base.util.WebUtils;
import org.apache.streampark.console.core.bean.Dependency;
import org.apache.streampark.console.core.bean.DockerConfig;
import org.apache.streampark.console.core.entity.ApplicationBuildPipeline;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkApplicationConfig;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.entity.Message;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.enums.CandidateTypeEnum;
import org.apache.streampark.console.core.enums.NoticeTypeEnum;
import org.apache.streampark.console.core.enums.OptionStateEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.enums.ResourceTypeEnum;
import org.apache.streampark.console.core.mapper.ApplicationBuildPipelineMapper;
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
import org.apache.streampark.console.core.util.ServiceHelper;
import org.apache.streampark.console.core.watcher.FlinkAppHttpWatcher;
import org.apache.streampark.flink.packer.docker.DockerConf;
import org.apache.streampark.flink.packer.maven.Artifact;
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
import org.apache.streampark.flink.packer.pipeline.PipelineStatusEnum;
import org.apache.streampark.flink.packer.pipeline.PipelineTypeEnum;
import org.apache.streampark.flink.packer.pipeline.impl.FlinkK8sApplicationBuildPipeline;
import org.apache.streampark.flink.packer.pipeline.impl.FlinkK8sSessionBuildPipeline;
import org.apache.streampark.flink.packer.pipeline.impl.FlinkRemoteBuildPipeline;
import org.apache.streampark.flink.packer.pipeline.impl.FlinkYarnApplicationBuildPipeline;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.streampark.common.enums.ApplicationType.APACHE_FLINK;
import static org.apache.streampark.console.core.enums.OperationEnum.RELEASE;

@Service
@Slf4j
@Transactional(propagation = Propagation.SUPPORTS, rollbackFor = Exception.class)
public class FlinkApplicationBuildPipelineServiceImpl
    extends
        ServiceImpl<ApplicationBuildPipelineMapper, ApplicationBuildPipeline>
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
        ApplicationLog applicationLog = getApplicationLog(app);

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
        if (app.isNeedRollback() && app.isJobTypeFlinkSqlOrCDC()) {
            flinkSqlService.rollback(app);
        }

        // 1) flink sql setDependency
        FlinkSql newFlinkSql = flinkSqlService.getCandidate(app.getId(), CandidateTypeEnum.NEW);
        FlinkSql effectiveFlinkSql = flinkSqlService.getEffective(app.getId(), false);
        FlinkJobType jobType = app.getJobTypeEnum();
        if (jobType == FlinkJobType.FLINK_SQL || jobType == FlinkJobType.PYFLINK || jobType == FlinkJobType.FLINK_CDC) {
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
                    ApplicationBuildPipeline buildPipeline = ApplicationBuildPipeline.fromPipeSnapshot(snapshot)
                        .setAppId(app.getId());
                    saveEntity(buildPipeline);

                    app.setRelease(ReleaseStateEnum.RELEASING.get());
                    applicationManageService.updateRelease(app);

                    if (flinkAppHttpWatcher.isWatchingApp(app.getId())) {
                        flinkAppHttpWatcher.init();
                    }

                    // 1) checkEnv
                    applicationInfoService.checkEnv(app);

                    // 2) some preparatory work
                    String appUploads = app.getWorkspace().APP_UPLOADS();

                    if (app.isJobTypeFlinkJarOrPyFlink()) {
                        // flinkJar upload jar to appHome...
                        String appHome = app.getAppHome();
                        FsOperator fsOperator = app.getFsOperator();
                        fsOperator.delete(appHome);
                        if (app.isResourceFromUpload()) {
                            String uploadJar = appUploads.concat("/").concat(app.getJar());
                            File localJar = new File(
                                String.format(
                                    "%s/%d/%s",
                                    Workspace.local().APP_UPLOADS(),
                                    app.getTeamId(),
                                    app.getJar()));
                            if (!localJar.exists()) {
                                Resource resource = resourceService.findByResourceName(app.getTeamId(),
                                    app.getJar());
                                if (resource != null && StringUtils.isNotBlank(resource.getFilePath())) {
                                    localJar = new File(resource.getFilePath());
                                    uploadJar = appUploads.concat("/").concat(localJar.getName());
                                } else {
                                    localJar =
                                        new File(WebUtils.getAppTempDir(), app.getJar());
                                    uploadJar = appUploads.concat("/").concat(localJar.getName());
                                }
                            }
                            // upload jar copy to appHome
                            checkOrElseUploadJar(app.getFsOperator(), localJar, uploadJar, appUploads);

                            switch (app.getApplicationType()) {
                                case STREAMPARK_FLINK:
                                    fsOperator.mkdirs(app.getAppLib());
                                    fsOperator.copy(uploadJar, app.getAppLib(), false, true);
                                    break;
                                case APACHE_FLINK:
                                    fsOperator.mkdirs(appHome);
                                    fsOperator.copy(uploadJar, appHome, false, true);
                                    break;
                                default:
                                    throw new IllegalArgumentException(
                                        "[StreamPark] unsupported ApplicationType of FlinkJar: "
                                            + app.getApplicationType());
                            }
                        } else {
                            fsOperator.upload(app.getDistHome(), appHome);
                        }
                    } else {
                        if (!app.getDependencyObject().getJar().isEmpty()) {
                            String localUploads = Workspace.local().APP_UPLOADS();
                            // copy jar to local upload dir
                            for (String jar : app.getDependencyObject().getJar()) {
                                File localJar = new File(WebUtils.getAppTempDir(), jar);
                                File uploadJar = new File(localUploads, jar);
                                if (!localJar.exists() && !uploadJar.exists()) {
                                    throw new ApiAlertException(
                                        "Missing file: " + jar + ", please upload again");
                                }
                                if (localJar.exists()) {
                                    checkOrElseUploadJar(
                                        FsOperator.lfs(), localJar, uploadJar.getAbsolutePath(),
                                        localUploads);
                                }
                            }
                        }
                    }
                }

                @Override
                public void onStepStateChange(PipelineSnapshot snapshot) {
                    ApplicationBuildPipeline buildPipeline = ApplicationBuildPipeline.fromPipeSnapshot(snapshot)
                        .setAppId(app.getId());
                    saveEntity(buildPipeline);
                }

                @Override
                public void onFinish(PipelineSnapshot snapshot, BuildResult result) {
                    ApplicationBuildPipeline buildPipeline = ApplicationBuildPipeline.fromPipeSnapshot(snapshot)
                        .setAppId(app.getId())
                        .setBuildResult(result);
                    saveEntity(buildPipeline);
                    if (result.pass()) {
                        // running job ...
                        if (app.isRunning()) {
                            app.setRelease(ReleaseStateEnum.NEED_RESTART.get());
                        } else {
                            app.setOptionState(OptionStateEnum.NONE.getValue());
                            app.setRelease(ReleaseStateEnum.DONE.get());
                            // If the current task is not running, or the task has just been added, directly
                            // set
                            // the candidate version to the official version
                            if (app.isJobTypeFlinkSqlOrCDC()) {
                                applicationManageService.toEffective(app);
                            } else {
                                if (app.isAppTypeStreamPark()) {
                                    FlinkApplicationConfig config =
                                        applicationConfigService.getLatest(app.getId());
                                    if (config != null) {
                                        config.setToApplication(app);
                                        applicationConfigService.toEffective(app.getId(),
                                            app.getConfigId());
                                    }
                                }
                            }
                        }
                        // backup.
                        if (!app.isNeedRollback()) {
                            if (app.isJobTypeFlinkSqlOrCDC() && newFlinkSql != null) {
                                backUpService.backup(app, newFlinkSql);
                            } else {
                                backUpService.backup(app, null);
                            }
                        }
                        applicationLog.setSuccess(true);
                        app.setBuild(false);

                    } else {
                        Message message = new Message(
                            ServiceHelper.getUserId(),
                            app.getId(),
                            app.getJobName().concat(" release failed"),
                            ExceptionUtils.stringifyException(snapshot.error().exception()),
                            NoticeTypeEnum.EXCEPTION);
                        messageService.push(message);
                        app.setRelease(ReleaseStateEnum.FAILED.get());
                        app.setOptionState(OptionStateEnum.NONE.getValue());
                        app.setBuild(true);
                        applicationLog.setException(
                            ExceptionUtils.stringifyException(snapshot.error().exception()));
                        applicationLog.setSuccess(false);
                    }
                    applicationManageService.updateRelease(app);
                    applicationLogService.save(applicationLog);
                    if (flinkAppHttpWatcher.isWatchingApp(app.getId())) {
                        flinkAppHttpWatcher.init();
                    }
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

    @Nonnull
    private ApplicationLog getApplicationLog(FlinkApplication app) {
        ApplicationLog applicationLog = new ApplicationLog();
        applicationLog.setOptionName(RELEASE.getValue());
        applicationLog.setAppId(app.getId());
        applicationLog.setCreateTime(new Date());
        applicationLog.setUserId(ServiceHelper.getUserId());
        return applicationLog;
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
        boolean checkVersion = env.getFlinkVersion().checkVersion(false);
        ApiAlertException.throwIfFalse(
            checkVersion, "Unsupported flink version:" + env.getFlinkVersion().version());

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
                    localWorkspace, yarnProvidedPath);
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
                                                                                   String yarnProvidedPath) {
        return new FlinkYarnApplicationBuildRequest(
            app.getJobName(),
            mainClass,
            localWorkspace,
            yarnProvidedPath,
            app.getJobTypeEnum(),
            getMergedDependencyInfo(app));
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
            flinkEnv.getFlinkVersion(),
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
            flinkEnv.getFlinkVersion(),
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
            app.isJobTypeFlinkJar(),
            app.getDeployModeEnum(),
            app.getJobTypeEnum(),
            flinkEnv.getFlinkVersion(),
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
                    String clientPath = Workspace.remote().APP_CLIENT();
                    return String.format("%s/%s", clientPath, sqlDistJar);
                }
                return Workspace.local().APP_CLIENT().concat("/").concat(sqlDistJar);
            case FLINK_CDC:
                String cdcDistJar = ServiceHelper.getFlinkCDCClientJar(flinkEnv);
                if (app.getDeployModeEnum() == FlinkDeployMode.YARN_APPLICATION) {
                    String clientPath = Workspace.remote().APP_CLIENT();
                    return String.format("%s/%s", clientPath, cdcDistJar);
                }
                return Workspace.local().APP_CLIENT().concat("/").concat(cdcDistJar);
            default:
                throw new UnsupportedOperationException(
                    "[StreamPark] unsupported JobType: " + app.getJobTypeEnum());
        }
    }

    @Override
    public Optional<ApplicationBuildPipeline> getCurrentBuildPipeline(@Nonnull Long appId) {
        return Optional.ofNullable(getById(appId));
    }

    @Override
    public DockerResolvedSnapshot getDockerProgressDetailSnapshot(@Nonnull Long appId) {
        return new DockerResolvedSnapshot(
            DOCKER_PULL_PG_SNAPSHOTS.getIfPresent(appId),
            DOCKER_BUILD_PG_SNAPSHOTS.getIfPresent(appId),
            DOCKER_PUSH_PG_SNAPSHOTS.getIfPresent(appId));
    }

    @Override
    public boolean allowToBuildNow(@Nonnull Long appId) {
        return getCurrentBuildPipeline(appId)
            .map(pipeline -> PipelineStatusEnum.running != pipeline.getPipelineStatus())
            .orElse(true);
    }

    @Override
    public Map<Long, PipelineStatusEnum> listAppIdPipelineStatusMap(List<Long> appIds) {
        if (CollectionUtils.isEmpty(appIds)) {
            return new HashMap<>();
        }
        List<ApplicationBuildPipeline> appBuildPipelines =
            this.lambdaQuery().in(ApplicationBuildPipeline::getAppId, appIds).list();
        if (CollectionUtils.isEmpty(appBuildPipelines)) {
            return new HashMap<>();
        }
        return appBuildPipelines.stream()
            .collect(Collectors.toMap(ApplicationBuildPipeline::getAppId, ApplicationBuildPipeline::getPipelineStatus));
    }

    @Override
    public void removeByAppId(Long appId) {
        this.lambdaUpdate().eq(ApplicationBuildPipeline::getAppId, appId).remove();
    }

    /**
     * save or update build pipeline
     *
     * @param pipe application build pipeline
     * @return value after the save or update
     */
    public boolean saveEntity(ApplicationBuildPipeline pipe) {
        ApplicationBuildPipeline old = getById(pipe.getAppId());
        if (old == null) {
            return save(pipe);
        }
        return updateById(pipe);
    }

    /**
     * Check if the jar exists, and upload a copy if it does not exist
     *
     * @param fsOperator
     * @param localJar
     * @param targetJar
     * @param targetDir
     */
    private void checkOrElseUploadJar(
                                      FsOperator fsOperator, File localJar, String targetJar, String targetDir) {
        if (!fsOperator.exists(targetJar)) {
            fsOperator.upload(localJar.getAbsolutePath(), targetDir, false, true);
        } else {
            // The file exists to check whether it is consistent, and if it is inconsistent, re-upload it
            if (!FileUtils.equals(localJar, new File(targetJar))) {
                fsOperator.upload(localJar.getAbsolutePath(), targetDir, false, true);
            }
        }
    }

    /**
     * Gets and parses dependencies on the application
     *
     * @param application
     * @return DependencyInfo
     */
    private DependencyInfo getMergedDependencyInfo(FlinkApplication application) {
        DependencyInfo dependencyInfo = application.getDependencyInfo();
        if (StringUtils.isBlank(application.getTeamResource())) {
            return dependencyInfo;
        }

        try {
            String[] resourceIds = JacksonUtils.read(application.getTeamResource(), String[].class);

            List<Artifact> mvnArtifacts = new ArrayList<Artifact>();
            List<String> jarLibs = new ArrayList<String>();

            Arrays.stream(resourceIds)
                .forEach(
                    resourceId -> {
                        Resource resource = resourceService.getById(resourceId);

                        if (resource.getResourceType() != ResourceTypeEnum.GROUP) {
                            mergeDependency(application, mvnArtifacts, jarLibs, resource);
                        } else {
                            try {
                                String[] groupElements =
                                    JacksonUtils.read(resource.getResource(),
                                        String[].class);
                                Arrays.stream(groupElements)
                                    .forEach(
                                        resourceIdInGroup -> mergeDependency(
                                            application,
                                            mvnArtifacts,
                                            jarLibs,
                                            resourceService.getById(
                                                resourceIdInGroup)));
                            } catch (JsonProcessingException e) {
                                throw new ApiAlertException("Parse resource group failed.", e);
                            }
                        }
                    });
            return dependencyInfo.merge(mvnArtifacts, jarLibs);
        } catch (Exception e) {
            log.error("Merge team dependency failed.", e);
            return dependencyInfo;
        }
    }

    private static void mergeDependency(
                                        FlinkApplication application,
                                        List<Artifact> mvnArtifacts,
                                        List<String> jarLibs,
                                        Resource resource) {
        Dependency dependency = Dependency.toDependency(resource.getResource());
        dependency
            .getPom()
            .forEach(
                pom -> mvnArtifacts.add(
                    new Artifact(
                        pom.getGroupId(),
                        pom.getArtifactId(),
                        pom.getVersion(),
                        pom.getClassifier())));
        dependency
            .getJar()
            .forEach(
                jar -> jarLibs.add(
                    String.format(
                        "%s/%d/%s",
                        Workspace.local().APP_UPLOADS(),
                        application.getTeamId(), jar)));
    }
}
