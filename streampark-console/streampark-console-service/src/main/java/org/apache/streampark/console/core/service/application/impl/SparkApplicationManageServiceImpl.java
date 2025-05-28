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
import org.apache.streampark.common.enums.SparkDeployMode;
import org.apache.streampark.common.enums.StorageType;
import org.apache.streampark.common.fs.HdfsOperator;
import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.base.util.WebUtils;
import org.apache.streampark.console.core.bean.AppControl;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.entity.SparkApplication;
import org.apache.streampark.console.core.entity.SparkApplicationConfig;
import org.apache.streampark.console.core.entity.SparkSql;
import org.apache.streampark.console.core.enums.CandidateTypeEnum;
import org.apache.streampark.console.core.enums.ChangeTypeEnum;
import org.apache.streampark.console.core.enums.OptionStateEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.enums.SparkAppStateEnum;
import org.apache.streampark.console.core.mapper.SparkApplicationMapper;
import org.apache.streampark.console.core.service.ProjectService;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.console.core.service.SparkEffectiveService;
import org.apache.streampark.console.core.service.SparkSqlService;
import org.apache.streampark.console.core.service.YarnQueueService;
import org.apache.streampark.console.core.service.application.ApplicationLogService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBuildPipelineService;
import org.apache.streampark.console.core.service.application.SparkApplicationBackupService;
import org.apache.streampark.console.core.service.application.SparkApplicationConfigService;
import org.apache.streampark.console.core.service.application.SparkApplicationManageService;
import org.apache.streampark.console.core.util.ServiceHelper;
import org.apache.streampark.flink.packer.pipeline.PipelineStatusEnum;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.annotations.VisibleForTesting;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class SparkApplicationManageServiceImpl
    extends
        ServiceImpl<SparkApplicationMapper, SparkApplication>
    implements
        SparkApplicationManageService {

    private static final String ERROR_APP_QUEUE_HINT =
        "Queue label '%s' isn't available for teamId '%d', please add it into the team first.";

    @Autowired
    private ProjectService projectService;

    @Autowired
    private SparkApplicationBackupService backUpService;

    @Autowired
    private SparkApplicationConfigService configService;

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private SparkSqlService sparkSqlService;

    @Autowired
    private SparkEffectiveService effectiveService;

    @Autowired
    private FlinkApplicationBuildPipelineService appBuildPipeService;

    @Autowired
    private YarnQueueService yarnQueueService;

    @Autowired
    private ResourceService resourceService;

    @PostConstruct
    public void resetOptionState() {
        this.lambdaUpdate().set(SparkApplication::getOptionState, OptionStateEnum.NONE.getValue())
            .update();
    }

    @Override
    public void toEffective(SparkApplication appParam) {
        SparkApplicationConfig config = configService.getLatest(appParam.getId());
        // set latest to Effective
        if (config != null) {
            this.configService.toEffective(appParam.getId(), config.getId());
        }
        if (appParam.isSparkSqlJob()) {
            SparkSql sparkSql = sparkSqlService.getCandidate(appParam.getId(), null);
            if (sparkSql != null) {
                sparkSqlService.toEffective(appParam.getId(), sparkSql.getId());
                // clean candidate
                sparkSqlService.cleanCandidate(sparkSql.getId());
            }
        }
    }

    @Override
    public void persistMetrics(SparkApplication appParam) {
        this.baseMapper.persistMetrics(appParam);
    }

    @Override
    public boolean mapping(SparkApplication appParam) {
        return this.lambdaUpdate()
            .set(SparkApplication::getClusterId, appParam.getClusterId())
            .set(SparkApplication::getEndTime, null)
            .set(SparkApplication::getState, SparkAppStateEnum.MAPPING.getValue())
            .set(SparkApplication::getTracking, 1)
            .eq(SparkApplication::getId, appParam.getId())
            .update();
    }

    @Override
    public Boolean remove(Long appId) {

        SparkApplication application = getById(appId);

        // 1) remove flink sql
        sparkSqlService.removeByAppId(application.getId());

        // 2) remove log
        applicationLogService.removeByAppId(application.getId());

        // 3) remove config
        configService.removeByAppId(application.getId());

        // 4) remove effective
        effectiveService.removeByAppId(application.getId());

        // remove related hdfs
        // 5) remove backup
        // backUpService.remove(application);

        // 6) remove savepoint
        // savepointService.remove(application);

        // 7) remove BuildPipeline
        appBuildPipeService.removeByAppId(application.getId());

        // 8) remove app
        removeApp(application);
        return true;
    }

    private void removeApp(SparkApplication application) {
        Long appId = application.getId();
        removeById(appId);
        try {
            application
                .getFsOperator()
                .delete(application.getWorkspace().APP_WORKSPACE().concat("/").concat(appId.toString()));
            // try to delete yarn-application, and leave no trouble.
            String path = Workspace.of(StorageType.HDFS).APP_WORKSPACE().concat("/").concat(appId.toString());
            if (HdfsOperator.exists(path)) {
                HdfsOperator.delete(path);
            }
        } catch (Exception e) {
            // skip
        }
    }

    @Override
    public IPage<SparkApplication> page(SparkApplication appParam, RestRequest request) {
        if (appParam.getTeamId() == null) {
            return null;
        }
        Page<SparkApplication> page = MybatisPager.getPage(request);
        this.baseMapper.selectPage(page, appParam);
        List<SparkApplication> records = page.getRecords();

        List<Long> appIds = records.stream().map(SparkApplication::getId).collect(Collectors.toList());
        Map<Long, PipelineStatusEnum> pipeStates = appBuildPipeService.listAppIdPipelineStatusMap(appIds);

        List<SparkApplication> newRecords = records.stream()
            .peek(
                record -> {
                    if (pipeStates.containsKey(record.getId())) {
                        record.setBuildStatus(pipeStates.get(record.getId()).getCode());
                    }

                    AppControl appControl = new AppControl()
                        .setAllowBuild(
                            record.getBuildStatus() == null
                                || !PipelineStatusEnum.running
                                    .getCode()
                                    .equals(record.getBuildStatus()))
                        .setAllowStart(
                            !record.shouldTracking()
                                && PipelineStatusEnum.success
                                    .getCode()
                                    .equals(record.getBuildStatus()))
                        .setAllowStop(record.isRunning())
                        .setAllowView(record.shouldTracking());
                    record.setAppControl(appControl);
                })
            .collect(Collectors.toList());
        page.setRecords(newRecords);
        return page;
    }

    @Override
    public void changeOwnership(Long userId, Long targetUserId) {
        LambdaUpdateWrapper<SparkApplication> updateWrapper = new LambdaUpdateWrapper<SparkApplication>()
            .eq(SparkApplication::getUserId, userId)
            .set(SparkApplication::getUserId, targetUserId);
        this.baseMapper.update(null, updateWrapper);
    }

    @SneakyThrows
    @Override
    public boolean create(SparkApplication appParam) {
        ApiAlertException.throwIfNull(
            appParam.getTeamId(), "The teamId can't be null. Create application failed.");

        appParam.setUserId(ServiceHelper.getUserId());
        appParam.setState(SparkAppStateEnum.ADDED.getValue());
        appParam.setRelease(ReleaseStateEnum.NEED_RELEASE.get());
        appParam.setOptionState(OptionStateEnum.NONE.getValue());
        appParam.setCreateTime(new Date());
        appParam.setModifyTime(appParam.getCreateTime());

        boolean success = validateQueueIfNeeded(appParam);
        ApiAlertException.throwIfFalse(
            success,
            String.format(ERROR_APP_QUEUE_HINT, appParam.getYarnQueue(), appParam.getTeamId()));
        if (appParam.isSparkOnYarnJob()) {
            appParam.resolveYarnQueue();
            if (appParam.isSparkSqlJob()) {
                appParam.setMainClass(Constants.STREAMPARK_SPARKSQL_CLIENT_CLASS);
            }
        }
        if (appParam.isFromUploadJob()) {
            String jarPath = String.format(
                "%s/%d/%s", Workspace.local().APP_UPLOADS(), appParam.getTeamId(), appParam.getJar());
            if (!new File(jarPath).exists()) {
                Resource resource = resourceService.findByResourceName(appParam.getTeamId(), appParam.getJar());
                if (resource != null && StringUtils.isNotBlank(resource.getFilePath())) {
                    jarPath = resource.getFilePath();
                }
            }
            appParam.setJarCheckSum(org.apache.commons.io.FileUtils.checksumCRC32(new File(jarPath)));
        }

        boolean saveSuccess = save(appParam);

        if (saveSuccess) {
            if (appParam.isSparkSqlJob()) {
                SparkSql sparkSql = new SparkSql(appParam);
                sparkSqlService.create(sparkSql);
            }
            if (appParam.getConfig() != null) {
                configService.create(appParam, true);
            }
            return true;
        } else {
            throw new ApiAlertException("create application failed");
        }
    }

    private boolean existsByAppName(String jobName) {
        return this.lambdaQuery().eq(SparkApplication::getAppName, jobName).exists();
    }

    @SuppressWarnings("checkstyle:WhitespaceAround")
    @Override
    @SneakyThrows
    public Long copy(SparkApplication appParam) {
        boolean existsByAppName = this.existsByAppName(appParam.getAppName());
        ApiAlertException.throwIfFalse(
            !existsByAppName,
            "[StreamPark] Application names can't be repeated, copy application failed.");

        SparkApplication oldApp = getById(appParam.getId());
        SparkApplication newApp = new SparkApplication();

        newApp.setTeamId(oldApp.getTeamId());
        newApp.setJobType(oldApp.getJobType());
        newApp.setAppType(oldApp.getAppType());
        newApp.setVersionId(oldApp.getVersionId());
        newApp.setAppName(appParam.getAppName());
        newApp.setDeployMode(oldApp.getDeployMode());
        newApp.setResourceFrom(oldApp.getResourceFrom());
        newApp.setProjectId(oldApp.getProjectId());
        newApp.setModule(oldApp.getModule());
        newApp.setMainClass(oldApp.getMainClass());
        newApp.setJar(oldApp.getJar());
        newApp.setJarCheckSum(oldApp.getJarCheckSum());
        newApp.setAppProperties(oldApp.getAppProperties());
        newApp.setAppArgs(appParam.getAppArgs() != null ? appParam.getAppArgs() : oldApp.getAppArgs());
        newApp.setYarnQueue(oldApp.getYarnQueue());
        newApp.resolveYarnQueue();
        newApp.setK8sMasterUrl(oldApp.getK8sMasterUrl());
        newApp.setK8sContainerImage(oldApp.getK8sContainerImage());
        newApp.setK8sImagePullPolicy(oldApp.getK8sImagePullPolicy());
        newApp.setK8sServiceAccount(oldApp.getK8sServiceAccount());
        newApp.setK8sNamespace(oldApp.getK8sNamespace());
        newApp.setK8sDriverPodTemplate(oldApp.getK8sDriverPodTemplate());
        newApp.setK8sExecutorPodTemplate(oldApp.getK8sExecutorPodTemplate());
        newApp.setK8sHadoopIntegration(oldApp.getK8sHadoopIntegration());

        newApp.setHadoopUser(oldApp.getHadoopUser());
        newApp.setRestartSize(oldApp.getRestartSize());
        newApp.setState(SparkAppStateEnum.ADDED.getValue());
        newApp.setOptions(oldApp.getOptions());
        newApp.setOptionState(OptionStateEnum.NONE.getValue());
        newApp.setUserId(ServiceHelper.getUserId());
        newApp.setDescription(oldApp.getDescription());
        newApp.setRelease(ReleaseStateEnum.NEED_RELEASE.get());
        newApp.setAlertId(oldApp.getAlertId());
        newApp.setCreateTime(new Date());
        newApp.setModifyTime(newApp.getCreateTime());
        newApp.setTags(oldApp.getTags());

        boolean saved = save(newApp);
        if (saved) {
            if (newApp.isSparkSqlJob()) {
                SparkSql copySparkSql = sparkSqlService.getLatestSparkSql(appParam.getId(), true);
                newApp.setSparkSql(copySparkSql.getSql());
                newApp.setTeamResource(copySparkSql.getTeamResource());
                newApp.setDependency(copySparkSql.getDependency());
                SparkSql sparkSql = new SparkSql(newApp);
                sparkSqlService.create(sparkSql);
            }
            SparkApplicationConfig copyConfig = configService.getEffective(appParam.getId());
            if (copyConfig != null) {
                SparkApplicationConfig config = new SparkApplicationConfig();
                config.setAppId(newApp.getId());
                config.setFormat(copyConfig.getFormat());
                config.setContent(copyConfig.getContent());
                config.setCreateTime(new Date());
                config.setVersion(1);
                configService.save(config);
                configService.setLatestOrEffective(true, config.getId(), newApp.getId());
            }
            return newApp.getId();
        } else {
            throw new ApiAlertException(
                "create application from copy failed, copy source app: " + oldApp.getAppName());
        }
    }

    @Override
    public boolean update(SparkApplication appParam) {
        SparkApplication application = getById(appParam.getId());

        /* If the original mode is remote, k8s-session, yarn-session, check cluster status */
        SparkDeployMode sparkDeployMode = application.getDeployModeEnum();

        boolean success = validateQueueIfNeeded(application, appParam);
        ApiAlertException.throwIfFalse(
            success,
            String.format(ERROR_APP_QUEUE_HINT, appParam.getYarnQueue(), appParam.getTeamId()));

        application.setRelease(ReleaseStateEnum.NEED_RELEASE.get());

        // 1) jar job jar file changed
        if (application.isFromUploadJob()) {
            if (!Objects.equals(application.getJar(), appParam.getJar())) {
                application.setBuild(true);
            } else {
                File jarFile = new File(WebUtils.getAppTempDir(), appParam.getJar());
                if (jarFile.exists()) {
                    try {
                        long checkSum = org.apache.commons.io.FileUtils.checksumCRC32(jarFile);
                        if (!Objects.equals(checkSum, application.getJarCheckSum())) {
                            application.setBuild(true);
                        }
                    } catch (IOException e) {
                        log.error("Error in checksumCRC32 for {}.", jarFile);
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        // 3) flink version changed
        if (!application.getBuild()
            && !Objects.equals(application.getVersionId(), appParam.getVersionId())) {
            application.setBuild(true);
        }

        // 4) yarn application mode change
        if (!application.getBuild() && isYarnApplicationModeChange(application, appParam)) {
            application.setBuild(true);
        }

        appParam.setJobType(application.getJobType());
        // changes to the following parameters need to be re-release to take effect
        application.setVersionId(appParam.getVersionId());
        application.setAppName(appParam.getAppName());
        application.setDeployMode(appParam.getDeployMode());
        application.setAppProperties(appParam.getAppProperties());
        application.setAppArgs(appParam.getAppArgs());
        application.setOptions(appParam.getOptions());

        application.setYarnQueue(appParam.getYarnQueue());
        application.resolveYarnQueue();

        application.setK8sMasterUrl(appParam.getK8sMasterUrl());
        application.setK8sContainerImage(appParam.getK8sContainerImage());
        application.setK8sImagePullPolicy(appParam.getK8sImagePullPolicy());
        application.setK8sServiceAccount(appParam.getK8sServiceAccount());
        application.setK8sNamespace(appParam.getK8sNamespace());

        // changes to the following parameters do not affect running tasks
        application.setDescription(appParam.getDescription());
        application.setAlertId(appParam.getAlertId());
        application.setRestartSize(appParam.getRestartSize());
        application.setTags(appParam.getTags());

        switch (appParam.getDeployModeEnum()) {
            case YARN_CLUSTER:
            case YARN_CLIENT:
                application.setHadoopUser(appParam.getHadoopUser());
                break;
            default:
                break;
        }

        if (application.isSparkSqlJob()) {
            updateSparkSqlJob(application, appParam);
        } else if (application.isSparkJarJob()) {
            application.setJar(appParam.getJar());
            application.setMainClass(appParam.getMainClass());
        }

        // update config
        configService.update(appParam, application.isRunning());
        this.updateById(application);
        return true;
    }

    /**
     * update FlinkSql type jobs, there are 3 aspects to consider<br>
     * 1. flink sql has changed <br>
     * 2. dependency has changed<br>
     * 3. parameter has changed<br>
     *
     * @param application
     * @param appParam
     */
    private void updateSparkSqlJob(SparkApplication application, SparkApplication appParam) {
        SparkSql effectiveSparkSql = sparkSqlService.getEffective(application.getId(), true);
        if (effectiveSparkSql == null) {
            effectiveSparkSql = sparkSqlService.getCandidate(application.getId(), CandidateTypeEnum.NEW);
            sparkSqlService.removeById(effectiveSparkSql.getId());
            SparkSql sql = new SparkSql(appParam);
            sparkSqlService.create(sql);
            application.setBuild(true);
        } else {
            // get previous spark sql and decode
            SparkSql copySourceSparkSql = sparkSqlService.getById(appParam.getSqlId());
            ApiAlertException.throwIfNull(
                copySourceSparkSql, "Spark sql is null, update spark sql job failed.");
            copySourceSparkSql.decode();

            // get submit spark sql
            SparkSql targetSparkSql = new SparkSql(appParam);

            // judge sql and dependency has changed
            ChangeTypeEnum changeTypeEnum = copySourceSparkSql.checkChange(targetSparkSql);

            log.info("updateSparkSqlJob changeTypeEnum: {}", changeTypeEnum);

            // if has been changed
            if (changeTypeEnum.hasChanged()) {
                // check if there is a candidate version for the newly added record
                SparkSql newSparkSql = sparkSqlService.getCandidate(application.getId(), CandidateTypeEnum.NEW);
                // If the candidate version of the new record exists, it will be deleted directly,
                // and only one candidate version will be retained. If the new candidate version is not
                // effective,
                // if it is edited again and the next record comes in, the previous candidate version will
                // be deleted.
                if (newSparkSql != null) {
                    // delete all records about candidates
                    sparkSqlService.removeById(newSparkSql.getId());
                }
                SparkSql historySparkSql = sparkSqlService.getCandidate(application.getId(), CandidateTypeEnum.HISTORY);
                // remove candidate flags that already exist but are set as candidates
                if (historySparkSql != null) {
                    sparkSqlService.cleanCandidate(historySparkSql.getId());
                }
                SparkSql sql = new SparkSql(appParam);
                sparkSqlService.create(sql);
                if (changeTypeEnum.isDependencyChanged()) {
                    application.setBuild(true);
                }
            } else {
                // judge version has changed
                boolean versionChanged = !effectiveSparkSql.getId().equals(appParam.getSqlId());
                if (versionChanged) {
                    // sql and dependency not changed, but version changed, means that rollback to the version
                    CandidateTypeEnum type = CandidateTypeEnum.HISTORY;
                    sparkSqlService.setCandidate(type, appParam.getId(), appParam.getSqlId());
                    application.setRelease(ReleaseStateEnum.NEED_ROLLBACK.get());
                    application.setBuild(true);
                }
            }
        }
        this.updateById(application);
    }

    @Override
    public void updateRelease(SparkApplication appParam) {
        LambdaUpdateWrapper<SparkApplication> updateWrapper = Wrappers.lambdaUpdate();
        updateWrapper.eq(SparkApplication::getId, appParam.getId());
        updateWrapper.set(SparkApplication::getRelease, appParam.getRelease());
        updateWrapper.set(SparkApplication::getBuild, appParam.getBuild());
        if (appParam.getOptionState() != null) {
            updateWrapper.set(SparkApplication::getOptionState, appParam.getOptionState());
        }
        this.update(updateWrapper);
    }

    @Override
    public List<SparkApplication> listByProjectId(Long id) {
        return this.lambdaQuery().eq(SparkApplication::getProjectId, id).list();
    }

    @Override
    public List<SparkApplication> listByTeamId(Long teamId) {
        return baseMapper.selectAppsByTeamId(teamId);
    }

    @Override
    public List<SparkApplication> listByTeamIdAndDeployModes(
                                                             Long teamId,
                                                             Collection<SparkDeployMode> deployModeEnums) {

        return this.lambdaQuery().eq(SparkApplication::getTeamId, teamId)
            .in(
                SparkApplication::getDeployMode,
                deployModeEnums.stream()
                    .map(SparkDeployMode::getMode)
                    .collect(Collectors.toSet()))
            .list();
    }

    @Override
    public boolean checkBuildAndUpdate(SparkApplication appParam) {
        boolean build = appParam.getBuild();
        if (!build) {
            LambdaUpdateWrapper<SparkApplication> updateWrapper = Wrappers.lambdaUpdate();
            updateWrapper.eq(SparkApplication::getId, appParam.getId());
            if (appParam.isRunning()) {
                updateWrapper.set(SparkApplication::getRelease, ReleaseStateEnum.NEED_RESTART.get());
            } else {
                updateWrapper.set(SparkApplication::getRelease, ReleaseStateEnum.DONE.get());
                updateWrapper.set(SparkApplication::getOptionState, OptionStateEnum.NONE.getValue());
            }
            this.update(updateWrapper);

            // If the current task is not running, or the task has just been added,
            // directly set the candidate version to the official version
            SparkSql sparkSql = sparkSqlService.getEffective(appParam.getId(), false);
            if (!appParam.isRunning() || sparkSql == null) {
                this.toEffective(appParam);
            }
        }
        return build;
    }

    @Override
    public void clean(SparkApplication appParam) {
        appParam.setRelease(ReleaseStateEnum.DONE.get());
        this.updateRelease(appParam);
    }

    @Override
    public SparkApplication getApp(Long id) {
        SparkApplication application = this.baseMapper.selectApp(id);
        SparkApplicationConfig config = configService.getEffective(id);
        config = config == null ? configService.getLatest(id) : config;
        if (config != null) {
            config.setToApplication(application);
        }
        if (application.isSparkSqlJob()) {
            SparkSql sparkSql = sparkSqlService.getEffective(application.getId(), true);
            if (sparkSql == null) {
                sparkSql = sparkSqlService.getCandidate(application.getId(), CandidateTypeEnum.NEW);
                sparkSql.setSql(DeflaterUtils.unzipString(sparkSql.getSql()));
            }
            sparkSql.setToApplication(application);
        } else {
            if (application.isFromBuildJob()) {
                String path = this.projectService.getAppConfPath(application.getProjectId(), application.getModule());
                application.setConfPath(path);
            }
        }

        application.resolveYarnQueue();

        return application;
    }

    /**
     * Check queue label validation when create the application if needed.
     *
     * @param appParam the app to create.
     * @return <code>true</code> if validate it successfully, <code>false</code> else.
     */
    @VisibleForTesting
    public boolean validateQueueIfNeeded(SparkApplication appParam) {
        yarnQueueService.checkQueueLabel(appParam.getDeployModeEnum(), appParam.getYarnQueue());
        if (!isYarnNotDefaultQueue(appParam)) {
            return true;
        }
        return yarnQueueService.existByTeamIdQueueLabel(appParam.getTeamId(), appParam.getYarnQueue());
    }

    /**
     * Check queue label validation when update the application if needed.
     *
     * @param oldApp the old app to update.
     * @param newApp the new app payload.
     * @return <code>true</code> if validate it successfully, <code>false</code> else.
     */
    @VisibleForTesting
    public boolean validateQueueIfNeeded(SparkApplication oldApp, SparkApplication newApp) {
        yarnQueueService.checkQueueLabel(newApp.getDeployModeEnum(), newApp.getYarnQueue());
        if (!isYarnNotDefaultQueue(newApp)) {
            return true;
        }

        oldApp.resolveYarnQueue();
        if (SparkDeployMode.isYarnMode(newApp.getDeployModeEnum())
            && StringUtils.equals(oldApp.getYarnQueue(), newApp.getYarnQueue())) {
            return true;
        }
        return yarnQueueService.existByTeamIdQueueLabel(newApp.getTeamId(), newApp.getYarnQueue());
    }

    /**
     * Judge the execution mode whether is the Yarn PerJob or Application mode with not default or
     * empty queue label.
     *
     * @param application application entity.
     * @return If the deployMode is (Yarn PerJob or application mode) and the queue label is not
     *     (empty or default), return true, false else.
     */
    private boolean isYarnNotDefaultQueue(SparkApplication application) {
        return SparkDeployMode.isYarnMode(application.getDeployModeEnum())
            && !yarnQueueService.isDefaultQueue(application.getYarnQueue());
    }

    private boolean isYarnApplicationModeChange(
                                                SparkApplication application, SparkApplication appParam) {
        return !application.getDeployMode().equals(appParam.getDeployMode())
            && (SparkDeployMode.YARN_CLIENT == appParam.getDeployModeEnum()
                || SparkDeployMode.YARN_CLUSTER == application.getDeployModeEnum());
    }
}
