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

package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.common.conf.ConfigConst;
import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.enums.ApplicationType;
import org.apache.streampark.common.enums.DevelopmentMode;
import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.enums.FlinkK8sRestExposedType;
import org.apache.streampark.common.enums.ResolveOrder;
import org.apache.streampark.common.enums.StorageType;
import org.apache.streampark.common.fs.HdfsOperator;
import org.apache.streampark.common.fs.LfsOperator;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.common.util.HadoopUtils;
import org.apache.streampark.common.util.PropertiesUtils;
import org.apache.streampark.common.util.ThreadUtils;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.ApiDetailException;
import org.apache.streampark.console.base.exception.ApplicationException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.base.util.CommonUtils;
import org.apache.streampark.console.base.util.ObjectUtils;
import org.apache.streampark.console.base.util.WebUtils;
import org.apache.streampark.console.core.bean.AppControl;
import org.apache.streampark.console.core.bean.MavenDependency;
import org.apache.streampark.console.core.entity.AppBuildPipeline;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationConfig;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.entity.Savepoint;
import org.apache.streampark.console.core.enums.AppExistsState;
import org.apache.streampark.console.core.enums.CandidateType;
import org.apache.streampark.console.core.enums.ChangedType;
import org.apache.streampark.console.core.enums.CheckPointType;
import org.apache.streampark.console.core.enums.ConfigFileType;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.enums.Operation;
import org.apache.streampark.console.core.enums.OptionState;
import org.apache.streampark.console.core.enums.ReleaseState;
import org.apache.streampark.console.core.mapper.ApplicationMapper;
import org.apache.streampark.console.core.metrics.flink.JobsOverview;
import org.apache.streampark.console.core.runner.EnvInitializer;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationBackUpService;
import org.apache.streampark.console.core.service.ApplicationConfigService;
import org.apache.streampark.console.core.service.ApplicationLogService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.EffectiveService;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.core.service.ProjectService;
import org.apache.streampark.console.core.service.SavepointService;
import org.apache.streampark.console.core.service.ServiceHelper;
import org.apache.streampark.console.core.service.SettingService;
import org.apache.streampark.console.core.service.VariableService;
import org.apache.streampark.console.core.service.YarnQueueService;
import org.apache.streampark.console.core.task.CheckpointProcessor;
import org.apache.streampark.console.core.task.FlinkAppHttpWatcher;
import org.apache.streampark.console.core.task.FlinkK8sWatcherWrapper;
import org.apache.streampark.console.system.service.MemberService;
import org.apache.streampark.flink.client.FlinkClient;
import org.apache.streampark.flink.client.bean.CancelRequest;
import org.apache.streampark.flink.client.bean.CancelResponse;
import org.apache.streampark.flink.client.bean.SubmitRequest;
import org.apache.streampark.flink.client.bean.SubmitResponse;
import org.apache.streampark.flink.core.conf.ParameterCli;
import org.apache.streampark.flink.kubernetes.FlinkK8sWatcher;
import org.apache.streampark.flink.kubernetes.helper.KubernetesDeploymentHelper;
import org.apache.streampark.flink.kubernetes.model.FlinkMetricCV;
import org.apache.streampark.flink.kubernetes.model.TrackId;
import org.apache.streampark.flink.packer.pipeline.BuildResult;
import org.apache.streampark.flink.packer.pipeline.PipelineStatus;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.streampark.common.enums.StorageType.LFS;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class ApplicationServiceImpl extends ServiceImpl<ApplicationMapper, Application>
    implements ApplicationService {

  private static final String ERROR_APP_QUEUE_HINT =
      "Queue label '%s' isn't available for teamId '%d', please add it into the team first.";

  private static final int DEFAULT_HISTORY_RECORD_LIMIT = 25;

  private static final int DEFAULT_HISTORY_POD_TMPL_RECORD_LIMIT = 5;

  private static final Pattern JOB_NAME_PATTERN =
      Pattern.compile("^[.\\x{4e00}-\\x{9fa5}A-Za-z\\d_\\-\\s]+$");

  private static final Pattern SINGLE_SPACE_PATTERN = Pattern.compile("^\\S+(\\s\\S+)*$");

  @Autowired private ProjectService projectService;

  @Autowired private ApplicationBackUpService backUpService;

  @Autowired private ApplicationConfigService configService;

  @Autowired private ApplicationLogService applicationLogService;

  @Autowired private FlinkEnvService flinkEnvService;

  @Autowired private FlinkSqlService flinkSqlService;

  @Autowired private SavepointService savepointService;

  @Autowired private EffectiveService effectiveService;

  @Autowired private SettingService settingService;

  @Autowired private ServiceHelper serviceHelper;

  @Autowired private EnvInitializer envInitializer;

  @Autowired private FlinkK8sWatcher flinkK8sWatcher;

  @Autowired private AppBuildPipeService appBuildPipeService;

  @Autowired private FlinkClusterService flinkClusterService;

  @Autowired private VariableService variableService;

  @Autowired private YarnQueueService yarnQueueService;

  @Autowired private FlinkK8sWatcherWrapper k8sWatcherWrapper;

  @Autowired private CheckpointProcessor checkpointProcessor;

  @Autowired private MemberService memberService;

  private static final int CPU_NUM = Math.max(2, Runtime.getRuntime().availableProcessors() * 4);

  private final ExecutorService bootstrapExecutor =
      new ThreadPoolExecutor(
          CPU_NUM,
          CPU_NUM * 5,
          60L,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(),
          ThreadUtils.threadFactory("streampark-flink-app-bootstrap"));

  @PostConstruct
  public void resetOptionState() {
    this.baseMapper.resetOptionState();
  }

  @PreDestroy
  public void shutdown() {
    bootstrapExecutor.shutdown();
  }

  private final Map<Long, CompletableFuture<SubmitResponse>> startFutureMap =
      new ConcurrentHashMap<>();

  private final Map<Long, CompletableFuture<CancelResponse>> cancelFutureMap =
      new ConcurrentHashMap<>();

  @Override
  public Map<String, Serializable> dashboard(Long teamId) {
    JobsOverview.Task overview = new JobsOverview.Task();
    Integer totalJmMemory = 0;
    Integer totalTmMemory = 0;
    Integer totalTm = 0;
    Integer totalSlot = 0;
    Integer availableSlot = 0;
    Integer runningJob = 0;

    // stat metrics from other than kubernetes mode
    for (Application app : FlinkAppHttpWatcher.getWatchingApps()) {
      if (!teamId.equals(app.getTeamId())) {
        continue;
      }
      // 1) only yarn-application, yarn-perjob mode
      if (app.getJmMemory() != null) {
        totalJmMemory += app.getJmMemory();
      }
      if (app.getTmMemory() != null) {
        totalTmMemory += app.getTmMemory() * (app.getTotalTM() == null ? 1 : app.getTotalTM());
      }
      if (app.getTotalTM() != null) {
        totalTm += app.getTotalTM();
      }
      if (app.getTotalSlot() != null) {
        totalSlot += app.getTotalSlot();
      }
      if (app.getAvailableSlot() != null) {
        availableSlot += app.getAvailableSlot();
      }
      if (app.getState() == FlinkAppState.RUNNING.getValue()) {
        runningJob++;
      }
      JobsOverview.Task task = app.getOverview();
      if (task != null) {
        overview.setTotal(overview.getTotal() + task.getTotal());
        overview.setCreated(overview.getCreated() + task.getCreated());
        overview.setScheduled(overview.getScheduled() + task.getScheduled());
        overview.setDeploying(overview.getDeploying() + task.getDeploying());
        overview.setRunning(overview.getRunning() + task.getRunning());
        overview.setFinished(overview.getFinished() + task.getFinished());
        overview.setCanceling(overview.getCanceling() + task.getCanceling());
        overview.setCanceled(overview.getCanceled() + task.getCanceled());
        overview.setFailed(overview.getFailed() + task.getFailed());
        overview.setReconciling(overview.getReconciling() + task.getReconciling());
      }
    }

    // merge metrics from flink kubernetes cluster
    FlinkMetricCV k8sMetric = flinkK8sWatcher.getAccGroupMetrics(teamId.toString());
    if (k8sMetric != null) {
      totalJmMemory += k8sMetric.totalJmMemory();
      totalTmMemory += k8sMetric.totalTmMemory();
      totalTm += k8sMetric.totalTm();
      totalSlot += k8sMetric.totalSlot();
      availableSlot += k8sMetric.availableSlot();
      runningJob += k8sMetric.runningJob();
      overview.setTotal(overview.getTotal() + k8sMetric.totalJob());
      overview.setRunning(overview.getRunning() + k8sMetric.runningJob());
      overview.setFinished(overview.getFinished() + k8sMetric.finishedJob());
      overview.setCanceled(overview.getCanceled() + k8sMetric.cancelledJob());
      overview.setFailed(overview.getFailed() + k8sMetric.failedJob());
    }

    // result json
    Map<String, Serializable> map = new HashMap<>(8);
    map.put("task", overview);
    map.put("jmMemory", totalJmMemory);
    map.put("tmMemory", totalTmMemory);
    map.put("totalTM", totalTm);
    map.put("availableSlot", availableSlot);
    map.put("totalSlot", totalSlot);
    map.put("runningJob", runningJob);

    return map;
  }

  @Override
  public String upload(MultipartFile file) throws Exception {
    File temp = WebUtils.getAppTempDir();
    String fileName = FilenameUtils.getName(Objects.requireNonNull(file.getOriginalFilename()));
    File saveFile = new File(temp, fileName);
    // delete when exists
    if (saveFile.exists()) {
      saveFile.delete();
    }
    // save file to temp dir
    try {
      file.transferTo(saveFile);
    } catch (Exception e) {
      log.error("Upload file {} failed!", fileName, e);
      throw new ApiDetailException(e);
    }
    return saveFile.getAbsolutePath();
  }

  @Override
  public void toEffective(Application application) {
    // set latest to Effective
    ApplicationConfig config = configService.getLatest(application.getId());
    if (config != null) {
      this.configService.toEffective(application.getId(), config.getId());
    }
    if (application.isFlinkSqlJob()) {
      FlinkSql flinkSql = flinkSqlService.getCandidate(application.getId(), null);
      if (flinkSql != null) {
        flinkSqlService.toEffective(application.getId(), flinkSql.getId());
        // clean candidate
        flinkSqlService.cleanCandidate(flinkSql.getId());
      }
    }
  }

  @Override
  public void revoke(Application appParma) throws ApplicationException {
    Application application = getById(appParma.getId());
    ApiAlertException.throwIfNull(
        application,
        String.format("The application id=%s not found, revoke failed.", appParma.getId()));

    // 1) delete files that have been published to workspace
    application.getFsOperator().delete(application.getAppHome());

    // 2) rollback the files to the workspace
    backUpService.revoke(application);

    // 3) restore related status
    LambdaUpdateWrapper<Application> updateWrapper = Wrappers.lambdaUpdate();
    updateWrapper.eq(Application::getId, application.getId());
    if (application.isFlinkSqlJob()) {
      updateWrapper.set(Application::getRelease, ReleaseState.FAILED.get());
    } else {
      updateWrapper.set(Application::getRelease, ReleaseState.NEED_RELEASE.get());
    }
    if (!application.isRunning()) {
      updateWrapper.set(Application::getState, FlinkAppState.REVOKED.getValue());
    }
    baseMapper.update(null, updateWrapper);
  }

  @Override
  @Transactional(rollbackFor = {Exception.class})
  public Boolean delete(Application paramApp) {

    Application application = getById(paramApp.getId());

    // 1) remove flink sql
    flinkSqlService.removeApp(application.getId());

    // 2) remove log
    applicationLogService.removeApp(application.getId());

    // 3) remove config
    configService.removeApp(application.getId());

    // 4) remove effective
    effectiveService.removeApp(application.getId());

    // remove related hdfs
    // 5) remove backup
    backUpService.removeApp(application);

    // 6) remove savepoint
    savepointService.removeApp(application);

    // 7) remove BuildPipeline
    appBuildPipeService.removeApp(application.getId());

    // 8) remove app
    removeApp(application);

    return true;
  }

  @Override
  public void restart(Application application) throws Exception {
    this.cancel(application);
    this.start(application, false);
  }

  @Override
  public boolean checkEnv(Application appParam) throws ApplicationException {
    Application application = getById(appParam.getId());
    try {
      FlinkEnv flinkEnv;
      if (application.getVersionId() != null) {
        flinkEnv = flinkEnvService.getByIdOrDefault(application.getVersionId());
      } else {
        flinkEnv = flinkEnvService.getDefault();
      }
      if (flinkEnv == null) {
        return false;
      }
      envInitializer.checkFlinkEnv(application.getStorageType(), flinkEnv);
      envInitializer.storageInitialize(application.getStorageType());

      if (ExecutionMode.YARN_SESSION.equals(application.getExecutionModeEnum())
          || ExecutionMode.REMOTE.equals(application.getExecutionModeEnum())) {
        FlinkCluster flinkCluster = flinkClusterService.getById(application.getFlinkClusterId());
        boolean conned = flinkCluster.verifyClusterConnection();
        if (!conned) {
          throw new ApiAlertException("the target cluster is unavailable, please check!");
        }
      }
      return true;
    } catch (Exception e) {
      log.error(Utils.stringifyException(e));
      throw new ApiDetailException(e);
    }
  }

  @Override
  public boolean checkAlter(Application application) {
    Long appId = application.getId();
    FlinkAppState state = application.getFlinkAppStateEnum();
    if (!FlinkAppState.CANCELED.equals(state)) {
      return false;
    }
    long cancelUserId = FlinkAppHttpWatcher.getCanceledJobUserId(appId);
    long appUserId = application.getUserId();
    return cancelUserId != -1 && cancelUserId != appUserId;
  }

  @Override
  public Map<String, String> getRumtimeConfig(Long id) {
    Application application = getById(id);
    if (application != null && application.getVersionId() != null) {
      FlinkEnv flinkEnv = flinkEnvService.getByIdOrDefault(application.getVersionId());
      if (flinkEnv != null) {
        File yaml = new File(flinkEnv.getFlinkHome().concat("/conf/flink-conf.yaml"));
        Map<String, String> config = PropertiesUtils.loadFlinkConfYaml(yaml);
        Map<String, String> dynamicConf =
            PropertiesUtils.extractDynamicPropertiesAsJava(application.getDynamicProperties());
        config.putAll(dynamicConf);
        return config;
      }
    }
    return Collections.emptyMap();
  }

  private void removeApp(Application application) {
    Long appId = application.getId();
    removeById(appId);
    try {
      application
          .getFsOperator()
          .delete(application.getWorkspace().APP_WORKSPACE().concat("/").concat(appId.toString()));
      // try to delete yarn-application, and leave no trouble.
      String path =
          Workspace.of(StorageType.HDFS).APP_WORKSPACE().concat("/").concat(appId.toString());
      if (HdfsOperator.exists(path)) {
        HdfsOperator.delete(path);
      }
    } catch (Exception e) {
      log.error("Remove application {} failed!", appId, e);
    }
  }

  @Override
  public IPage<Application> page(Application appParam, RestRequest request) {
    if (appParam.getTeamId() == null) {
      return null;
    }
    Page<Application> page = MybatisPager.getPage(request);
    if (CommonUtils.notEmpty((Object) appParam.getStateArray())) {
      if (Arrays.stream(appParam.getStateArray())
          .anyMatch(x -> x == FlinkAppState.FINISHED.getValue())) {
        Integer[] newArray =
            CommonUtils.arrayInsertIndex(
                appParam.getStateArray(),
                appParam.getStateArray().length,
                FlinkAppState.POS_TERMINATED.getValue());
        appParam.setStateArray(newArray);
      }
    }
    this.baseMapper.page(page, appParam);

    List<Application> records = page.getRecords();
    long now = System.currentTimeMillis();

    List<Long> appIds = records.stream().map(Application::getId).collect(Collectors.toList());
    Map<Long, PipelineStatus> pipeStates = appBuildPipeService.listPipelineStatus(appIds);

    // add building pipeline status info and app control info
    records.forEach(
        record -> {
          // 1) running Duration
          if (record.getTracking() == 1) {
            FlinkAppState state = record.getFlinkAppStateEnum();
            if (state == FlinkAppState.RUNNING
                || state == FlinkAppState.CANCELLING
                || state == FlinkAppState.MAPPING) {
              record.setDuration(now - record.getStartTime().getTime());
            }
            // 2) k8s restURL
            if (record.isKubernetesModeJob()) {
              // set duration
              String restUrl =
                  flinkK8sWatcher.getRemoteRestUrl(k8sWatcherWrapper.toTrackId(record));
              record.setFlinkRestUrl(restUrl);
            }
          }

          // 3) buildStatus
          if (pipeStates.containsKey(record.getId())) {
            record.setBuildStatus(pipeStates.get(record.getId()).getCode());
          }

          // 4) appControl
          record.setAppControl(buildAppControl(record));
        });

    return page;
  }

  @Override
  public boolean existsByTeamId(Long teamId) {
    return baseMapper.existsByTeamId(teamId);
  }

  @Override
  public boolean existsRunningJobByClusterId(Long clusterId) {
    boolean exists = baseMapper.existsRunningJobByClusterId(clusterId);
    if (exists) {
      return true;
    }
    for (Application application : FlinkAppHttpWatcher.getWatchingApps()) {
      if (clusterId.equals(application.getFlinkClusterId())
          && FlinkAppState.RUNNING.equals(application.getFlinkAppStateEnum())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean existsJobByClusterId(Long clusterId) {
    return baseMapper.existsJobByClusterId(clusterId);
  }

  @Override
  public boolean existsJobByFlinkEnvId(Long flinkEnvId) {
    LambdaQueryWrapper<Application> lambdaQueryWrapper =
        new LambdaQueryWrapper<Application>().eq(Application::getVersionId, flinkEnvId);
    return getBaseMapper().exists(lambdaQueryWrapper);
  }

  @Override
  public List<String> getRecentK8sNamespace() {
    return baseMapper.getRecentK8sNamespace(DEFAULT_HISTORY_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentK8sClusterId() {
    return baseMapper.getRecentK8sClusterId(DEFAULT_HISTORY_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentFlinkBaseImage() {
    return baseMapper.getRecentFlinkBaseImage(DEFAULT_HISTORY_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentK8sPodTemplate() {
    return baseMapper.getRecentK8sPodTemplate(DEFAULT_HISTORY_POD_TMPL_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentK8sJmPodTemplate() {
    return baseMapper.getRecentK8sJmPodTemplate(DEFAULT_HISTORY_POD_TMPL_RECORD_LIMIT);
  }

  @Override
  public List<String> getRecentK8sTmPodTemplate() {
    return baseMapper.getRecentK8sTmPodTemplate(DEFAULT_HISTORY_POD_TMPL_RECORD_LIMIT);
  }

  @Override
  public List<String> historyUploadJars() {
    return Arrays.stream(LfsOperator.listDir(Workspace.of(LFS).APP_UPLOADS()))
        .filter(File::isFile)
        .sorted(Comparator.comparingLong(File::lastModified).reversed())
        .map(File::getName)
        .filter(fn -> fn.endsWith(".jar"))
        .limit(DEFAULT_HISTORY_RECORD_LIMIT)
        .collect(Collectors.toList());
  }

  @Override
  public String k8sStartLog(Long id, Integer offset, Integer limit) throws Exception {
    Application application = getById(id);
    ApiAlertException.throwIfNull(
        application, String.format("The application id=%s can't be found.", id));
    if (ExecutionMode.isKubernetesMode(application.getExecutionModeEnum())) {
      CompletableFuture<String> future =
          CompletableFuture.supplyAsync(
              () ->
                  KubernetesDeploymentHelper.watchDeploymentLog(
                      application.getK8sNamespace(),
                      application.getJobName(),
                      application.getJobId()));

      return future
          .exceptionally(
              e -> {
                String errorLog =
                    String.format(
                        "%s/%s_err.log",
                        WebUtils.getAppTempDir().getAbsolutePath(), application.getJobId());
                File file = new File(errorLog);
                if (file.exists() && file.isFile()) {
                  return file.getAbsolutePath();
                }
                return null;
              })
          .thenApply(
              path -> {
                if (!future.isDone()) {
                  future.cancel(true);
                }
                if (path != null) {
                  return serviceHelper.rollViewLog(path, offset, limit);
                }
                return null;
              })
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
    } else {
      log.error("Job executionMode must be kubernetes-session|kubernetes-application.");
      throw new ApiAlertException(
          "Job executionMode must be kubernetes-session|kubernetes-application.");
    }
  }

  @Override
  public AppExistsState checkStart(Application appParam) {
    Application application = getById(appParam.getId());
    if (application == null) {
      return AppExistsState.INVALID;
    }
    if (ExecutionMode.isYarnMode(application.getExecutionMode())) {
      boolean exists = !getYARNApplication(application.getJobName()).isEmpty();
      return exists ? AppExistsState.IN_YARN : AppExistsState.NO;
    }
    // todo on k8s check...
    return AppExistsState.NO;
  }

  @Override
  public String getYarnName(Application appParam) throws IOException {
    File file = getReadableConfFile(appParam);
    String[] args = new String[2];
    args[0] = "--name";
    args[1] = file.getAbsolutePath();
    return ParameterCli.read(args);
  }

  /** Check if the current jobName and other key identifiers already exist in db and yarn/k8s */
  @Override
  public AppExistsState checkExists(Application appParam) {

    if (!checkJobName(appParam.getJobName())) {
      return AppExistsState.INVALID;
    }

    boolean existsByJobName = this.existsByJobName(appParam.getJobName());

    if (appParam.getId() != null) {
      Application app = getById(appParam.getId());
      if (app.getJobName().equals(appParam.getJobName())) {
        return AppExistsState.NO;
      }

      if (existsByJobName) {
        return AppExistsState.IN_DB;
      }

      FlinkAppState state = app.getFlinkAppStateEnum();
      // has stopped status
      if (state.equals(FlinkAppState.ADDED)
          || state.equals(FlinkAppState.CREATED)
          || state.equals(FlinkAppState.FAILED)
          || state.equals(FlinkAppState.CANCELED)
          || state.equals(FlinkAppState.LOST)
          || state.equals(FlinkAppState.KILLED)) {
        // check whether jobName exists on yarn
        if (ExecutionMode.isYarnMode(appParam.getExecutionMode())
            && YarnUtils.isContains(appParam.getJobName())) {
          return AppExistsState.IN_YARN;
        }
        // check whether clusterId, namespace, jobId on kubernetes
        else if (ExecutionMode.isKubernetesMode(appParam.getExecutionMode())
            && flinkK8sWatcher.checkIsInRemoteCluster(k8sWatcherWrapper.toTrackId(app))) {
          return AppExistsState.IN_KUBERNETES;
        }
      }
    } else {
      if (existsByJobName) {
        return AppExistsState.IN_DB;
      }

      // check whether jobName exists on yarn
      if (ExecutionMode.isYarnMode(appParam.getExecutionMode())
          && YarnUtils.isContains(appParam.getJobName())) {
        return AppExistsState.IN_YARN;
      }
      // check whether clusterId, namespace, jobId on kubernetes
      else if (ExecutionMode.isKubernetesMode(appParam.getExecutionMode())
          && flinkK8sWatcher.checkIsInRemoteCluster(k8sWatcherWrapper.toTrackId(appParam))) {
        return AppExistsState.IN_KUBERNETES;
      }
    }
    return AppExistsState.NO;
  }

  @SneakyThrows
  @Override
  @Transactional(rollbackFor = {Exception.class})
  public boolean create(Application appParam) {
    ApiAlertException.throwIfNull(
        appParam.getTeamId(), "The teamId can't be null. Create application failed.");

    appParam.setBuild(true);
    appParam.setUserId(serviceHelper.getUserId());
    appParam.setState(FlinkAppState.ADDED.getValue());
    appParam.setRelease(ReleaseState.NEED_RELEASE.get());
    appParam.setOptionState(OptionState.NONE.getValue());

    // createTime & modifyTime
    Date date = new Date();
    appParam.setCreateTime(date);
    appParam.setModifyTime(date);

    appParam.setDefaultModeIngress(settingService.getIngressModeDefault());

    boolean success = validateQueueIfNeeded(appParam);
    ApiAlertException.throwIfFalse(
        success,
        String.format(ERROR_APP_QUEUE_HINT, appParam.getYarnQueue(), appParam.getTeamId()));

    appParam.doSetHotParams();
    if (appParam.isUploadJob()) {
      String jarPath =
          WebUtils.getAppTempDir().getAbsolutePath().concat("/").concat(appParam.getJar());
      appParam.setJarCheckSum(FileUtils.checksumCRC32(new File(jarPath)));
    }

    if (save(appParam)) {
      if (appParam.isFlinkSqlJob()) {
        FlinkSql flinkSql = new FlinkSql(appParam);
        flinkSqlService.create(flinkSql);
      }
      if (appParam.getConfig() != null) {
        configService.create(appParam, true);
      }
      return true;
    } else {
      log.error("Create application failed!");
      throw new ApiAlertException("create application failed");
    }
  }

  @Override
  public boolean save(Application entity) {
    String dependency = entity.getDependency();
    if (entity.isFlinkSqlJob()) {
      entity.setDependency(null);
    }
    boolean flag = super.save(entity);
    entity.setDependency(dependency);
    return flag;
  }

  private boolean existsByJobName(String jobName) {
    return this.baseMapper.existsByJobName(jobName);
  }

  @SuppressWarnings("checkstyle:WhitespaceAround")
  @Override
  @SneakyThrows
  @Transactional(rollbackFor = {Exception.class})
  public Long copy(Application appParam) {
    boolean existsByJobName = this.existsByJobName(appParam.getJobName());
    ApiAlertException.throwIfFalse(
        !existsByJobName,
        "[StreamPark] Application names can't be repeated, copy application failed.");

    Application persist = getById(appParam.getId());
    Application newApp = new Application();
    String jobName = appParam.getJobName();

    newApp.setJobName(jobName);
    newApp.setClusterId(
        ExecutionMode.isSessionMode(persist.getExecutionModeEnum())
            ? persist.getClusterId()
            : null);
    newApp.setArgs(appParam.getArgs() != null ? appParam.getArgs() : persist.getArgs());
    newApp.setVersionId(persist.getVersionId());

    newApp.setFlinkClusterId(persist.getFlinkClusterId());
    newApp.setRestartSize(persist.getRestartSize());
    newApp.setJobType(persist.getJobType());
    newApp.setOptions(persist.getOptions());
    newApp.setDynamicProperties(persist.getDynamicProperties());
    newApp.setResolveOrder(persist.getResolveOrder());
    newApp.setExecutionMode(persist.getExecutionMode());
    newApp.setFlinkImage(persist.getFlinkImage());
    newApp.setK8sNamespace(persist.getK8sNamespace());
    newApp.setK8sRestExposedType(persist.getK8sRestExposedType());
    newApp.setK8sPodTemplate(persist.getK8sPodTemplate());
    newApp.setK8sJmPodTemplate(persist.getK8sJmPodTemplate());
    newApp.setK8sTmPodTemplate(persist.getK8sTmPodTemplate());
    newApp.setK8sHadoopIntegration(persist.getK8sHadoopIntegration());
    newApp.setDescription(persist.getDescription());
    newApp.setAlertId(persist.getAlertId());
    newApp.setCpFailureAction(persist.getCpFailureAction());
    newApp.setCpFailureRateInterval(persist.getCpFailureRateInterval());
    newApp.setCpMaxFailureInterval(persist.getCpMaxFailureInterval());
    newApp.setMainClass(persist.getMainClass());
    newApp.setAppType(persist.getAppType());
    newApp.setResourceFrom(persist.getResourceFrom());
    newApp.setProjectId(persist.getProjectId());
    newApp.setModule(persist.getModule());
    newApp.setUserId(serviceHelper.getUserId());
    newApp.setState(FlinkAppState.ADDED.getValue());
    newApp.setRelease(ReleaseState.NEED_RELEASE.get());
    newApp.setOptionState(OptionState.NONE.getValue());
    newApp.setHotParams(persist.getHotParams());

    // createTime & modifyTime
    Date date = new Date();
    newApp.setCreateTime(date);
    newApp.setModifyTime(date);

    newApp.setJar(persist.getJar());
    newApp.setJarCheckSum(persist.getJarCheckSum());
    newApp.setTags(persist.getTags());
    newApp.setTeamId(persist.getTeamId());
    newApp.setDependency(persist.getDependency());

    boolean saved = save(newApp);
    if (saved) {
      if (newApp.isFlinkSqlJob()) {
        FlinkSql copyFlinkSql = flinkSqlService.getLatestFlinkSql(appParam.getId(), true);
        newApp.setFlinkSql(copyFlinkSql.getSql());
        newApp.setDependency(copyFlinkSql.getDependency());
        FlinkSql flinkSql = new FlinkSql(newApp);
        flinkSqlService.create(flinkSql);
      }
      ApplicationConfig copyConfig = configService.getEffective(appParam.getId());
      if (copyConfig != null) {
        ApplicationConfig config = new ApplicationConfig();
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
          "create application from copy failed, copy source app: " + persist.getJobName());
    }
  }

  @Override
  @Transactional(rollbackFor = {Exception.class})
  public boolean update(Application appParam) {
    Application application = getById(appParam.getId());

    boolean success = validateQueueIfNeeded(application, appParam);
    ApiAlertException.throwIfFalse(
        success,
        String.format(ERROR_APP_QUEUE_HINT, appParam.getYarnQueue(), appParam.getTeamId()));

    application.setRelease(ReleaseState.NEED_RELEASE.get());

    if (application.isApacheFlinkCustomCodeJob()) {
      MavenDependency thisDependency = MavenDependency.of(appParam.getDependency());
      MavenDependency targetDependency = MavenDependency.of(application.getDependency());

      if (!thisDependency.equals(targetDependency)) {
        application.setDependency(appParam.getDependency());
        application.setBuild(true);
      } else if (!ObjectUtils.safeEquals(application.getJar(), appParam.getJar())) {
        application.setBuild(true);
      } else {
        File jarFile = new File(WebUtils.getAppTempDir(), appParam.getJar());
        if (jarFile.exists()) {
          long checkSum = 0;
          try {
            checkSum = FileUtils.checksumCRC32(jarFile);
          } catch (IOException e) {
            log.error("Error in checksumCRC32 for {}.", jarFile);
            throw new RuntimeException(e);
          }
          if (!ObjectUtils.safeEquals(checkSum, application.getJarCheckSum())) {
            application.setBuild(true);
          }
        }
      }
    }

    if (!application.getBuild()) {
      if (!application.getExecutionMode().equals(appParam.getExecutionMode())) {
        if (appParam.getExecutionModeEnum().equals(ExecutionMode.YARN_APPLICATION)
            || application.getExecutionModeEnum().equals(ExecutionMode.YARN_APPLICATION)) {
          application.setBuild(true);
        }
      }
    }

    if (ExecutionMode.isKubernetesMode(appParam.getExecutionMode())) {
      if (!ObjectUtils.safeTrimEquals(
              application.getK8sRestExposedType(), appParam.getK8sRestExposedType())
          || !ObjectUtils.safeTrimEquals(
              application.getK8sJmPodTemplate(), appParam.getK8sJmPodTemplate())
          || !ObjectUtils.safeTrimEquals(
              application.getK8sTmPodTemplate(), appParam.getK8sTmPodTemplate())
          || !ObjectUtils.safeTrimEquals(
              application.getK8sPodTemplates(), appParam.getK8sPodTemplates())
          || !ObjectUtils.safeTrimEquals(
              application.getK8sHadoopIntegration(), appParam.getK8sHadoopIntegration())
          || !ObjectUtils.safeTrimEquals(application.getFlinkImage(), appParam.getFlinkImage())) {
        application.setBuild(true);
      }
    }

    // when flink version has changed, we should rebuild the application. Otherwise, the shims jar
    // may be not suitable for the new flink version.
    if (!ObjectUtils.safeEquals(application.getVersionId(), appParam.getVersionId())) {
      application.setBuild(true);
    }

    appParam.setJobType(application.getJobType());
    // changes to the following parameters need to be re-release to take effect
    application.setJobName(appParam.getJobName());
    application.setVersionId(appParam.getVersionId());
    application.setArgs(appParam.getArgs());
    application.setOptions(appParam.getOptions());
    application.setDynamicProperties(appParam.getDynamicProperties());
    application.setResolveOrder(appParam.getResolveOrder());
    application.setExecutionMode(appParam.getExecutionMode());
    application.setFlinkImage(appParam.getFlinkImage());
    application.updateHotParams(appParam);
    application.setK8sRestExposedType(appParam.getK8sRestExposedType());
    application.setK8sPodTemplate(appParam.getK8sPodTemplate());
    application.setK8sJmPodTemplate(appParam.getK8sJmPodTemplate());
    application.setK8sTmPodTemplate(appParam.getK8sTmPodTemplate());
    application.setK8sHadoopIntegration(appParam.getK8sHadoopIntegration());

    // changes to the following parameters do not affect running tasks
    application.setModifyTime(new Date());
    application.setDescription(appParam.getDescription());
    application.setAlertId(appParam.getAlertId());
    application.setRestartSize(appParam.getRestartSize());
    application.setCpFailureAction(appParam.getCpFailureAction());
    application.setCpFailureRateInterval(appParam.getCpFailureRateInterval());
    application.setCpMaxFailureInterval(appParam.getCpMaxFailureInterval());
    application.setTags(appParam.getTags());

    switch (appParam.getExecutionModeEnum()) {
      case YARN_APPLICATION:
      case YARN_PER_JOB:
      case KUBERNETES_NATIVE_APPLICATION:
        application.setFlinkClusterId(null);
        if (appParam.getExecutionModeEnum() == ExecutionMode.KUBERNETES_NATIVE_APPLICATION) {
          application.setK8sNamespace(appParam.getK8sNamespace());
          application.setServiceAccount(appParam.getServiceAccount());
          application.doSetHotParams();
        }
        break;
      case REMOTE:
      case YARN_SESSION:
      case KUBERNETES_NATIVE_SESSION:
        application.setFlinkClusterId(appParam.getFlinkClusterId());
        break;
      default:
        break;
    }

    // Flink Sql job...
    if (application.isFlinkSqlJob()) {
      updateFlinkSqlJob(application, appParam);
    } else {
      if (application.isStreamParkJob()) {
        configService.update(appParam, application.isRunning());
      } else {
        application.setJar(appParam.getJar());
        application.setMainClass(appParam.getMainClass());
      }
    }
    baseMapper.updateById(application);
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
  private void updateFlinkSqlJob(Application application, Application appParam) {
    FlinkSql effectiveFlinkSql = flinkSqlService.getEffective(application.getId(), true);
    if (effectiveFlinkSql == null) {
      effectiveFlinkSql = flinkSqlService.getCandidate(application.getId(), CandidateType.NEW);
      flinkSqlService.removeById(effectiveFlinkSql.getId());
      FlinkSql sql = new FlinkSql(appParam);
      flinkSqlService.create(sql);
      application.setBuild(true);
    } else {
      // get previous flink sql and decode
      FlinkSql copySourceFlinkSql = flinkSqlService.getById(appParam.getSqlId());
      ApiAlertException.throwIfNull(
          copySourceFlinkSql, "Flink sql is null, update flink sql job failed.");
      copySourceFlinkSql.decode();

      // get submit flink sql
      FlinkSql targetFlinkSql = new FlinkSql(appParam);

      // judge sql and dependency has changed
      ChangedType changedType = copySourceFlinkSql.checkChange(targetFlinkSql);

      log.info("updateFlinkSqlJob changedType: {}", changedType);

      // if has been changed
      if (changedType.hasChanged()) {
        // check if there is a candidate version for the newly added record
        FlinkSql newFlinkSql = flinkSqlService.getCandidate(application.getId(), CandidateType.NEW);
        // If the candidate version of the new record exists, it will be deleted directly,
        // and only one candidate version will be retained. If the new candidate version is not
        // effective,
        // if it is edited again and the next record comes in, the previous candidate version will
        // be deleted.
        if (newFlinkSql != null) {
          // delete all records about candidates
          flinkSqlService.removeById(newFlinkSql.getId());
        }
        FlinkSql historyFlinkSql =
            flinkSqlService.getCandidate(application.getId(), CandidateType.HISTORY);
        // remove candidate flags that already exist but are set as candidates
        if (historyFlinkSql != null) {
          flinkSqlService.cleanCandidate(historyFlinkSql.getId());
        }
        FlinkSql sql = new FlinkSql(appParam);
        flinkSqlService.create(sql);
        if (changedType.isDependencyChanged()) {
          application.setBuild(true);
        }
      } else {
        // judge version has changed
        boolean versionChanged = !effectiveFlinkSql.getId().equals(appParam.getSqlId());
        if (versionChanged) {
          // sql and dependency not changed, but version changed, means that rollback to the version
          CandidateType type = CandidateType.HISTORY;
          flinkSqlService.setCandidate(type, appParam.getId(), appParam.getSqlId());
          application.setRelease(ReleaseState.NEED_ROLLBACK.get());
          application.setBuild(true);
        }
      }
    }
    this.configService.update(appParam, application.isRunning());
  }

  @Override
  public void updateRelease(Application application) {
    LambdaUpdateWrapper<Application> updateWrapper = Wrappers.lambdaUpdate();
    updateWrapper.eq(Application::getId, application.getId());
    updateWrapper.set(Application::getRelease, application.getRelease());
    updateWrapper.set(Application::getBuild, application.getBuild());
    if (application.getOptionState() != null) {
      updateWrapper.set(Application::getOptionState, application.getOptionState());
    }
    this.update(updateWrapper);
  }

  @Override
  public List<Application> getByProjectId(Long id) {
    return baseMapper.getByProjectId(id);
  }

  @Override
  public List<Application> getByTeamId(Long teamId) {
    return baseMapper.getByTeamId(teamId);
  }

  @Override
  public List<Application> getByTeamIdAndExecutionModes(
      Long teamId, @Nonnull Collection<ExecutionMode> executionModes) {
    return getBaseMapper()
        .selectList(
            new LambdaQueryWrapper<Application>()
                .eq((SFunction<Application, Long>) Application::getTeamId, teamId)
                .in(
                    Application::getExecutionMode,
                    executionModes.stream()
                        .map(ExecutionMode::getMode)
                        .collect(Collectors.toSet())));
  }

  @Override
  public boolean checkBuildAndUpdate(Application application) {
    boolean build = application.getBuild();
    if (!build) {
      LambdaUpdateWrapper<Application> updateWrapper = Wrappers.lambdaUpdate();
      updateWrapper.eq(Application::getId, application.getId());
      if (application.isRunning()) {
        updateWrapper.set(Application::getRelease, ReleaseState.NEED_RESTART.get());
      } else {
        updateWrapper.set(Application::getRelease, ReleaseState.DONE.get());
        updateWrapper.set(Application::getOptionState, OptionState.NONE.getValue());
      }
      this.update(updateWrapper);

      // backup
      if (application.isFlinkSqlJob()) {
        FlinkSql newFlinkSql = flinkSqlService.getCandidate(application.getId(), CandidateType.NEW);
        if (!application.isNeedRollback() && newFlinkSql != null) {
          backUpService.backup(application, newFlinkSql);
        }
      }

      // If the current task is not running, or the task has just been added,
      // directly set the candidate version to the official version
      FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), false);
      if (!application.isRunning() || flinkSql == null) {
        this.toEffective(application);
      }
    }
    return build;
  }

  @Override
  public void abort(Application app) {
    CompletableFuture<SubmitResponse> startFuture = startFutureMap.remove(app.getId());
    CompletableFuture<CancelResponse> cancelFuture = cancelFutureMap.remove(app.getId());
    Application application = this.baseMapper.getApp(app);
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
      this.doAbort(app);
    }
  }

  @Override
  public void clean(Application appParam) {
    appParam.setRelease(ReleaseState.DONE.get());
    this.updateRelease(appParam);
  }

  @Override
  public String readConf(Application application) throws IOException {
    File file = getReadableConfFile(application);
    String conf = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    return Base64.getEncoder().encodeToString(conf.getBytes());
  }

  @VisibleForTesting
  File getReadableConfFile(Application application) throws IOException {
    ApiAlertException.throwIfNull(application, "Invalid application.");
    ApiAlertException.throwIfNull(application.getProjectId(), "Invalid project.");
    ApiAlertException.throwIfNull(application.getTeamId(), "Invalid team.");
    ApiAlertException.throwIfTrue(StringUtils.isBlank(application.getModule()), "Invalid module.");
    ApiAlertException.throwIfTrue(
        StringUtils.containsAny(application.getModule(), '/', '\\'), "Invalid module.");
    ApiAlertException.throwIfTrue(StringUtils.isBlank(application.getConfig()), "Invalid config.");

    Project project = projectService.getById(application.getProjectId());
    ApiAlertException.throwIfNull(project, "Invalid project.");
    ApiAlertException.throwIfFalse(
        application.getTeamId().equals(project.getTeamId()), "Invalid project.");

    File projectDistHome = project.getDistHome().getCanonicalFile();
    ApiAlertException.throwIfFalse(projectDistHome.isDirectory(), "Invalid project.");

    File moduleArchive = new File(projectDistHome, application.getModule()).getCanonicalFile();
    File moduleHome =
        new File(StringUtils.removeEnd(moduleArchive.getAbsolutePath(), ".tar.gz"))
            .getCanonicalFile();
    ApiAlertException.throwIfFalse(
        isDirectChildPath(projectDistHome, moduleArchive), "Invalid module.");
    ApiAlertException.throwIfFalse(
        isDirectChildPath(projectDistHome, moduleHome), "Invalid module.");
    ApiAlertException.throwIfFalse(moduleHome.isDirectory(), "Invalid module.");

    File confHome = new File(moduleHome, "conf").getCanonicalFile();
    ApiAlertException.throwIfFalse(isDescendantPath(moduleHome, confHome), "Invalid config.");
    ApiAlertException.throwIfFalse(confHome.isDirectory(), "Invalid config.");

    File configFile = new File(application.getConfig()).getCanonicalFile();

    ApiAlertException.throwIfFalse(configFile.isFile(), "Invalid config.");
    ApiAlertException.throwIfFalse(isDescendantPath(confHome, configFile), "Invalid config.");
    return configFile;
  }

  private boolean isDescendantPath(File parent, File child) throws IOException {
    String parentPath = parent.getCanonicalPath();
    String childPath = child.getCanonicalPath();
    return childPath.startsWith(parentPath.concat(File.separator));
  }

  private boolean isDirectChildPath(File parent, File child) throws IOException {
    File childParent = child.getCanonicalFile().getParentFile();
    return childParent != null && parent.getCanonicalFile().equals(childParent.getCanonicalFile());
  }

  @Override
  public Application getApp(Application appParam) {
    Application application = this.baseMapper.getApp(appParam);
    ApplicationConfig config = configService.getEffective(appParam.getId());
    config = config == null ? configService.getLatest(appParam.getId()) : config;
    if (config != null) {
      config.setToApplication(application);
    }
    if (application.isFlinkSqlJob()) {
      FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), true);
      if (flinkSql == null) {
        flinkSql = flinkSqlService.getCandidate(application.getId(), CandidateType.NEW);
        flinkSql.setSql(DeflaterUtils.unzipString(flinkSql.getSql()));
      }
      flinkSql.setToApplication(application);
    } else {
      if (application.isCICDJob()) {
        String path =
            this.projectService.getAppConfPath(application.getProjectId(), application.getModule());
        application.setConfPath(path);
      }
    }
    // add flink web url info for k8s-mode
    if (application.isKubernetesModeJob()) {
      String restUrl = flinkK8sWatcher.getRemoteRestUrl(k8sWatcherWrapper.toTrackId(application));
      application.setFlinkRestUrl(restUrl);

      // set duration
      long now = System.currentTimeMillis();
      if (application.getTracking() == 1
          && application.getStartTime() != null
          && application.getStartTime().getTime() > 0) {
        application.setDuration(now - application.getStartTime().getTime());
      }
    }
    application.setByHotParams();
    application.setAppControl(buildAppControl(application));
    return application;
  }

  private AppControl buildAppControl(Application app) {
    return new AppControl()
        .setAllowBuild(
            app.getBuildStatus() == null
                || !PipelineStatus.running.getCode().equals(app.getBuildStatus()))
        .setAllowStart(
            !app.shouldBeTrack() && PipelineStatus.success.getCode().equals(app.getBuildStatus()))
        .setAllowStop(app.isRunning())
        .setAllowView(app.shouldBeTrack());
  }

  @Override
  public String getMain(Application application) {
    File jarFile;
    if (application.getProjectId() == null) {
      jarFile = new File(application.getJar());
    } else {
      Project project = new Project();
      project.setId(application.getProjectId());
      String modulePath =
          project.getDistHome().getAbsolutePath().concat("/").concat(application.getModule());
      jarFile = new File(modulePath, application.getJar());
    }
    return Utils.getJarManClass(jarFile);
  }

  @Override
  public boolean mapping(Application appParam) {
    boolean mapping = this.baseMapper.mapping(appParam);
    Application application = getById(appParam.getId());
    if (application.isKubernetesModeJob()) {
      flinkK8sWatcher.doWatching(k8sWatcherWrapper.toTrackId(application));
    } else {
      FlinkAppHttpWatcher.doWatching(application);
    }
    return mapping;
  }

  @Override
  public void cancel(Application appParam) throws Exception {
    FlinkAppHttpWatcher.setOptionState(appParam.getId(), OptionState.CANCELLING);

    Application application = getById(appParam.getId());
    application.setState(FlinkAppState.CANCELLING.getValue());

    ApplicationLog applicationLog = new ApplicationLog();
    applicationLog.setOptionName(Operation.CANCEL.getValue());
    applicationLog.setAppId(application.getId());
    applicationLog.setJobManagerUrl(application.getJobManagerUrl());
    applicationLog.setOptionTime(new Date());

    if (ExecutionMode.isYarnMode(application.getExecutionMode())) {
      applicationLog.setYarnAppId(application.getClusterId());
    }

    if (appParam.getRestoreOrTriggerSavepoint()) {
      if (!application.isKubernetesModeJob()) {
        FlinkAppHttpWatcher.addSavepoint(application.getId());
        application.setOptionState(OptionState.SAVEPOINTING.getValue());
      }
    } else {
      application.setOptionState(OptionState.CANCELLING.getValue());
    }

    application.setOptionTime(new Date());
    this.baseMapper.updateById(application);

    Long userId = serviceHelper.getUserId();
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
      if (StringUtils.isBlank(customSavepoint)
          && ExecutionMode.isYarnMode(application.getExecutionMode())) {
        customSavepoint = Workspace.remote().APP_SAVEPOINTS();
      }
      if (StringUtils.isNotBlank(customSavepoint)) {
        savepointService.processPath(
            customSavepoint, application.getJobName(), application.getId());
      } else {
        throw new IllegalArgumentException(
            String.format(
                "[StreamPark] executionMode: %s, savePoint path is null or invalid.",
                application.getExecutionModeEnum().getName()));
      }
    }

    Map<String, Object> properties = new HashMap<>();

    if (ExecutionMode.isRemoteMode(application.getExecutionModeEnum())) {
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

    Tuple2<String, String> clusterIdNamespace = getNamespaceClusterId(application);
    String namespace = clusterIdNamespace._1;
    String clusterId = clusterIdNamespace._2;

    CancelRequest cancelRequest =
        new CancelRequest(
            flinkEnv.getFlinkVersion(),
            ExecutionMode.of(application.getExecutionMode()),
            properties,
            clusterId,
            application.getJobId(),
            appParam.getRestoreOrTriggerSavepoint(),
            appParam.getDrain() != null && appParam.getDrain(),
            customSavepoint,
            namespace);

    final Date triggerTime = new Date();
    CompletableFuture<CancelResponse> cancelFuture =
        CompletableFuture.supplyAsync(() -> FlinkClient.cancel(cancelRequest), bootstrapExecutor);

    cancelFutureMap.put(application.getId(), cancelFuture);

    TrackId trackId =
        application.isKubernetesModeJob() ? k8sWatcherWrapper.toTrackId(application) : null;

    cancelFuture.whenCompleteAsync(
        (cancelResponse, throwable) -> {
          cancelFutureMap.remove(application.getId());

          if (throwable != null) {
            String exception = Utils.stringifyException(throwable);
            applicationLog.setException(exception);
            applicationLog.setSuccess(false);
            applicationLogService.save(applicationLog);

            if (throwable instanceof CancellationException) {
              doAbort(application);
            } else {
              log.error("abort flink job failed.", throwable);
              application.setOptionState(OptionState.NONE.getValue());
              application.setState(FlinkAppState.FAILED.getValue());
              updateById(application);

              if (appParam.getRestoreOrTriggerSavepoint()) {
                savepointService.expire(application.getId());
              }
              if (application.isKubernetesModeJob()) {
                try {
                  KubernetesDeploymentHelper.delete(
                      application.getK8sNamespace(), application.getJobName());
                } catch (Exception e) {
                  log.error("job abort failed!", e);
                }
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
            Savepoint savepoint = new Savepoint();
            savepoint.setPath(savepointDir);
            savepoint.setAppId(application.getId());
            savepoint.setLatest(true);
            savepoint.setType(CheckPointType.SAVEPOINT.get());
            savepoint.setCreateTime(new Date());
            savepoint.setTriggerTime(triggerTime);
            savepointService.saveSavePoint(savepoint);
          }

          if (application.isKubernetesModeJob()) {
            flinkK8sWatcher.unWatching(trackId);
          }
        });
  }

  @Override
  public String checkSavepointPath(Application appParam) throws Exception {
    String savepointPath = appParam.getSavepointPath();
    if (StringUtils.isBlank(savepointPath)) {
      savepointPath = savepointService.getSavePointPath(appParam);
    }

    if (StringUtils.isNotBlank(savepointPath)) {
      final URI uri = URI.create(savepointPath);
      final String scheme = uri.getScheme();
      final String pathPart = uri.getPath();
      if (scheme == null) {
        return "This state savepoint dir value "
            + savepointPath
            + " scheme (hdfs://, file://, etc) of  is null. Please specify the file system scheme explicitly in the URI.";
      }
      if (pathPart == null) {
        return "This state savepoint dir value "
            + savepointPath
            + " path part to store the checkpoint data in is null. Please specify a directory path for the checkpoint data.";
      }
      if (pathPart.isEmpty() || "/".equals(pathPart)) {
        return "This state savepoint dir value "
            + savepointPath
            + " Cannot use the root directory for checkpoints.";
      }
      return null;
    } else {
      return "When a custom savepoint is not set, state.savepoints.dir or execution.checkpointing.savepoint-dir needs to be configured in the properties or flink-conf.yaml of the application.";
    }
  }

  @Override
  public void persistMetrics(Application appParam) {
    if (appParam.getState() != null && appParam.getFlinkAppStateEnum() == FlinkAppState.RUNNING) {
      appParam.setEndTime(null);
      appParam.setDuration(null);
    }
    this.baseMapper.persistMetrics(appParam);
  }

  /**
   * Setup task is starting (for webUI "state" display)
   *
   * @param application
   */
  @Override
  public void starting(Application application) {
    application.setState(FlinkAppState.STARTING.getValue());
    application.setJobManagerUrl(null);
    application.setOptionTime(new Date());
    updateById(application);
  }

  @Override
  @Transactional(rollbackFor = {Exception.class})
  public void start(Application appParam, boolean auto) throws Exception {
    final Application application = getById(appParam.getId());
    AssertUtils.notNull(application);
    if (!application.isCanBeStart()) {
      throw new ApiAlertException("[StreamPark] The application cannot be started repeatedly.");
    }

    FlinkEnv flinkEnv = flinkEnvService.getByIdOrDefault(application.getVersionId());
    if (flinkEnv == null) {
      throw new ApiAlertException("[StreamPark] can no found flink version");
    }

    // check job on yarn is already running
    if (ExecutionMode.isYarnMode(application.getExecutionMode())) {
      ApiAlertException.throwIfTrue(
          !getYARNApplication(application.getJobName()).isEmpty(),
          "The same job name is already running in the yarn queue");
    }

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

    starting(application);

    application.setAllowNonRestored(
        appParam.getAllowNonRestored() != null && appParam.getAllowNonRestored());

    String appConf;
    String flinkUserJar = null;
    String jobId = new JobID().toHexString();
    ApplicationLog applicationLog = new ApplicationLog();
    applicationLog.setOptionName(Operation.START.getValue());
    applicationLog.setAppId(application.getId());
    applicationLog.setOptionTime(new Date());

    // set the latest to Effective, (it will only become the current effective at this time)
    this.toEffective(application);

    ApplicationConfig applicationConfig = configService.getEffective(application.getId());
    ExecutionMode executionMode = ExecutionMode.of(application.getExecutionMode());
    ApiAlertException.throwIfNull(
        executionMode, "ExecutionMode can't be null, start application failed.");
    if (application.isCustomCodeJob()) {
      if (application.isUploadJob()) {
        appConf =
            String.format(
                "json://{\"%s\":\"%s\"}",
                ConfigConst.KEY_FLINK_APPLICATION_MAIN_CLASS(), application.getMainClass());
      } else {
        switch (application.getApplicationType()) {
          case STREAMPARK_FLINK:
            ConfigFileType fileType = ConfigFileType.of(applicationConfig.getFormat());
            if (fileType != null && !fileType.equals(ConfigFileType.UNKNOWN)) {
              appConf =
                  String.format("%s://%s", fileType.getTypeName(), applicationConfig.getContent());
            } else {
              throw new IllegalArgumentException(
                  "application' config type error,must be ( yaml| properties| hocon )");
            }
            break;
          case APACHE_FLINK:
            appConf =
                String.format(
                    "json://{\"%s\":\"%s\"}",
                    ConfigConst.KEY_FLINK_APPLICATION_MAIN_CLASS(), application.getMainClass());
            break;
          default:
            throw new IllegalArgumentException(
                "[StreamPark] ApplicationType must be (StreamPark flink | Apache flink)... ");
        }
      }

      if (ExecutionMode.YARN_APPLICATION.equals(executionMode)) {
        switch (application.getApplicationType()) {
          case STREAMPARK_FLINK:
            flinkUserJar =
                String.format(
                    "%s/%s", application.getAppHome(), application.getModule().concat(".jar"));
            break;
          case APACHE_FLINK:
            flinkUserJar = String.format("%s/%s", application.getAppHome(), application.getJar());
            break;
          default:
            throw new IllegalArgumentException(
                "[StreamPark] ApplicationType must be (StreamPark flink | Apache flink)... ");
        }
      }
    } else if (application.isFlinkSqlJob()) {
      FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), false);
      AssertUtils.notNull(flinkSql);
      // 1) dist_userJar
      String sqlDistJar = serviceHelper.getSqlClientJar(flinkEnv);
      // 2) appConfig
      appConf =
          applicationConfig == null
              ? null
              : String.format("yaml://%s", applicationConfig.getContent());
      // 3) client
      if (ExecutionMode.YARN_APPLICATION.equals(executionMode)) {
        String clientPath = Workspace.remote().APP_CLIENT();
        flinkUserJar = String.format("%s/%s", clientPath, sqlDistJar);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported...");
    }

    Map<String, Object> extraParameter = new HashMap<>(0);
    if (application.isFlinkSqlJob()) {
      FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), true);
      // Get the sql of the replaced placeholder
      String realSql = variableService.replaceVariable(application.getTeamId(), flinkSql.getSql());
      flinkSql.setSql(DeflaterUtils.zipString(realSql));
      extraParameter.put(ConfigConst.KEY_FLINK_SQL(null), flinkSql.getSql());
    }

    AppBuildPipeline buildPipeline = appBuildPipeService.getById(application.getId());
    AssertUtils.notNull(buildPipeline);

    BuildResult buildResult = buildPipeline.getBuildResult();
    if (ExecutionMode.YARN_APPLICATION.equals(executionMode)) {
      buildResult = new ShadedBuildResponse(null, flinkUserJar, true);
    }

    // Get the args after placeholder replacement
    String args =
        StringUtils.isBlank(appParam.getArgs()) ? application.getArgs() : appParam.getArgs();
    String applicationArgs = variableService.replaceVariable(application.getTeamId(), args);

    String k8sNamespace;
    String k8sClusterId;
    FlinkK8sRestExposedType exposedType = null;
    if (application.getExecutionModeEnum() == ExecutionMode.KUBERNETES_NATIVE_SESSION) {
      FlinkCluster cluster = flinkClusterService.getById(application.getFlinkClusterId());
      k8sClusterId = cluster.getClusterId();
      k8sNamespace = cluster.getK8sNamespace();
      exposedType = cluster.getK8sRestExposedTypeEnum();
    } else if (application.getExecutionModeEnum() == ExecutionMode.KUBERNETES_NATIVE_APPLICATION) {
      k8sClusterId = application.getJobName();
      k8sNamespace = application.getK8sNamespace();
      exposedType = application.getK8sRestExposedTypeEnum();
    } else {
      k8sNamespace = null;
      k8sClusterId = null;
    }

    SubmitRequest submitRequest =
        SubmitRequest.apply(
            flinkEnv.getFlinkVersion(),
            ExecutionMode.of(application.getExecutionMode()),
            getProperties(application, flinkEnv),
            flinkEnv.getFlinkConf(),
            DevelopmentMode.of(application.getJobType()),
            application.getId(),
            jobId,
            application.getJobName(),
            appConf,
            application.getApplicationType(),
            getSavepointPath(appParam, application.getJobName(), application.getId()),
            applicationArgs,
            buildResult,
            extraParameter,
            k8sClusterId,
            k8sNamespace,
            exposedType);

    CompletableFuture<SubmitResponse> future =
        CompletableFuture.supplyAsync(() -> FlinkClient.submit(submitRequest), bootstrapExecutor);

    startFutureMap.put(application.getId(), future);

    future.whenCompleteAsync(
        (response, throwable) -> {
          // 1) remove Future
          startFutureMap.remove(application.getId());

          // 2) exception
          if (throwable != null) {
            log.info(" start exception : " + throwable);
            String exception = Utils.stringifyException(throwable);
            applicationLog.setException(exception);
            applicationLog.setSuccess(false);
            applicationLogService.save(applicationLog);
            if (throwable instanceof CancellationException) {
              doAbort(application);
            } else {
              Application app = getById(appParam.getId());
              app.setState(FlinkAppState.FAILED.getValue());
              app.setOptionState(OptionState.NONE.getValue());
              updateById(app);
              if (app.isKubernetesModeJob()) {
                TrackId trackId = k8sWatcherWrapper.toTrackId(application);
                flinkK8sWatcher.unWatching(trackId);
              } else {
                FlinkAppHttpWatcher.unWatching(appParam.getId());
              }
            }
            return;
          }

          // 3) success
          applicationLog.setSuccess(true);

          if (response.flinkConfig() != null) {
            String jmMemory = response.flinkConfig().get(ConfigConst.KEY_FLINK_JM_PROCESS_MEMORY());
            if (jmMemory != null) {
              application.setJmMemory(MemorySize.parse(jmMemory).getMebiBytes());
            }
            String tmMemory = response.flinkConfig().get(ConfigConst.KEY_FLINK_TM_PROCESS_MEMORY());
            if (tmMemory != null) {
              application.setTmMemory(MemorySize.parse(tmMemory).getMebiBytes());
            }
          }

          if (ExecutionMode.isYarnMode(application.getExecutionMode())) {
            application.setClusterId(response.clusterId());
            applicationLog.setYarnAppId(response.clusterId());
          }

          if (StringUtils.isNoneEmpty(response.jobId())) {
            application.setJobId(response.jobId());
          }

          if (StringUtils.isNotEmpty(response.jobManagerUrl())) {
            application.setJobManagerUrl(response.jobManagerUrl());
            applicationLog.setJobManagerUrl(response.jobManagerUrl());
          }

          application.setStartTime(new Date());
          application.setEndTime(null);

          // if start completed, will be added task to tracking queue
          if (application.isKubernetesModeJob()) {
            log.info(
                "start job {} on {} success, doWatching...",
                application.getJobName(),
                application.getExecutionModeEnum().getName());
            application.setRelease(ReleaseState.DONE.get());

            TrackId trackId = k8sWatcherWrapper.toTrackId(application);
            flinkK8sWatcher.doWatching(trackId);

            if (ExecutionMode.isKubernetesApplicationMode(application.getExecutionMode())) {
              try {
                serviceHelper.configureIngress(k8sClusterId, k8sNamespace);
              } catch (KubernetesClientException e) {
                log.info("Failed to create ingress, stack info:{}", e.getMessage());
                applicationLog.setException(e.getMessage());
                applicationLog.setSuccess(false);
                applicationLogService.save(applicationLog);
                application.setState(FlinkAppState.FAILED.getValue());
                application.setOptionState(OptionState.NONE.getValue());
              }
            }
          } else {
            FlinkAppHttpWatcher.setOptionState(appParam.getId(), OptionState.STARTING);
            FlinkAppHttpWatcher.doWatching(application);
          }
          // update app
          updateById(application);
          // save log
          applicationLogService.save(applicationLog);
        });
  }

  private Map<String, Object> getProperties(Application application, FlinkEnv flinkEnv) {
    Map<String, Object> properties = application.getOptionMap();
    if (ExecutionMode.isRemoteMode(application.getExecutionModeEnum())) {
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
    } else if (ExecutionMode.isYarnMode(application.getExecutionModeEnum())) {
      if (ExecutionMode.YARN_SESSION.equals(application.getExecutionModeEnum())) {
        FlinkCluster cluster = flinkClusterService.getById(application.getFlinkClusterId());
        ApiAlertException.throwIfNull(
            cluster,
            String.format(
                "The yarn session clusterId=%s cannot be find, maybe the clusterId is wrong or "
                    + "the cluster has been deleted. Please contact the Admin.",
                application.getFlinkClusterId()));
        properties.put(ConfigConst.KEY_YARN_APP_ID(), cluster.getClusterId());
      } else {
        String yarnQueue =
            (String) application.getHotParamsMap().get(ConfigConst.KEY_YARN_APP_QUEUE());
        String yarnLabelExpr =
            (String) application.getHotParamsMap().get(ConfigConst.KEY_YARN_APP_NODE_LABEL());
        Optional.ofNullable(yarnQueue)
            .ifPresent(yq -> properties.put(ConfigConst.KEY_YARN_APP_QUEUE(), yq));
        Optional.ofNullable(yarnLabelExpr)
            .ifPresent(yLabel -> properties.put(ConfigConst.KEY_YARN_APP_NODE_LABEL(), yLabel));
      }
    } else if (ExecutionMode.isKubernetesMode(application.getExecutionModeEnum())) {
      properties.put(ConfigConst.KEY_K8S_IMAGE_PULL_POLICY(), "Always");
    }

    if (ExecutionMode.isKubernetesApplicationMode(application.getExecutionMode())) {
      String archiveDir =
          flinkEnvService
              .getFlinkConfig(flinkEnv, application)
              .getProperty(JobManagerOptions.ARCHIVE_DIR.key());
      if (archiveDir != null) {
        properties.put(JobManagerOptions.ARCHIVE_DIR.key(), archiveDir);
      }
    }

    if (application.getAllowNonRestored()) {
      properties.put(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key(), true);
    }

    Map<String, String> dynamicProperties =
        PropertiesUtils.extractDynamicPropertiesAsJava(application.getDynamicProperties());

    properties.putAll(dynamicProperties);

    String kerberosKeySvcAccount = ConfigConst.KEY_KERBEROS_SERVICE_ACCOUNT();
    String svcAcc1 = (String) application.getHotParamsMap().get(kerberosKeySvcAccount);
    String svcAcc2 = dynamicProperties.get(kerberosKeySvcAccount);
    if (svcAcc1 != null) {
      properties.put(kerberosKeySvcAccount, svcAcc1);
    } else if (svcAcc2 != null) {
      properties.put(kerberosKeySvcAccount, svcAcc2);
    }

    ResolveOrder resolveOrder = ResolveOrder.of(application.getResolveOrder());
    if (resolveOrder != null) {
      properties.put(CoreOptions.CLASSLOADER_RESOLVE_ORDER.key(), resolveOrder.getName());
    }

    return properties;
  }

  private void doAbort(Application appParam) {
    Application application = getById(appParam);
    application.setOptionState(OptionState.NONE.getValue());
    application.setState(FlinkAppState.CANCELED.getValue());
    application.setOptionTime(new Date());
    updateById(application);
    savepointService.expire(application.getId());
    if (application.isKubernetesModeJob()) {
      try {
        KubernetesDeploymentHelper.delete(application.getK8sNamespace(), application.getJobName());
      } catch (Exception e) {
        log.error("job abort failed!", e);
      }
    } else {
      FlinkAppHttpWatcher.unWatching(application.getId());
    }

    // kill application
    if (ExecutionMode.isYarnMode(application.getExecutionModeEnum())) {
      try {
        List<ApplicationReport> applications = getYARNApplication(application.getJobName());
        if (!applications.isEmpty()) {
          YarnClient yarnClient = HadoopUtils.yarnClient();
          yarnClient.killApplication(applications.get(0).getApplicationId());
        }
      } catch (Exception e) {
        log.error("job abort failed!", e);
      }
    }
  }

  private Boolean checkJobName(String jobName) {
    if (!StringUtils.isEmpty(jobName.trim())) {
      return JOB_NAME_PATTERN.matcher(jobName).matches()
          && SINGLE_SPACE_PATTERN.matcher(jobName).matches();
    }
    return false;
  }

  private String getSavepointPath(Application appParam, String jobName, Long jobId) {
    String path = null;
    if (appParam.getRestoreOrTriggerSavepoint() != null
        && appParam.getRestoreOrTriggerSavepoint()) {
      if (StringUtils.isBlank(appParam.getSavepointPath())) {
        Savepoint savepoint = savepointService.getLatest(appParam.getId());
        if (savepoint != null) {
          path = savepoint.getPath();
        }
      } else {
        path = appParam.getSavepointPath();
      }
    }
    return savepointService.processPath(path, jobName, jobId);
  }

  /**
   * Check queue label validation when create the application if needed.
   *
   * @param appParam the app to create.
   * @return <code>true</code> if validate it successfully, <code>false</code> else.
   */
  @VisibleForTesting
  public boolean validateQueueIfNeeded(Application appParam) {
    yarnQueueService.checkQueueLabel(appParam.getExecutionModeEnum(), appParam.getYarnQueue());
    if (isYarnNotDefaultQueue(appParam)) {
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
  public boolean validateQueueIfNeeded(Application oldApp, Application newApp) {
    yarnQueueService.checkQueueLabel(newApp.getExecutionModeEnum(), newApp.getYarnQueue());
    if (isYarnNotDefaultQueue(newApp)) {
      return true;
    }
    oldApp.setByHotParams();
    if (ExecutionMode.isYarnPerJobOrAppMode(newApp.getExecutionModeEnum())
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
   * @return If the executionMode is (Yarn PerJob or application mode) and the queue label is not
   *     (empty or default), return true, false else.
   */
  private boolean isYarnNotDefaultQueue(Application application) {
    return !ExecutionMode.isYarnPerJobOrAppMode(application.getExecutionModeEnum())
        || yarnQueueService.isDefaultQueue(application.getYarnQueue());
  }

  @Override
  public List<ApplicationReport> getYARNApplication(String appName) {
    try {
      YarnClient yarnClient = HadoopUtils.yarnClient();
      Set<String> types =
          Sets.newHashSet(
              ApplicationType.STREAMPARK_FLINK.getName(), ApplicationType.APACHE_FLINK.getName());
      EnumSet<YarnApplicationState> states =
          EnumSet.of(
              YarnApplicationState.NEW,
              YarnApplicationState.NEW_SAVING,
              YarnApplicationState.SUBMITTED,
              YarnApplicationState.ACCEPTED,
              YarnApplicationState.RUNNING);
      Set<String> yarnTag = Sets.newHashSet("streampark");
      List<ApplicationReport> applications = yarnClient.getApplications(types, states, yarnTag);
      return applications.stream()
          .filter(report -> report.getName().equals(appName))
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Failed to connect hadoop YARN, detail: {}", Utils.stringifyException(e));
      throw new RuntimeException(
          "Failed to connect hadoop YARN. Ensure that hadoop yarn is running.");
    }
  }

  @Override
  public RestResponse buildApplication(Long appId, boolean forceBuild) throws Exception {
    Application app = this.getById(appId);

    ApiAlertException.throwIfNull(
        app.getVersionId(), "Please bind a Flink version to the current flink job.");
    // 1) check flink version
    FlinkEnv env = flinkEnvService.getById(app.getVersionId());
    boolean checkVersion = env.getFlinkVersion().checkVersion(false);
    if (!checkVersion) {
      throw new ApiAlertException("Unsupported flink version: " + env.getFlinkVersion().version());
    }

    // 2) check env
    boolean envOk = this.checkEnv(app);
    if (!envOk) {
      throw new ApiAlertException(
          "Check flink env failed, please check the flink version of this job");
    }

    if (!forceBuild && !appBuildPipeService.allowToBuildNow(appId)) {
      throw new ApiAlertException(
          "The job is invalid, or the job cannot be built while it is running");
    }
    // check if you need to go through the build process (if the jar and pom have changed,
    // you need to go through the build process, if other common parameters are modified,
    // you don't need to go through the build process)

    ApplicationLog applicationLog = new ApplicationLog();
    applicationLog.setOptionName(
        org.apache.streampark.console.core.enums.Operation.RELEASE.getValue());
    applicationLog.setAppId(app.getId());
    applicationLog.setOptionTime(new Date());

    boolean needBuild = this.checkBuildAndUpdate(app);
    if (!needBuild) {
      applicationLog.setSuccess(true);
      applicationLogService.save(applicationLog);
      return RestResponse.success(true);
    }

    // rollback
    if (app.isNeedRollback() && app.isFlinkSqlJob()) {
      flinkSqlService.rollback(app);
    }
    boolean actionResult = appBuildPipeService.buildApplication(app, applicationLog);
    return RestResponse.success(actionResult);
  }

  @Override
  public void updateJobManagerUrl(Long id, String url) {
    baseMapper.updateJobManagerUrl(id, url);
  }

  private Tuple2<String, String> getNamespaceClusterId(Application application) {
    String clusterId = null;
    String k8sNamespace = null;
    FlinkCluster cluster;
    switch (application.getExecutionModeEnum()) {
      case YARN_APPLICATION:
      case YARN_PER_JOB:
      case YARN_SESSION:
        clusterId = application.getClusterId();
        break;
      case KUBERNETES_NATIVE_APPLICATION:
        clusterId = application.getJobName();
        k8sNamespace = application.getK8sNamespace();
        break;
      case KUBERNETES_NATIVE_SESSION:
        cluster = flinkClusterService.getById(application.getFlinkClusterId());
        ApiAlertException.throwIfNull(
            cluster,
            String.format(
                "The Kubernetes session clusterId=%s can't found, maybe the clusterId is wrong or the cluster has been deleted. Please contact the Admin.",
                application.getFlinkClusterId()));
        clusterId = cluster.getClusterId();
        k8sNamespace = cluster.getK8sNamespace();
        break;
      default:
        break;
    }
    return Tuple2.apply(k8sNamespace, clusterId);
  }
}
