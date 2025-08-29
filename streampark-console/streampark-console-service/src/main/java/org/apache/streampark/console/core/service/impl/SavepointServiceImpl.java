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

import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.CompletableFutureUtils;
import org.apache.streampark.common.util.PropertiesUtils;
import org.apache.streampark.common.util.ThreadUtils;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.base.util.CommonUtils;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationConfig;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.Savepoint;
import org.apache.streampark.console.core.enums.CheckPointType;
import org.apache.streampark.console.core.enums.Operation;
import org.apache.streampark.console.core.enums.OptionState;
import org.apache.streampark.console.core.mapper.SavepointMapper;
import org.apache.streampark.console.core.service.ApplicationConfigService;
import org.apache.streampark.console.core.service.ApplicationLogService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.SavepointService;
import org.apache.streampark.console.core.task.FlinkAppHttpWatcher;
import org.apache.streampark.flink.client.FlinkClient;
import org.apache.streampark.flink.client.bean.SavepointResponse;
import org.apache.streampark.flink.client.bean.TriggerSavepointRequest;
import org.apache.streampark.flink.util.FlinkUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.RestOptions;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class SavepointServiceImpl extends ServiceImpl<SavepointMapper, Savepoint>
    implements SavepointService {

  @Autowired private FlinkEnvService flinkEnvService;

  @Autowired private ApplicationService applicationService;

  @Autowired private ApplicationConfigService configService;

  @Autowired private FlinkClusterService flinkClusterService;

  @Autowired private ApplicationLogService applicationLogService;

  @Autowired private FlinkAppHttpWatcher flinkAppHttpWatcher;

  private static final String SAVEPOINT_DIRECTORY_NEW_KEY = "execution.checkpointing.savepoint-dir";

  private static final String MAX_RETAINED_CHECKPOINTS_NEW_KEY =
      "execution.checkpointing.num-retained";

  private static final int CPU_NUM = Math.max(2, Runtime.getRuntime().availableProcessors());

  private final ExecutorService flinkTriggerExecutor =
      new ThreadPoolExecutor(
          CPU_NUM,
          CPU_NUM,
          60L,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(),
          ThreadUtils.threadFactory("streampark-flink-savepoint-trigger"));

  @Autowired private SavepointMapper savepointMapper;

  @Override
  public void expire(Long appId) {
    this.cleanLatest(appId);
  }

  private void clearExpire(Savepoint entity) {
    FlinkEnv flinkEnv = flinkEnvService.getByAppId(entity.getAppId());
    Application application = applicationService.getById(entity.getAppId());
    AssertUtils.notNull(flinkEnv);
    AssertUtils.notNull(application);

    String numRetainedKey = CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key();
    String numRetainedFromDynamicProp =
        PropertiesUtils.extractDynamicPropertiesAsJava(application.getDynamicProperties())
            .get(numRetainedKey);

    if (StringUtils.isBlank(numRetainedFromDynamicProp)) {
      // for flink 1.20
      numRetainedFromDynamicProp =
          PropertiesUtils.extractDynamicPropertiesAsJava(application.getDynamicProperties())
              .get(MAX_RETAINED_CHECKPOINTS_NEW_KEY);
    }

    int cpThreshold = 0;
    if (numRetainedFromDynamicProp != null) {
      try {
        int value = Integer.parseInt(numRetainedFromDynamicProp.trim());
        if (value > 0) {
          cpThreshold = value;
        } else {
          log.warn(
              "this value of dynamicProperties key: state.checkpoints.num-retained or execution.checkpointing.num-retained is invalid, must be gt 0");
        }
      } catch (NumberFormatException e) {
        log.warn(
            "this value of dynamicProperties key: state.checkpoints.num-retained or execution.checkpointing.num-retained invalid, must be number");
      }
    }

    if (cpThreshold == 0) {
      String flinkConfNumRetained =
          flinkEnvService.getFlinkConfig(flinkEnv, application).getProperty(numRetainedKey);
      int numRetainedDefaultValue = 1;
      if (flinkConfNumRetained != null) {
        try {
          int value = Integer.parseInt(flinkConfNumRetained.trim());
          if (value > 0) {
            cpThreshold = value;
          } else {
            cpThreshold = numRetainedDefaultValue;
            log.warn(
                "the value of key: state.checkpoints.num-retained or execution.checkpointing.num-retained in flink-conf.yaml is invalid, must be gt 0, default value: {} will be use",
                numRetainedDefaultValue);
          }
        } catch (NumberFormatException e) {
          cpThreshold = numRetainedDefaultValue;
          log.warn(
              "the value of key: state.checkpoints.num-retained or execution.checkpointing.num-retained in flink-conf.yaml is invalid, must be number, flink env: {}, default value: {} will be use",
              flinkEnv.getFlinkHome(),
              flinkConfNumRetained);
        }
      } else {
        cpThreshold = numRetainedDefaultValue;
        log.info(
            "the application: {} is not set {} in dynamicProperties or value is invalid, and flink-conf.yaml is the same problem of flink env: {}, default value: {} will be use.",
            application.getJobName(),
            numRetainedKey,
            flinkEnv.getFlinkHome(),
            numRetainedDefaultValue);
      }
    }

    if (CheckPointType.CHECKPOINT.equals(CheckPointType.of(entity.getType()))) {
      cpThreshold = cpThreshold - 1;
    }

    if (cpThreshold == 0) {
      LambdaQueryWrapper<Savepoint> queryWrapper =
          new LambdaQueryWrapper<Savepoint>()
              .eq(Savepoint::getAppId, entity.getAppId())
              .eq(Savepoint::getType, 1);
      this.remove(queryWrapper);
    } else {
      LambdaQueryWrapper<Savepoint> queryWrapper =
          new LambdaQueryWrapper<Savepoint>()
              .select(Savepoint::getTriggerTime)
              .eq(Savepoint::getAppId, entity.getAppId())
              .eq(Savepoint::getType, CheckPointType.CHECKPOINT.get())
              .orderByDesc(Savepoint::getTriggerTime);

      Page<Savepoint> savepointPage =
          this.baseMapper.selectPage(new Page<>(1, cpThreshold + 1), queryWrapper);
      if (!savepointPage.getRecords().isEmpty()
          && savepointPage.getRecords().size() > cpThreshold) {
        Savepoint savepoint = savepointPage.getRecords().get(cpThreshold - 1);
        LambdaQueryWrapper<Savepoint> lambdaQueryWrapper =
            new LambdaQueryWrapper<Savepoint>()
                .eq(Savepoint::getAppId, entity.getAppId())
                .eq(Savepoint::getType, 1)
                .lt(Savepoint::getTriggerTime, savepoint.getTriggerTime());
        this.remove(lambdaQueryWrapper);
      }
    }
  }

  @Override
  public Savepoint getLatest(Long id) {
    List<Savepoint> savepointList =
        this.lambdaQuery()
            .eq(Savepoint::getAppId, id)
            .eq(Savepoint::getLatest, true)
            .orderByDesc(Savepoint::getCreateTime)
            .list();

    if (!savepointList.isEmpty()) {
      return savepointList.get(0);
    }
    return this.lambdaQuery()
        .eq(Savepoint::getAppId, id)
        .orderByDesc(Savepoint::getTriggerTime)
        .one();
  }

  @Override
  public String getSavePointPath(Application appParam) throws Exception {
    Application application = applicationService.getById(appParam.getId());

    // 1) properties have the highest priority, read the properties are set: -Dstate.savepoints.dir
    Map<String, String> properties =
        PropertiesUtils.extractDynamicPropertiesAsJava(application.getDynamicProperties());

    String savepointPath =
        properties.getOrDefault(
            CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
            properties.get(SAVEPOINT_DIRECTORY_NEW_KEY));

    // Application conf configuration has the second priority. If it is a streampark|flinksql type
    // task,
    // see if Application conf is configured when the task is defined, if checkpoints are configured
    // and enabled,
    // read `state.savepoints.dir`
    if (StringUtils.isBlank(savepointPath)) {
      if (application.isStreamParkJob() || application.isFlinkSqlJob()) {
        ApplicationConfig applicationConfig = configService.getEffective(application.getId());
        if (applicationConfig != null) {
          Map<String, String> map = applicationConfig.readConfig();
          if (FlinkUtils.isCheckpointEnabled(map)) {
            savepointPath =
                map.getOrDefault(
                    CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                    map.get(SAVEPOINT_DIRECTORY_NEW_KEY));
          }
        }
      }
    }

    // 3) If the savepoint is not obtained above, try to obtain the savepoint path according to the
    // deployment type (remote|on yarn)
    if (StringUtils.isBlank(savepointPath)) {
      // 3.1) At the remote mode, request the flink webui interface to get the savepoint path
      if (ExecutionMode.isRemoteMode(application.getExecutionMode())) {
        FlinkCluster cluster = flinkClusterService.getById(application.getFlinkClusterId());
        AssertUtils.notNull(
            cluster,
            String.format(
                "The clusterId=%s cannot be find, maybe the clusterId is wrong or "
                    + "the cluster has been deleted. Please contact the Admin.",
                application.getFlinkClusterId()));
        Map<String, String> config = cluster.getFlinkConfig();
        if (!config.isEmpty()) {
          savepointPath =
              config.getOrDefault(
                  CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                  config.get(SAVEPOINT_DIRECTORY_NEW_KEY));
        }
      }
    }

    // 3.2) read the savepoint in flink-conf.yml in the bound
    if (StringUtils.isBlank(savepointPath)) {
      // flink
      FlinkEnv flinkEnv = flinkEnvService.getById(application.getVersionId());
      Properties flinkConfig = flinkEnvService.getFlinkConfig(flinkEnv, application);
      savepointPath =
          flinkConfig.getProperty(
              CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
              flinkConfig.getProperty(SAVEPOINT_DIRECTORY_NEW_KEY));
    }

    return processPath(savepointPath, application.getJobName(), application.getId());
  }

  @Override
  public String processPath(String path, String jobName, Long jobId) {
    if (StringUtils.isNotBlank(path)) {
      return path.replaceAll("\\$\\{job(Name|name)}|\\$job(Name|name)", jobName)
          .replaceAll("\\$\\{job(Id|id)}|\\$job(Id|id)", jobId.toString());
    }
    return path;
  }

  @Override
  public void saveSavePoint(Savepoint savepoint) {
    this.clearExpire(savepoint);
    this.cleanLatest(savepoint.getAppId());
    super.save(savepoint);
  }

  private void cleanLatest(Long appId) {
    this.lambdaUpdate().eq(Savepoint::getAppId, appId).set(Savepoint::getLatest, false).update();
  }

  @Override
  public void trigger(Long appId, @Nullable String savepointPath) throws Exception {
    log.info("Start to trigger savepoint for app {}", appId);
    Application application = applicationService.getById(appId);

    ApplicationLog applicationLog = new ApplicationLog();
    applicationLog.setOptionName(Operation.SAVEPOINT.getValue());
    applicationLog.setAppId(application.getId());
    applicationLog.setJobManagerUrl(application.getJobManagerUrl());
    applicationLog.setOptionTime(new Date());
    if (ExecutionMode.isYarnMode(application.getExecutionMode())) {
      applicationLog.setYarnAppId(application.getClusterId());
    }
    if (!application.isKubernetesModeJob()) {
      FlinkAppHttpWatcher.addSavepoint(application.getId());
      application.setOptionState(OptionState.SAVEPOINTING.getValue());
      application.setOptionTime(new Date());
      this.applicationService.updateById(application);
      flinkAppHttpWatcher.initialize();
    }

    FlinkEnv flinkEnv = flinkEnvService.getById(application.getVersionId());

    // infer savepoint
    String customSavepoint = this.getFinalSavepointDir(savepointPath, application);

    FlinkCluster cluster = flinkClusterService.getById(application.getFlinkClusterId());
    String clusterId = getClusterId(application, cluster);

    Map<String, Object> properties = this.tryGetRestProps(application, cluster);

    TriggerSavepointRequest request =
        new TriggerSavepointRequest(
            flinkEnv.getFlinkVersion(),
            application.getExecutionModeEnum(),
            properties,
            clusterId,
            application.getJobId(),
            customSavepoint,
            application.getK8sNamespace());

    CompletableFuture<SavepointResponse> savepointFuture =
        CompletableFuture.supplyAsync(
            () -> FlinkClient.triggerSavepoint(request), flinkTriggerExecutor);

    handleSavepointResponseFuture(application, applicationLog, savepointFuture);
  }

  private void handleSavepointResponseFuture(
      Application application,
      ApplicationLog applicationLog,
      CompletableFuture<SavepointResponse> savepointFuture) {
    CompletableFutureUtils.runTimeout(
            savepointFuture,
            10L,
            TimeUnit.MINUTES,
            savepointResponse -> {
              if (savepointResponse != null && savepointResponse.savepointDir() != null) {
                applicationLog.setSuccess(true);
                String savepointDir = savepointResponse.savepointDir();
                log.info("Request savepoint successful, savepointDir: {}", savepointDir);
              }
            },
            e -> {
              log.error("Trigger savepoint for flink job failed.", e);
              String exception = Utils.stringifyException(e);
              applicationLog.setException(exception);
              if (!(e instanceof TimeoutException)) {
                applicationLog.setSuccess(false);
              }
            })
        .whenComplete(
            (t, e) -> {
              applicationLogService.save(applicationLog);
              if (!application.isKubernetesModeJob()) {
                application.setOptionState(OptionState.NONE.getValue());
                application.setOptionTime(new Date());
                applicationService.update(application);
                flinkAppHttpWatcher.cleanSavepoint(application);
                flinkAppHttpWatcher.initialize();
              }
            });
  }

  private String getFinalSavepointDir(@Nullable String savepointPath, Application application)
      throws Exception {
    String result = savepointPath;
    if (StringUtils.isBlank(savepointPath)) {
      result = this.getSavePointPath(application);
    }

    if (StringUtils.isBlank(result) && ExecutionMode.isYarnMode(application.getExecutionMode())) {
      result = Workspace.remote().APP_SAVEPOINTS();
    }

    if (StringUtils.isNotBlank(result)) {
      result = processPath(result, application.getJobName(), application.getId());
    } else {
      throw new IllegalArgumentException(
          String.format(
              "[StreamPark] executionMode: %s, savePoint path is null or invalid.",
              application.getExecutionModeEnum().getName()));
    }
    return result;
  }

  @Nonnull
  private Map<String, Object> tryGetRestProps(Application application, FlinkCluster cluster) {
    Map<String, Object> properties = new HashMap<>();

    if (ExecutionMode.isRemoteMode(application.getExecutionModeEnum())) {
      AssertUtils.notNull(
          cluster,
          String.format(
              "The clusterId=%s cannot be find, maybe the clusterId is wrong or the cluster has been deleted. Please contact the Admin.",
              application.getFlinkClusterId()));
      URI activeAddress = cluster.getRemoteURI();
      properties.put(RestOptions.ADDRESS.key(), activeAddress.getHost());
      properties.put(RestOptions.PORT.key(), activeAddress.getPort());
    }
    return properties;
  }

  private String getClusterId(Application application, FlinkCluster cluster) {
    switch (application.getExecutionModeEnum()) {
      case YARN_APPLICATION:
      case YARN_PER_JOB:
        return application.getClusterId();
      case KUBERNETES_NATIVE_APPLICATION:
        return application.getJobName();
      case KUBERNETES_NATIVE_SESSION:
      case YARN_SESSION:
        AssertUtils.notNull(
            cluster,
            String.format(
                "The %s clusterId=%s cannot be find, maybe the clusterId is wrong or the cluster has been deleted. Please contact the Admin.",
                application.getExecutionModeEnum().getName(), application.getFlinkClusterId()));
        return cluster.getClusterId();
      default:
        return null;
    }
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public Boolean delete(Long id, Application application) throws InternalException {
    Savepoint savepoint = getById(id);
    try {
      if (CommonUtils.notEmpty(savepoint.getPath())) {
        application.getFsOperator().delete(savepoint.getPath());
      }
      removeById(id);
      return true;
    } catch (Exception e) {
      log.error("Delete application {} failed!", id, e);
      throw new InternalException(e.getMessage());
    }
  }

  @Override
  public IPage<Savepoint> page(Savepoint savepoint, RestRequest request) {
    request.setSortField("trigger_time");
    Page<Savepoint> page = MybatisPager.getPage(request);
    LambdaQueryWrapper<Savepoint> queryWrapper =
        new LambdaQueryWrapper<Savepoint>().eq(Savepoint::getAppId, savepoint.getAppId());
    return this.page(page, queryWrapper);
  }

  @Override
  public void removeApp(Application application) {
    Long appId = application.getId();

    LambdaQueryWrapper<Savepoint> queryWrapper =
        new LambdaQueryWrapper<Savepoint>().eq(Savepoint::getAppId, appId);
    this.remove(queryWrapper);

    try {
      application
          .getFsOperator()
          .delete(application.getWorkspace().APP_SAVEPOINTS().concat("/").concat(appId.toString()));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
