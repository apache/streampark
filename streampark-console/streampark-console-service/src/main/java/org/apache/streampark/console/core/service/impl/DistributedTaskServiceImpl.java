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

import org.apache.streampark.console.base.mybatis.entity.BaseEntity;
import org.apache.streampark.console.base.util.ConsistentHash;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.base.util.WebUtils;
import org.apache.streampark.console.core.bean.FlinkTaskItem;
import org.apache.streampark.console.core.bean.SparkTaskItem;
import org.apache.streampark.console.core.entity.DistributedTask;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.SparkApplication;
import org.apache.streampark.console.core.enums.DistributedTaskEnum;
import org.apache.streampark.console.core.enums.EngineTypeEnum;
import org.apache.streampark.console.core.mapper.DistributedTaskMapper;
import org.apache.streampark.console.core.service.DistributedTaskService;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.SparkApplicationActionService;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class DistributedTaskServiceImpl extends ServiceImpl<DistributedTaskMapper, DistributedTask>
    implements
        DistributedTaskService {

    @Qualifier("streamparkDistributedTaskExecutor")
    @Autowired
    private Executor taskExecutor;

    @Autowired
    private FlinkApplicationActionService flinkApplicationActionService;

    @Autowired
    private SparkApplicationActionService sparkApplicationActionService;

    /**
     * Server Id
     */
    private String serverId;

    /**
     * Consistent hash algorithm for task distribution
     */
    private final ConsistentHash<String> consistentHash = new ConsistentHash<>(Collections.emptyList());

    /**
     * Task execution status
     */
    private final ConcurrentHashMap<Long, Boolean> runningTasks = new ConcurrentHashMap<>();

    /**
     * Initialize the consistent hash ring.
     * @param allServers All servers
     * @param serverId The name of the current server
     */
    public void init(Set<String> allServers, String serverId) {
        this.serverId = serverId;
        for (String server : allServers) {
            consistentHash.add(server);
        }
    }

    @Scheduled(fixedDelay = 50)
    public void pollDistributedTask() {
        List<DistributedTask> distributedTaskList = this.list();
        for (DistributedTask DistributedTask : distributedTaskList) {
            long taskId = DistributedTask.getId();
            if (DistributedTask.getEngineType() != EngineTypeEnum.FLINK || !isLocalProcessing(taskId)) {
                continue;
            }
            if (runningTasks.putIfAbsent(taskId, true) == null) {
                taskExecutor.execute(() -> {
                    try {
                        // Execute Distributed task
                        executeDistributedTask(DistributedTask);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    } finally {
                        runningTasks.remove(DistributedTask.getId());
                        this.removeById(DistributedTask.getId());
                    }
                });
            }
        }
    }

    /**
     * This interface is responsible for polling the database to retrieve task records and execute the corresponding operations.
     * @param distributedTask distributedTask
     */
    @Override
    public void executeDistributedTask(DistributedTask distributedTask) throws Exception {
        // Execute Distributed task
        log.info("Execute Distributed task: {}", distributedTask);
        if (distributedTask.getEngineType() == EngineTypeEnum.FLINK) {
            FlinkTaskItem flinkTaskItem = getFlinkTaskItem(distributedTask);
            FlinkApplication appParam = getAppByFlinkTaskItem(flinkTaskItem);
            switch (distributedTask.getAction()) {
                case START:
                    flinkApplicationActionService.start(appParam, flinkTaskItem.getAutoStart());
                    break;
                case RESTART:
                    flinkApplicationActionService.restart(appParam);
                    break;
                case REVOKE:
                    flinkApplicationActionService.revoke(appParam.getId());
                    break;
                case CANCEL:
                    flinkApplicationActionService.cancel(appParam);
                    break;
                case ABORT:
                    flinkApplicationActionService.abort(appParam.getId());
                    break;
                default:
                    log.error("Unsupported flink task action: {}", distributedTask.getAction());
            }
        } else if (distributedTask.getEngineType() == EngineTypeEnum.SPARK) {
            SparkTaskItem sparkTaskItem = getSparkTaskItem(distributedTask);
            SparkApplication appParam = getAppBySparkTaskItem(sparkTaskItem);
            switch (distributedTask.getAction()) {
                case START:
                    sparkApplicationActionService.start(appParam, sparkTaskItem.getAutoStart());
                    break;
                case RESTART:
                    sparkApplicationActionService.restart(appParam);
                    break;
                case REVOKE:
                    sparkApplicationActionService.revoke(appParam.getId());
                    break;
                case CANCEL:
                    sparkApplicationActionService.cancel(appParam);
                    break;
                case FORCED_STOP:
                    sparkApplicationActionService.forcedStop(appParam.getId());
                    break;
                default:
                    log.error("Unsupported spark task action: {}", distributedTask.getAction());
            }
        }
    }

    /**
     * This interface handles task redistribution when server nodes are added.
     *
     * @param serverId String
     */
    @Override
    public void addServer(String serverId) {
        consistentHash.add(serverId);
    }

    /**
     * This interface handles task redistribution when server nodes are removed.
     *
     * @param serverId String
     */
    @Override
    public void removeServer(String serverId) {
        consistentHash.remove(serverId);
    }

    /**
     * Determine whether the task is processed locally.
     *
     * @param appId Long
     * @return boolean
     */
    @Override
    public boolean isLocalProcessing(Long appId) {
        if (!WebUtils.isHaEnable()) {
            return true;
        }
        return consistentHash.get(appId).equals(serverId);
    }

    /**
     * Save Distributed task.
     *
     * @param appParam  Application
     * @param autoStart boolean
     * @param action It may be one of the following values: START, RESTART, REVOKE, CANCEL, ABORT
     */
    @Override
    public void saveDistributedTask(BaseEntity appParam, boolean autoStart, DistributedTaskEnum action) {
        try {
            DistributedTask distributedTask;
            if (appParam instanceof FlinkApplication) {
                distributedTask = getDistributedTaskByFlinkApp((FlinkApplication) appParam, autoStart, action);
            } else if (appParam instanceof SparkApplication) {
                distributedTask = getDistributedTaskBySparkApp((SparkApplication) appParam, autoStart, action);
            } else {
                log.error("Unsupported application type: {}", appParam.getClass().getName());
                return;
            }
            this.save(distributedTask);
        } catch (Exception e) {
            log.error("Failed to save Distributed task: {}", e.getMessage());
        }
    }

    public DistributedTask getDistributedTaskByFlinkApp(FlinkApplication appParam, boolean autoStart,
                                                        DistributedTaskEnum action) throws Exception {
        FlinkTaskItem flinkTaskItem = new FlinkTaskItem();
        flinkTaskItem.setAppId(appParam.getId());
        flinkTaskItem.setAutoStart(autoStart);
        flinkTaskItem.setArgs(appParam.getArgs());
        flinkTaskItem.setDynamicProperties(appParam.getDynamicProperties());
        flinkTaskItem.setSavepointPath(appParam.getSavepointPath());
        flinkTaskItem.setRestoreOrTriggerSavepoint(appParam.getRestoreOrTriggerSavepoint());
        flinkTaskItem.setDrain(appParam.getDrain());
        flinkTaskItem.setNativeFormat(appParam.getNativeFormat());
        flinkTaskItem.setRestoreMode(appParam.getRestoreMode());

        DistributedTask distributedTask = new DistributedTask();
        distributedTask.setAction(action);
        distributedTask.setEngineType(EngineTypeEnum.FLINK);
        distributedTask.setProperties(JacksonUtils.write(flinkTaskItem));
        return distributedTask;
    }

    public DistributedTask getDistributedTaskBySparkApp(SparkApplication appParam, boolean autoStart,
                                                        DistributedTaskEnum action) throws Exception {
        SparkTaskItem sparkTaskItem = new SparkTaskItem();
        sparkTaskItem.setAppId(appParam.getId());
        sparkTaskItem.setAutoStart(autoStart);

        DistributedTask distributedTask = new DistributedTask();
        distributedTask.setAction(action);
        distributedTask.setEngineType(EngineTypeEnum.SPARK);
        distributedTask.setProperties(JacksonUtils.write(sparkTaskItem));
        return distributedTask;
    }

    public FlinkTaskItem getFlinkTaskItem(DistributedTask distributedTask) throws JsonProcessingException {
        return JacksonUtils.read(distributedTask.getProperties(), FlinkTaskItem.class);
    }

    public SparkTaskItem getSparkTaskItem(DistributedTask distributedTask) throws JsonProcessingException {
        return JacksonUtils.read(distributedTask.getProperties(), SparkTaskItem.class);
    }

    public FlinkApplication getAppByFlinkTaskItem(FlinkTaskItem flinkTaskItem) {
        FlinkApplication appParam = new FlinkApplication();
        appParam.setId(flinkTaskItem.getAppId());
        appParam.setArgs(flinkTaskItem.getArgs());
        appParam.setDynamicProperties(flinkTaskItem.getDynamicProperties());
        appParam.setSavepointPath(flinkTaskItem.getSavepointPath());
        appParam.setRestoreOrTriggerSavepoint(flinkTaskItem.getRestoreOrTriggerSavepoint());
        appParam.setDrain(flinkTaskItem.getDrain());
        appParam.setNativeFormat(flinkTaskItem.getNativeFormat());
        appParam.setRestoreMode(flinkTaskItem.getRestoreMode());
        return appParam;
    }

    public SparkApplication getAppBySparkTaskItem(SparkTaskItem sparkTaskItem) {
        SparkApplication appParam = new SparkApplication();
        appParam.setId(sparkTaskItem.getAppId());
        return appParam;
    }

    public long getConsistentHashSize() {
        return consistentHash.getSize();
    }
}
