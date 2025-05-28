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

package org.apache.streampark.console.core.watcher;

import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.util.DateUtils;
import org.apache.streampark.common.util.HttpClientUtils;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.core.bean.AlertTemplate;
import org.apache.streampark.console.core.component.FlinkCheckpointProcessor;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.FlinkStateChangeEvent;
import org.apache.streampark.console.core.enums.FlinkAppStateEnum;
import org.apache.streampark.console.core.enums.OptionStateEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.enums.StopFromEnum;
import org.apache.streampark.console.core.metrics.flink.CheckPoints;
import org.apache.streampark.console.core.metrics.flink.JobsOverview;
import org.apache.streampark.console.core.metrics.flink.Overview;
import org.apache.streampark.console.core.metrics.yarn.YarnAppInfo;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.SavepointService;
import org.apache.streampark.console.core.service.alert.AlertService;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.core.util.AlertTemplateUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.util.Timeout;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** This implementation is currently used for tracing flink job on yarn,standalone,remote mode */
@Slf4j
@Component
public class FlinkAppHttpWatcher {

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Autowired
    private FlinkApplicationActionService applicationActionService;

    @Autowired
    private FlinkApplicationInfoService applicationInfoService;

    @Autowired
    private AlertService alertService;

    @Autowired
    private FlinkCheckpointProcessor checkpointProcessor;

    @Autowired
    private FlinkClusterService flinkClusterService;

    @Autowired
    private SavepointService savepointService;

    // track interval every 5 seconds
    public static final Duration WATCHING_INTERVAL = Duration.ofSeconds(5);

    // option interval within 10 seconds
    private static final Duration OPTION_INTERVAL = Duration.ofSeconds(10);

    private static final Timeout HTTP_TIMEOUT = Timeout.ofSeconds(5);

    /**
     *
     *
     * <pre>
     * record task requires save points
     * It will only be used in the RUNNING state. If it is checked that the task is running and the save point is required,
     * set the state of the task to savepoint
     * </pre>
     */
    private static final Cache<Long, Byte> SAVEPOINT_CACHE =
        Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();

    /**
     * Record the status of the first tracking task, because after the task is started, the overview
     * of the task will be obtained during the first tracking
     */
    private static final Cache<Long, Byte> STARTING_CACHE =
        Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    private static final Cache<Long, Date> LOST_CACHE =
        Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    /** tracking task list */
    private static final Map<Long, FlinkApplication> WATCHING_APPS = new ConcurrentHashMap<>(0);

    /**
     *
     *
     * <pre>
     * StopFrom: marked a task stopped from the stream-park web or other ways.
     * If stop from stream-park web, you can know whether to make a savepoint when you stop the task, and if you make a savepoint,
     * you can set the savepoint as the last effect savepoint, and the next time start, will be automatically choose to start.
     * In other words, if stop from other ways, there is no way to know the savepoint has been done, directly set all the savepoint
     * to expire, and needs to be manually specified when started again.
     * </pre>
     */
    private static final Map<Long, StopFromEnum> STOP_FROM_MAP = new ConcurrentHashMap<>(0);

    /**
     * Cancelling tasks are placed in this cache with an expiration time of 10 seconds (the time of 2
     * task monitoring polls).
     */
    private static final Cache<Long, Byte> CANCELING_CACHE =
        Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.SECONDS).build();

    private static final Cache<Long, FlinkStateChangeEvent> PREVIOUS_STATUS =
        Caffeine.newBuilder().expireAfterWrite(24, TimeUnit.HOURS).build();

    /**
     * Task canceled tracking list, record who cancelled the tracking task Map<applicationId,userId>
     */
    private static final Map<Long, Long> CANCELLED_JOB_MAP = new ConcurrentHashMap<>(0);

    private static final Map<Long, FlinkCluster> FLINK_CLUSTER_MAP = new ConcurrentHashMap<>(0);

    private static final Map<Long, OptionStateEnum> OPTIONING = new ConcurrentHashMap<>(0);

    private Long lastWatchTime = 0L;

    private Long lastOptionTime = 0L;

    private static final Byte DEFAULT_FLAG_BYTE = Byte.valueOf("0");

    @Qualifier("flinkRestAPIWatchingExecutor")
    @Autowired
    private Executor watchExecutor;

    @PostConstruct
    public void init() {
        WATCHING_APPS.clear();
        List<FlinkApplication> applications = applicationManageService.list(
            new LambdaQueryWrapper<FlinkApplication>()
                .eq(FlinkApplication::getTracking, 1)
                .notIn(FlinkApplication::getDeployMode, FlinkDeployMode.getKubernetesMode()));
        applications.forEach(app -> {
            Long appId = app.getId();
            WATCHING_APPS.put(appId, app);
            STARTING_CACHE.put(appId, DEFAULT_FLAG_BYTE);
        });
    }

    @PreDestroy
    public void doStop() {
        log.info(
            "[StreamPark][FlinkAppHttpWatcher] StreamPark Console will be shutdown,persistent application to database.");
        WATCHING_APPS.forEach((k, v) -> applicationManageService.persistMetrics(v));
    }

    /**
     * <strong>NOTE: The following conditions must be met for execution</strong>
     *
     * <p><strong>1) Program started or page operated task, such as start/stop, needs to return the
     * state immediately. (the frequency of 1 second once, continued 10 seconds (10 times))</strong>
     *
     * <p><strong>2) Normal information obtain, once every 5 seconds</strong>
     */
    @Scheduled(fixedDelay = 1, initialDelay = 5, timeUnit = TimeUnit.SECONDS)
    public void start() {
        Long timeMillis = System.currentTimeMillis();
        if (lastWatchTime == null
            || !OPTIONING.isEmpty()
            || timeMillis - lastOptionTime <= OPTION_INTERVAL.toMillis()
            || timeMillis - lastWatchTime >= WATCHING_INTERVAL.toMillis()) {
            lastWatchTime = timeMillis;
            WATCHING_APPS.forEach(this::watch);
        }
    }

    private void watch(Long id, FlinkApplication application) {
        watchExecutor.execute(
            () -> {
                try {
                    // query status from flink rest api
                    getStateFromFlink(application);
                    cleanupLost(application);
                } catch (Exception flinkException) {
                    // query status from yarn rest api
                    try {
                        getStateFromYarn(application);
                        cleanupLost(application);
                    } catch (Exception yarnException) {
                        doStateFailed(application);
                    }
                }
            });
    }

    private void cleanupLost(FlinkApplication application) {
        LOST_CACHE.invalidate(application.getId());
    }

    private void doStateFailed(FlinkApplication application) {
        /*
         * Query from flink's restAPI and yarn's restAPI both failed. In this case, it is necessary to decide whether to
         * return to the final state depending on the state being operated
         */
        StopFromEnum stopFrom = getStopFrom(application);
        OptionStateEnum optionState = OPTIONING.get(application.getId());
        if (optionState == null || !optionState.equals(OptionStateEnum.STARTING)) {
            // non-mapping
            if (application.getState() != FlinkAppStateEnum.MAPPING.getValue()) {
                log.error(
                    "[StreamPark][FlinkAppHttpWatcher] getFromFlinkRestApi and getFromYarnRestApi error,job failed,savepoint expired!");
                if (StopFromEnum.NONE.equals(stopFrom)) {
                    Date lostTime = LOST_CACHE.getIfPresent(application.getId());
                    if (lostTime == null) {
                        LOST_CACHE.put(application.getId(), new Date());
                    } else if (DateUtils.toSecondDuration(lostTime, new Date()) >= 30) {
                        savepointService.expire(application.getId());
                        application.setState(FlinkAppStateEnum.LOST.getValue());
                        WATCHING_APPS.remove(application.getId());
                        LOST_CACHE.invalidate(application.getId());
                    }
                } else {
                    application.setState(FlinkAppStateEnum.CANCELED.getValue());
                }
            }
            /*
             * This step means that the above two ways to get information have failed, and this step is the last step,
             * which will directly identify the mission as cancelled or lost. Need clean savepoint.
             */
            application.setEndTime(new Date());
            cleanSavepoint(application);
            cleanOptioning(optionState, application.getId());
            doPersistMetrics(application, true);
            FlinkAppStateEnum appState = application.getStateEnum();
            if (appState.equals(FlinkAppStateEnum.FAILED) || appState.equals(FlinkAppStateEnum.LOST)) {
                doAlert(application, appState);
                if (appState.equals(FlinkAppStateEnum.FAILED)) {
                    try {
                        applicationActionService.start(application, true);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * Get the current task running status information from flink restapi
     *
     * @param application application
     */
    private void getStateFromFlink(FlinkApplication application) throws Exception {
        StopFromEnum stopFrom = getStopFrom(application);
        JobsOverview jobsOverview = httpJobsOverview(application);
        Optional<JobsOverview.Job> optional;
        FlinkDeployMode deployMode = application.getDeployModeEnum();
        if (FlinkDeployMode.YARN_APPLICATION.equals(deployMode)
            || FlinkDeployMode.YARN_PER_JOB.equals(deployMode)) {
            if (jobsOverview.getJobs() != null) {
                optional =
                    jobsOverview.getJobs().size() > 1
                        ? jobsOverview.getJobs().stream()
                            .filter(a -> StringUtils.equals(application.getJobId(), a.getId()))
                            .findFirst()
                        : jobsOverview.getJobs().stream().findFirst();
            } else {
                optional = Optional.empty();
            }
        } else {
            optional =
                jobsOverview.getJobs().stream()
                    .filter(x -> x.getId().equals(application.getJobId()))
                    .findFirst();
        }

        if (optional.isPresent()) {
            JobsOverview.Job jobOverview = optional.get();
            FlinkAppStateEnum currentState = FlinkAppStateEnum.getState(jobOverview.getState());

            if (!FlinkAppStateEnum.OTHER.equals(currentState)) {
                try {
                    // 1) set info from JobOverview
                    handleJobOverview(application, jobOverview);
                } catch (Exception e) {
                    log.error("get flink jobOverview error: {}", e.getMessage(), e);
                }
                try {
                    // 2) CheckPoints
                    handleCheckPoints(application);
                } catch (Exception e) {
                    log.error("get flink jobOverview error: {}", e.getMessage(), e);
                }
                // 3) savepoint obsolete check and NEED_START check
                OptionStateEnum optionState = OPTIONING.get(application.getId());
                if (currentState.equals(FlinkAppStateEnum.RUNNING)) {
                    handleRunningState(application, optionState, currentState);
                } else {
                    handleNotRunState(application, optionState, currentState, stopFrom);
                }
            }
        }
    }

    private StopFromEnum getStopFrom(FlinkApplication application) {
        return STOP_FROM_MAP.getOrDefault(application.getId(), null) == null
            ? StopFromEnum.NONE
            : STOP_FROM_MAP.get(application.getId());
    }

    /**
     * handle job overview
     *
     * @param application application
     * @param jobOverview jobOverview
     */
    private void handleJobOverview(FlinkApplication application, JobsOverview.Job jobOverview) throws IOException {
        // compute duration
        long startTime = jobOverview.getStartTime();
        long endTime = jobOverview.getEndTime();
        if (application.getStartTime() == null || startTime != application.getStartTime().getTime()) {
            application.setStartTime(new Date(startTime));
        }
        if (endTime != -1) {
            if (application.getEndTime() == null || endTime != application.getEndTime().getTime()) {
                application.setEndTime(new Date(endTime));
            }
        }

        application.setJobId(jobOverview.getId());
        application.setDuration(jobOverview.getDuration());
        application.setTotalTask(jobOverview.getTasks().getTotal());
        application.setOverview(jobOverview.getTasks());

        // get overview info at the first start time
        if (STARTING_CACHE.getIfPresent(application.getId()) != null) {
            Overview override = httpOverview(application);
            if (override != null && override.getSlotsTotal() > 0) {
                application.setTotalTM(override.getTaskmanagers());
                application.setTotalSlot(override.getSlotsTotal());
                application.setAvailableSlot(override.getSlotsAvailable());
            }
            STARTING_CACHE.invalidate(application.getId());
        }
    }

    /** get latest checkpoint */
    private void handleCheckPoints(FlinkApplication application) throws Exception {
        CheckPoints checkPoints = httpCheckpoints(application);
        if (checkPoints != null) {
            checkpointProcessor.process(application, checkPoints);
        }
    }

    /**
     * Handle running task
     *
     * @param application application
     * @param optionState optionState
     * @param currentState currentState
     */
    private void handleRunningState(
                                    FlinkApplication application, OptionStateEnum optionState,
                                    FlinkAppStateEnum currentState) {
        /*
         * if the last recorded state is STARTING and the latest state obtained this time is RUNNING, which means it is
         * the first tracking after restart. Then: the following the job status needs to be updated to the restart
         * status: NEED_RESTART_AFTER_CONF_UPDATE (Need to restart after modified configuration)
         * NEED_RESTART_AFTER_SQL_UPDATE (Need to restart after modified flink sql) NEED_RESTART_AFTER_ROLLBACK (Need to
         * restart after rollback) NEED_RESTART_AFTER_DEPLOY (Need to rollback after deploy)
         */
        Long appId = application.getId();
        if (OptionStateEnum.STARTING.equals(optionState)) {
            FlinkApplication latestApp = WATCHING_APPS.get(appId);
            ReleaseStateEnum releaseState = latestApp.getReleaseState();
            switch (releaseState) {
                case NEED_RESTART:
                case NEED_ROLLBACK:
                    LambdaUpdateWrapper<FlinkApplication> updateWrapper =
                        new LambdaUpdateWrapper<FlinkApplication>()
                            .eq(FlinkApplication::getId, appId)
                            .set(FlinkApplication::getRelease, ReleaseStateEnum.DONE.get());
                    applicationManageService.update(updateWrapper);
                    break;
                default:
                    break;
            }
        }

        // The current state is running, and there is a current task in the savepointCache,
        // indicating that the task is doing savepoint
        if (SAVEPOINT_CACHE.getIfPresent(appId) != null) {
            application.setOptionState(OptionStateEnum.SAVEPOINTING.getValue());
        } else {
            application.setOptionState(OptionStateEnum.NONE.getValue());
        }
        application.setState(currentState.getValue());
        doPersistMetrics(application, false);
        cleanOptioning(optionState, appId);
    }

    private void doPersistMetrics(FlinkApplication application, boolean stopWatch) {
        Long appId = application.getId();
        if (FlinkAppStateEnum.isEndState(application.getState())) {
            application.setOverview(null);
            application.setTotalTM(null);
            application.setTotalSlot(null);
            application.setTotalTask(null);
            application.setAvailableSlot(null);
            application.setJmMemory(null);
            application.setTmMemory(null);
            unWatching(appId);
        } else if (stopWatch) {
            unWatching(appId);
        } else {
            WATCHING_APPS.put(appId, application);
        }

        FlinkStateChangeEvent event = PREVIOUS_STATUS.getIfPresent(appId);
        FlinkStateChangeEvent nowEvent = createStateChangeEvent(application);
        if (!nowEvent.equals(event)) {
            PREVIOUS_STATUS.put(appId, nowEvent);
            applicationManageService.persistMetrics(application);
        }
    }

    /**
     * Handle not running task
     *
     * @param application application
     * @param optionState optionState
     * @param currentState currentState
     * @param stopFrom stopFrom
     */
    private void handleNotRunState(
                                   FlinkApplication application,
                                   OptionStateEnum optionState,
                                   FlinkAppStateEnum currentState,
                                   StopFromEnum stopFrom) throws Exception {
        switch (currentState) {
            case CANCELLING:
                CANCELING_CACHE.put(application.getId(), DEFAULT_FLAG_BYTE);
                cleanSavepoint(application);
                application.setState(currentState.getValue());
                doPersistMetrics(application, false);
                break;
            case CANCELED:
            case FINISHED:
                log.info(
                    "[StreamPark][FlinkAppHttpWatcher] getFromFlinkRestApi, job state {}, stop tracking and delete stopFrom!",
                    currentState.name());
                cleanSavepoint(application);
                application.setState(currentState.getValue());
                if (StopFromEnum.NONE.equals(stopFrom) || applicationInfoService.checkAlter(application)) {
                    if (StopFromEnum.NONE.equals(stopFrom)) {
                        log.info(
                            "[StreamPark][FlinkAppHttpWatcher] getFromFlinkRestApi, job cancel is not form StreamPark,savepoint expired!");
                        savepointService.expire(application.getId());
                    }
                    stopCanceledJob(application.getId());
                    doAlert(application, FlinkAppStateEnum.CANCELED);
                }
                STOP_FROM_MAP.remove(application.getId());
                doPersistMetrics(application, true);
                cleanOptioning(optionState, application.getId());
                break;
            case FAILED:
                cleanSavepoint(application);
                STOP_FROM_MAP.remove(application.getId());
                application.setState(FlinkAppStateEnum.FAILED.getValue());
                doPersistMetrics(application, true);
                doAlert(application, FlinkAppStateEnum.FAILED);
                applicationActionService.start(application, true);
                break;
            case RESTARTING:
                log.info(
                    "[StreamPark][FlinkAppHttpWatcher] getFromFlinkRestApi, job state {},add to starting",
                    currentState.name());
                STARTING_CACHE.put(application.getId(), DEFAULT_FLAG_BYTE);
                break;
            default:
                application.setState(currentState.getValue());
                doPersistMetrics(application, false);
                break;
        }
    }

    /**
     * <strong>Query the job history in yarn, indicating that the task has stopped, and the final
     * status of the task is CANCELED</strong>
     *
     * @param application application
     */
    private void getStateFromYarn(FlinkApplication application) throws Exception {
        log.debug("[StreamPark][FlinkAppHttpWatcher] getFromYarnRestApi starting...");
        StopFromEnum stopFrom = getStopFrom(application);
        OptionStateEnum optionState = OPTIONING.get(application.getId());

        /*
         * If the status of the last time is CANCELING (flink rest server is not closed at the time of getting
         * information) and the status is not obtained this time (flink rest server is closed), the task is considered
         * CANCELED
         */
        Byte flag = CANCELING_CACHE.getIfPresent(application.getId());
        if (flag != null) {
            log.info("[StreamPark][FlinkAppHttpWatcher] previous state: canceling.");
            FlinkAppStateEnum flinkAppState = FlinkAppStateEnum.CANCELED;
            try {
                YarnAppInfo yarnAppInfo = httpYarnAppInfo(application);
                if (yarnAppInfo != null) {
                    String state = yarnAppInfo.getApp().getFinalStatus();
                    flinkAppState = FlinkAppStateEnum.getState(state);
                }
            } finally {
                if (StopFromEnum.NONE.equals(stopFrom)) {
                    log.error(
                        "[StreamPark][FlinkAppHttpWatcher] query previous state was canceling and stopFrom NotFound,savepoint expired!");
                    savepointService.expire(application.getId());
                    if (flinkAppState == FlinkAppStateEnum.KILLED
                        || flinkAppState == FlinkAppStateEnum.FAILED) {
                        doAlert(application, flinkAppState);
                    }
                }
                application.setState(flinkAppState.getValue());
                cleanSavepoint(application);
                cleanOptioning(optionState, application.getId());
                doPersistMetrics(application, true);
            }

        } else {
            // query the status from the yarn rest Api
            YarnAppInfo yarnAppInfo = httpYarnAppInfo(application);
            if (yarnAppInfo == null) {
                if (!FlinkDeployMode.REMOTE.equals(application.getDeployModeEnum())) {
                    throw new RuntimeException(
                        "[StreamPark][FlinkAppHttpWatcher] getFromYarnRestApi failed ");
                }
            } else {
                try {
                    String state = yarnAppInfo.getApp().getFinalStatus();
                    FlinkAppStateEnum flinkAppState = FlinkAppStateEnum.getState(state);
                    if (FlinkAppStateEnum.OTHER.equals(flinkAppState)) {
                        return;
                    }
                    if (FlinkAppStateEnum.KILLED.equals(flinkAppState)) {
                        if (StopFromEnum.NONE.equals(stopFrom)) {
                            log.error(
                                "[StreamPark][FlinkAppHttpWatcher] getFromYarnRestApi,job was killed and stopFrom NotFound,savepoint expired!");
                            savepointService.expire(application.getId());
                        }
                        flinkAppState = FlinkAppStateEnum.CANCELED;
                        cleanSavepoint(application);
                        application.setEndTime(new Date());
                    }
                    if (FlinkAppStateEnum.SUCCEEDED.equals(flinkAppState)) {
                        flinkAppState = FlinkAppStateEnum.FINISHED;
                    }
                    application.setState(flinkAppState.getValue());
                    cleanOptioning(optionState, application.getId());
                    doPersistMetrics(application, true);
                    if (flinkAppState.equals(FlinkAppStateEnum.FAILED)
                        || flinkAppState.equals(FlinkAppStateEnum.LOST)
                        || (flinkAppState.equals(FlinkAppStateEnum.CANCELED)
                            && StopFromEnum.NONE.equals(stopFrom))
                        || applicationInfoService.checkAlter(application)) {
                        doAlert(application, flinkAppState);
                        stopCanceledJob(application.getId());
                        if (flinkAppState.equals(FlinkAppStateEnum.FAILED)) {
                            applicationActionService.start(application, true);
                        }
                    }
                } catch (Exception e) {
                    if (!FlinkDeployMode.REMOTE.equals(application.getDeployModeEnum())) {
                        throw new RuntimeException(
                            "[StreamPark][FlinkAppHttpWatcher] getFromYarnRestApi error,", e);
                    }
                }
            }
        }
    }

    private void doAlert(FlinkApplication application, FlinkAppStateEnum flinkAppState) {
        AlertTemplate alertTemplate =
            AlertTemplateUtils.createAlertTemplate(application, flinkAppState);
        alertService.alert(application.getAlertId(), alertTemplate);
    }

    private void cleanOptioning(OptionStateEnum optionState, Long key) {
        if (optionState != null) {
            lastOptionTime = System.currentTimeMillis();
            OPTIONING.remove(key);
        }
    }

    public void cleanSavepoint(FlinkApplication application) {
        application.setOptionState(OptionStateEnum.NONE.getValue());
        FlinkStateChangeEvent event = PREVIOUS_STATUS.getIfPresent(application.getId());
        if (event != null && event.getOptionState() == OptionStateEnum.SAVEPOINTING) {
            doPersistMetrics(application, false);
        }
        SAVEPOINT_CACHE.invalidate(application.getId());
    }

    /** set current option state */
    public static void setOptionState(Long appId, OptionStateEnum state) {
        if (isKubernetesApp(appId)) {
            return;
        }
        log.info("[StreamPark][FlinkAppHttpWatcher] setOptioning");
        OPTIONING.put(appId, state);
        if (state.equals(OptionStateEnum.CANCELLING)) {
            STOP_FROM_MAP.put(appId, StopFromEnum.STREAMPARK);
        }
    }

    public static void doWatching(FlinkApplication application) {
        if (application.isKubernetesModeJob()) {
            return;
        }
        log.info("[StreamPark][FlinkAppHttpWatcher] add app to tracking,appId:{}", application.getId());
        WATCHING_APPS.put(application.getId(), application);
        STARTING_CACHE.put(application.getId(), DEFAULT_FLAG_BYTE);
    }

    public static void addSavepoint(Long appId) {
        if (isKubernetesApp(appId)) {
            return;
        }
        log.info("[StreamPark][FlinkAppHttpWatcher] add app to savepoint,appId:{}", appId);
        SAVEPOINT_CACHE.put(appId, DEFAULT_FLAG_BYTE);

        // update to PREVIOUS_STATUS
        FlinkStateChangeEvent event = PREVIOUS_STATUS.getIfPresent(appId);
        if (event != null) {
            event.setOptionState(OptionStateEnum.SAVEPOINTING);
            PREVIOUS_STATUS.put(appId, event);
        }
    }

    public static void unWatching(Long appId) {
        if (isKubernetesApp(appId)) {
            return;
        }
        log.info("[StreamPark][FlinkAppHttpWatcher] stop app,appId:{}", appId);
        WATCHING_APPS.remove(appId);
    }

    public static void stopCanceledJob(Long appId) {
        if (!CANCELLED_JOB_MAP.containsKey(appId)) {
            return;
        }
        log.info("flink job canceled app appId:{} by useId:{}", appId, CANCELLED_JOB_MAP.get(appId));
        CANCELLED_JOB_MAP.remove(appId);
    }

    public static void addCanceledApp(Long appId, Long userId) {
        log.info("flink job addCanceledApp app appId:{}, useId:{}", appId, userId);
        CANCELLED_JOB_MAP.put(appId, userId);
    }

    public static Long getCanceledJobUserId(Long appId) {
        return CANCELLED_JOB_MAP.get(appId) == null ? Long.valueOf(-1) : CANCELLED_JOB_MAP.get(appId);
    }

    public static Collection<FlinkApplication> getWatchingApps() {
        return WATCHING_APPS.values();
    }

    private static boolean isKubernetesApp(Long appId) {
        FlinkApplication app = WATCHING_APPS.get(appId);
        if (app == null) {
            return false;
        }
        return app.isKubernetesModeJob();
    }

    private YarnAppInfo httpYarnAppInfo(FlinkApplication application) throws Exception {
        String reqURL = "ws/v1/cluster/apps/".concat(application.getClusterId());
        return yarnRestRequest(reqURL, YarnAppInfo.class);
    }

    private Overview httpOverview(FlinkApplication application) throws IOException {
        String appId = application.getClusterId();
        if (appId != null) {
            if (application.getDeployModeEnum().equals(FlinkDeployMode.YARN_APPLICATION)
                || application.getDeployModeEnum().equals(FlinkDeployMode.YARN_PER_JOB)) {
                String reqURL;
                if (StringUtils.isEmpty(application.getJobManagerUrl())) {
                    String format = "proxy/%s/overview";
                    reqURL = String.format(format, appId);
                } else {
                    String format = "%s/overview";
                    reqURL = String.format(format, application.getJobManagerUrl());
                }
                return yarnRestRequest(reqURL, Overview.class);
            }
        }
        return null;
    }

    private JobsOverview httpJobsOverview(FlinkApplication application) throws Exception {
        final String flinkUrl = "jobs/overview";
        FlinkDeployMode deployMode = application.getDeployModeEnum();
        if (FlinkDeployMode.isYarnMode(deployMode)) {
            String reqURL;
            if (StringUtils.isEmpty(application.getJobManagerUrl())) {
                String format = "proxy/%s/" + flinkUrl;
                reqURL = String.format(format, application.getClusterId());
            } else {
                String format = "%s/" + flinkUrl;
                reqURL = String.format(format, application.getJobManagerUrl());
            }
            return yarnRestRequest(reqURL, JobsOverview.class);
        }

        if (application.getJobId() != null && FlinkDeployMode.isRemoteMode(deployMode)) {
            return httpRemoteCluster(
                application.getFlinkClusterId(),
                cluster -> {
                    String remoteUrl = cluster.getAddress() + "/" + flinkUrl;
                    JobsOverview jobsOverview = httpRestRequest(remoteUrl, JobsOverview.class);
                    if (jobsOverview != null) {
                        List<JobsOverview.Job> jobs =
                            jobsOverview.getJobs().stream()
                                .filter(x -> x.getId().equals(application.getJobId()))
                                .collect(Collectors.toList());
                        jobsOverview.setJobs(jobs);
                    }
                    return jobsOverview;
                });
        }
        return null;
    }

    private CheckPoints httpCheckpoints(FlinkApplication application) throws Exception {
        final String flinkUrl = "jobs/%s/checkpoints";
        FlinkDeployMode deployMode = application.getDeployModeEnum();
        if (FlinkDeployMode.isYarnMode(deployMode)) {
            String reqURL;
            if (StringUtils.isEmpty(application.getJobManagerUrl())) {
                String format = "proxy/%s/" + flinkUrl;
                reqURL = String.format(format, application.getClusterId(), application.getJobId());
            } else {
                String format = "%s/" + flinkUrl;
                reqURL = String.format(format, application.getJobManagerUrl(), application.getJobId());
            }
            return yarnRestRequest(reqURL, CheckPoints.class);
        }

        if (application.getJobId() != null && FlinkDeployMode.isRemoteMode(deployMode)) {
            return httpRemoteCluster(
                application.getFlinkClusterId(),
                cluster -> {
                    String remoteUrl =
                        cluster.getAddress() + "/" + String.format(flinkUrl, application.getJobId());
                    return httpRestRequest(remoteUrl, CheckPoints.class);
                });
        }
        return null;
    }

    private <T> T yarnRestRequest(String url, Class<T> clazz) throws IOException {
        String result = YarnUtils.restRequest(url, HTTP_TIMEOUT);
        return JacksonUtils.read(result, clazz);
    }

    private <T> T httpRestRequest(String url, Class<T> clazz) throws IOException {
        String result =
            HttpClientUtils.httpGetRequest(
                url, RequestConfig.custom().setConnectTimeout(HTTP_TIMEOUT).build());
        if (null == result) {
            return null;
        }
        return JacksonUtils.read(result, clazz);
    }

    public boolean isWatchingApp(Long id) {
        return WATCHING_APPS.containsKey(id);
    }

    private <T> T httpRemoteCluster(Long clusterId, Callback<FlinkCluster, T> function) throws Exception {
        FlinkCluster flinkCluster = getFlinkRemoteCluster(clusterId, false);
        try {
            return function.call(flinkCluster);
        } catch (Exception e) {
            flinkCluster = getFlinkRemoteCluster(clusterId, true);
            return function.call(flinkCluster);
        }
    }

    private FlinkCluster getFlinkRemoteCluster(Long clusterId, boolean flush) {
        FlinkCluster flinkCluster = FLINK_CLUSTER_MAP.get(clusterId);
        if (flinkCluster == null || flush) {
            flinkCluster = flinkClusterService.getById(clusterId);
            FLINK_CLUSTER_MAP.put(clusterId, flinkCluster);
        }
        return flinkCluster;
    }

    interface Callback<T, R> {

        R call(T e) throws Exception;
    }

    public static FlinkStateChangeEvent createStateChangeEvent(FlinkApplication application) {
        FlinkStateChangeEvent event = new FlinkStateChangeEvent();
        event.setId(application.getId());
        event.setOptionState(OptionStateEnum.getState(application.getOptionState()));
        event.setAppState(application.getStateEnum());
        event.setJobId(application.getJobId());
        event.setJobManagerUrl(application.getJobManagerUrl());
        return event;
    }
}
