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

import org.apache.streampark.common.util.HadoopUtils;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.core.bean.AlertTemplate;
import org.apache.streampark.console.core.entity.SparkApplication;
import org.apache.streampark.console.core.enums.SparkAppStateEnum;
import org.apache.streampark.console.core.enums.SparkOptionStateEnum;
import org.apache.streampark.console.core.enums.StopFromEnum;
import org.apache.streampark.console.core.metrics.spark.Job;
import org.apache.streampark.console.core.metrics.spark.SparkApplicationSummary;
import org.apache.streampark.console.core.metrics.yarn.YarnAppInfo;
import org.apache.streampark.console.core.service.alert.AlertService;
import org.apache.streampark.console.core.service.application.SparkApplicationActionService;
import org.apache.streampark.console.core.service.application.SparkApplicationInfoService;
import org.apache.streampark.console.core.service.application.SparkApplicationManageService;
import org.apache.streampark.console.core.util.AlertTemplateUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hc.core5.util.Timeout;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class SparkAppHttpWatcher {

    @Autowired
    private SparkApplicationManageService applicationManageService;

    @Autowired
    private SparkApplicationActionService applicationActionService;

    @Autowired
    private SparkApplicationInfoService applicationInfoService;

    @Autowired
    private AlertService alertService;

    @Qualifier("sparkRestAPIWatchingExecutor")
    @Autowired
    private Executor executorService;

    // track interval every 5 seconds
    public static final Duration WATCHING_INTERVAL = Duration.ofSeconds(5);

    // option interval within 10 seconds
    private static final Duration OPTION_INTERVAL = Duration.ofSeconds(10);

    private static final Timeout HTTP_TIMEOUT = Timeout.ofSeconds(5);

    /**
     * Record the status of the first tracking task, because after the task is started, the overview
     * of the task will be obtained during the first tracking
     */
    private static final Cache<Long, Byte> STARTING_CACHE =
        Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

    /** tracking task list */
    private static final Map<Long, SparkApplication> WATCHING_APPS = new ConcurrentHashMap<>(0);

    /**
     *
     *
     * <pre>
     * StopFrom: Recording spark application stopped by streampark or stopped by other actions
     * </pre>
     */
    private static final Map<Long, StopFromEnum> STOP_FROM_MAP = new ConcurrentHashMap<>(0);

    /**
     * Task canceled tracking list, record who cancelled the tracking task Map<applicationId,userId>
     */
    private static final Map<Long, Long> CANCELLED_JOB_MAP = new ConcurrentHashMap<>(0);

    private static final Map<Long, SparkOptionStateEnum> OPTIONING = new ConcurrentHashMap<>(0);

    private Long lastWatchTime = 0L;

    private Long lastOptionTime = 0L;

    private static final Byte DEFAULT_FLAG_BYTE = Byte.valueOf("0");

    @PostConstruct
    public void init() {
        WATCHING_APPS.clear();
        List<SparkApplication> applications = applicationManageService.list(
            new LambdaQueryWrapper<SparkApplication>()
                .eq(SparkApplication::getTracking, 1)
                .ne(SparkApplication::getState, SparkAppStateEnum.LOST.getValue()));

        applications.forEach(app -> {
            Long appId = app.getId();
            WATCHING_APPS.put(appId, app);
            STARTING_CACHE.put(appId, DEFAULT_FLAG_BYTE);
        });
    }

    @PreDestroy
    public void doStop() {
        log.info(
            "[StreamPark][SparkAppHttpWatcher] StreamPark Console will be shutdown, persistent application to database.");
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

    @VisibleForTesting
    public @Nullable SparkAppStateEnum tryQuerySparkAppState(@Nonnull Long appId) {
        SparkApplication app = WATCHING_APPS.get(appId);
        return (app == null || app.getState() == null) ? null : app.getStateEnum();
    }

    private void watch(Long id, SparkApplication application) {
        executorService.execute(
            () -> {
                try {
                    getStateFromYarn(application);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
    }

    private StopFromEnum getAppStopFrom(Long appId) {
        return STOP_FROM_MAP.getOrDefault(appId, StopFromEnum.NONE);
    }

    /**
     * Query the job state from yarn and query the resource usage from spark when job state is RUNNING
     *
     * @param application spark application
     */
    private void getStateFromYarn(SparkApplication application) throws Exception {
        SparkOptionStateEnum optionStateEnum = OPTIONING.get(application.getId());

        // query the status from the yarn rest Api
        YarnAppInfo yarnAppInfo = httpYarnAppInfo(application);
        if (yarnAppInfo == null) {
            throw new RuntimeException("[StreamPark][SparkAppHttpWatcher] getStateFromYarn failed!");
        } else {
            try {
                String state = yarnAppInfo.getApp().getState();
                FinalApplicationStatus appFinalStatus =
                    HadoopUtils.toYarnFinalStatus(yarnAppInfo.getApp().getFinalStatus());
                SparkAppStateEnum sparkAppStateEnum = SparkAppStateEnum.of(state);
                if (SparkAppStateEnum.OTHER == sparkAppStateEnum) {
                    return;
                }
                if (SparkAppStateEnum.isEndState(sparkAppStateEnum.getValue())) {
                    log.info(
                        "[StreamPark][SparkAppHttpWatcher] getStateFromYarn, app {} was ended, appId is {}, state is {}",
                        application.getId(),
                        application.getClusterId(),
                        sparkAppStateEnum);
                    application.setEndTime(new Date());
                    if (appFinalStatus.equals(FinalApplicationStatus.FAILED)) {
                        sparkAppStateEnum = SparkAppStateEnum.FAILED;
                    }
                }
                if (SparkAppStateEnum.RUNNING == sparkAppStateEnum) {
                    if (application.getStartTime() != null
                        && application.getStartTime().getTime() > 0) {
                        application.setDuration(System.currentTimeMillis() - application.getStartTime().getTime());
                    }
                    SparkApplicationSummary summary;
                    try {
                        summary = httpStageAndTaskStatus(application);
                        summary.setUsedMemory(Long.parseLong(yarnAppInfo.getApp().getAllocatedMB()));
                        summary.setUsedVCores(Long.parseLong(yarnAppInfo.getApp().getAllocatedVCores()));
                        application.fillRunningMetrics(summary);
                    } catch (IOException e) {
                        // This may happen when the job is finished right after the job status is abtained from
                        // yarn.
                        log.warn(
                            "[StreamPark][SparkAppHttpWatcher] getStateFromYarn, fetch spark job status failed. The job may have already been finished.");
                    }
                }
                application.setState(sparkAppStateEnum.getValue());
                cleanOptioning(optionStateEnum, application.getId());
                doPersistMetrics(application, false);
                if (SparkAppStateEnum.FAILED == sparkAppStateEnum
                    || SparkAppStateEnum.LOST == sparkAppStateEnum
                    || applicationInfoService.checkAlter(application)) {
                    doAlert(application, sparkAppStateEnum);
                    if (SparkAppStateEnum.FAILED == sparkAppStateEnum) {
                        applicationActionService.start(application, true);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("[StreamPark][SparkAppHttpWatcher] getStateFromYarn failed!", e);
            }
        }
    }

    private void doPersistMetrics(SparkApplication application, boolean stopWatch) {
        if (SparkAppStateEnum.isEndState(application.getState())) {
            application.setUsedMemory(null);
            application.setUsedVCores(null);
            application.setNumTasks(null);
            application.setNumCompletedTasks(null);
            application.setNumStages(null);
            application.setNumCompletedStages(null);
            unWatching(application.getId());
        } else if (stopWatch) {
            unWatching(application.getId());
        } else {
            WATCHING_APPS.put(application.getId(), application);
        }
        applicationManageService.persistMetrics(application);
    }

    private void cleanOptioning(SparkOptionStateEnum optionStateEnum, Long key) {
        if (optionStateEnum != null) {
            lastOptionTime = System.currentTimeMillis();
            OPTIONING.remove(key);
        }
    }

    /** set current option state */
    public static void setOptionState(Long appId, SparkOptionStateEnum state) {
        log.info("[StreamPark][SparkAppHttpWatcher] setOptioning");
        OPTIONING.put(appId, state);
        if (SparkOptionStateEnum.STOPPING == state) {
            STOP_FROM_MAP.put(appId, StopFromEnum.STREAMPARK);
        }
    }

    public static void doWatching(SparkApplication application) {
        log.info(
            "[StreamPark][SparkAppHttpWatcher] add app to tracking, appId:{}", application.getId());
        WATCHING_APPS.put(application.getId(), application);
        STARTING_CACHE.put(application.getId(), DEFAULT_FLAG_BYTE);
    }

    public static void unWatching(Long appId) {
        log.info("[StreamPark][SparkAppHttpWatcher] stop app, appId:{}", appId);
        WATCHING_APPS.remove(appId);
    }

    public static void addCanceledApp(Long appId, Long userId) {
        log.info(
            "[StreamPark][SparkAppHttpWatcher] addCanceledApp app appId:{}, useId:{}", appId, userId);
        CANCELLED_JOB_MAP.put(appId, userId);
    }

    public static Long getCanceledJobUserId(Long appId) {
        return CANCELLED_JOB_MAP.get(appId) == null ? Long.valueOf(-1) : CANCELLED_JOB_MAP.get(appId);
    }

    public static Collection<SparkApplication> getWatchingApps() {
        return WATCHING_APPS.values();
    }

    private YarnAppInfo httpYarnAppInfo(SparkApplication application) throws Exception {
        String reqURL = "ws/v1/cluster/apps/".concat(application.getClusterId());
        return yarnRestRequest(reqURL, YarnAppInfo.class);
    }
    private Job[] httpJobsStatus(SparkApplication application) throws IOException {
        String format = "proxy/%s/api/v1/applications/%s/jobs";
        String reqURL = String.format(format, application.getClusterId(), application.getClusterId());
        return yarnRestRequest(reqURL, Job[].class);
    }

    /**
     * Calculate spark stage and task metric from yarn rest api. Only available when yarn application
     * status is RUNNING.
     *
     * @param application
     * @return task progress
     * @throws Exception
     */
    private SparkApplicationSummary httpStageAndTaskStatus(SparkApplication application) throws IOException {
        Job[] jobs = httpJobsStatus(application);
        SparkApplicationSummary summary = new SparkApplicationSummary(0L, 0L, 0L, 0L, null, null);
        if (jobs == null) {
            return summary;
        }
        Arrays.stream(jobs)
            .forEach(
                job -> {
                    summary.setNumTasks(job.getNumTasks() + summary.getNumTasks());
                    summary.setNumCompletedTasks(
                        job.getNumCompletedTasks() + summary.getNumCompletedTasks());
                    summary.setNumStages(job.getStageIds().size() + summary.getNumStages());
                    summary.setNumStages(job.getNumCompletedStages() + summary.getNumCompletedStages());
                });
        return summary;
    }

    private <T> T yarnRestRequest(String url, Class<T> clazz) throws IOException {
        String result = YarnUtils.restRequest(url, HTTP_TIMEOUT);
        if (null == result) {
            return null;
        }
        return JacksonUtils.read(result, clazz);
    }

    public boolean isWatchingApp(Long id) {
        return WATCHING_APPS.containsKey(id);
    }

    /**
     * Describes the alarming behavior under abnormal operation for jobs running in yarn mode.
     *
     * @param application spark application
     * @param appState spark application state
     */
    private void doAlert(SparkApplication application, SparkAppStateEnum appState) {
        AlertTemplate alertTemplate = AlertTemplateUtils.createAlertTemplate(application, appState);
        alertService.alert(application.getAlertId(), alertTemplate);
    }
}
