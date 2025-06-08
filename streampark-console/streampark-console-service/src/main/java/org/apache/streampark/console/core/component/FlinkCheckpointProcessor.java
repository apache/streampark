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

package org.apache.streampark.console.core.component;

import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkSavepoint;
import org.apache.streampark.console.core.enums.CheckPointStatusEnum;
import org.apache.streampark.console.core.enums.FailoverStrategyEnum;
import org.apache.streampark.console.core.metrics.flink.CheckPoints;
import org.apache.streampark.console.core.service.SavepointService;
import org.apache.streampark.console.core.service.alert.AlertService;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.util.AlertTemplateUtils;
import org.apache.streampark.console.core.watcher.FlinkAppHttpWatcher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class FlinkCheckpointProcessor {

    private static final Byte DEFAULT_FLAG_BYTE = Byte.valueOf("0");
    private static final Integer SAVEPOINT_CACHE_HOUR = 1;

    private final Cache<String, Long> checkPointCache = Caffeine.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
        .build();

    /**
     * Cache to store the savepoint if be stored in the db. Use the {appId}_{jobID}_{chkId} from
     * {@link CheckPointKey#getSavePointId()} as the cache key to save the trace of the savepoint. And
     * try best to make sure the every savepoint would be stored into DB. Especially for the case
     * 'maxConcurrent of Checkpoint' > 1: 1. savepoint(n-1) is completed after completed
     * checkpoint(n); 2. savepoint(n-1) is completed after completed savepoint(n).
     */
    private final Cache<String, Byte> savepointedCache = Caffeine.newBuilder()
        .expireAfterWrite(SAVEPOINT_CACHE_HOUR, TimeUnit.HOURS).build();

    private final Map<Long, Counter> checkPointFailedCache = new ConcurrentHashMap<>(0);

    @Autowired
    private FlinkApplicationActionService applicationActionService;

    @Autowired
    private AlertService alertService;

    @Autowired
    private SavepointService savepointService;

    @Autowired
    private FlinkAppHttpWatcher flinkAppHttpWatcher;

    public void process(FlinkApplication application, @Nonnull CheckPoints checkPoints) {
        checkPoints.getLatestCheckpoint().forEach(checkPoint -> process(application, checkPoint));
    }

    private void process(FlinkApplication application, @Nonnull CheckPoints.CheckPoint checkPoint) {
        String jobID = application.getJobId();
        Long appId = application.getId();
        CheckPointStatusEnum status = checkPoint.getCheckPointStatus();
        CheckPointKey checkPointKey = new CheckPointKey(appId, jobID, checkPoint.getId());

        if (CheckPointStatusEnum.COMPLETED == status) {
            if (shouldStoreAsSavepoint(checkPointKey, checkPoint)) {
                savepointedCache.put(checkPointKey.getSavePointId(), DEFAULT_FLAG_BYTE);
                saveSavepoint(checkPoint, application.getId());
                flinkAppHttpWatcher.cleanSavepoint(application);
                return;
            }

            Long latestChkId = getLatestCheckpointedId(appId, checkPointKey.getCheckPointId());
            if (shouldStoreAsCheckpoint(checkPoint, latestChkId)) {
                checkPointCache.put(checkPointKey.getCheckPointId(), checkPoint.getId());
                saveSavepoint(checkPoint, application.getId());
            }
        } else if (shouldProcessFailedTrigger(checkPoint, application.cpFailedTrigger(), status)) {
            processFailedCheckpoint(application, checkPoint, appId);
        }
    }

    private void processFailedCheckpoint(
                                         FlinkApplication application, @Nonnull CheckPoints.CheckPoint checkPoint,
                                         Long appId) {
        Counter counter = checkPointFailedCache.get(appId);
        Long ckTriggerTimestamp = checkPoint.getTriggerTimestamp();
        Integer cpFailureRateInterval = application.getCpFailureRateInterval();
        if (counter == null || counter.getDuration(ckTriggerTimestamp) >= cpFailureRateInterval) {
            checkPointFailedCache.put(appId, new Counter(ckTriggerTimestamp));
            return;
        }

        long minute = counter.getDuration(ckTriggerTimestamp);
        if (minute > cpFailureRateInterval || counter.getCount() < application.getCpMaxFailureInterval()) {
            counter.increment();
            return;
        }
        checkPointFailedCache.remove(appId);
        FailoverStrategyEnum failoverStrategyEnum = FailoverStrategyEnum.of(application.getCpFailureAction());
        AssertUtils.required(
            failoverStrategyEnum != null,
            "Unexpected cpFailureAction: " + application.getCpFailureAction());
        processFailoverStrategy(application, failoverStrategyEnum);
    }

    private void processFailoverStrategy(
                                         FlinkApplication application, FailoverStrategyEnum failoverStrategyEnum) {
        switch (failoverStrategyEnum) {
            case ALERT:
                alertService.alert(
                    application.getAlertId(),
                    AlertTemplateUtils.createAlertTemplate(application, CheckPointStatusEnum.FAILED));
                break;
            case RESTART:
                try {
                    applicationActionService.restart(application);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                break;
            default:
                // do nothing
                break;
        }
    }

    private static boolean shouldStoreAsCheckpoint(
                                                   @Nonnull CheckPoints.CheckPoint checkPoint, Long latestId) {
        return !checkPoint.getIsSavepoint() && (latestId == null || latestId < checkPoint.getId());
    }

    private boolean shouldStoreAsSavepoint(
                                           CheckPointKey checkPointKey, @Nonnull CheckPoints.CheckPoint checkPoint) {
        if (!checkPoint.getIsSavepoint()) {
            return false;
        }
        return savepointedCache.getIfPresent(checkPointKey.getSavePointId()) == null
            // If the savepoint triggered before SAVEPOINT_CACHE_HOUR span, we'll see it as out-of-time
            // savepoint and ignore it.
            && checkPoint.getTriggerTimestamp() >= System.currentTimeMillis()
                - TimeUnit.HOURS.toMillis(SAVEPOINT_CACHE_HOUR);
    }

    @Nullable
    private Long getLatestCheckpointedId(Long appId, String cacheId) {
        return checkPointCache.get(
            cacheId,
            key -> {
                FlinkSavepoint savepoint = savepointService.getLatest(appId);
                return Optional.ofNullable(savepoint).map(FlinkSavepoint::getChkId).orElse(null);
            });
    }

    private boolean shouldProcessFailedTrigger(
                                               CheckPoints.CheckPoint checkPoint, boolean cpFailedTrigger,
                                               CheckPointStatusEnum status) {
        return CheckPointStatusEnum.FAILED == status && !checkPoint.getIsSavepoint() && cpFailedTrigger;
    }

    private void saveSavepoint(CheckPoints.CheckPoint checkPoint, Long appId) {
        FlinkSavepoint savepoint = new FlinkSavepoint();
        savepoint.setAppId(appId);
        savepoint.setChkId(checkPoint.getId());
        savepoint.setLatest(true);
        savepoint.setType(checkPoint.getCheckPointType().get());
        savepoint.setPath(checkPoint.getExternalPath());
        savepoint.setTriggerTime(new Date(checkPoint.getTriggerTimestamp()));
        savepoint.setCreateTime(new Date());
        savepointService.save(savepoint);
    }

    public static class Counter {

        private final Long timestamp;
        private final AtomicInteger count;

        public Counter(Long timestamp) {
            this.timestamp = timestamp;
            this.count = new AtomicInteger(1);
        }

        public void increment() {
            this.count.incrementAndGet();
        }

        public Integer getCount() {
            return count.get();
        }

        public long getDuration(Long currentTimestamp) {
            return (currentTimestamp - this.timestamp) / 1000 / 60;
        }
    }

    /** Util class for checkpoint key. */
    @Getter
    @Setter
    public static class CheckPointKey {

        private Long appId;
        private String jobId;
        private Long checkId;

        public CheckPointKey(Long appId, String jobId, Long checkId) {
            this.appId = appId;
            this.jobId = jobId;
            this.checkId = checkId;
        }

        /** Get savepoint cache id, see {@link #savepointedCache}. */
        public String getSavePointId() {
            return String.format("%s_%s_%s", appId, jobId, checkId);
        }

        /** Get checkpoint cache id. */
        public String getCheckPointId() {
            return String.format("%s_%s", appId, jobId);
        }
    }
}
