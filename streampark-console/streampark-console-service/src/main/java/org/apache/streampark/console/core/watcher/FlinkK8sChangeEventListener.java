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
import org.apache.streampark.console.core.component.FlinkCheckpointProcessor;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.enums.FlinkAppStateEnum;
import org.apache.streampark.console.core.enums.OptionStateEnum;
import org.apache.streampark.console.core.metrics.flink.CheckPoints;
import org.apache.streampark.console.core.service.alert.AlertService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.core.util.AlertTemplateUtils;
import org.apache.streampark.flink.kubernetes.enums.FlinkJobState;
import org.apache.streampark.flink.kubernetes.enums.FlinkKubernetesDeployMode;
import org.apache.streampark.flink.kubernetes.event.FlinkClusterMetricChangeEvent;
import org.apache.streampark.flink.kubernetes.event.FlinkJobCheckpointChangeEvent;
import org.apache.streampark.flink.kubernetes.event.FlinkJobStatusChangeEvent;
import org.apache.streampark.flink.kubernetes.model.FlinkMetricCV;
import org.apache.streampark.flink.kubernetes.model.JobStatusCV;
import org.apache.streampark.flink.kubernetes.model.TrackId;
import org.apache.streampark.flink.kubernetes.watcher.FlinkJobStatusWatcher;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.Executor;

import static org.apache.streampark.console.core.enums.FlinkAppStateEnum.Bridge.fromK8sFlinkJobState;
import static org.apache.streampark.console.core.enums.FlinkAppStateEnum.Bridge.toK8sFlinkJobState;

/**
 * Event Listener for K8sFlinkTrackMonitor。
 *
 * <p>Use FlinkK8sChangeListenerV2 listeners instead:
 *
 * @link org.apache.streampark.console.core.watcher.FlinkK8sChangeListenerV2
 */
@Slf4j
@Component
public class FlinkK8sChangeEventListener {

    @Lazy
    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Lazy
    @Autowired
    private AlertService alertService;

    @Lazy
    @Autowired
    private FlinkCheckpointProcessor checkpointProcessor;

    @Qualifier("streamparkNotifyExecutor")
    @Autowired
    private Executor executor;

    /**
     * Catch FlinkJobStatusChangeEvent then storage it persistently to db. Actually update
     * org.apache.streampark.console.core.entity.Application records.
     */
    @SuppressWarnings("UnstableApiUsage")
    @AllowConcurrentEvents
    @Subscribe
    public void subscribeJobStatusChange(FlinkJobStatusChangeEvent event) {
        JobStatusCV jobStatus = event.jobStatus();
        TrackId trackId = event.trackId();
        // get pre application record
        FlinkApplication app = applicationManageService.getById(trackId.appId());
        if (app == null) {
            return;
        }
        // update application record
        setByJobStatusCV(app, jobStatus);
        applicationManageService.persistMetrics(app);

        // email alerts when necessary
        FlinkAppStateEnum state = app.getStateEnum();
        if (FlinkAppStateEnum.FAILED == state
            || FlinkAppStateEnum.LOST == state
            || FlinkAppStateEnum.RESTARTING == state
            || FlinkAppStateEnum.FINISHED == state) {
            executor.execute(
                () -> alertService.alert(app.getAlertId(), AlertTemplateUtils.createAlertTemplate(app, state)));
        }
    }

    /**
     * Catch FlinkClusterMetricChangeEvent then storage it persistently to db. Actually update
     * org.apache.streampark.console.core.entity.Application records.
     */
    @SuppressWarnings("UnstableApiUsage")
    @AllowConcurrentEvents
    @Subscribe
    public void subscribeMetricsChange(FlinkClusterMetricChangeEvent event) {
        TrackId trackId = event.trackId();
        FlinkDeployMode mode = FlinkKubernetesDeployMode.toFlinkDeployMode(trackId.executeMode());
        // discard session mode change
        if (FlinkDeployMode.KUBERNETES_NATIVE_SESSION == mode) {
            return;
        }

        FlinkApplication app = applicationManageService.getById(trackId.appId());
        if (app == null) {
            return;
        }

        FlinkMetricCV metrics = event.metrics();
        app.setJmMemory(metrics.totalJmMemory());
        app.setTmMemory(metrics.totalTmMemory());
        app.setTotalTM(metrics.totalTm());
        app.setTotalSlot(metrics.totalSlot());
        app.setAvailableSlot(metrics.availableSlot());

        applicationManageService.persistMetrics(app);
    }

    @SuppressWarnings("UnstableApiUsage")
    @AllowConcurrentEvents
    @Subscribe
    public void subscribeCheckpointChange(FlinkJobCheckpointChangeEvent event) {
        CheckPoints.CheckPoint completed = new CheckPoints.CheckPoint();
        completed.setId(event.checkpoint().id());
        completed.setCheckpointType(event.checkpoint().checkpointType());
        completed.setExternalPath(event.checkpoint().externalPath());
        completed.setIsSavepoint(event.checkpoint().isSavepoint());
        completed.setStatus(event.checkpoint().status());
        completed.setTriggerTimestamp(event.checkpoint().triggerTimestamp());

        CheckPoints.Latest latest = new CheckPoints.Latest();
        latest.setCompleted(completed);
        CheckPoints checkPoint = new CheckPoints();
        checkPoint.setLatest(latest);

        checkpointProcessor.process(
            applicationManageService.getById(event.trackId().appId()), checkPoint);
    }

    private void setByJobStatusCV(FlinkApplication app, JobStatusCV jobStatus) {
        FlinkJobState state =
            FlinkJobStatusWatcher.inferFlinkJobStateFromPersist(
                jobStatus.jobState(), toK8sFlinkJobState(app.getStateEnum()));

        // corrective start-time / end-time / duration
        long preStartTime = app.getStartTime() != null ? app.getStartTime().getTime() : 0;
        long startTime = Math.max(jobStatus.jobStartTime(), preStartTime);
        long preEndTime = app.getEndTime() != null ? app.getEndTime().getTime() : 0;
        long endTime = Math.max(jobStatus.jobEndTime(), preEndTime);
        long duration = jobStatus.duration();

        if (FlinkJobState.isEndState(state)) {
            if (endTime < startTime) {
                endTime = System.currentTimeMillis();
            }
            if (duration <= 0) {
                duration = endTime - startTime;
            }
        }

        app.setState(fromK8sFlinkJobState(state).getValue());
        app.setJobId(jobStatus.jobId());
        app.setTotalTask(jobStatus.taskTotal());

        app.setStartTime(new Date(startTime > 0 ? startTime : 0));
        app.setEndTime(endTime > 0 && endTime >= startTime ? new Date(endTime) : null);
        app.setDuration(duration > 0 ? duration : 0);
        // A non-terminal event can race with an in-flight cancellation. Keep the operation state
        // until the watcher observes a terminal job state.
        if (state != FlinkJobState.CANCELLING()) {
            app.setOptionState(OptionStateEnum.NONE.getValue());
        }
    }
}
