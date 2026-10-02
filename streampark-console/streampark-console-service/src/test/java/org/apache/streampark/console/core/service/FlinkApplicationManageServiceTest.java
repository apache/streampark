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

package org.apache.streampark.console.core.service;

import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.console.SpringUnitTestBase;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.YarnQueue;
import org.apache.streampark.console.core.enums.FlinkAppStateEnum;
import org.apache.streampark.console.core.enums.OptionStateEnum;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.core.service.application.impl.FlinkApplicationManageServiceImpl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/** org.apache.streampark.console.core.service.ApplicationServiceUnitTest. */
class FlinkApplicationManageServiceTest extends SpringUnitTestBase {

    @Autowired
    private FlinkApplicationManageService applicationManageService;
    @Autowired
    private FlinkApplicationActionService applicationActionService;
    @Autowired
    private YarnQueueService yarnQueueService;

    @AfterEach
    void cleanTestRecordsInDatabase() {
        applicationManageService.remove(new QueryWrapper<>());
        yarnQueueService.remove(new QueryWrapper<>());
    }

    @Test
    void testRevoke() {
        Date now = new Date();
        FlinkApplication app = new FlinkApplication();
        app.setId(100001L);
        app.setJobType(1);
        app.setUserId(100000L);
        app.setJobName("socket-test");
        app.setVersionId(1L);
        app.setK8sNamespace("default");
        app.setState(0);
        app.setRelease(2);
        app.setBuild(true);
        app.setRestartSize(0);
        app.setOptionState(0);
        app.setArgs("--hostname hadoop001 --port 8111");
        app.setOptions("{\"taskmanager.numberOfTaskSlots\":1,\"parallelism.default\":1}");
        app.setResolveOrder(0);
        app.setDeployMode(4);
        app.setAppType(2);
        app.setTracking(0);
        app.setJar("SocketWindowWordCount.jar");
        app.setJarCheckSum(1553115525L);
        app.setMainClass("org.apache.flink.streaming.examples.socket.SocketWindowWordCount");
        app.setCreateTime(now);
        app.setModifyTime(now);
        app.setResourceFrom(2);
        app.setK8sHadoopIntegration(false);
        app.setBackUp(false);
        app.setRestart(false);
        app.setRestoreOrTriggerSavepoint(false);
        app.setDrain(false);
        app.setAllowNonRestored(false);

        Assertions.assertDoesNotThrow(() -> applicationManageService.updateRelease(app));
    }

    @Test
    @Disabled("We couldn't do integration test with external services or components.")
    void testStart() throws Exception {
        FlinkApplication application = new FlinkApplication();
        application.setId(1304056220683497473L);
        application.setRestart(false);
        application.setRestoreOrTriggerSavepoint(false);
        application.setAllowNonRestored(false);

        applicationActionService.start(application, false);
    }

    @Test
    void testCheckQueueValidationIfNeeded() {
        FlinkApplicationManageServiceImpl applicationServiceImpl =
            (FlinkApplicationManageServiceImpl) applicationManageService;

        // ------- Test it for the create operation. -------
        final String queueLabel = "queue1@label1";
        final Long targetTeamId = 1L;

        // Test application with available queue
        YarnQueue yarnQueue = mockYarnQueue(targetTeamId, queueLabel);
        yarnQueueService.save(yarnQueue);
        FlinkApplication application = mockYarnModeJobApp(targetTeamId, "app1", queueLabel,
            FlinkDeployMode.YARN_APPLICATION);
        assertThat(applicationServiceImpl.validateQueueIfNeeded(application)).isTrue();

        // Test application without available queue
        application.setYarnQueue("non-exited-queue");
        assertThat(applicationServiceImpl.validateQueueIfNeeded(application)).isFalse();

        // ------- Test it for the update operation. -------
        final String queueLabel1 = "queue1@label1";
        final String queueLabel2 = "queue1@label2";
        final String nonExistedQueue = "nonExistedQueue";
        final String appName = "app1";
        final Long teamId2 = 2L;

        // Test update for both versions in yarn-app or per-job with same yarn queue
        FlinkApplication app1 = mockYarnModeJobApp(teamId2, appName, queueLabel1, FlinkDeployMode.YARN_APPLICATION);
        FlinkApplication app2 = mockYarnModeJobApp(teamId2, appName, queueLabel1, FlinkDeployMode.YARN_PER_JOB);
        assertThat(applicationServiceImpl.validateQueueIfNeeded(app1, app2)).isTrue();

        // Test available queue
        YarnQueue yarnQueueLabel1 = mockYarnQueue(teamId2, queueLabel1);
        yarnQueueService.save(yarnQueueLabel1);
        YarnQueue yarnQueueLabel2 = mockYarnQueue(teamId2, queueLabel2);
        yarnQueueService.save(yarnQueueLabel2);
        app2.setYarnQueue(queueLabel2);
        assertThat(applicationServiceImpl.validateQueueIfNeeded(app1, app2)).isTrue();

        // Test non-existed queue
        app1.setDeployMode(FlinkDeployMode.KUBERNETES_NATIVE_APPLICATION.getMode());
        app2.setYarnQueue(nonExistedQueue);
        assertThat(applicationServiceImpl.validateQueueIfNeeded(app1, app2)).isFalse();
    }

    @Test
    void testPersistMetricsPreservesInFlightCancellation() {
        assertNonTerminalMetricsPreserveOperation(OptionStateEnum.CANCELLING);
        assertNonTerminalMetricsPreserveOperation(OptionStateEnum.SAVEPOINTING);
    }

    @Test
    void testPersistMetricsRecoversInterruptedCancellation() {
        assertInterruptedCancellationRecovers(FlinkAppStateEnum.RUNNING);
        assertInterruptedCancellationRecovers(FlinkAppStateEnum.FAILING);
    }

    @Test
    void testPersistMetricsUpdatesUnprotectedNonTerminalState() {
        FlinkApplication persisted = createApplication(
            FlinkAppStateEnum.RUNNING, OptionStateEnum.NONE);

        FlinkApplication snapshot = new FlinkApplication();
        snapshot.setId(persisted.getId());
        snapshot.setState(FlinkAppStateEnum.FAILING.getValue());
        snapshot.setOptionState(OptionStateEnum.NONE.getValue());
        applicationManageService.persistMetrics(snapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(FlinkAppStateEnum.FAILING.getValue());
        assertThat(actual.getOptionState()).isEqualTo(OptionStateEnum.NONE.getValue());
    }

    @Test
    void testPersistMetricsDoesNotBlockStandaloneSavepointCompletion() {
        FlinkApplication persisted = createApplication(
            FlinkAppStateEnum.RUNNING, OptionStateEnum.SAVEPOINTING);

        FlinkApplication completedSnapshot = new FlinkApplication();
        completedSnapshot.setId(persisted.getId());
        completedSnapshot.setState(FlinkAppStateEnum.RUNNING.getValue());
        completedSnapshot.setOptionState(OptionStateEnum.NONE.getValue());
        applicationManageService.persistMetrics(completedSnapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(FlinkAppStateEnum.RUNNING.getValue());
        assertThat(actual.getOptionState()).isEqualTo(OptionStateEnum.NONE.getValue());
    }

    @Test
    void testPersistMetricsConvergesCancellationOnTerminalState() {
        FlinkApplication persisted = createApplication(
            FlinkAppStateEnum.CANCELLING, OptionStateEnum.CANCELLING);

        FlinkApplication terminalSnapshot = new FlinkApplication();
        terminalSnapshot.setId(persisted.getId());
        terminalSnapshot.setState(FlinkAppStateEnum.CANCELED.getValue());
        terminalSnapshot.setOptionState(OptionStateEnum.NONE.getValue());
        applicationManageService.persistMetrics(terminalSnapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(FlinkAppStateEnum.CANCELED.getValue());
        assertThat(actual.getOptionState()).isEqualTo(OptionStateEnum.NONE.getValue());
    }

    @Test
    void testPersistMetricsRejectsLateNonTerminalSnapshotAfterCancellationCompletes() {
        assertCompletedCancellationRejectsLateSnapshot(
            FlinkAppStateEnum.FAILING, OptionStateEnum.NONE);
        assertCompletedCancellationRejectsLateSnapshot(
            FlinkAppStateEnum.RUNNING, OptionStateEnum.NONE);
        assertCompletedCancellationRejectsLateSnapshot(
            FlinkAppStateEnum.CANCELLING, OptionStateEnum.CANCELLING);
    }

    @Test
    void testPersistMetricsAllowsExplicitRestartAfterCancellationCompletes() {
        FlinkApplication persisted = createApplication(
            FlinkAppStateEnum.CANCELED, OptionStateEnum.NONE);

        FlinkApplication starting = new FlinkApplication();
        starting.setId(persisted.getId());
        starting.setState(FlinkAppStateEnum.STARTING.getValue());
        assertThat(applicationManageService.updateById(starting)).isTrue();

        FlinkApplication runningSnapshot = new FlinkApplication();
        runningSnapshot.setId(persisted.getId());
        runningSnapshot.setState(FlinkAppStateEnum.RUNNING.getValue());
        runningSnapshot.setOptionState(OptionStateEnum.NONE.getValue());
        runningSnapshot.setTotalTask(2);
        applicationManageService.persistMetrics(runningSnapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(FlinkAppStateEnum.RUNNING.getValue());
        assertThat(actual.getOptionState()).isEqualTo(OptionStateEnum.NONE.getValue());
        assertThat(actual.getTotalTask()).isEqualTo(2);
    }

    @Test
    void testPersistMetricsAllowsTerminalCorrectionAfterCancellationCompletes() {
        FlinkApplication persisted = createApplication(
            FlinkAppStateEnum.CANCELED, OptionStateEnum.NONE);

        FlinkApplication failedSnapshot = new FlinkApplication();
        failedSnapshot.setId(persisted.getId());
        failedSnapshot.setState(FlinkAppStateEnum.FAILED.getValue());
        failedSnapshot.setOptionState(OptionStateEnum.NONE.getValue());
        applicationManageService.persistMetrics(failedSnapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(FlinkAppStateEnum.FAILED.getValue());
        assertThat(actual.getOptionState()).isEqualTo(OptionStateEnum.NONE.getValue());
    }

    @Test
    void testPersistMetricsDoesNotFreezeRecoverableLostState() {
        FlinkApplication persisted = createApplication(
            FlinkAppStateEnum.LOST, OptionStateEnum.NONE);

        FlinkApplication recoveredSnapshot = new FlinkApplication();
        recoveredSnapshot.setId(persisted.getId());
        recoveredSnapshot.setState(FlinkAppStateEnum.RUNNING.getValue());
        recoveredSnapshot.setOptionState(OptionStateEnum.NONE.getValue());
        applicationManageService.persistMetrics(recoveredSnapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(FlinkAppStateEnum.RUNNING.getValue());
        assertThat(actual.getOptionState()).isEqualTo(OptionStateEnum.NONE.getValue());
    }

    private void assertCompletedCancellationRejectsLateSnapshot(
                                                                FlinkAppStateEnum lateState,
                                                                OptionStateEnum lateOptionState) {
        FlinkApplication persisted = createApplication(
            FlinkAppStateEnum.CANCELLING, OptionStateEnum.CANCELLING);

        FlinkApplication terminalSnapshot = new FlinkApplication();
        terminalSnapshot.setId(persisted.getId());
        terminalSnapshot.setState(FlinkAppStateEnum.CANCELED.getValue());
        terminalSnapshot.setOptionState(OptionStateEnum.NONE.getValue());
        applicationManageService.persistMetrics(terminalSnapshot);

        FlinkApplication lateSnapshot = new FlinkApplication();
        lateSnapshot.setId(persisted.getId());
        lateSnapshot.setState(lateState.getValue());
        lateSnapshot.setOptionState(lateOptionState.getValue());
        lateSnapshot.setTotalTask(7);
        applicationManageService.persistMetrics(lateSnapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(FlinkAppStateEnum.CANCELED.getValue());
        assertThat(actual.getOptionState()).isEqualTo(OptionStateEnum.NONE.getValue());
        assertThat(actual.getTotalTask()).isNull();
    }

    private void assertInterruptedCancellationRecovers(FlinkAppStateEnum currentState) {
        FlinkApplication persisted = createApplication(
            FlinkAppStateEnum.CANCELLING, OptionStateEnum.NONE);

        FlinkApplication currentSnapshot = new FlinkApplication();
        currentSnapshot.setId(persisted.getId());
        currentSnapshot.setState(currentState.getValue());
        currentSnapshot.setOptionState(OptionStateEnum.NONE.getValue());
        currentSnapshot.setTotalTask(4);
        applicationManageService.persistMetrics(currentSnapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(currentState.getValue());
        assertThat(actual.getOptionState()).isEqualTo(OptionStateEnum.NONE.getValue());
        assertThat(actual.getTotalTask()).isEqualTo(4);
    }

    private void assertNonTerminalMetricsPreserveOperation(OptionStateEnum optionState) {
        FlinkApplication persisted = createApplication(FlinkAppStateEnum.CANCELLING, optionState);

        FlinkApplication staleSnapshot = new FlinkApplication();
        staleSnapshot.setId(persisted.getId());
        staleSnapshot.setState(FlinkAppStateEnum.FAILING.getValue());
        staleSnapshot.setOptionState(OptionStateEnum.NONE.getValue());
        staleSnapshot.setTotalTask(3);
        applicationManageService.persistMetrics(staleSnapshot);

        FlinkApplication actual = applicationManageService.getById(persisted.getId());
        assertThat(actual.getState()).isEqualTo(FlinkAppStateEnum.CANCELLING.getValue());
        assertThat(actual.getOptionState()).isEqualTo(optionState.getValue());
        assertThat(actual.getTotalTask()).isEqualTo(3);
    }

    private FlinkApplication createApplication(
                                               FlinkAppStateEnum state,
                                               OptionStateEnum optionState) {
        FlinkApplication application = new FlinkApplication();
        application.setTeamId(1L);
        application.setState(state.getValue());
        application.setOptionState(optionState.getValue());
        assertThat(applicationManageService.save(application)).isTrue();
        return application;
    }
}
