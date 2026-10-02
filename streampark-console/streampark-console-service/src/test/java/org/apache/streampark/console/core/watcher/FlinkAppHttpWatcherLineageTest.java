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

import org.apache.streampark.console.core.enums.FlinkAppStateEnum;
import org.apache.streampark.console.core.service.GravitinoLineageService;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Covers only the pure COMPLETE-vs-FAIL mapping this watcher adds for lineage terminal events —
 * not the surrounding {@code doPersistMetrics} state-tracking machinery, which is exercised
 * end-to-end in {@code SpringIntegrationTestBase}-backed tests elsewhere.
 */
class FlinkAppHttpWatcherLineageTest {

    private final FlinkAppHttpWatcher watcher = new FlinkAppHttpWatcher(mock(GravitinoLineageService.class));

    @Test
    void finishedAndSucceededMapToLineageSuccess() {
        assertThat(watcher.isLineageSuccessState(FlinkAppStateEnum.FINISHED.getValue())).isTrue();
        assertThat(watcher.isLineageSuccessState(FlinkAppStateEnum.SUCCEEDED.getValue())).isTrue();
    }

    @Test
    void otherEndStatesMapToLineageFailure() {
        assertThat(watcher.isLineageSuccessState(FlinkAppStateEnum.FAILED.getValue())).isFalse();
        assertThat(watcher.isLineageSuccessState(FlinkAppStateEnum.CANCELED.getValue())).isFalse();
        assertThat(watcher.isLineageSuccessState(FlinkAppStateEnum.KILLED.getValue())).isFalse();
        assertThat(watcher.isLineageSuccessState(FlinkAppStateEnum.LOST.getValue())).isFalse();
        assertThat(watcher.isLineageSuccessState(FlinkAppStateEnum.TERMINATED.getValue())).isFalse();
    }
}
