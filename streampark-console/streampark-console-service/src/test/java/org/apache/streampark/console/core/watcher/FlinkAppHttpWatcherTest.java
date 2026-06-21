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
import org.apache.streampark.console.core.metrics.yarn.YarnAppInfo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkAppHttpWatcherTest {

    @Test
    void resolveYarnAppStateFallsBackToStateWhenFinalStatusIsUndefined() {
        YarnAppInfo yarnAppInfo = yarnAppInfo("RUNNING", "UNDEFINED");

        FlinkAppStateEnum state = FlinkAppHttpWatcher.resolveYarnAppState(yarnAppInfo);

        assertThat(state).isEqualTo(FlinkAppStateEnum.RUNNING);
    }

    @Test
    void resolveYarnAppStatePrefersFinalStatusWhenItIsKnown() {
        YarnAppInfo yarnAppInfo = yarnAppInfo("FINISHED", "SUCCEEDED");

        FlinkAppStateEnum state = FlinkAppHttpWatcher.resolveYarnAppState(yarnAppInfo);

        assertThat(state).isEqualTo(FlinkAppStateEnum.SUCCEEDED);
    }

    @Test
    void resolveYarnAppStateReturnsOtherWhenAppInfoIsMissing() {
        assertThat(FlinkAppHttpWatcher.resolveYarnAppState(null)).isEqualTo(FlinkAppStateEnum.OTHER);
        assertThat(FlinkAppHttpWatcher.resolveYarnAppState(new YarnAppInfo()))
            .isEqualTo(FlinkAppStateEnum.OTHER);
    }

    private YarnAppInfo yarnAppInfo(String state, String finalStatus) {
        YarnAppInfo yarnAppInfo = new YarnAppInfo();
        YarnAppInfo.App app = new YarnAppInfo.App();
        app.setState(state);
        app.setFinalStatus(finalStatus);
        yarnAppInfo.setApp(app);
        return yarnAppInfo;
    }
}
