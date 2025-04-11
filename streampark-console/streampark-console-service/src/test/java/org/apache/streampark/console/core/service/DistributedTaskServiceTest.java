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

import org.apache.streampark.console.core.bean.FlinkTaskItem;
import org.apache.streampark.console.core.bean.SparkTaskItem;
import org.apache.streampark.console.core.entity.DistributedTask;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.SparkApplication;
import org.apache.streampark.console.core.enums.DistributedTaskEnum;
import org.apache.streampark.console.core.service.impl.DistributedTaskServiceImpl;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Slf4j
class DistributedTaskServiceTest {

    private final DistributedTaskServiceImpl distributionTaskService = new DistributedTaskServiceImpl();

    private final String serverId = "testServer";
    private final Set<String> allServers = new HashSet<>(Collections.singleton(serverId));

    // the number of virtual nodes for each server
    private final int numberOfReplicas = 2 << 16;

    @Test
    void testInit() {
        distributionTaskService.init(allServers, serverId);
        assert (distributionTaskService.getConsistentHashSize() == numberOfReplicas);
    }

    @Test
    void testIsLocalProcessing() {
        distributionTaskService.init(allServers, serverId);
        for (long i = 0; i < numberOfReplicas; i++) {
            assert (distributionTaskService.isLocalProcessing(i));
        }
    }

    @Test
    void testFlinkTaskAndApp() {
        FlinkApplication application = new FlinkApplication();
        application.setId(0L);
        try {
            DistributedTask distributedTask =
                distributionTaskService.getDistributedTaskByFlinkApp(application, false, DistributedTaskEnum.START);
            FlinkTaskItem flinkTaskItem = distributionTaskService.getFlinkTaskItem(distributedTask);
            FlinkApplication newApplication = distributionTaskService.getAppByFlinkTaskItem(flinkTaskItem);
            assert (application.equals(newApplication));
        } catch (Exception e) {
            log.error("testFlinkTaskAndApp failed:", e);
        }
    }

    @Test
    void testSparkTaskAndApp() {
        SparkApplication application = new SparkApplication();
        application.setId(0L);
        try {
            DistributedTask distributedTask =
                distributionTaskService.getDistributedTaskBySparkApp(application, false, DistributedTaskEnum.START);
            SparkTaskItem sparkTaskItem = distributionTaskService.getSparkTaskItem(distributedTask);
            SparkApplication newApplication = distributionTaskService.getAppBySparkTaskItem(sparkTaskItem);
            assert (application.equals(newApplication));
        } catch (Exception e) {
            log.error("testSparkTaskAndApp failed:", e);
        }
    }

}
