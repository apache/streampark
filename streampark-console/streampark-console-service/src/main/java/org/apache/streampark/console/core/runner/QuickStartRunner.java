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

package org.apache.streampark.console.core.runner;

import org.apache.streampark.common.enums.ClusterState;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.util.FlinkConfigurationUtils;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBuildPipelineService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Map;

@Order
@Slf4j
@Component
public class QuickStartRunner implements ApplicationRunner {

    @Autowired
    private FlinkEnvService flinkEnvService;

    @Autowired
    private FlinkClusterService flinkClusterService;

    @Autowired
    private FlinkSqlService flinkSqlService;

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Autowired
    private FlinkApplicationBuildPipelineService appBuildPipeService;

    private static final Long defaultId = 100000L;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        Map<String, Map<String, String>> map =
            FlinkConfigurationUtils.extractMultipleArgumentsAsJava(args.getSourceArgs());

        Map<String, String> quickstart = map.get("quickstart");

        if (quickstart != null && quickstart.size() == 3) {
            // 1) create flinkEnv
            FlinkEnv flinkEnv = new FlinkEnv();
            flinkEnv.setFlinkName(quickstart.get("flink_name"));
            flinkEnv.setFlinkHome(quickstart.get("flink_home"));
            flinkEnvService.create(flinkEnv);

            // 2) create flinkCluster
            FlinkCluster flinkCluster = new FlinkCluster();
            flinkCluster.setClusterName("quickstart");
            flinkCluster.setVersionId(flinkEnv.getId());
            flinkCluster.setClusterState(ClusterState.RUNNING.getState());
            flinkCluster.setDeployMode(FlinkDeployMode.REMOTE.getMode());
            flinkCluster.setAddress("http://localhost:" + quickstart.get("flink_port"));
            flinkClusterService.create(flinkCluster, defaultId);

            // 3) set flink version and cluster
            FlinkApplication app = new FlinkApplication();
            app.setId(defaultId);
            FlinkApplication application = applicationManageService.getApp(app.getId());
            application.setFlinkClusterId(flinkCluster.getId());
            application.setVersionId(flinkEnv.getId());
            application.setDeployMode(FlinkDeployMode.REMOTE.getMode());

            FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), true);
            application.setFlinkSql(flinkSql.getSql());

            boolean success = applicationManageService.update(application);
            if (success) {
                // 4) build application
                appBuildPipeService.buildApplication(defaultId, false);
            }
        }
    }
}
