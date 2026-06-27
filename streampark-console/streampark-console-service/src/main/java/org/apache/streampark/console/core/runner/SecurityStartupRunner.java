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

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Slf4j
@Order(1)
@Component
public class SecurityStartupRunner implements ApplicationRunner {

    @Value("${spring.datasource.url:}")
    private String datasourceUrl;

    @Override
    public void run(ApplicationArguments args) {
        if (datasourceUrl != null && datasourceUrl.toLowerCase().contains("jdbc:h2:")) {
            log.warn(
                "[StreamPark][Security] H2 embedded database is enabled for development. "
                    + "Change the default admin password (streampark/streampark) before exposing "
                    + "this instance to a network.");
        }
    }
}
