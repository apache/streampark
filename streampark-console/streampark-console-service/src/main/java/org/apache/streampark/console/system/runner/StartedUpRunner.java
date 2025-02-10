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

package org.apache.streampark.console.system.runner;

import org.apache.streampark.common.util.SystemPropertyUtils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Order
@Slf4j
@Component
public class StartedUpRunner implements ApplicationRunner {

    @Autowired
    private ConfigurableApplicationContext context;

    @Override
    public void run(ApplicationArguments args) {
        if (context.isActive()) {
            String port = SystemPropertyUtils.get("server.port", "10000");
            System.out.println("\n");
            System.out.println("        _____ __                                             __       ");
            System.out.println("       / ___// /_________  ____ _____ ___  ____  ____ ______/ /__     ");
            System.out.println("       \\__ \\/ __/ ___/ _ \\/ __ `/ __ `__ \\/ __ \\  __ `/ ___/ //_/");
            System.out.println("      ___/ / /_/ /  /  __/ /_/ / / / / / / /_/ / /_/ / /  / ,<        ");
            System.out.println("     /____/\\__/_/   \\___/\\__,_/_/ /_/ /_/ ____/\\__,_/_/  /_/|_|   ");
            System.out.println("                                       /_/                        \n\n");
            System.out.println("    Version:  2.2.0                                                   ");
            System.out.println("    WebSite:  https://streampark.apache.org                           ");
            System.out.println("    GitHub :  https://github.com/apache/streampark          ");
            System.out.println("    Info   :  streampark-console start successful                     ");
            System.out.println("    Local  :  http://localhost:" + port);
            System.out.println("    Time   :  " + LocalDateTime.now() + "\n\n");
            System.setProperty("streampark.start.timestamp", System.currentTimeMillis() + "");
        }
    }
}
