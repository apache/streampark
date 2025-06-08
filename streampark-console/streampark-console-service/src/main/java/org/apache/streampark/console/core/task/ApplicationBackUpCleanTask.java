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

package org.apache.streampark.console.core.task;

import org.apache.streampark.console.core.entity.FlinkApplicationBackup;
import org.apache.streampark.console.core.service.application.FlinkApplicationBackupService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ApplicationBackUpCleanTask {

    private final FlinkApplicationBackupService backUpService;

    @Value("${streampark.backup-clean.max-backup-num:5}")
    public Integer maxBackupNum;

    @Scheduled(cron = "${streampark.backup-clean.exec-cron:0 0 1 * * ?}")
    public void backUpClean() {
        log.info("Start to clean application backup");
        // select all application backup which count > maxBackupNum group by app_id
        backUpService.lambdaQuery()
            .select(FlinkApplicationBackup::getAppId)
            .groupBy(FlinkApplicationBackup::getAppId)
            .having("COUNT(*) > {0}", maxBackupNum)
            .list()
            .stream()
            .map(FlinkApplicationBackup::getAppId)
            .forEach(
                appId -> {
                    // order by create_time desc and skip first maxBackupNum records and delete
                    // others
                    backUpService.lambdaQuery()
                        .eq(FlinkApplicationBackup::getAppId, appId)
                        .orderByDesc(FlinkApplicationBackup::getCreateTime)
                        .list()
                        .stream()
                        .skip(maxBackupNum)
                        .forEach(
                            backUp -> {
                                try {
                                    backUpService.removeById(
                                        backUp.getId());
                                } catch (Exception e) {
                                    log.error(
                                        "Clean application backup failed for app id: {} , backup id: {}",
                                        appId,
                                        backUp.getId(),
                                        e);
                                }
                            });
                });
        log.info("Clean application backup finished");
    }
}
