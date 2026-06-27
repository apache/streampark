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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        List<FlinkApplicationBackup> allBackups =
            backUpService.lambdaQuery().orderByDesc(FlinkApplicationBackup::getCreateTime).list();
        if (allBackups.isEmpty()) {
            log.info("Clean application backup finished");
            return;
        }

        Map<Long, List<FlinkApplicationBackup>> backupsByApp =
            allBackups.stream().collect(Collectors.groupingBy(FlinkApplicationBackup::getAppId));

        List<Long> toDelete = new ArrayList<>();
        for (List<FlinkApplicationBackup> appBackups : backupsByApp.values()) {
            appBackups.sort(Comparator.comparing(FlinkApplicationBackup::getCreateTime).reversed());
            appBackups.stream()
                .skip(maxBackupNum)
                .map(FlinkApplicationBackup::getId)
                .forEach(toDelete::add);
        }

        if (!toDelete.isEmpty()) {
            try {
                backUpService.removeByIds(toDelete);
                log.info("Clean application backup finished, deleted {} records", toDelete.size());
            } catch (Exception e) {
                log.error("Clean application backup failed", e);
            }
            return;
        }
        log.info("Clean application backup finished");
    }
}
