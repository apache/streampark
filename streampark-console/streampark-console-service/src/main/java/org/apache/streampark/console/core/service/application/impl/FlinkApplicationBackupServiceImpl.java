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

package org.apache.streampark.console.core.service.application.impl;

import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkApplicationBackup;
import org.apache.streampark.console.core.entity.FlinkApplicationConfig;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.enums.EffectiveTypeEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.mapper.FlinkApplicationBackupMapper;
import org.apache.streampark.console.core.service.FlinkEffectiveService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBackupService;
import org.apache.streampark.console.core.service.application.FlinkApplicationConfigService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class FlinkApplicationBackupServiceImpl
    extends
        ServiceImpl<FlinkApplicationBackupMapper, FlinkApplicationBackup>
    implements
        FlinkApplicationBackupService {

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Autowired
    private FlinkApplicationConfigService configService;

    @Autowired
    private FlinkEffectiveService effectiveService;

    @Autowired
    private FlinkSqlService flinkSqlService;

    @Override
    public IPage<FlinkApplicationBackup> getPage(FlinkApplicationBackup bakParam, RestRequest request) {
        Page<FlinkApplicationBackup> page = MybatisPager.getPage(request);
        return this.lambdaQuery().eq(FlinkApplicationBackup::getAppId, bakParam.getAppId()).page(page);
    }

    @Override
    public void rollback(FlinkApplicationBackup bakParam) {

        FlinkApplication application = applicationManageService.getById(bakParam.getAppId());

        FsOperator fsOperator = application.getFsOperator();
        // backup files not exist
        if (!fsOperator.exists(bakParam.getPath())) {
            return;
        }

        // if backup files exists, will be rollback
        // When rollback, determine the currently effective project is necessary to be
        // backed up.
        // If necessary, perform the backup first
        if (bakParam.isBackup()) {
            application.setBackUpDescription(bakParam.getDescription());
            if (application.isFlinkSql()) {
                FlinkSql flinkSql = flinkSqlService.getEffective(application.getId(), false);
                backup(application, flinkSql);
            } else {
                backup(application, null);
            }
        }

        // restore config and sql

        // if running, set Latest
        if (application.isRunning()) {
            // rollback to back up config
            configService.setLatestOrEffective(true, bakParam.getId(), bakParam.getAppId());
        } else {
            effectiveService.saveOrUpdate(
                bakParam.getAppId(), EffectiveTypeEnum.CONFIG, bakParam.getId());
            // if flink sql task, will be rollback sql and dependencies
            if (application.isFlinkSql()) {
                effectiveService.saveOrUpdate(
                    bakParam.getAppId(), EffectiveTypeEnum.FLINKSQL, bakParam.getSqlId());
            }
        }

        // delete the current valid project files (Note: If the rollback failed, need to
        // restore)
        fsOperator.delete(application.getAppHome());

        // copy backup files to a valid dir
        fsOperator.copyDir(bakParam.getPath(), application.getAppHome());

        // update restart status
        applicationManageService.update(
            new UpdateWrapper<FlinkApplication>()
                .lambda()
                .eq(FlinkApplication::getId, application.getId())
                .set(FlinkApplication::getRelease, ReleaseStateEnum.NEED_RESTART.get()));
    }

    @Override
    public void revoke(FlinkApplication appParam) {
        Page<FlinkApplicationBackup> page = new Page<>();
        page.setCurrent(0).setSize(1).setSearchCount(false);

        Page<FlinkApplicationBackup> backUpPages = this.lambdaQuery().eq(
            FlinkApplicationBackup::getAppId,
            appParam.getId())
            .orderByDesc(FlinkApplicationBackup::getCreateTime).page(page);

        if (!backUpPages.getRecords().isEmpty()) {
            FlinkApplicationBackup backup = backUpPages.getRecords().get(0);
            String path = backup.getPath();
            appParam.getFsOperator().move(path, appParam.getWorkspace().APP_WORKSPACE());
            super.removeById(backup.getId());
        }
    }

    @Override
    public void remove(FlinkApplication appParam) {
        try {
            this.lambdaUpdate().eq(FlinkApplicationBackup::getAppId, appParam.getId()).remove();
            appParam
                .getFsOperator()
                .delete(
                    appParam
                        .getWorkspace()
                        .APP_BACKUPS()
                        .concat("/")
                        .concat(appParam.getId().toString()));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void rollbackFlinkSql(FlinkApplication appParam, FlinkSql flinkSqlParam) {
        FlinkApplicationBackup backUp = this.lambdaQuery().eq(FlinkApplicationBackup::getAppId, appParam.getId())
            .eq(FlinkApplicationBackup::getSqlId, flinkSqlParam.getId()).one();
        ApiAlertException.throwIfNull(
            backUp, "Application backup can't be null. Rollback flink sql failed.");
        // rollback config and sql
        effectiveService.saveOrUpdate(backUp.getAppId(), EffectiveTypeEnum.CONFIG, backUp.getId());
        effectiveService.saveOrUpdate(backUp.getAppId(), EffectiveTypeEnum.FLINKSQL, backUp.getSqlId());
    }

    @Override
    public Boolean removeById(Long id) throws InternalException {
        FlinkApplicationBackup backUp = getById(id);
        try {
            FlinkApplication application = applicationManageService.getById(backUp.getAppId());
            application.getFsOperator().delete(backUp.getPath());
            super.removeById(id);
            return true;
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
    }

    @Override
    public void backup(FlinkApplication appParam, FlinkSql flinkSqlParam) {
        // basic configuration file backup
        String appHome = (appParam.isFlinkJar() && appParam.isBuildResource())
            ? appParam.getDistHome()
            : appParam.getAppHome();
        FsOperator fsOperator = appParam.getFsOperator();
        if (fsOperator.exists(appHome)) {
            // move files to back up directory
            FlinkApplicationConfig config = configService.getEffective(appParam.getId());
            if (config != null) {
                appParam.setConfigId(config.getId());
            }
            // flink sql tasks need to back up sql and dependencies
            int version = 1;
            if (flinkSqlParam != null) {
                appParam.setSqlId(flinkSqlParam.getId());
                version = flinkSqlParam.getVersion();
            } else if (config != null) {
                version = config.getVersion();
            }

            FlinkApplicationBackup applicationBackUp = new FlinkApplicationBackup(appParam);
            applicationBackUp.setVersion(version);

            this.save(applicationBackUp);
            fsOperator.mkdirs(applicationBackUp.getPath());
            fsOperator.copyDir(appHome, applicationBackUp.getPath());
        }
    }
}
