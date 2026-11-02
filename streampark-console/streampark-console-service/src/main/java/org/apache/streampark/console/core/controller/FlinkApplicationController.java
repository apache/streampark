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

package org.apache.streampark.console.core.controller;

import org.apache.streampark.common.util.Utils;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.base.web.FormOrJson;
import org.apache.streampark.console.core.annotation.AppChangeEvent;
import org.apache.streampark.console.core.assembler.AppLogAssembler;
import org.apache.streampark.console.core.assembler.FlinkApplicationAssembler;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.enums.AppExistsStateEnum;
import org.apache.streampark.console.core.request.app.AppBackupDeleteRequest;
import org.apache.streampark.console.core.request.app.AppBackupQueryRequest;
import org.apache.streampark.console.core.request.app.AppOptLogDeleteRequest;
import org.apache.streampark.console.core.request.app.AppOptLogQueryRequest;
import org.apache.streampark.console.core.request.common.TeamIdRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppCancelRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppCheckNameRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppCheckSavepointPathRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppConfigRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppCopyRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppCreateRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppGetMainRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppIdRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppK8sLogRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppListQueryRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppMappingRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppStartRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppUpdateRequest;
import org.apache.streampark.console.core.response.app.AppBackupResponse;
import org.apache.streampark.console.core.response.app.AppOptLogResponse;
import org.apache.streampark.console.core.response.flink.FlinkAppDashboardResponse;
import org.apache.streampark.console.core.response.flink.FlinkAppResponse;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.console.core.service.application.ApplicationLogService;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBackupService;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;

@Slf4j
@Validated
@RestController
@RequestMapping("flink/app")
public class FlinkApplicationController {

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Autowired
    private FlinkApplicationActionService applicationActionService;

    @Autowired
    private FlinkApplicationInfoService applicationInfoService;

    @Autowired
    private FlinkApplicationBackupService backUpService;

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private ResourceService resourceService;

    @PostMapping("get")
    @RequiresPermissions("app:detail")
    public RestResponseBody<FlinkAppResponse> get(@Valid FlinkAppIdRequest request) {
        FlinkApplication application = applicationManageService.getApp(request.getId());
        FlinkAppResponse response = FlinkApplicationAssembler.toResponse(application);
        return RestResponseBody.success(response);
    }

    @PostMapping("create")
    @RequiresPermissions("app:create")
    public RestResponseBody<Boolean> create(@Valid @FormOrJson FlinkAppCreateRequest request) throws IOException {
        FlinkApplication app = FlinkApplicationAssembler.toEntity(request);
        boolean saved = applicationManageService.create(app);
        return RestResponseBody.success(saved);
    }

    @PostMapping("copy")
    @RequiresPermissions("app:copy")
    public RestResponseBody<Void> copy(@Valid @FormOrJson FlinkAppCopyRequest request) throws IOException {
        applicationManageService.copy(FlinkApplicationAssembler.toEntity(request));
        return RestResponseBody.success();
    }

    @AppChangeEvent
    @PostMapping("update")
    @RequiresPermissions("app:update")
    public RestResponseBody<Boolean> update(@Valid @FormOrJson FlinkAppUpdateRequest request) {
        applicationManageService.update(FlinkApplicationAssembler.toEntity(request));
        return RestResponseBody.success(true);
    }

    @PostMapping("dashboard")
    public RestResponseBody<FlinkAppDashboardResponse> dashboard(@Valid TeamIdRequest request) {
        Map<String, Serializable> dashboardMap = applicationInfoService.getDashboardDataMap(request.getTeamId());
        return RestResponseBody.success(FlinkApplicationAssembler.toDashboardResponse(dashboardMap));
    }

    @PostMapping("list")
    @RequiresPermissions("app:view")
    public RestResponseBody<IPage<FlinkAppResponse>> list(@Valid FlinkAppListQueryRequest query, RestRequest request) {
        FlinkApplication appParam = FlinkApplicationAssembler.toEntity(query);
        IPage<FlinkApplication> applicationList = applicationManageService.page(appParam, request);
        return RestResponseBody.success(FlinkApplicationAssembler.toPageResponse(applicationList));
    }

    @AppChangeEvent
    @PostMapping("mapping")
    @RequiresPermissions("app:mapping")
    public RestResponseBody<Boolean> mapping(@Valid @FormOrJson FlinkAppMappingRequest request) {
        boolean flag = applicationManageService.mapping(FlinkApplicationAssembler.toEntity(request));
        return RestResponseBody.success(flag);
    }

    @AppChangeEvent
    @PostMapping("revoke")
    @RequiresPermissions("app:release")
    public RestResponseBody<Void> revoke(@Valid @FormOrJson FlinkAppIdRequest request) {
        applicationActionService.revoke(request.getId());
        return RestResponseBody.success();
    }

    @PostMapping("check/start")
    @RequiresPermissions("app:start")
    public RestResponseBody<Integer> checkStart(@Valid FlinkAppIdRequest request) {
        AppExistsStateEnum stateEnum = applicationInfoService.checkStart(request.getId());
        return RestResponseBody.success(stateEnum.get());
    }

    @PostMapping("start")
    @RequiresPermissions("app:start")
    public RestResponseBody<Boolean> start(@Valid @FormOrJson FlinkAppStartRequest request) throws Exception {
        applicationActionService.start(FlinkApplicationAssembler.toEntity(request), false);
        return RestResponseBody.success(true);
    }

    @PostMapping("cancel")
    @RequiresPermissions("app:cancel")
    public RestResponseBody<Void> cancel(@Valid @FormOrJson FlinkAppCancelRequest request) throws Exception {
        applicationActionService.cancel(FlinkApplicationAssembler.toEntity(request));
        return RestResponseBody.success();
    }

    /** force stop(stop normal start or in progress) */
    @PostMapping("abort")
    @RequiresPermissions("app:cancel")
    public RestResponseBody<Void> abort(@Valid @FormOrJson FlinkAppIdRequest request) {
        applicationActionService.abort(request.getId());
        return RestResponseBody.success();
    }

    @PostMapping("yarn")
    public RestResponseBody<String> yarn() {
        return RestResponseBody.success(YarnUtils.getRMWebAppProxyURL());
    }

    @PostMapping("name")
    public RestResponseBody<String> yarnName(FlinkAppConfigRequest request) {
        String yarnName = applicationInfoService.getYarnName(request.getConfig());
        return RestResponseBody.success(yarnName);
    }

    @PostMapping("check/name")
    public RestResponseBody<Integer> checkName(@Valid FlinkAppCheckNameRequest request) {
        AppExistsStateEnum exists = applicationInfoService.checkExists(FlinkApplicationAssembler.toEntity(request));
        return RestResponseBody.success(exists.get());
    }

    @PostMapping("read_conf")
    public RestResponseBody<String> readConf(FlinkAppConfigRequest request) throws IOException {
        String config = applicationInfoService.readConf(request.getConfig());
        return RestResponseBody.success(config);
    }

    @PostMapping("main")
    public RestResponseBody<String> getMain(FlinkAppGetMainRequest request) {
        String mainClass = applicationInfoService.getMain(FlinkApplicationAssembler.toEntity(request));
        return RestResponseBody.success(mainClass);
    }

    @PostMapping("backups")
    public RestResponseBody<IPage<AppBackupResponse>> backups(AppBackupQueryRequest query, RestRequest request) {
        return RestResponseBody.success(
            AppLogAssembler.toBackupPage(backUpService.getPage(AppLogAssembler.toEntity(query), request)));
    }

    @PostMapping("opt_log")
    public RestResponseBody<IPage<AppOptLogResponse>> log(AppOptLogQueryRequest query, RestRequest request) {
        return RestResponseBody.success(
            AppLogAssembler.toOptLogPage(applicationLogService.getPage(AppLogAssembler.toEntity(query), request)));
    }

    @PostMapping("delete/opt_log")
    @RequiresPermissions("app:delete")
    public RestResponseBody<Boolean> deleteLog(@Valid @FormOrJson AppOptLogDeleteRequest request) {
        Boolean deleted = applicationLogService.delete(AppLogAssembler.toEntity(request));
        return RestResponseBody.success(deleted);
    }

    @PostMapping("delete")
    @RequiresPermissions("app:delete")
    public RestResponseBody<Boolean> delete(@Valid @FormOrJson FlinkAppIdRequest request) throws InternalException {
        Boolean deleted = applicationManageService.remove(request.getId());
        return RestResponseBody.success(deleted);
    }

    @PostMapping("delete/backup")
    public RestResponseBody<Boolean> deleteBackup(@Valid @FormOrJson AppBackupDeleteRequest request) throws InternalException {
        Boolean deleted = backUpService.removeById(request.getId());
        return RestResponseBody.success(deleted);
    }

    @PostMapping("check/jar")
    public RestResponseBody<Boolean> checkJar(String jar) throws IOException {
        Utils.requireCheckJarFile(new File(jar).toURI().toURL());
        return RestResponseBody.success(true);
    }

    @PostMapping("verify_schema")
    public RestResponseBody<Boolean> verifySchema(String path) {
        final URI uri = URI.create(path);
        final String scheme = uri.getScheme();
        final String pathPart = uri.getPath();
        RestResponseBody<Boolean> restResponse = RestResponseBody.success(true);
        String error = null;
        if (scheme == null) {
            error =
                "The scheme (hdfs://, file://, etc) is null. Please specify the file system scheme explicitly in the URI.";
        } else if (pathPart == null) {
            error =
                "The path to store the checkpoint data in is null. Please specify a directory path for the checkpoint data.";
        } else if (pathPart.isEmpty() || "/".equals(pathPart)) {
            error = "Cannot use the root directory for checkpoints.";
        }
        if (error != null) {
            restResponse = RestResponseBody.success(false).message(error);
        }
        return restResponse;
    }

    @PostMapping("check/savepoint_path")
    public RestResponseBody<Boolean> checkSavepointPath(FlinkAppCheckSavepointPathRequest request) throws Exception {
        String error = applicationInfoService.checkSavepointPath(FlinkApplicationAssembler.toEntity(request));
        if (error == null) {
            return RestResponseBody.success(true);
        }
        return RestResponseBody.success(false).message(error);
    }

    @PostMapping("k8s_log")
    public RestResponseBody<String> k8sStartLog(FlinkAppK8sLogRequest request) throws Exception {
        String resp = applicationInfoService.k8sStartLog(request.getId(), request.getOffset(), request.getLimit());
        return RestResponseBody.success(resp);
    }
}
