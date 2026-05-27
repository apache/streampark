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
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.core.annotation.AppUpdated;
import org.apache.streampark.console.core.annotation.PermissionScope;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationBackUp;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.enums.AppExistsState;
import org.apache.streampark.console.core.service.ApplicationBackUpService;
import org.apache.streampark.console.core.service.ApplicationLogService;
import org.apache.streampark.console.core.service.ApplicationService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Validated
@RestController
@RequestMapping("flink/app")
public class ApplicationController {

  @Autowired private ApplicationService applicationService;

  @Autowired private ApplicationBackUpService backUpService;

  @Autowired private ApplicationLogService applicationLogService;

  @PostMapping("get")
  @PermissionScope(app = "#app.id")
  @RequiresPermissions("app:detail")
  public RestResponse get(Application app) {
    Application application = applicationService.getApp(app);
    return RestResponse.success(application);
  }

  @PermissionScope(team = "#app.teamId")
  @PostMapping("create")
  @RequiresPermissions("app:create")
  public RestResponse create(Application app) throws IOException {
    boolean saved = applicationService.create(app);
    return RestResponse.success(saved);
  }

  @PermissionScope(app = "#app.id", team = "#app.teamId")
  @PostMapping(value = "copy")
  @RequiresPermissions("app:copy")
  public RestResponse copy(Application app) throws IOException {
    Long id = applicationService.copy(app);
    Map<String, String> data = new HashMap<>();
    data.put("id", Long.toString(id));
    return id.equals(0L)
        ? RestResponse.success(false).data(data)
        : RestResponse.success(true).data(data);
  }

  @AppUpdated
  @PermissionScope(app = "#app.id")
  @PostMapping("update")
  @RequiresPermissions("app:update")
  public RestResponse update(Application app) {
    applicationService.update(app);
    return RestResponse.success(true);
  }

  @PostMapping("dashboard")
  @PermissionScope(team = "#app.teamId")
  public RestResponse dashboard(Application app) {
    Map<String, Serializable> map = applicationService.dashboard(app.getTeamId());
    return RestResponse.success(map);
  }

  @PostMapping("list")
  @PermissionScope(team = "#app.teamId")
  @RequiresPermissions("app:view")
  public RestResponse list(Application app, RestRequest request) {
    IPage<Application> applicationList = applicationService.page(app, request);
    return RestResponse.success(applicationList);
  }

  @AppUpdated
  @PostMapping("mapping")
  @PermissionScope(app = "#app.id")
  @RequiresPermissions("app:mapping")
  public RestResponse mapping(Application app) {
    boolean flag = applicationService.mapping(app);
    return RestResponse.success(flag);
  }

  @AppUpdated
  @PermissionScope(app = "#app.id")
  @PostMapping("revoke")
  @RequiresPermissions("app:release")
  public RestResponse revoke(Application app) {
    applicationService.revoke(app);
    return RestResponse.success();
  }

  @PermissionScope(app = "#app.id", team = "#app.teamId")
  @PostMapping(value = "check_start")
  @RequiresPermissions("app:start")
  public RestResponse checkStart(Application app) {
    AppExistsState stateEnum = applicationService.checkStart(app);
    return RestResponse.success(stateEnum.get());
  }

  @PermissionScope(app = "#app.id", team = "#app.teamId")
  @PostMapping(value = "start")
  @RequiresPermissions("app:start")
  public RestResponse start(Application app) {
    try {
      applicationService.checkEnv(app);
      applicationService.start(app, false);
      return RestResponse.success(true);
    } catch (Exception e) {
      return RestResponse.success(false).message(e.getMessage());
    }
  }

  @PermissionScope(app = "#app.id", team = "#app.teamId")
  @PostMapping(value = "cancel")
  @RequiresPermissions("app:cancel")
  public RestResponse cancel(Application app) throws Exception {
    applicationService.cancel(app);
    return RestResponse.success();
  }

  /** force stop(stop normal start or in progress) */
  @PermissionScope(app = "#app.id")
  @PostMapping("abort")
  @RequiresPermissions("app:cancel")
  public RestResponse abort(Application app) {
    applicationService.abort(app);
    return RestResponse.success();
  }

  @PostMapping("yarn")
  public RestResponse yarn() {
    return RestResponse.success(YarnUtils.getRMWebAppProxyURL());
  }

  @PostMapping("name")
  @PermissionScope(app = "#app.id", team = "#app.teamId")
  public RestResponse yarnName(Application app) throws IOException {
    String yarnName = applicationService.getYarnName(app);
    return RestResponse.success(yarnName);
  }

  @PostMapping("checkName")
  @PermissionScope(app = "#app.id", team = "#app.teamId")
  public RestResponse checkName(Application app) {
    AppExistsState exists = applicationService.checkExists(app);
    return RestResponse.success(exists.get());
  }

  @PostMapping("readConf")
  @PermissionScope(team = "#app.teamId")
  public RestResponse readConf(Application app) throws IOException {
    String content = applicationService.readConf(app);
    return RestResponse.success(content);
  }

  @PostMapping("main")
  @PermissionScope(app = "#app.id", team = "#app.teamId")
  public RestResponse getMain(Application app) {
    String mainClass = applicationService.getMain(app);
    return RestResponse.success(mainClass);
  }

  @PostMapping("backups")
  @PermissionScope(app = "#backUp.appId", team = "#backUp.teamId")
  public RestResponse backups(ApplicationBackUp backUp, RestRequest request) {
    IPage<ApplicationBackUp> backups = backUpService.page(backUp, request);
    return RestResponse.success(backups);
  }

  @PostMapping("optionlog")
  @PermissionScope(app = "#log.appId", team = "#log.teamId")
  public RestResponse log(ApplicationLog log, RestRequest request) {
    IPage<ApplicationLog> applicationList = applicationLogService.page(log, request);
    return RestResponse.success(applicationList);
  }

  @PermissionScope(app = "#log.appId", team = "#log.teamId")
  @PostMapping("deleteOperationLog")
  @RequiresPermissions("app:delete")
  public RestResponse deleteOperationLog(ApplicationLog log) {
    Boolean deleted = applicationLogService.delete(log);
    return RestResponse.success(deleted);
  }

  @PermissionScope(app = "#app.id", team = "#app.teamId")
  @PostMapping("delete")
  @RequiresPermissions("app:delete")
  public RestResponse delete(Application app) throws InternalException {
    Boolean deleted = applicationService.delete(app);
    return RestResponse.success(deleted);
  }

  @PermissionScope(app = "#backUp.appId")
  @PostMapping("deletebak")
  public RestResponse deleteBak(ApplicationBackUp backUp) throws InternalException {
    Boolean deleted = backUpService.delete(backUp.getId());
    return RestResponse.success(deleted);
  }

  @PostMapping("checkjar")
  public RestResponse checkjar(String jar) {
    File file = new File(jar);
    try {
      Utils.checkJarFile(file.toURI().toURL());
      return RestResponse.success(true);
    } catch (IOException e) {
      return RestResponse.success(file).message(e.getLocalizedMessage());
    }
  }

  @PostMapping("upload")
  @RequiresPermissions("app:create")
  public RestResponse upload(MultipartFile file) throws Exception {
    String uploadPath = applicationService.upload(file);
    return RestResponse.success(uploadPath);
  }

  @PostMapping("verifySchema")
  public RestResponse verifySchema(String path) {
    final URI uri = URI.create(path);
    final String scheme = uri.getScheme();
    final String pathPart = uri.getPath();
    RestResponse restResponse = RestResponse.success(true);
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
      restResponse = RestResponse.success(false).message(error);
    }
    return restResponse;
  }

  @PostMapping("checkSavepointPath")
  @PermissionScope(app = "#app.id", team = "#app.teamId")
  public RestResponse checkSavepointPath(Application app) throws Exception {
    String error = applicationService.checkSavepointPath(app);
    if (error == null) {
      return RestResponse.success(true);
    } else {
      return RestResponse.success(false).message(error);
    }
  }

  @PermissionScope(app = "#id")
  @PostMapping(value = "k8sStartLog")
  public RestResponse k8sStartLog(Long id, Integer offset, Integer limit) throws Exception {
    String resp = applicationService.k8sStartLog(id, offset, limit);
    return RestResponse.success(resp);
  }
}
