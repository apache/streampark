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

package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.common.conf.CommonConfig;
import org.apache.streampark.common.conf.InternalConfigHolder;
import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.CompletableFutureUtils;
import org.apache.streampark.common.util.ThreadUtils;
import org.apache.streampark.console.base.domain.ResponseCode;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.ApiDetailException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.base.util.CommonUtils;
import org.apache.streampark.console.base.util.FileUtils;
import org.apache.streampark.console.base.util.GZipUtils;
import org.apache.streampark.console.base.util.GitUtils;
import org.apache.streampark.console.base.util.ObjectUtils;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.enums.BuildState;
import org.apache.streampark.console.core.enums.GitAuthorizedError;
import org.apache.streampark.console.core.enums.ReleaseState;
import org.apache.streampark.console.core.mapper.ProjectMapper;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.ProjectService;
import org.apache.streampark.console.core.task.FlinkAppHttpWatcher;
import org.apache.streampark.console.core.task.ProjectBuildTask;

import org.apache.flink.configuration.MemorySize;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class ProjectServiceImpl extends ServiceImpl<ProjectMapper, Project>
    implements ProjectService {

  @Autowired private ApplicationService applicationService;

  @Autowired private FlinkAppHttpWatcher flinkAppHttpWatcher;

  @Value("${streampark.project.max-build:6}")
  public Long maxProjectBuildNum;

  private static final int CPU_NUM = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);

  private final ExecutorService projectBuildExecutor =
      new ThreadPoolExecutor(
          CPU_NUM,
          CPU_NUM,
          60L,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(),
          ThreadUtils.threadFactory("streampark-project-build"));

  @Override
  public RestResponse create(Project project) {
    RestResponse response = RestResponse.success();
    project.setId(null);
    ApiAlertException.throwIfTrue(
        checkExists(project), "project name already exists, add project failed");
    Date date = new Date();
    project.setCreateTime(date);
    project.setModifyTime(date);
    boolean status = save(project);
    if (status) {
      return response.message("Add project successfully").data(true);
    } else {
      return response.message("Add project failed").data(false);
    }
  }

  @Override
  @Transactional(rollbackFor = {Exception.class})
  public boolean update(Project projectParam) {
    Project project = getById(projectParam.getId());
    AssertUtils.notNull(project);
    ApiAlertException.throwIfFalse(
        project.getTeamId().equals(projectParam.getTeamId()),
        "Team can't be changed, update project failed.");
    ApiAlertException.throwIfFalse(
        !project.getBuildState().equals(BuildState.BUILDING.get()),
        "The project is being built, update project failed.");
    project.setName(projectParam.getName());
    project.setUrl(projectParam.getUrl());
    project.setRefs(projectParam.getRefs());
    project.setPrvkeyPath(projectParam.getPrvkeyPath());
    project.setUserName(projectParam.getUserName());
    project.setPassword(projectParam.getPassword());
    project.setPom(projectParam.getPom());
    project.setDescription(projectParam.getDescription());
    project.setBuildArgs(projectParam.getBuildArgs());
    project.setModifyTime(new Date());
    if (GitUtils.isSshRepositoryUrl(project.getUrl())) {
      project.setUserName(null);
    } else {
      project.setPrvkeyPath(null);
    }
    if (projectParam.getBuildState() != null) {
      project.setBuildState(projectParam.getBuildState());
      if (BuildState.of(projectParam.getBuildState()).equals(BuildState.NEED_REBUILD)) {
        List<Application> applications = getApplications(project);
        // Update deployment status
        applications.forEach(
            (app) -> {
              log.info(
                  "update deploy by project: {}, appName:{}", project.getName(), app.getJobName());
              app.setRelease(ReleaseState.NEED_CHECK.get());
              applicationService.updateRelease(app);
            });
      }
    }
    baseMapper.updateById(project);
    return true;
  }

  @Override
  @Transactional(rollbackFor = {Exception.class})
  public boolean delete(Long id) {
    Project project = getById(id);
    AssertUtils.notNull(project);
    LambdaQueryWrapper<Application> queryWrapper =
        new LambdaQueryWrapper<Application>().eq(Application::getProjectId, id);
    long count = applicationService.count(queryWrapper);
    if (count > 0) {
      return false;
    }
    try {
      project.delete();
      removeById(id);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public IPage<Project> page(Project project, RestRequest request) {
    Page<Project> page = MybatisPager.getPage(request);
    return this.baseMapper.page(page, project);
  }

  @Override
  public Boolean existsByTeamId(Long teamId) {
    return this.baseMapper.existsByTeamId(teamId);
  }

  @Override
  public List<Project> findByTeamId(Long teamId) {
    return this.baseMapper.selectByTeamId(teamId);
  }

  @Override
  public void build(Long id) throws Exception {
    Long currentBuildCount = this.baseMapper.getBuildingCount();

    ApiAlertException.throwIfTrue(
        maxProjectBuildNum > -1 && currentBuildCount > maxProjectBuildNum,
        String.format(
            "The number of running Build projects exceeds the maximum number: %d of max-build-num",
            maxProjectBuildNum));

    Project project = getById(id);
    this.baseMapper.updateBuildState(project.getId(), BuildState.BUILDING.get());
    String logPath = getBuildLogPath(id);
    ProjectBuildTask projectBuildTask =
        new ProjectBuildTask(
            logPath,
            project,
            buildState -> {
              baseMapper.updateBuildState(id, buildState.get());
              if (buildState == BuildState.SUCCESSFUL) {
                baseMapper.updateBuildTime(id);
              }
              flinkAppHttpWatcher.initialize();
            },
            fileLogger -> {
              List<Application> applications =
                  this.applicationService.getByProjectId(project.getId());
              applications.forEach(
                  (app) -> {
                    fileLogger.info(
                        "update deploy by project: {}, appName:{}",
                        project.getName(),
                        app.getJobName());
                    app.setRelease(ReleaseState.NEED_RELEASE.get());
                    app.setBuild(true);
                    this.applicationService.updateRelease(app);
                  });
              flinkAppHttpWatcher.initialize();
            });
    CompletableFuture<Void> buildTask =
        CompletableFuture.runAsync(projectBuildTask, projectBuildExecutor);
    // TODO May need to define parameters to set the build timeout in the future.
    CompletableFutureUtils.runTimeout(buildTask, 20, TimeUnit.MINUTES);
  }

  @Override
  public List<String> modules(Long id) {
    Project project = getById(id);
    AssertUtils.notNull(project);
    BuildState buildState = BuildState.of(project.getBuildState());
    if (BuildState.SUCCESSFUL.equals(buildState)) {
      File appHome = project.getDistHome();
      if (appHome.exists()) {
        List<String> list = new ArrayList<>();
        File[] files = appHome.listFiles();
        if (CommonUtils.notEmpty(files)) {
          for (File file : files) {
            list.add(file.getName());
          }
        }
        return list;
      } else {
        return Collections.emptyList();
      }
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public List<String> jars(Project project) {
    List<String> list = new ArrayList<>(0);
    ApiAlertException.throwIfNull(
        project.getModule(), "Project module can't be null, please check.");
    File apps = new File(project.getDistHome(), project.getModule());
    for (File file : Objects.requireNonNull(apps.listFiles())) {
      if (file.getName().endsWith(".jar")) {
        list.add(file.getName());
      }
    }
    return list;
  }

  @Override
  public String getAppConfPath(Long id, String module) {
    Project project = getById(id);
    File appHome = project.getDistHome();
    File[] files = appHome.listFiles();
    if (!appHome.exists() || files == null) {
      return null;
    }
    Optional<File> fileOptional =
        Arrays.stream(files).filter((x) -> x.getName().equals(module)).findFirst();
    return fileOptional.map(File::getAbsolutePath).orElse(null);
  }

  @Override
  public List<Application> getApplications(Project project) {
    return this.applicationService.getByProjectId(project.getId());
  }

  @Override
  public boolean checkExists(Project project) {
    if (project.getId() != null) {
      Project proj = getById(project.getId());
      if (proj != null && ObjectUtils.safeEquals(project.getName(), proj.getName())) {
        return false;
      }
    }
    LambdaQueryWrapper<Project> queryWrapper =
        new LambdaQueryWrapper<Project>()
            .eq(Project::getName, project.getName())
            .eq(Project::getTeamId, project.getTeamId());
    return this.baseMapper.selectCount(queryWrapper) > 0;
  }

  @Override
  public List<String> getAllBranches(Project project) {
    try {
      GitUtils.GitGetRequest request = new GitUtils.GitGetRequest();
      request.setUrl(project.getUrl());
      request.setUsername(project.getUserName());
      request.setPassword(project.getPassword());
      request.setPrivateKey(project.getPrvkeyPath());
      return GitUtils.getBranches(request);
    } catch (Exception e) {
      throw new ApiDetailException(e);
    }
  }

  @Override
  public GitAuthorizedError gitCheck(Project project) {
    try {
      GitUtils.GitGetRequest request = new GitUtils.GitGetRequest();
      request.setUrl(project.getUrl());
      request.setUsername(project.getUserName());
      request.setPassword(project.getPassword());
      request.setPrivateKey(project.getPrvkeyPath());
      GitUtils.getBranches(request);
      return GitAuthorizedError.SUCCESS;
    } catch (Exception e) {
      String err = e.getMessage();
      if (err.contains("not authorized")) {
        return GitAuthorizedError.ERROR;
      } else if (err.contains("Authentication is required")) {
        return GitAuthorizedError.REQUIRED;
      }
      return GitAuthorizedError.UNKNOW;
    }
  }

  @Override
  public List<String> getAllTags(Project project) {
    try {
      GitUtils.GitGetRequest request = new GitUtils.GitGetRequest();
      request.setUrl(project.getUrl());
      request.setUsername(project.getUserName());
      request.setPassword(project.getPassword());
      request.setPrivateKey(project.getPrvkeyPath());
      return GitUtils.getTags(request);
    } catch (Exception e) {
      throw new ApiDetailException(e);
    }
  }

  @Override
  public List<Map<String, Object>> listConf(Project project) {
    try {
      File file = new File(project.getDistHome(), project.getModule());
      File unzipFile = new File(file.getAbsolutePath().replaceAll(".tar.gz", ""));
      if (!unzipFile.exists()) {
        GZipUtils.decompress(file.getAbsolutePath(), file.getParentFile().getAbsolutePath());
      }
      List<Map<String, Object>> list = new ArrayList<>();
      File[] files = unzipFile.listFiles(x -> "conf".equals(x.getName()));
      AssertUtils.notNull(files);
      for (File item : files) {
        eachFile(item, list, true);
      }
      return list;
    } catch (Exception e) {
      log.error("List conf Failed!", e);
      log.error(e.getMessage());
    }
    return null;
  }

  private void eachFile(File file, List<Map<String, Object>> list, Boolean isRoot) {
    if (file != null && file.exists() && file.listFiles() != null) {
      if (isRoot) {
        Map<String, Object> map = new HashMap<>(0);
        map.put("key", file.getName());
        map.put("title", file.getName());
        map.put("value", file.getAbsolutePath());
        List<Map<String, Object>> children = new ArrayList<>();
        eachFile(file, children, false);
        if (!children.isEmpty()) {
          map.put("children", children);
        }
        list.add(map);
      } else {
        for (File item : Objects.requireNonNull(file.listFiles())) {
          String title = item.getName();
          String value = item.getAbsolutePath();
          Map<String, Object> map = new HashMap<>(0);
          map.put("key", title);
          map.put("title", title);
          map.put("value", value);
          List<Map<String, Object>> children = new ArrayList<>();
          eachFile(item, children, false);
          if (!children.isEmpty()) {
            map.put("children", children);
          }
          list.add(map);
        }
      }
    }
  }

  @Override
  public RestResponse getBuildLog(Long id, Long startOffset) {
    File logFile = Paths.get(getBuildLogPath(id)).toFile();
    if (!logFile.exists()) {
      String errorMsg =
          String.format("Build log file(fileName=%s) not found, please build first.", logFile);
      log.warn(errorMsg);
      return RestResponse.success().data(errorMsg);
    }
    boolean isBuilding = this.getById(id).getBuildState() == 0;
    byte[] fileContent;
    long endOffset = 0L;
    boolean readFinished = true;
    // Read log from earliest when project is building
    if (startOffset == null && isBuilding) {
      startOffset = 0L;
    }
    try {
      long maxSize =
          MemorySize.parse(InternalConfigHolder.get(CommonConfig.READ_LOG_MAX_SIZE())).getBytes();
      if (startOffset == null) {
        fileContent = FileUtils.readEndOfFile(logFile, maxSize);
      } else {
        fileContent = FileUtils.readFileFromOffset(logFile, startOffset, maxSize);
        endOffset = startOffset + fileContent.length;
        readFinished = logFile.length() == endOffset && !isBuilding;
      }
      return RestResponse.success()
          .data(new String(fileContent, StandardCharsets.UTF_8))
          .put("offset", endOffset)
          .put("readFinished", readFinished);
    } catch (IOException e) {
      String error =
          String.format("Read build log file(fileName=%s) caused an exception: ", logFile);
      log.error(error, e);
      return RestResponse.fail(error + e.getMessage(), ResponseCode.CODE_FAIL);
    }
  }

  private String getBuildLogPath(Long projectId) {
    return String.format("%s/%s/build.log", Workspace.PROJECT_BUILD_LOG_PATH(), projectId);
  }
}
