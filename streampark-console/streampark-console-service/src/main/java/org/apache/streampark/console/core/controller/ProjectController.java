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

import org.apache.streampark.console.base.domain.ResponseCode;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.web.FormOrJson;
import org.apache.streampark.console.core.annotation.AppChangeEvent;
import org.apache.streampark.console.core.assembler.ProjectAssembler;
import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.enums.GitAuthorizedErrorEnum;
import org.apache.streampark.console.core.request.common.TeamIdRequest;
import org.apache.streampark.console.core.request.common.TeamScopedIdRequest;
import org.apache.streampark.console.core.request.project.ProjectBuildLogRequest;
import org.apache.streampark.console.core.request.project.ProjectCreateRequest;
import org.apache.streampark.console.core.request.project.ProjectExistsRequest;
import org.apache.streampark.console.core.request.project.ProjectGitRequest;
import org.apache.streampark.console.core.request.project.ProjectListQueryRequest;
import org.apache.streampark.console.core.request.project.ProjectModuleRequest;
import org.apache.streampark.console.core.request.project.ProjectUpdateRequest;
import org.apache.streampark.console.core.response.project.ProjectBranchesResponse;
import org.apache.streampark.console.core.response.project.ProjectResponse;
import org.apache.streampark.console.core.service.ProjectService;
import org.apache.streampark.console.core.service.result.ProjectBuildLogResult;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Validated
@RestController
@RequestMapping("project")
public class ProjectController {

    @Autowired
    private ProjectService projectService;

    @PostMapping("create")
    @RequiresPermissions("project:create")
    public RestResponseBody<Boolean> create(@Valid @FormOrJson ProjectCreateRequest request) {
        ApiAlertException.throwIfNull(
            request.getTeamId(), "The teamId can't be null. Create team failed.");
        boolean status = projectService.create(ProjectAssembler.toEntity(request));
        if (status) {
            return RestResponseBody.success(true).message("Add project successfully");
        }
        return RestResponseBody.success(false).message("Add project failed");
    }

    @AppChangeEvent
    @PostMapping("update")
    @RequiresPermissions("project:update")
    public RestResponseBody<Boolean> update(@Valid @FormOrJson ProjectUpdateRequest request) {
        boolean update = projectService.update(ProjectAssembler.toEntity(request));
        return RestResponseBody.success(update);
    }

    @PostMapping("get")
    public RestResponseBody<ProjectResponse> get(@Valid TeamScopedIdRequest request) {
        return RestResponseBody.success(ProjectAssembler.toResponse(projectService.getById(request.getId())));
    }

    @PostMapping("build")
    @RequiresPermissions("project:build")
    public RestResponseBody<Void> build(@Valid @FormOrJson TeamScopedIdRequest request) throws Exception {
        projectService.build(request.getId());
        return RestResponseBody.success();
    }

    @PostMapping("build_log")
    @RequiresPermissions("project:build")
    public RestResponseBody<String> buildLog(ProjectBuildLogRequest request) {
        ProjectBuildLogResult result = projectService.getBuildLog(request.getId(), request.getStartOffset());
        if (result.isFailed()) {
            return RestResponseBody.fail(ResponseCode.CODE_FAIL, result.getContent());
        }
        RestResponseBody<String> response = RestResponseBody.success(result.getContent());
        if (result.getOffset() != null) {
            response.extra("offset", result.getOffset());
        }
        if (result.getReadFinished() != null) {
            response.extra("readFinished", result.getReadFinished());
        }
        return response;
    }

    @PostMapping("list")
    @RequiresPermissions("project:view")
    public RestResponseBody<IPage<ProjectResponse>> list(ProjectListQueryRequest query, RestRequest restRequest) {
        if (query.getTeamId() == null) {
            Page<ProjectResponse> emptyPage = new Page<>();
            emptyPage.setRecords(Collections.emptyList());
            return RestResponseBody.success(emptyPage);
        }
        IPage<Project> page = projectService.getPage(ProjectAssembler.toEntity(query), restRequest);
        return RestResponseBody.success(ProjectAssembler.toPageResponse(page));
    }

    @PostMapping("branches")
    public RestResponseBody<ProjectBranchesResponse> branches(ProjectGitRequest request) {
        Project project = ProjectAssembler.toEntity(request);
        List<String> branches = projectService.getAllBranches(project);
        List<String> tags = projectService.getAllTags(project);
        return RestResponseBody.success(ProjectAssembler.toBranchesResponse(branches, tags));
    }

    @PostMapping("delete")
    @RequiresPermissions("project:delete")
    public RestResponseBody<Boolean> delete(@Valid @FormOrJson TeamScopedIdRequest request) {
        Boolean deleted = projectService.removeById(request.getId());
        return RestResponseBody.success(deleted);
    }

    @PostMapping("git_check")
    public RestResponseBody<Integer> gitCheck(ProjectGitRequest request) {
        GitAuthorizedErrorEnum error = projectService.gitCheck(ProjectAssembler.toEntity(request));
        return RestResponseBody.success(error.getType());
    }

    @PostMapping("exists")
    public RestResponseBody<Boolean> exists(ProjectExistsRequest request) {
        boolean exists = projectService.exists(ProjectAssembler.toEntity(request));
        return RestResponseBody.success(exists);
    }

    @PostMapping("modules")
    public RestResponseBody<List<String>> modules(@Valid TeamScopedIdRequest request) {
        List<String> result = projectService.listModules(request.getId());
        return RestResponseBody.success(result);
    }

    @PostMapping("jars")
    public RestResponseBody<List<String>> jars(ProjectModuleRequest request) {
        List<String> result = projectService.listJars(ProjectAssembler.toEntity(request));
        return RestResponseBody.success(result);
    }

    @PostMapping("list_conf")
    public RestResponseBody<List<Map<String, Object>>> listConf(ProjectModuleRequest request) {
        List<Map<String, Object>> list =
            projectService.listConf(ProjectAssembler.toEntity(request));
        return RestResponseBody.success(list);
    }

    @PostMapping("select")
    public RestResponseBody<List<ProjectResponse>> select(@Valid TeamIdRequest request) {
        List<Project> list = projectService.listByTeamId(request.getTeamId());
        return RestResponseBody.success(ProjectAssembler.toListResponse(list));
    }
}
