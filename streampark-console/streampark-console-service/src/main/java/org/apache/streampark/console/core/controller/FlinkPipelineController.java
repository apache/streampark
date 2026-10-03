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

import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.web.FormOrJson;
import org.apache.streampark.console.core.assembler.FlinkPipelineAssembler;
import org.apache.streampark.console.core.bean.AppBuildDockerResolvedDetail;
import org.apache.streampark.console.core.entity.ApplicationBuildPipeline;
import org.apache.streampark.console.core.request.flink.FlinkPipelineBuildRequest;
import org.apache.streampark.console.core.request.flink.FlinkPipelineDetailRequest;
import org.apache.streampark.console.core.response.flink.FlinkPipelineDetailResponse;
import org.apache.streampark.console.core.service.application.FlinkApplicationBuildPipelineService;
import org.apache.streampark.flink.packer.pipeline.DockerResolvedSnapshot;
import org.apache.streampark.flink.packer.pipeline.PipelineTypeEnum;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.util.Optional;

@Slf4j
@Validated
@RestController
@RequestMapping("flink/pipe")
public class FlinkPipelineController {

    @Autowired
    private FlinkApplicationBuildPipelineService appBuildPipeService;

    @PostMapping("build")
    @RequiresPermissions("app:create")
    public RestResponseBody<Boolean> buildApplication(@Valid @FormOrJson FlinkPipelineBuildRequest request) throws Exception {
        boolean actionResult = appBuildPipeService.buildApplication(request.getAppId(), request.isForceBuild());
        return RestResponseBody.success(actionResult);
    }

    /**
     * Get application building pipeline progress detail.
     *
     * @param request application id
     * @return pipeline and docker resolved snapshot details
     */
    @PostMapping("/detail")
    @RequiresPermissions("app:view")
    public RestResponseBody<FlinkPipelineDetailResponse> getBuildProgressDetail(@Valid FlinkPipelineDetailRequest request) {
        Long appId = request.getAppId();
        Optional<ApplicationBuildPipeline> pipeline = appBuildPipeService.getCurrentBuildPipeline(appId);
        ApplicationBuildPipeline.View pipelineView =
            pipeline.map(ApplicationBuildPipeline::toView).orElse(null);

        AppBuildDockerResolvedDetail dockerDetail = null;
        if (pipeline.isPresent()
            && PipelineTypeEnum.FLINK_NATIVE_K8S_APPLICATION == pipeline.get().getPipeType()) {
            DockerResolvedSnapshot dockerProgress = appBuildPipeService.getDockerProgressDetailSnapshot(appId);
            dockerDetail = AppBuildDockerResolvedDetail.of(dockerProgress);
        }
        return RestResponseBody.success(FlinkPipelineAssembler.toDetailResponse(pipelineView, dockerDetail));
    }
}
