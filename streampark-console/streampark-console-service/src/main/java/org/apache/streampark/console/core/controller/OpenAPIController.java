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

import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.core.annotation.OpenAPI;
import org.apache.streampark.console.core.annotation.Permission;
import org.apache.streampark.console.core.bean.OpenAPISchema;
import org.apache.streampark.console.core.component.OpenAPIComponent;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Validated
@RestController
@RequestMapping("openapi")
public class OpenAPIController {

    @Autowired
    private OpenAPIComponent openAPIComponent;

    @Autowired
    private FlinkApplicationActionService applicationActionService;

    @OpenAPI(name = "flinkStart", header = {
            @OpenAPI.Param(name = "Authorization", description = "Access authorization token", required = true, type = String.class)
    }, param = {
            @OpenAPI.Param(name = "id", description = "current flink application id", required = true, type = Long.class),
            @OpenAPI.Param(name = "teamId", description = "current user teamId", required = true, type = Long.class),
            @OpenAPI.Param(name = "argument", description = "flink program run arguments", required = false, type = String.class, bindFor = "args"),
            @OpenAPI.Param(name = "restoreFromSavepoint", description = "restored app from the savepoint or checkpoint", required = false, type = Boolean.class, defaultValue = "false", bindFor = "restoreOrTriggerSavepoint"),
            @OpenAPI.Param(name = "savepointPath", description = "savepoint or checkpoint path", required = false, type = String.class),
            @OpenAPI.Param(name = "allowNonRestored", description = "ignore savepoint if cannot be restored", required = false, type = Boolean.class, defaultValue = "false"),
    })
    @Permission(app = "#app.id", team = "#app.teamId")
    @PostMapping("app/start")
    @RequiresPermissions("app:start")
    public RestResponse flinkStart(FlinkApplication app) throws Exception {
        applicationActionService.start(app, false);
        return RestResponse.success(true);
    }

    @OpenAPI(name = "flinkCancel", header = {
            @OpenAPI.Param(name = "Authorization", description = "Access authorization token", required = true, type = String.class)
    }, param = {
            @OpenAPI.Param(name = "id", description = "current flink application id", required = true, type = Long.class),
            @OpenAPI.Param(name = "teamId", description = "current user teamId", required = true, type = Long.class),
            @OpenAPI.Param(name = "triggerSavepoint", description = "trigger savepoint before taking stopping", required = false, type = Boolean.class, defaultValue = "false", bindFor = "restoreOrTriggerSavepoint"),
            @OpenAPI.Param(name = "savepointPath", description = "savepoint path", required = false, type = String.class),
            @OpenAPI.Param(name = "drain", description = "send max watermark before canceling", required = false, type = Boolean.class, defaultValue = "false"),
    })
    @Permission(app = "#app.id", team = "#app.teamId")
    @PostMapping("app/cancel")
    @RequiresPermissions("app:cancel")
    public RestResponse flinkCancel(FlinkApplication app) throws Exception {
        applicationActionService.cancel(app);
        return RestResponse.success();
    }

    @PostMapping("curl")
    public RestResponse copyOpenApiCurl(String name,
                                        String baseUrl,
                                        @NotNull Long appId,
                                        @NotNull Long teamId) {
        String url = openAPIComponent.getOpenApiCUrl(name, baseUrl, appId, teamId);
        return RestResponse.success(url);
    }

    @PostMapping("schema")
    public RestResponse schema(@NotBlank(message = "{required}") String name) {
        OpenAPISchema openAPISchema = openAPIComponent.getOpenAPISchema(name);
        return RestResponse.success(openAPISchema);
    }

}
