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
import org.apache.streampark.console.core.annotation.OpenAPI;
import org.apache.streampark.console.core.assembler.FlinkApplicationAssembler;
import org.apache.streampark.console.core.bean.ApiContractDocument;
import org.apache.streampark.console.core.bean.OpenAPISchema;
import org.apache.streampark.console.core.component.ApiContractExportService;
import org.apache.streampark.console.core.component.ApiTypeScriptGenerator;
import org.apache.streampark.console.core.component.OpenAPIComponent;
import org.apache.streampark.console.core.request.flink.FlinkAppCancelRequest;
import org.apache.streampark.console.core.request.flink.FlinkAppStartRequest;
import org.apache.streampark.console.core.request.flink.OpenAPICurlRequest;
import org.apache.streampark.console.core.request.flink.OpenAPISchemaRequest;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@Validated
@RestController
@RequestMapping("openapi")
public class OpenAPIController {

    @Autowired
    private OpenAPIComponent openAPIComponent;

    @Autowired
    private ApiContractExportService apiContractExportService;

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
    @PostMapping("app/start")
    @RequiresPermissions("app:start")
    public RestResponseBody<Boolean> flinkStart(@Valid @FormOrJson FlinkAppStartRequest request) throws Exception {
        applicationActionService.start(FlinkApplicationAssembler.toEntity(request), false);
        return RestResponseBody.success(true);
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
    @PostMapping("app/cancel")
    @RequiresPermissions("app:cancel")
    public RestResponseBody<Void> flinkCancel(@Valid @FormOrJson FlinkAppCancelRequest request) throws Exception {
        applicationActionService.cancel(FlinkApplicationAssembler.toEntity(request));
        return RestResponseBody.success();
    }

    @PostMapping("curl")
    public RestResponseBody<String> copyOpenApiCurl(OpenAPICurlRequest request) {
        String url = openAPIComponent.getOpenApiCUrl(
            request.getName(), request.getBaseUrl(), request.getAppId(), request.getTeamId());
        return RestResponseBody.success(url);
    }

    @PostMapping("schema")
    public RestResponseBody<OpenAPISchema> schema(@Valid OpenAPISchemaRequest request) {
        OpenAPISchema openAPISchema = openAPIComponent.getOpenAPISchema(request.getName());
        return RestResponseBody.success(openAPISchema);
    }

    @PostMapping("contracts")
    public RestResponseBody<ApiContractDocument> exportContracts() {
        return RestResponseBody.success(apiContractExportService.exportContracts());
    }

    @PostMapping("contracts/typescript")
    public RestResponseBody<String> exportTypeScript() {
        ApiContractDocument document = apiContractExportService.exportContracts();
        return RestResponseBody.success(ApiTypeScriptGenerator.generate(document));
    }

}
