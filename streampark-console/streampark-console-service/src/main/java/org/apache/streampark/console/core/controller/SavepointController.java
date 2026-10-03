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

import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.base.web.FormOrJson;
import org.apache.streampark.console.core.assembler.SavepointAssembler;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkSavepoint;
import org.apache.streampark.console.core.request.flink.SavepointDeleteRequest;
import org.apache.streampark.console.core.request.flink.SavepointHistoryQueryRequest;
import org.apache.streampark.console.core.request.flink.SavepointTriggerRequest;
import org.apache.streampark.console.core.response.flink.SavepointResponse;
import org.apache.streampark.console.core.service.SavepointService;
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

@Slf4j
@Validated
@RestController
@RequestMapping("flink/savepoint")
public class SavepointController {

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Autowired
    private SavepointService savepointService;

    @PostMapping("history")
    public RestResponseBody<IPage<SavepointResponse>> history(SavepointHistoryQueryRequest query, RestRequest request) {
        FlinkSavepoint sp = SavepointAssembler.toEntity(query);
        IPage<FlinkSavepoint> page = savepointService.getPage(sp, request);
        return RestResponseBody.success(SavepointAssembler.toPageResponse(page));
    }

    @PostMapping("delete")
    @RequiresPermissions("savepoint:delete")
    public RestResponseBody<Boolean> delete(@Valid @FormOrJson SavepointDeleteRequest request) throws InternalException {
        FlinkSavepoint savepoint = savepointService.getById(request.getId());
        FlinkApplication application = applicationManageService.getById(savepoint.getAppId());
        Boolean deleted = savepointService.remove(request.getId(), application);
        return RestResponseBody.success(deleted);
    }

    @PostMapping("trigger")
    @RequiresPermissions("savepoint:trigger")
    public RestResponseBody<Boolean> trigger(@Valid @FormOrJson SavepointTriggerRequest request) {
        savepointService.trigger(request.getAppId(), request.getSavepointPath(), request.getNativeFormat());
        return RestResponseBody.success(true);
    }
}
