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

import org.apache.streampark.common.util.DateUtils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.exception.AlertException;
import org.apache.streampark.console.core.assembler.AlertAssembler;
import org.apache.streampark.console.core.bean.AlertTemplate;
import org.apache.streampark.console.core.entity.AlertConfig;
import org.apache.streampark.console.core.request.alert.AlertConfigExistsRequest;
import org.apache.streampark.console.core.request.alert.AlertConfigIdRequest;
import org.apache.streampark.console.core.request.alert.AlertConfigPageRequest;
import org.apache.streampark.console.core.request.alert.AlertConfigRequest;
import org.apache.streampark.console.core.request.alert.AlertSendRequest;
import org.apache.streampark.console.core.request.common.IdRequest;
import org.apache.streampark.console.core.response.alert.AlertConfigResponse;
import org.apache.streampark.console.core.service.alert.AlertConfigService;
import org.apache.streampark.console.core.service.alert.AlertService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;

@Slf4j
@Validated
@RequiredArgsConstructor
@RestController
@RequestMapping("/flink/alert")
public class AlertController {

    private final AlertConfigService alertConfigService;

    private final AlertService alertService;

    @PostMapping("/add")
    @RequiresPermissions("setting:update")
    public RestResponseBody<Boolean> createAlertConfig(@Valid @RequestBody AlertConfigRequest request) {
        boolean save = alertConfigService.save(AlertAssembler.toEntity(request));
        return RestResponseBody.success(save);
    }

    @PostMapping("/exists")
    public RestResponseBody<Boolean> verifyAlertConfig(@Valid @RequestBody AlertConfigExistsRequest request) {
        AlertConfig probe = new AlertConfig();
        probe.setAlertName(request.getAlertName());
        boolean exist = alertConfigService.exist(probe);
        return RestResponseBody.success(exist);
    }

    @PostMapping("/update")
    @RequiresPermissions("setting:update")
    public RestResponseBody<Boolean> updateAlertConfig(@Valid @RequestBody AlertConfigRequest request) {
        boolean update = alertConfigService.updateById(AlertAssembler.toEntity(request));
        return RestResponseBody.success(update);
    }

    @PostMapping("/get")
    public RestResponseBody<AlertConfigResponse> getAlertConfig(@Valid @RequestBody AlertConfigIdRequest request) {
        AlertConfig alertConfig = alertConfigService.getById(request.getId());
        return RestResponseBody.success(AlertAssembler.toResponse(alertConfig));
    }

    @PostMapping("/page")
    public RestResponseBody<IPage<AlertConfigResponse>> pageAlertConfig(
                                                                        @RequestBody AlertConfigPageRequest request,
                                                                        RestRequest restRequest) {
        IPage<AlertConfig> page = alertConfigService.pageEntities(request.getUserId(), restRequest);
        return RestResponseBody.success(AlertAssembler.toPageResponse(page));
    }

    @PostMapping("/list")
    public RestResponseBody<List<AlertConfigResponse>> listAlertConfig() {
        return RestResponseBody.success(AlertAssembler.toListResponse(alertConfigService.list()));
    }

    @DeleteMapping("/delete")
    @RequiresPermissions("setting:update")
    public RestResponseBody<Boolean> deleteAlertConfig(@NotNull(message = "{required}") @Valid IdRequest request) {
        boolean result = alertConfigService.removeById(request.getId());
        return RestResponseBody.success(result);
    }

    @PostMapping("/send")
    @RequiresPermissions("setting:update")
    public RestResponseBody<Boolean> sendAlert(@Valid AlertSendRequest request) throws AlertException {
        AlertTemplate alertTemplate = new AlertTemplate();
        alertTemplate.setTitle("Notify: StreamPark alert job for test");
        alertTemplate.setJobName("StreamPark alert job for test");
        alertTemplate.setSubject("StreamPark Alert: Test");
        alertTemplate.setStatus("TEST");
        alertTemplate.setType(1);
        alertTemplate.setRestart(false);
        Date date = new Date();
        alertTemplate.setStartTime(
            DateUtils.format(date, DateUtils.fullFormat(), TimeZone.getDefault()));
        alertTemplate.setEndTime(DateUtils.format(date, DateUtils.fullFormat(), TimeZone.getDefault()));
        alertTemplate.setDuration("");
        return RestResponseBody.success(alertService.alert(request.getId(), alertTemplate));
    }
}
