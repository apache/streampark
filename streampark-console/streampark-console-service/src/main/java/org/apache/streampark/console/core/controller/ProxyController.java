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

import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.SparkApplication;
import org.apache.streampark.console.core.enums.EngineTypeEnum;
import org.apache.streampark.console.core.service.ProxyService;
import org.apache.streampark.console.core.service.application.ApplicationLogService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.core.service.application.SparkApplicationManageService;
import org.apache.streampark.console.core.util.ServiceHelper;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Slf4j
@Validated
@RestController
@RequestMapping("proxy")
public class ProxyController {

    @Autowired
    private ProxyService proxyService;

    @Autowired
    private FlinkApplicationManageService flinkApplicationManageService;

    @Autowired
    private SparkApplicationManageService sparkApplicationManageService;

    @Autowired
    private ApplicationLogService logService;

    @GetMapping("{type}/{id}/assets/**")
    public void proxyFlinkAssets(HttpServletRequest request, HttpServletResponse response,
                                 @PathVariable("type") String type, @PathVariable("id") Long id) throws Exception {
        proxy(type, request, response, id);
    }

    @GetMapping("{type}/{id}/**")
    @RequiresPermissions("app:view")
    public void proxyFlink(HttpServletRequest request, HttpServletResponse response,
                           @PathVariable("type") String type, @PathVariable("id") Long id) throws Exception {
        proxy(type, request, response, id);
    }

    private void proxy(String type, HttpServletRequest request, HttpServletResponse response,
                       Long id) throws Exception {
        ApiAlertException.throwIfNull(ServiceHelper.getLoginUser(), "Permission denied, please login first.");
        ApplicationLog log;
        switch (type) {
            case "flink":
                FlinkApplication flinkApplication = flinkApplicationManageService.getApp(id);
                proxyService.proxyFlink(request, response, flinkApplication);
                return;
            case "spark":
                SparkApplication sparkApplication = sparkApplicationManageService.getApp(id);
                proxyService.proxySpark(request, response, sparkApplication);
                return;
            case "flink_cluster":
                proxyService.proxyFlinkCluster(request, response, id);
                return;
            case "history":
                log = logService.getById(id);
                checkProxyAppLog(log);
                proxyService.proxyHistory(request, response, log);
                return;
            case "yarn":
                log = logService.getById(id);
                checkProxyAppLog(log);
                proxyService.proxyYarn(request, response, log);
                return;
            default:
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private void checkProxyAppLog(ApplicationLog log) {
        ApiAlertException.throwIfNull(log, "Invalid operation, The application log not found.");
        ApiAlertException.throwIfFalse(
            log.getJobType() == EngineTypeEnum.FLINK.getCode()
                || log.getJobType() == EngineTypeEnum.SPARK.getCode(),
            "Invalid operation, unknown application log type.");
    }
}
