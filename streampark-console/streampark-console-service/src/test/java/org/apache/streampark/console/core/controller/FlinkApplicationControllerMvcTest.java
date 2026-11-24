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

import org.apache.streampark.console.base.handler.GlobalExceptionHandler;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.enums.AppExistsStateEnum;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.console.core.service.application.ApplicationLogService;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBackupService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBuildPipelineService;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(FlinkApplicationController.class)
@AutoConfigureMockMvc(addFilters = false)
@Import(GlobalExceptionHandler.class)
class FlinkApplicationControllerMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private FlinkApplicationManageService applicationManageService;

    @MockBean
    private FlinkApplicationBuildPipelineService appBuildPipeService;

    @MockBean
    private FlinkApplicationActionService applicationActionService;

    @MockBean
    private FlinkApplicationInfoService applicationInfoService;

    @MockBean
    private FlinkApplicationBackupService backUpService;

    @MockBean
    private ApplicationLogService applicationLogService;

    @MockBean
    private ResourceService resourceService;

    @Test
    void checkNameShouldRejectBlankJobName() throws Exception {
        mockMvc.perform(
            post("/flink/app/check/name")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("teamId", "100000"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value("error"));
    }

    @Test
    void checkNameShouldReturnExistsState() throws Exception {
        when(applicationInfoService.checkExists(any())).thenReturn(AppExistsStateEnum.IN_DB);

        mockMvc.perform(
            post("/flink/app/check/name")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("jobName", "demo")
                .param("teamId", "100000"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("success"))
            .andExpect(jsonPath("$.data").value(AppExistsStateEnum.IN_DB.get()));
    }

    @Test
    void downloadArtifactRejectsDirtyApp() throws Exception {
        FlinkApplication application = new FlinkApplication();
        application.setId(42L);
        application.setRelease(ReleaseStateEnum.DONE.get());
        application.setBuild(true);
        when(applicationManageService.getApp(42L)).thenReturn(application);

        mockMvc.perform(
            post("/flink/app/downloadArtifact")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("id", "42")
                .param("teamId", "100000"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.status").value("error"))
            .andExpect(jsonPath("$.code").value(501))
            .andExpect(jsonPath("$.message").value("The job has changes that require a new build."));
    }
}
