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

import org.apache.streampark.common.configuration.option.CoreOptions;
import org.apache.streampark.common.enums.ApplicationType;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.console.base.handler.GlobalExceptionHandler;
import org.apache.streampark.console.base.util.SpringContextUtils;
import org.apache.streampark.console.core.aspect.PermissionAspect;
import org.apache.streampark.console.core.entity.ApplicationBuildPipeline;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.enums.AuthenticationType;
import org.apache.streampark.console.core.enums.ReleaseStateEnum;
import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.console.core.service.application.ApplicationLogService;
import org.apache.streampark.console.core.service.application.FlinkApplicationActionService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBackupService;
import org.apache.streampark.console.core.service.application.FlinkApplicationBuildPipelineService;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.system.authentication.JWTUtil;
import org.apache.streampark.console.system.entity.Member;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.MemberService;
import org.apache.streampark.console.system.service.UserService;
import org.apache.streampark.flink.packer.pipeline.PipelineStatusEnum;
import org.apache.streampark.flink.packer.pipeline.PipelineTypeEnum;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;

import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.config.Ini;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.shiro.spring.security.interceptor.AuthorizationAttributeSourceAdvisor;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Exercises the download endpoint through the real Shiro advisor and application permission aspect. */
class FlinkArtifactPermissionMvcTest {

    @TempDir
    Path temporary;

    private final FlinkApplicationManageService applications = mock(FlinkApplicationManageService.class);
    private final FlinkApplicationBuildPipelineService builds = mock(FlinkApplicationBuildPipelineService.class);
    private final ResourceService resources = mock(ResourceService.class);
    private final MemberService members = mock(MemberService.class);
    private final FlinkApplication application = mock(FlinkApplication.class);
    private final User user = new User();
    private final StaticApplicationContext context = new StaticApplicationContext();

    private ApplicationContext previousContext;
    private String previousAppHome;
    private Path consoleHome;
    private ApplicationBuildPipeline pipeline;
    private MockMvc mvc;
    private String principal;
    private DefaultSecurityManager securityManager;

    @BeforeEach
    void setUp() throws Exception {
        Path home = temporary.toRealPath();
        previousAppHome = System.getProperty(CoreOptions.APP_HOME.key());
        consoleHome = Files.createDirectory(home.resolve("console"));
        System.setProperty(CoreOptions.APP_HOME.key(), consoleHome.toString());
        Files.write(home.resolve("job.jar"), "job".getBytes(StandardCharsets.UTF_8));
        Files.createDirectory(home.resolve("lib"));
        Files.write(home.resolve("lib/connector.jar"), "connector".getBytes(StandardCharsets.UTF_8));
        user.setUserId(1L);
        user.setUsername("member");
        user.setUserType(UserTypeEnum.USER);
        UserService users = mock(UserService.class);
        when(users.getById(1L)).thenReturn(user);
        context.getBeanFactory().registerSingleton("userService", users);
        previousContext =
            (ApplicationContext) ReflectionTestUtils.getField(SpringContextUtils.class, "applicationContext");
        ReflectionTestUtils.setField(SpringContextUtils.class, "applicationContext", context);

        // Initialize JWT signing in the temporary fixture, never in the developer's home directory.
        String userHome = System.getProperty("user.home");
        try {
            System.setProperty("user.home", home.toString());
            principal = JWTUtil.decrypt(JWTUtil.sign(user, AuthenticationType.SIGN));
        } finally {
            System.setProperty("user.home", userHome);
        }

        when(application.getId()).thenReturn(101L);
        when(application.getUserId()).thenReturn(2L);
        when(application.getTeamId()).thenReturn(10L);
        when(application.getLocalAppHome()).thenReturn(home.toString());
        when(application.getAppHome()).thenReturn(home.toString());
        when(application.getAppLib()).thenReturn(home.resolve("lib").toString());
        when(application.getDeployModeEnum()).thenReturn(FlinkDeployMode.REMOTE);
        when(application.getApplicationType()).thenReturn(ApplicationType.APACHE_FLINK);
        when(application.isFlinkJar()).thenReturn(true);
        when(application.getJar()).thenReturn("job.jar");
        when(application.getBuild()).thenReturn(false);
        when(application.getRelease()).thenReturn(ReleaseStateEnum.DONE.get());
        when(applications.getById(101L)).thenReturn(application);
        when(applications.getApp(101L)).thenReturn(application);
        when(members.getByTeamIdUserName(10L, "member")).thenReturn(new Member());
        pipeline = new ApplicationBuildPipeline().setAppId(101L)
            .setPipeType(PipelineTypeEnum.FLINK_STANDALONE)
            .setPipeStatus(PipelineStatusEnum.SUCCESS)
            .setBuildResult(new ShadedBuildResponse(home.toString(), home.resolve("job.jar").toString()));
        when(builds.getCurrentBuildPipeline(101L)).thenReturn(Optional.of(pipeline));

        FlinkApplicationController controller = new FlinkApplicationController(
            applications,
            builds,
            mock(FlinkApplicationActionService.class),
            mock(FlinkApplicationInfoService.class),
            mock(FlinkApplicationBackupService.class),
            mock(ApplicationLogService.class),
            resources);
        PermissionAspect permission = new PermissionAspect();
        ReflectionTestUtils.setField(permission, "applicationManageService", applications);
        ReflectionTestUtils.setField(permission, "memberService", members);
        AuthorizationAttributeSourceAdvisor authorization = new AuthorizationAttributeSourceAdvisor();
        AspectJProxyFactory factory = new AspectJProxyFactory(controller);
        factory.setProxyTargetClass(true);
        factory.addAdvisor(authorization);
        factory.addAspect(permission);
        FlinkApplicationController secured = factory.getProxy();
        mvc = MockMvcBuilders.standaloneSetup(secured)
            .setControllerAdvice(new GlobalExceptionHandler()).build();
    }

    @AfterEach
    void tearDown() {
        if (previousAppHome == null) {
            System.clearProperty(CoreOptions.APP_HOME.key());
        } else {
            System.setProperty(CoreOptions.APP_HOME.key(), previousAppHome);
        }
        ThreadContext.unbindSubject();
        ThreadContext.unbindSecurityManager();
        if (securityManager != null) {
            securityManager.destroy();
        }
        ReflectionTestUtils.setField(SpringContextUtils.class, "applicationContext", previousContext);
        context.close();
    }

    @Test
    void teamMemberDownloadsCompleteZip() throws Exception {
        bindSubject(true);
        Files.writeString(Path.of(application.getAppLib()).resolve("readme.txt"), "not a dependency");
        when(application.getRelease()).thenReturn(ReleaseStateEnum.NEED_RESTART.get());
        MvcResult result = mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isOk())
            .andExpect(content().contentType("application/zip"))
            .andReturn();
        Path archive = Files.write(
            temporary.resolve("app-101-dependencies.zip"), result.getResponse().getContentAsByteArray());
        try (
            FileSystem zip = FileSystems.newFileSystem(URI.create("jar:" + archive.toUri()), Map.of());
            Stream<Path> roots = Files.list(zip.getPath("/"));
            Stream<Path> entries = Files.walk(zip.getPath("/app-101-dependencies"))) {
            assertThat(roots.map(Path::toString)).containsExactly("/app-101-dependencies");
            assertThat(entries.filter(Files::isRegularFile).map(Path::toString))
                .containsExactlyInAnyOrder(
                    "/app-101-dependencies/job.jar", "/app-101-dependencies/lib/connector.jar");
            assertThat(Files.readString(zip.getPath("/app-101-dependencies/job.jar"))).isEqualTo("job");
            assertThat(Files.readString(zip.getPath("/app-101-dependencies/lib/connector.jar"))).isEqualTo("connector");
        }
        assertThat(consoleHome.resolve("artifact-downloads")).isEmptyDirectory();
    }

    @Test
    void crossTeamMemberIsDenied() throws Exception {
        bindSubject(true);
        when(members.getByTeamIdUserName(10L, "member")).thenReturn(null);
        mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value(501));
        verifyNoInteractions(builds);
    }

    @Test
    void anonymousRequestIsDenied() throws Exception {
        securityManager = new DefaultSecurityManager();
        ThreadContext.bind(new Subject.Builder(securityManager).buildSubject());
        mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.code").value(401));
        verifyNoInteractions(builds);
    }

    @Test
    void detailPermissionIsRequired() throws Exception {
        bindSubject(false);
        mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.status").value("error"));
        verifyNoInteractions(builds);
    }

    @Test
    void missingBuildUsesExistingErrorContract() throws Exception {
        bindSubject(true);
        when(builds.getCurrentBuildPipeline(101L)).thenReturn(Optional.empty());
        mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value(501));
    }

    @Test
    void unfinishedBuildsAreDenied() throws Exception {
        bindSubject(true);
        for (PipelineStatusEnum state : new PipelineStatusEnum[]{PipelineStatusEnum.PENDING,
                PipelineStatusEnum.RUNNING}) {
            pipeline.setPipeStatus(state);
            mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value(501));
        }
    }

    @Test
    void foreignBuildIsDenied() throws Exception {
        bindSubject(true);
        pipeline.setAppId(102L);
        mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value(501));
    }

    @Test
    void unexpectedPipelineIsDenied() throws Exception {
        bindSubject(true);
        pipeline.setPipeType(PipelineTypeEnum.FLINK_NATIVE_K8S_SESSION);
        mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value(501));
    }

    @Test
    void failedAndUnknownBuildsAreDenied() throws Exception {
        bindSubject(true);
        for (PipelineStatusEnum state : new PipelineStatusEnum[]{PipelineStatusEnum.FAILURE,
                PipelineStatusEnum.UNKNOWN}) {
            pipeline.setPipeStatus(state);
            mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value(501));
        }
    }

    @Test
    void missingArtifactReturnsJsonFailure() throws Exception {
        bindSubject(true);
        Files.delete(Path.of(application.getLocalAppHome()).resolve("job.jar"));
        MvcResult result = mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.code").value(501))
            .andReturn();
        assertThat(result.getResponse().getHeader("Content-Disposition")).isNull();
        assertThat(consoleHome.resolve("artifact-downloads")).isEmptyDirectory();
    }

    @Test
    void missingSqlLibReturnsJsonFailure() throws Exception {
        bindSubject(true);
        when(application.isFlinkSqlJob()).thenReturn(true);
        when(application.isFlinkJar()).thenReturn(false);
        Path lib = Path.of(application.getAppLib());
        Files.delete(lib.resolve("connector.jar"));
        Files.delete(lib);

        MvcResult result = mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.code").value(501))
            .andReturn();
        assertThat(result.getResponse().getHeader("Content-Disposition")).isNull();
    }

    @Test
    void externalJarWithoutCopyFails() throws Exception {
        bindSubject(true);
        Path workspace = Files.createDirectory(temporary.toRealPath().resolve("app"));
        Path outside = temporary.toRealPath().resolve("job.jar");
        when(application.getLocalAppHome()).thenReturn(workspace.toString());
        when(application.isUploadResource()).thenReturn(true);
        pipeline.setBuildResult(new ShadedBuildResponse(workspace.toString(), outside.toString()));
        Resource resource = new Resource();
        resource.setTeamId(10L);
        resource.setResourceName("job.jar");
        resource.setResourcePath("job.jar:" + outside);
        when(resources.findByResourceName(10L, "job.jar")).thenReturn(resource);

        MvcResult result = mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.code").value(501))
            .andReturn();
        assertThat(result.getResponse().getHeader("Content-Disposition")).isNull();
    }

    @Test
    void detailReturnsCurrentBuildStatus() throws Exception {
        bindSubject(true);
        pipeline.setPipeStatus(PipelineStatusEnum.RUNNING);
        mvc.perform(post("/flink/app/get").param("id", "101").param("teamId", "10"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.data.buildStatus").value(PipelineStatusEnum.RUNNING.getCode()));
        when(builds.getCurrentBuildPipeline(101L)).thenReturn(Optional.empty());
        mvc.perform(post("/flink/app/get").param("id", "101").param("teamId", "10"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.data.buildStatus").value(PipelineStatusEnum.UNKNOWN.getCode()));
    }

    @Test
    void releasingApplicationIsDenied() throws Exception {
        bindSubject(true);
        when(application.getRelease()).thenReturn(ReleaseStateEnum.RELEASING.get());
        mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value(501));
        verifyNoInteractions(builds);
    }

    @Test
    void buildChangedDuringReadIsDenied() throws Exception {
        bindSubject(true);
        ApplicationBuildPipeline changed = new ApplicationBuildPipeline().setAppId(101L)
            .setPipeType(PipelineTypeEnum.FLINK_STANDALONE)
            .setPipeStatus(PipelineStatusEnum.RUNNING);
        when(builds.getCurrentBuildPipeline(101L))
            .thenReturn(Optional.of(pipeline), Optional.of(changed));
        MvcResult result = mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.code").value(501)).andReturn();
        assertThat(result.getResponse().getHeader("Content-Disposition")).isNull();
    }

    @Test
    void completedRebuildDuringReadIsDenied() throws Exception {
        bindSubject(true);
        pipeline.setModifyTime(new Date(1000));
        ApplicationBuildPipeline changed = new ApplicationBuildPipeline().setAppId(101L)
            .setPipeType(pipeline.getPipeType()).setPipeStatus(PipelineStatusEnum.SUCCESS)
            .setBuildResultJson(pipeline.getBuildResultJson()).setModifyTime(new Date(2000));
        when(builds.getCurrentBuildPipeline(101L))
            .thenReturn(Optional.of(pipeline), Optional.of(changed));
        MvcResult result = mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value(501)).andReturn();
        assertThat(result.getResponse().getHeader("Content-Disposition")).isNull();
    }

    @Test
    void applicationChangedDuringReadIsDenied() throws Exception {
        bindSubject(true);
        when(application.getBuild()).thenReturn(false, true);
        MvcResult result = mvc.perform(post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value(501)).andReturn();
        assertThat(result.getResponse().getHeader("Content-Disposition")).isNull();
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void unsafeJarNameReturnsJsonFailure() throws Exception {
        bindSubject(true);
        Path lib = Path.of(application.getAppLib());
        for (String name : List.of("dependency:extra.jar", "dependency\\extra.jar")) {
            Path jar = Files.writeString(lib.resolve(name), "dependency");
            try {
                MvcResult result = mvc.perform(
                    post("/flink/app/downloadArtifact").param("id", "101").param("teamId", "10"))
                    .andExpect(status().isInternalServerError())
                    .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                    .andExpect(jsonPath("$.code").value(501))
                    .andReturn();
                assertThat(result.getResponse().getHeader("Content-Disposition")).isNull();
            } finally {
                Files.delete(jar);
            }
        }
    }

    private void bindSubject(boolean detailPermission) {
        Ini ini = new Ini();
        ini.load("[users]\n" + principal + "=fixture" + (detailPermission ? ",reader" : "")
            + "\n[roles]\nreader=app:detail\n");
        securityManager = new DefaultSecurityManager(new IniRealm(ini));
        Subject subject = new Subject.Builder(securityManager).buildSubject();
        subject.login(new UsernamePasswordToken(principal, "fixture"));
        ThreadContext.bind(subject);
    }
}
