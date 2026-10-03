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

package org.apache.streampark.console.core.service;

import org.apache.streampark.console.SpringUnitTestBase;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.enums.EngineTypeEnum;
import org.apache.streampark.console.core.enums.ResourceTypeEnum;
import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.UserService;
import org.apache.streampark.console.system.service.result.UserUpdateResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/** org.apache.streampark.console.core.service.UserServiceTest. */
@Transactional
class UserServiceTest extends SpringUnitTestBase {

    @Autowired
    private UserService userService;
    @Autowired
    private FlinkApplicationManageService applicationManageService;
    @Autowired
    private FlinkApplicationInfoService applicationInfoService;
    @Autowired
    private ResourceService resourceService;

    @Test
    void testLockUser() throws Exception {
        User user = new User();
        user.setUsername("test");
        user.setNickName("test");
        user.setPassword("test");
        user.setUserType(UserTypeEnum.EDITOR);
        user.setStatus(User.STATUS_VALID);
        userService.createUser(user);
        // lock user
        user.setStatus(User.STATUS_LOCK);
        UserUpdateResult data = userService.updateUser(user);
        Assertions.assertNotEquals(Boolean.TRUE, data == null ? null : data.isNeedTransferResource());
        // unlock user
        user.setStatus(User.STATUS_VALID);
        UserUpdateResult data1 = userService.updateUser(user);
        Assertions.assertNotEquals(Boolean.TRUE, data1 == null ? null : data1.isNeedTransferResource());

        Resource resource = new Resource();
        resource.setResourceName("test");
        resource.setResourceType(ResourceTypeEnum.APP);
        resource.setEngineType(EngineTypeEnum.FLINK);
        resource.setTeamId(1L);
        resource.setCreatorId(user.getUserId());
        resourceService.save(resource);
        // lock user when has resource
        user.setStatus(User.STATUS_LOCK);
        UserUpdateResult data2 = userService.updateUser(user);
        Assertions.assertEquals(Boolean.TRUE, data2.isNeedTransferResource());
    }

    @Test
    void testTransferResource() {
        User user = new User();
        user.setUsername("test");
        user.setNickName("test");
        user.setPassword("test");
        user.setUserType(UserTypeEnum.EDITOR);
        user.setStatus(User.STATUS_VALID);
        userService.save(user);

        Resource resource = new Resource();
        resource.setResourceName("test");
        resource.setResourceType(ResourceTypeEnum.APP);
        resource.setEngineType(EngineTypeEnum.FLINK);
        resource.setTeamId(1L);
        resource.setCreatorId(user.getUserId());
        resourceService.save(resource);

        FlinkApplication app = new FlinkApplication();
        app.setUserId(user.getUserId());
        app.setTeamId(1L);
        applicationManageService.save(app);

        User targetUser = new User();
        targetUser.setUsername("test0");
        targetUser.setNickName("test0");
        targetUser.setPassword("test0");
        targetUser.setUserType(UserTypeEnum.EDITOR);
        targetUser.setStatus(User.STATUS_VALID);
        userService.save(targetUser);

        Assertions.assertTrue(applicationInfoService.existsByUserId(user.getUserId()));
        Assertions.assertTrue(resourceService.existsByUserId(user.getUserId()));

        userService.transferResource(user.getUserId(), targetUser.getUserId());

        Assertions.assertFalse(applicationInfoService.existsByUserId(user.getUserId()));
        Assertions.assertFalse(resourceService.existsByUserId(user.getUserId()));

        Assertions.assertTrue(applicationInfoService.existsByUserId(targetUser.getUserId()));
        Assertions.assertTrue(resourceService.existsByUserId(targetUser.getUserId()));
    }
}
