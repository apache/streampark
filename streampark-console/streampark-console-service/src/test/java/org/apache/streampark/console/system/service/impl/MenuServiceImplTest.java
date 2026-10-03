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

package org.apache.streampark.console.system.service.impl;

import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.system.entity.Menu;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.UserService;

import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class MenuServiceImplTest {

    @Test
    void adminKeepsTokenMenu() {
        assertThat(menuPaths(UserTypeEnum.ADMIN))
            .contains("/system", "/system/user", "/system/token")
            .doesNotContain("/system/role", "/system/team", "/system/member");
    }

    @Test
    void editorCannotSeeTokenMenu() {
        assertThat(menuPaths(UserTypeEnum.EDITOR))
            .contains("/flink/app")
            .doesNotContain("/system", "/system/user", "/system/token");
    }

    @SuppressWarnings("unchecked")
    private List<String> menuPaths(UserTypeEnum type) {
        MenuServiceImpl service = spy(new MenuServiceImpl());
        UserService users = mock(UserService.class);
        User user = new User();
        user.setUserType(type);
        when(users.getById(42L)).thenReturn(user);
        ReflectionTestUtils.setField(service, "userService", users);
        LambdaQueryChainWrapper<Menu> query = mock(LambdaQueryChainWrapper.class, RETURNS_SELF);
        doReturn(query).when(service).lambdaQuery();
        List<Menu> menus = new ArrayList<>();
        for (String path : Arrays.asList("/system", "/system/user", "/system/token",
            "/system/role", "/system/team", "/system/member", "/flink/app")) {
            Menu menu = new Menu();
            menu.setPath(path);
            menus.add(menu);
        }
        when(query.list()).thenReturn(menus);
        return service.listMenus(42L, 100001L).stream().map(Menu::getPath).collect(Collectors.toList());
    }
}
