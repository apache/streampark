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

package org.apache.streampark.console.system.assembler;

import org.apache.streampark.console.core.enums.LoginTypeEnum;
import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.response.user.UserResponse;
import org.apache.streampark.console.system.response.user.UserSessionResponse;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

class UserAssemblerTest {

    @Test
    void shouldExcludeSensitiveFieldsFromResponse() {
        User user = new User();
        user.setUserId(1L);
        user.setUsername("admin");
        user.setPassword("secret");
        user.setSalt("salt");
        user.setEmail("admin@example.com");
        user.setUserType(UserTypeEnum.EDITOR);
        user.setLoginType(LoginTypeEnum.PASSWORD);

        UserResponse response = UserAssembler.toResponse(user);

        Assertions.assertEquals("admin", response.getUsername());
        Assertions.assertEquals("admin@example.com", response.getEmail());

        var fieldNames = Arrays.stream(UserResponse.class.getDeclaredFields())
            .map(java.lang.reflect.Field::getName)
            .collect(Collectors.toSet());
        Assertions.assertFalse(fieldNames.contains("password"));
        Assertions.assertFalse(fieldNames.contains("salt"));
    }

    @Test
    void shouldExposeOnlyTheFixedRoleInSessionResponse() {
        User user = new User();
        user.setUserId(1L);
        user.setUsername("developer");
        user.setUserType(UserTypeEnum.EDITOR);

        Map<String, Object> session = new HashMap<>();
        session.put("user", user);
        session.put("roles", Collections.singleton("editor"));

        UserSessionResponse response = UserAssembler.toSessionResponse(session);

        Assertions.assertEquals(Collections.singleton("editor"), response.getRoles());
        Assertions.assertEquals(UserTypeEnum.EDITOR, response.getUser().getUserType());
    }
}
