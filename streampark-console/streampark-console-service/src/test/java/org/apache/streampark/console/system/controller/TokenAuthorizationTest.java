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

package org.apache.streampark.console.system.controller;

import org.apache.streampark.console.core.enums.UserTypeEnum;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.authz.UnauthorizedException;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.apache.shiro.authz.aop.PermissionAnnotationHandler;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenAuthorizationTest {

    @AfterEach
    void unbindSubject() {
        ThreadContext.unbindSubject();
    }

    @Test
    void editorCannotManageTokens() {
        bindUser(UserTypeEnum.EDITOR);
        for (RequiresPermissions permission : tokenPermissions()) {
            assertThatThrownBy(() -> new PermissionAnnotationHandler().assertAuthorized(permission))
                .isInstanceOf(UnauthorizedException.class);
        }
    }

    @Test
    void adminCanManageTokens() {
        bindUser(UserTypeEnum.ADMIN);
        for (RequiresPermissions permission : tokenPermissions()) {
            assertThatCode(() -> new PermissionAnnotationHandler().assertAuthorized(permission))
                .doesNotThrowAnyException();
        }
    }

    private List<RequiresPermissions> tokenPermissions() {
        List<String> names = Arrays.asList("createToken", "tokensList", "toggleToken", "deleteToken", "getNoTokenUser");
        List<RequiresPermissions> permissions = new java.util.ArrayList<>();
        for (Class<?> controller : Arrays.asList(AccessTokenController.class, UserController.class)) {
            for (Method method : controller.getDeclaredMethods()) {
                if (names.contains(method.getName())) {
                    RequiresPermissions permission = method.getAnnotation(RequiresPermissions.class);
                    assertThat(permission).as(method.getName()).isNotNull();
                    permissions.add(permission);
                }
            }
        }
        assertThat(permissions).hasSize(names.size());
        return permissions;
    }

    private void bindUser(UserTypeEnum type) {
        AuthorizingRealm realm = new AuthorizingRealm() {

            @Override
            protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
                SimpleAuthorizationInfo info = new SimpleAuthorizationInfo();
                info.setStringPermissions(type.getPermissions());
                return info;
            }

            @Override
            protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) {
                throw new UnsupportedOperationException();
            }
        };
        Subject subject = new Subject.Builder(new DefaultSecurityManager(realm))
            .principals(new SimplePrincipalCollection(type.getRoleName(), realm.getName()))
            .authenticated(true)
            .buildSubject();
        ThreadContext.bind(subject);
    }
}
