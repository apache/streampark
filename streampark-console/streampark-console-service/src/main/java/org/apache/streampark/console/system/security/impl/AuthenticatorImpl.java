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

package org.apache.streampark.console.system.security.impl;

import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.util.ShaHashUtils;
import org.apache.streampark.console.core.enums.LoginTypeEnum;
import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.security.Authenticator;
import org.apache.streampark.console.system.service.UserService;

import org.apache.commons.lang3.StringUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AuthenticatorImpl implements Authenticator {

    @Autowired
    private UserService usersService;
    @Autowired
    private LdapService ldapService;

    @Override
    public User authenticate(String username, String password, LoginTypeEnum loginType) throws Exception {
        ApiAlertException.throwIfNull(
            loginType, "the login type is null");

        switch (loginType) {
            case PASSWORD:
                return passwordAuthenticate(username, password);
            case LDAP:
                return ldapAuthenticate(username, password);
            case SSO:
                return ssoAuthenticate(username);
            default:
                throw new ApiAlertException(
                    String.format("the login type [%s] is not supported.", loginType));
        }
    }

    private User passwordAuthenticate(String username, String password) {
        User user = usersService.getByUsername(username);

        ApiAlertException.throwIfNull(user, String.format("User [%s] does not exist", username));

        ApiAlertException.throwIfTrue(
            user.getLoginType() != LoginTypeEnum.PASSWORD,
            "user [%s] can not login with PASSWORD",
            username);

        String salt = user.getSalt();
        password = ShaHashUtils.encrypt(salt, password);

        ApiAlertException.throwIfFalse(
            StringUtils.equals(user.getPassword(), password), "Incorrect password");

        return user;
    }

    private User ldapAuthenticate(String username, String password) throws Exception {
        boolean ldapLoginStatus = ldapService.ldapLogin(username, password);
        if (!ldapLoginStatus) {
            return null;
        }
        // check if user exist
        User user = usersService.getByUsername(username);

        if (user != null) {
            ApiAlertException.throwIfTrue(
                user.getLoginType() != LoginTypeEnum.LDAP,
                "user [%s] can only sign in with %s",
                username,
                user.getLoginType());

            return user;
        }
        return this.newUserCreate(LoginTypeEnum.LDAP, username);
    }

    private User ssoAuthenticate(String username) throws Exception {
        // check if user exist
        User user = usersService.getByUsername(username);

        if (user != null) {
            ApiAlertException.throwIfTrue(
                user.getLoginType() != LoginTypeEnum.SSO,
                "user [%s] can only sign in with %s",
                username,
                user.getLoginType());
            return user;
        }

        return this.newUserCreate(LoginTypeEnum.SSO, username);
    }

    private User newUserCreate(LoginTypeEnum loginTypeEnum, String username) throws Exception {
        User newUser = new User();
        newUser.setUsername(username);
        newUser.setNickName(username);
        newUser.setLoginType(loginTypeEnum);
        newUser.setUserType(UserTypeEnum.EDITOR);
        newUser.setStatus(User.STATUS_VALID);
        newUser.setSex(User.SEX_UNKNOWN);
        newUser.setPassword(User.DEFAULT_SECRET);
        usersService.createUser(newUser);
        return newUser;
    }
}
