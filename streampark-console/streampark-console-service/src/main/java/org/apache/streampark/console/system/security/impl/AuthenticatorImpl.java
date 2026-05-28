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
import org.apache.streampark.console.base.util.PasswordHashUtils;
import org.apache.streampark.console.core.enums.LoginType;
import org.apache.streampark.console.core.enums.UserType;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.security.Authenticator;
import org.apache.streampark.console.system.service.UserService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class AuthenticatorImpl implements Authenticator {
  @Autowired private UserService usersService;
  @Autowired private LdapService ldapService;

  @Override
  public User authenticate(String username, String password, LoginType loginType) throws Exception {
    if (loginType == null) {
      throw new ApiAlertException(String.format("the login type is null."));
    }

    if (loginType.equals(LoginType.PASSWORD)) {
      return passwordAuthenticate(username, password);
    } else {
      return ldapAuthenticate(username, password);
    }
  }

  private User passwordAuthenticate(String username, String password) throws Exception {
    User user = usersService.findByName(username);
    if (user == null || user.getLoginType() != LoginType.PASSWORD) {
      throw new ApiAlertException(
          String.format("user [%s] does not exist or can not login with PASSWORD", username));
    }
    if (!PasswordHashUtils.matches(password, user.getSalt(), user.getPassword())) {
      return null;
    }
    if (PasswordHashUtils.needsRehash(user.getPassword())) {
      user.setSalt(PasswordHashUtils.PASSWORD_SALT_NOT_REQUIRED);
      user.setPassword(PasswordHashUtils.encrypt(password));
      usersService.updateSaltPassword(user);
    }
    return user;
  }

  private User ldapAuthenticate(String username, String password) throws Exception {
    String ldapEmail = ldapService.ldapLogin(username, password);
    if (ldapEmail == null) {
      return null;
    }
    // check if user exist
    User user = usersService.findByName(username);

    if (user != null) {
      if (user.getLoginType() != LoginType.LDAP) {
        throw new ApiAlertException(
            String.format("user [%s] can only sign in with %s", username, user.getLoginType()));
      }
      boolean passwordMatched =
          PasswordHashUtils.matches(password, user.getSalt(), user.getPassword());
      // ldap password changed, or stored hash uses a legacy format, update user password hash
      if (!passwordMatched || PasswordHashUtils.needsRehash(user.getPassword())) {
        user.setSalt(PasswordHashUtils.PASSWORD_SALT_NOT_REQUIRED);
        user.setPassword(PasswordHashUtils.encrypt(password));
        usersService.updateSaltPassword(user);
      }
      return user;
    }

    User newUser = new User();
    newUser.setCreateTime(new Date());
    newUser.setUsername(username);
    newUser.setNickName(username);
    newUser.setUserType(UserType.USER);
    newUser.setLoginType(LoginType.LDAP);
    newUser.setStatus(User.STATUS_VALID);
    newUser.setSex(User.SEX_UNKNOWN);
    newUser.setPassword(password);
    usersService.createUser(newUser);
    return newUser;
  }
}
