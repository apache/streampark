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

package org.apache.streampark.console.system.authentication;

import org.apache.streampark.common.util.SystemPropertyUtils;
import org.apache.streampark.console.core.enums.AuthenticationType;
import org.apache.streampark.console.system.entity.AccessToken;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.AccessTokenService;
import org.apache.streampark.console.system.service.UserService;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;

/** Implementation of ShiroRealm, including two modules: authentication and authorization */
public class ShiroRealm extends AuthorizingRealm {

    @Autowired
    private UserService userService;

    @Autowired
    private AccessTokenService accessTokenService;

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof JWTToken;
    }

    /**
     * Authorization module to get user roles and permissions
     *
     * @param token token
     * @return AuthorizationInfo permission information
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection token) {
        Long userId = JWTUtil.getUserId(token.toString());

        SimpleAuthorizationInfo simpleAuthorizationInfo = new SimpleAuthorizationInfo();

        // Get user permission set
        Set<String> permissionSet = userService.listPermissions(userId, null);
        simpleAuthorizationInfo.setStringPermissions(permissionSet);
        return simpleAuthorizationInfo;
    }

    /**
     * User Authentication
     *
     * @param authenticationToken authentication token
     * @return AuthenticationInfo authentication information
     * @throws AuthenticationException authentication related exceptions
     */
    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken authenticationToken) throws AuthenticationException {
        // The token here is passed from the executeLogin method of JWTFilter and has been decrypted
        String credential = (String) authenticationToken.getCredentials();
        String username = JWTUtil.getUserName(credential);
        Long userId = JWTUtil.getUserId(credential);
        AuthenticationType authType = JWTUtil.getAuthType(credential);

        if (username == null || userId == null || authType == null) {
            throw new AuthenticationException("the authorization token is invalid");
        }

        // Query user information by username
        User user = userService.getByUsername(username);
        if (user == null || !user.getUserId().equals(userId)) {
            throw new AuthenticationException("the authorization token verification failed.");
        }

        switch (authType) {
            case SIGN:
                Long timestamp = JWTUtil.getTimestamp(credential);
                Long startTime = SystemPropertyUtils.getLong("streampark.start.timestamp", 0);
                if (timestamp < startTime) {
                    throw new AuthenticationException("the authorization token is expired");
                }
                break;
            case OPENAPI:
                // Check whether the token belongs to the api and whether the permission is valid
                AccessToken accessToken = accessTokenService.getByUserId(userId);
                try {
                    String encryptToken = JWTUtil.encrypt(credential);
                    if (accessToken == null || !accessToken.getToken().equals(encryptToken)) {
                        throw new AuthenticationException("the openapi authorization token is invalid");
                    }
                } catch (Exception e) {
                    throw new AuthenticationException(e);
                }

                if (AccessToken.STATUS_DISABLE.equals(accessToken.getStatus())) {
                    throw new AuthenticationException(
                        "The OpenAPI authorization token is disabled. Please contact the administrator.");
                }

                if (User.STATUS_LOCK.equals(accessToken.getUserStatus())) {
                    throw new AuthenticationException(
                        "the user [" + username + "] has been locked, please contact the administrator");
                }
                SecurityUtils.getSubject().getSession().setAttribute(AccessToken.IS_API_TOKEN, true);
                break;
            default:
                break;
        }

        return new SimpleAuthenticationInfo(credential, credential, "streampark_shiro_realm");
    }
}
