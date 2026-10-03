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

import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.core.assembler.DtoAssembler;
import org.apache.streampark.console.system.entity.Team;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.request.user.UserCreateRequest;
import org.apache.streampark.console.system.request.user.UserListQueryRequest;
import org.apache.streampark.console.system.request.user.UserPasswordUpdateRequest;
import org.apache.streampark.console.system.request.user.UserUpdateRequest;
import org.apache.streampark.console.system.response.team.TeamResponse;
import org.apache.streampark.console.system.response.user.UserBriefResponse;
import org.apache.streampark.console.system.response.user.UserResponse;
import org.apache.streampark.console.system.response.user.UserSessionResponse;
import org.apache.streampark.console.system.response.user.UserUpdateResponse;
import org.apache.streampark.console.system.service.result.UserLoginResult;
import org.apache.streampark.console.system.service.result.UserUpdateResult;

import com.baomidou.mybatisplus.core.metadata.IPage;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Converts between user entities and API request/response contracts. */
public final class UserAssembler {

    private UserAssembler() {
    }

    public static User toEntity(UserListQueryRequest request) {
        return DtoAssembler.toDto(request, User.class);
    }

    public static User toEntity(UserCreateRequest request) {
        return DtoAssembler.toDto(request, User.class);
    }

    public static User toEntity(UserUpdateRequest request) {
        return DtoAssembler.toDto(request, User.class);
    }

    public static User toEntity(UserPasswordUpdateRequest request) {
        return DtoAssembler.toDto(request, User.class);
    }

    public static UserResponse toResponse(User user) {
        if (user == null) {
            return null;
        }
        UserResponse response = new UserResponse();
        response.setUserId(user.getUserId());
        response.setUsername(user.getUsername());
        response.setEmail(user.getEmail());
        response.setUserType(user.getUserType());
        response.setLoginType(user.getLoginType());
        response.setStatus(user.getStatus());
        response.setCreateTime(user.getCreateTime());
        response.setModifyTime(user.getModifyTime());
        response.setLastLoginTime(user.getLastLoginTime());
        response.setSex(user.getSex());
        response.setDescription(user.getDescription());
        response.setNickName(user.getNickName());
        response.setLastTeamId(user.getLastTeamId());
        response.setId(user.getId());
        return response;
    }

    public static List<TeamResponse> toTeamList(List<Team> teams) {
        return DtoAssembler.toList(teams, team -> DtoAssembler.toDto(team, TeamResponse.class));
    }

    public static List<UserResponse> toResponseList(List<User> users) {
        return DtoAssembler.toList(users, UserAssembler::toResponse);
    }

    public static IPage<UserResponse> toPageResponse(IPage<User> page) {
        return DtoAssembler.toPage(page, UserAssembler::toResponse);
    }

    public static UserBriefResponse toBriefResponse(User user) {
        if (user == null) {
            return null;
        }
        UserBriefResponse response = new UserBriefResponse();
        response.setUserId(user.getUserId());
        response.setUsername(user.getUsername());
        response.setNickName(user.getNickName());
        response.setDescription(user.getDescription());
        response.setUserType(user.getUserType());
        response.setLastTeamId(user.getLastTeamId());
        response.setId(user.getId());
        return response;
    }

    @SuppressWarnings("unchecked")
    public static UserSessionResponse toSessionResponse(Map<String, Object> userInfo) {
        if (userInfo == null) {
            return null;
        }
        UserSessionResponse response = new UserSessionResponse();
        response.setToken((String) userInfo.get("token"));
        response.setExpire((String) userInfo.get("expire"));
        Object userObj = userInfo.get("user");
        if (userObj instanceof User) {
            response.setUser(toBriefResponse((User) userObj));
        }
        Object roles = userInfo.get("roles");
        if (roles instanceof Set) {
            response.setRoles((Set<String>) roles);
        }
        return response;
    }

    public static UserUpdateResponse toUpdateResponse(UserUpdateResult result) {
        UserUpdateResponse response = new UserUpdateResponse();
        if (result != null) {
            response.setNeedTransferResource(result.isNeedTransferResource());
        }
        return response;
    }

    public static RestResponseBody<UserSessionResponse> toLoginResponse(UserLoginResult result) {
        if (result == null) {
            return RestResponseBody.success(null);
        }
        if (result.getLoginCode() != null) {
            return RestResponseBody.<UserSessionResponse>success(null)
                .extra(RestResponse.CODE_KEY, result.getLoginCode());
        }
        return RestResponseBody.success(toSessionResponse(result.getUserInfo()));
    }
}
