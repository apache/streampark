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

import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.web.FormOrJson;
import org.apache.streampark.console.core.enums.LoginTypeEnum;
import org.apache.streampark.console.core.util.ServiceHelper;
import org.apache.streampark.console.system.assembler.UserAssembler;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.request.user.UserCheckNameRequest;
import org.apache.streampark.console.system.request.user.UserCreateRequest;
import org.apache.streampark.console.system.request.user.UserDeleteRequest;
import org.apache.streampark.console.system.request.user.UserListQueryRequest;
import org.apache.streampark.console.system.request.user.UserPasswordUpdateRequest;
import org.apache.streampark.console.system.request.user.UserResetPasswordRequest;
import org.apache.streampark.console.system.request.user.UserTeamIdRequest;
import org.apache.streampark.console.system.request.user.UserTransferResourceRequest;
import org.apache.streampark.console.system.request.user.UserUpdateRequest;
import org.apache.streampark.console.system.response.team.TeamResponse;
import org.apache.streampark.console.system.response.user.UserResponse;
import org.apache.streampark.console.system.response.user.UserUpdateResponse;
import org.apache.streampark.console.system.service.TeamService;
import org.apache.streampark.console.system.service.UserService;

import org.apache.shiro.authz.annotation.Logical;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequestMapping("user")
public class UserController {

    @Autowired
    private UserService userService;

    @Autowired
    private TeamService teamService;

    @PostMapping("teams")
    @RequiresPermissions("app:view")
    public RestResponseBody<List<TeamResponse>> teams() {
        return RestResponseBody.success(UserAssembler.toTeamList(teamService.list()));
    }

    @PostMapping("set_team")
    @RequiresPermissions("app:view")
    public RestResponseBody<Void> setTeam(@Valid @FormOrJson UserTeamIdRequest request) {
        userService.setLastTeam(request.getTeamId(), ServiceHelper.getUserId());
        return RestResponseBody.success();
    }

    @PostMapping("list")
    @RequiresPermissions(value = {"user:view", "app:view"}, logical = Logical.OR)
    public RestResponseBody<IPage<UserResponse>> userList(RestRequest restRequest, UserListQueryRequest query) {
        IPage<User> userList = userService.getPage(UserAssembler.toEntity(query), restRequest);
        return RestResponseBody.success(UserAssembler.toPageResponse(userList));
    }

    @PostMapping("post")
    @RequiresPermissions("user:add")
    public RestResponseBody<Void> addUser(@Valid @FormOrJson UserCreateRequest request) throws Exception {
        User user = UserAssembler.toEntity(request);
        ApiAlertException.throwIfNull(user, "User create request cannot be null.");
        user.setLoginType(LoginTypeEnum.PASSWORD);
        this.userService.createUser(user);
        return RestResponseBody.success();
    }

    @PutMapping("update")
    @RequiresPermissions("user:update")
    public RestResponseBody<UserUpdateResponse> updateUser(@Valid @FormOrJson UserUpdateRequest request) throws Exception {
        return RestResponseBody.success(
            UserAssembler.toUpdateResponse(this.userService.updateUser(UserAssembler.toEntity(request))));
    }

    @PutMapping("transferResource")
    @RequiresPermissions("user:update")
    public RestResponseBody<Void> transferResource(@Valid @FormOrJson UserTransferResourceRequest request) {
        this.userService.transferResource(request.getUserId(), request.getTargetUserId());
        return RestResponseBody.success();
    }

    @DeleteMapping("delete")
    @RequiresPermissions("user:delete")
    public RestResponseBody<Void> deleteUser(@Valid @FormOrJson UserDeleteRequest request) throws Exception {
        this.userService.deleteUser(request.getUserId());
        return RestResponseBody.success();
    }

    @PostMapping("getNoTokenUser")
    @RequiresPermissions("token:add")
    public RestResponseBody<List<UserResponse>> getNoTokenUser() {
        return RestResponseBody.success(UserAssembler.toResponseList(this.userService.listNoTokenUser()));
    }

    @PostMapping("check/name")
    public RestResponseBody<Boolean> checkUserName(@Valid UserCheckNameRequest request) {
        boolean result = this.userService.getByUsername(request.getUsername()) == null;
        return RestResponseBody.success(result);
    }

    @PutMapping("password")
    public RestResponseBody<Void> updatePassword(@Valid @FormOrJson UserPasswordUpdateRequest request) throws Exception {
        userService.updatePassword(UserAssembler.toEntity(request));
        return RestResponseBody.success();
    }

    @PutMapping("password/reset")
    @RequiresPermissions("user:reset")
    public RestResponseBody<String> resetPassword(@Valid @FormOrJson UserResetPasswordRequest request) throws Exception {
        String newPass = this.userService.resetPassword(request.getUsername());
        return RestResponseBody.success(newPass);
    }

    @PostMapping("appOwners")
    public RestResponseBody<List<UserResponse>> appOwners(@Valid UserTeamIdRequest request) {
        List<User> userList = userService.listByTeamId(request.getTeamId());
        userList.forEach(User::dataMasking);
        return RestResponseBody.success(UserAssembler.toResponseList(userList));
    }
}
