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

import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.DateUtils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.base.util.ShaHashUtils;
import org.apache.streampark.console.core.enums.AuthenticationType;
import org.apache.streampark.console.core.enums.LoginTypeEnum;
import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.console.core.service.application.FlinkApplicationInfoService;
import org.apache.streampark.console.core.service.application.FlinkApplicationManageService;
import org.apache.streampark.console.core.util.ServiceHelper;
import org.apache.streampark.console.system.authentication.JWTToken;
import org.apache.streampark.console.system.authentication.JWTUtil;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.mapper.UserMapper;
import org.apache.streampark.console.system.service.TeamService;
import org.apache.streampark.console.system.service.UserService;
import org.apache.streampark.console.system.service.result.UserLoginResult;
import org.apache.streampark.console.system.service.result.UserUpdateResult;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements UserService {

    @Autowired
    private FlinkApplicationManageService applicationManageService;

    @Autowired
    private FlinkApplicationInfoService applicationInfoService;

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private TeamService teamService;

    @Override
    public User getByUsername(String username) {
        return this.lambdaQuery().eq(User::getUsername, username).one();
    }

    @Override
    public IPage<User> getPage(User user, RestRequest request) {
        Page<User> page = MybatisPager.getPage(request);
        IPage<User> resPage = this.baseMapper.selectPage(page, user);
        AssertUtils.notNull(resPage);
        if (resPage.getTotal() == 0) {
            resPage.setRecords(Collections.emptyList());
        }
        return resPage;
    }

    @Override
    public void updateLoginTime(String username) {
        this.lambdaUpdate().eq(User::getUsername, username).set(User::getLastLoginTime, new Date()).update();
    }

    @Override
    public void createUser(User user) {
        if (user.getUserType() == null) {
            user.setUserType(UserTypeEnum.EDITOR);
        }
        user.setLastTeamId(teamService.getSysDefaultTeam().getId());
        if (StringUtils.isNoneBlank(user.getPassword())) {
            String salt = ShaHashUtils.getRandomSalt();
            String password = ShaHashUtils.encrypt(salt, user.getPassword());
            user.setSalt(salt);
            user.setPassword(password);
        }
        save(user);
    }

    @Override
    public UserUpdateResult updateUser(User user) {
        User existsUser = getById(user.getUserId());
        user.setLoginType(null);
        user.setPassword(null);
        UserUpdateResult result = new UserUpdateResult();
        if (needTransferResource(existsUser, user)) {
            result.setNeedTransferResource(true);
            return result;
        }
        updateById(user);
        return result;
    }

    private boolean needTransferResource(User existsUser, User user) {
        if (User.STATUS_LOCK.equals(existsUser.getStatus())
            || User.STATUS_VALID.equals(user.getStatus())) {
            return false;
        }
        return applicationInfoService.existsByUserId(user.getUserId())
            || resourceService.existsByUserId(user.getUserId());
    }

    @Override
    public void updatePassword(User userParam) {
        User currentUser = ServiceHelper.getLoginUser();
        ApiAlertException.throwIfTrue(
            currentUser.getUserType() != UserTypeEnum.ADMIN
                && !currentUser.getUserId().equals(userParam.getUserId()),
            "Permission denied, you can only change your own password.");

        User user = getById(userParam.getUserId());
        ApiAlertException.throwIfNull(user, "User is null. Update password failed.");
        ApiAlertException.throwIfFalse(
            user.getLoginType() == LoginTypeEnum.PASSWORD,
            "Can only update password for user who sign in with PASSWORD");

        String saltPassword = ShaHashUtils.encrypt(user.getSalt(), userParam.getOldPassword());
        ApiAlertException.throwIfFalse(
            StringUtils.equals(user.getPassword(), saltPassword),
            "Old password error. Update password failed.");

        String salt = ShaHashUtils.getRandomSalt();
        String password = ShaHashUtils.encrypt(salt, userParam.getPassword());
        user.setSalt(salt);
        user.setPassword(password);
        this.baseMapper.updateById(user);
    }

    @Override
    public String resetPassword(String username) {
        User user = new User();
        String salt = ShaHashUtils.getRandomSalt();
        String newPassword = ShaHashUtils.getRandomSalt(User.DEFAULT_PASSWORD_LENGTH);
        String password = ShaHashUtils.encrypt(salt, newPassword);
        user.setSalt(salt);
        user.setPassword(password);
        this.lambdaUpdate().eq(User::getUsername, username).update(user);
        return newPassword;
    }

    @Override
    public List<User> listNoTokenUser() {
        List<User> users = this.baseMapper.selectNoTokenUsers();
        if (!users.isEmpty()) {
            users.forEach(User::dataMasking);
        }
        return users;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void setLastTeam(Long teamId, Long userId) {
        ApiAlertException.throwIfNull(userId, "Current login user is null.");
        ApiAlertException.throwIfNull(teamService.getById(teamId), "TeamId is invalid, set team failed.");
        User update = new User();
        update.setUserId(userId);
        update.setLastTeamId(teamId);
        ApiAlertException.throwIfFalse(baseMapper.updateById(update) == 1, "User not found, set team failed.");
    }

    @Override
    public List<User> listByTeamId(Long teamId) {
        return baseMapper.selectUsersByAppOwner(teamId);
    }

    @Override
    public void transferResource(Long userId, Long targetUserId) {
        applicationManageService.changeOwnership(userId, targetUserId);
        resourceService.changeOwnership(userId, targetUserId);
    }

    @Override
    public UserLoginResult getLoginUserInfo(User user) throws Exception {
        UserLoginResult result = new UserLoginResult();
        if (user == null) {
            result.setLoginCode(0);
            return result;
        }

        if (User.STATUS_LOCK.equals(user.getStatus())) {
            result.setLoginCode(1);
            return result;
        }

        this.updateLoginTime(user.getUsername());
        String token = JWTUtil.sign(user, AuthenticationType.SIGN);

        LocalDateTime expireTime = LocalDateTime.now().plusSeconds(JWTUtil.getTTLOfSecond());
        String ttl = DateUtils.formatFullTime(expireTime);

        String userId = RandomStringUtils.randomAlphanumeric(20);
        user.setId(userId);
        JWTToken jwtToken = new JWTToken(token, ttl);
        result.setUserInfo(generateFrontendUserInfo(user, jwtToken));
        return result;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteUser(Long userId) {
        removeById(userId);
    }

    /**
     * Generate frontend user information with a fixed role.
     *
     * @param user user
     * @return UserInfo
     */
    @Override
    public Map<String, Object> generateFrontendUserInfo(User user, JWTToken token) {
        Map<String, Object> userInfo = new HashMap<>(8);

        // 1) token & expire
        if (token != null) {
            userInfo.put("token", token.getToken());
            userInfo.put("expire", token.getExpireAt());
        }

        // 2) user
        user.dataMasking();
        userInfo.put("user", user);

        if (user.getLastTeamId() == null) {
            user.setLastTeamId(teamService.getSysDefaultTeam().getId());
        }

        // 3) one fixed role; roles are not editable permission collections
        userInfo.put("roles", Collections.singleton(user.getUserType().getRoleName()));

        return userInfo;
    }
}
