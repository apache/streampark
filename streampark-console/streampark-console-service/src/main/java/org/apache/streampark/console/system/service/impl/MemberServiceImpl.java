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
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.core.enums.UserType;
import org.apache.streampark.console.core.service.ServiceHelper;
import org.apache.streampark.console.system.entity.Member;
import org.apache.streampark.console.system.entity.Team;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.mapper.MemberMapper;
import org.apache.streampark.console.system.service.MemberService;
import org.apache.streampark.console.system.service.RoleService;
import org.apache.streampark.console.system.service.TeamService;
import org.apache.streampark.console.system.service.UserService;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class MemberServiceImpl extends ServiceImpl<MemberMapper, Member> implements MemberService {

  @Autowired private UserService userService;

  @Autowired private RoleService roleService;

  @Autowired private TeamService teamService;
  @Autowired private ServiceHelper serviceHelper;

  @Override
  @Transactional
  public void deleteByUserId(Long userId) {
    baseMapper.deleteByUserId(userId);
  }

  @Override
  public void deleteByTeamId(Long teamId) {
    LambdaQueryWrapper<Member> queryWrapper =
        new LambdaQueryWrapper<Member>().eq(Member::getTeamId, teamId);
    this.remove(queryWrapper);
  }

  @Override
  public IPage<Member> page(Member member, RestRequest request) {
    ApiAlertException.throwIfNull(member.getTeamId(), "The team id is required.");
    Page<Member> page = MybatisPager.getPage(request);
    return baseMapper.findUsers(page, member);
  }

  @Override
  public List<User> findCandidateUsers(Long teamId) {
    return baseMapper.findUsersNotInTeam(teamId);
  }

  @Override
  public List<Team> findUserTeams(Long userId) {
    return teamService.findUserTeams(userId);
  }

  @Override
  public Member findByUserId(Long teamId, Long userId) {
    ApiAlertException.throwIfNull(teamId, "The team id is required.");
    LambdaQueryWrapper<Member> queryWrapper =
        new LambdaQueryWrapper<Member>()
            .eq(Member::getTeamId, teamId)
            .eq(Member::getUserId, userId);
    return baseMapper.selectOne(queryWrapper);
  }

  @Override
  public List<Long> findUserIdsByRoleId(Long roleId) {
    LambdaQueryWrapper<Member> queryWrapper =
        new LambdaQueryWrapper<Member>().eq(Member::getRoleId, roleId);
    List<Member> list = baseMapper.selectList(queryWrapper);
    return list.stream().map(Member::getUserId).collect(Collectors.toList());
  }

  @Override
  public void createMember(Member member) {
    User user =
        Optional.ofNullable(userService.findByName(member.getUserName()))
            .orElseThrow(
                () ->
                    new ApiAlertException(
                        String.format("The username [%s] not found", member.getUserName())));
    Optional.ofNullable(roleService.getById(member.getRoleId()))
        .orElseThrow(
            () ->
                new ApiAlertException(
                    String.format("The roleId [%s] not found", member.getRoleId())));
    Team team =
        Optional.ofNullable(teamService.getById(member.getTeamId()))
            .orElseThrow(
                () ->
                    new ApiAlertException(
                        String.format("The teamId [%s] not found", member.getTeamId())));
    ApiAlertException.throwIfFalse(
        findByUserId(member.getTeamId(), user.getUserId()) == null,
        String.format(
            "The user [%s] has been added the team [%s], please don't add it again.",
            member.getUserName(), team.getTeamName()));

    member.setId(null);
    member.setUserId(user.getUserId());

    Date date = new Date();
    member.setCreateTime(date);
    member.setModifyTime(date);
    this.save(member);
  }

  @Override
  public void deleteMember(Member memberArg) {
    checkPermission(memberArg);
    Member member =
        Optional.ofNullable(this.getById(memberArg.getId()))
            .orElseThrow(
                () ->
                    new ApiAlertException(
                        String.format("The member [id=%s] not found", memberArg.getId())));
    this.removeById(member);
    userService.clearLastTeam(member.getUserId(), member.getTeamId());
  }

  private void checkPermission(Member member) {
    User user = serviceHelper.getLoginUser();
    ApiAlertException.throwIfTrue(user == null, "Permission denied, invalid login");
    if (user.getUserType() == UserType.USER) {
      List<Team> teamList = this.findUserTeams(user.getUserId());
      Optional<Team> team =
          teamList.stream().filter(c -> c.getId().equals(member.getTeamId())).findFirst();
      ApiAlertException.throwIfTrue(
          !team.isPresent(), "Permission denied, The current user is not in the team");
    }
  }

  @Override
  public void updateMember(Member member) {
    checkPermission(member);
    Member oldMember =
        Optional.ofNullable(this.getById(member.getId()))
            .orElseThrow(
                () ->
                    new ApiAlertException(
                        String.format("The member [id=%s] not found", member.getId())));
    AssertUtils.required(
        oldMember.getTeamId().equals(member.getTeamId()), "Team id cannot be changed.");
    AssertUtils.required(
        oldMember.getUserId().equals(member.getUserId()), "User id cannot be changed.");
    Optional.ofNullable(roleService.getById(member.getRoleId()))
        .orElseThrow(
            () ->
                new ApiAlertException(
                    String.format("The roleId [%s] not found", member.getRoleId())));
    oldMember.setRoleId(member.getRoleId());
    oldMember.setModifyTime(new Date());
    updateById(oldMember);
  }
}
