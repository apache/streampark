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

import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.system.entity.Team;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.mapper.UserMapper;
import org.apache.streampark.console.system.service.TeamService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class UserTeamSelectionTest {

    private final UserServiceImpl service = new UserServiceImpl();
    private final TeamService teams = mock(TeamService.class);
    private final UserMapper users = mock(UserMapper.class);

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "teamService", teams);
        ReflectionTestUtils.setField(service, "baseMapper", users);
    }

    @Test
    void selectLegacyTeam() {
        Team team = new Team();
        team.setId(100001L);
        when(teams.getById(100001L)).thenReturn(team);
        when(users.updateById(any(User.class))).thenReturn(1);

        service.setLastTeam(100001L, 42L);

        ArgumentCaptor<User> update = ArgumentCaptor.forClass(User.class);
        verify(users).updateById(update.capture());
        assertThat(update.getValue().getUserId()).isEqualTo(42L);
        assertThat(update.getValue().getLastTeamId()).isEqualTo(100001L);
        assertThat(update.getValue().getUserType()).isNull();
    }

    @Test
    void rejectUnknownTeam() {
        assertThatThrownBy(() -> service.setLastTeam(999L, 42L))
            .isInstanceOf(ApiAlertException.class);
        verifyNoInteractions(users);
    }

    @Test
    void rejectMissingUser() {
        assertThatThrownBy(() -> service.setLastTeam(100001L, null))
            .isInstanceOf(ApiAlertException.class);
        verifyNoInteractions(teams, users);
    }
}
