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

package org.apache.streampark.console.core.service;

import org.apache.streampark.console.SpringUnitTestBase;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.system.authentication.JWTToken;
import org.apache.streampark.console.system.authentication.JWTUtil;
import org.apache.streampark.console.system.entity.AccessToken;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.AccessTokenService;
import org.apache.streampark.console.system.service.UserService;

import com.baomidou.mybatisplus.core.metadata.IPage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class AccessTokenServiceTest extends SpringUnitTestBase {

    @Autowired
    private AccessTokenService accessTokenService;

    @Autowired
    private UserService userService;

    @Test
    void testCrudToken() throws Exception {
        Long mockUserId = 100000L;
        RestResponse restResponse = accessTokenService.create(mockUserId, "");
        Assertions.assertNotNull(restResponse);
        Assertions.assertInstanceOf(AccessToken.class, restResponse.get("data"));

        // verify
        AccessToken accessToken = (AccessToken) restResponse.get("data");
        LOG.info(accessToken.getToken());
        JWTToken jwtToken = new JWTToken(JWTUtil.decrypt(accessToken.getToken()));
        LOG.info(jwtToken.getToken());
        String username = JWTUtil.getUserName(jwtToken.getToken());
        Assertions.assertNotNull(username);
        Assertions.assertEquals("admin", username);
        User user = userService.getByUsername(username);
        Assertions.assertNotNull(user);
        Assertions.assertTrue(JWTUtil.verify(jwtToken.getToken()));

        // list
        AccessToken mockToken1 = new AccessToken();
        mockToken1.setUserId(100000L);
        IPage<AccessToken> tokens1 = accessTokenService.getPage(mockToken1, new RestRequest());
        Assertions.assertEquals(1, tokens1.getRecords().size());
        AccessToken mockToken2 = new AccessToken();
        mockToken2.setUserId(100001L);
        IPage<AccessToken> tokens2 = accessTokenService.getPage(mockToken2, new RestRequest());
        Assertions.assertTrue(tokens2.getRecords().isEmpty());

        // toggle
        Long tokenId = accessToken.getId();
        RestResponse toggleTokenResp = accessTokenService.toggle(tokenId);
        Assertions.assertNotNull(toggleTokenResp);
        Assertions.assertTrue((Boolean) toggleTokenResp.get("data"));

        // get
        AccessToken afterToggle = accessTokenService.getByUserId(mockUserId);
        Assertions.assertNotNull(afterToggle);
        Assertions.assertEquals(AccessToken.STATUS_DISABLE, afterToggle.getStatus());

        // delete
        Assertions.assertTrue(accessTokenService.removeById(tokenId));
    }
}
