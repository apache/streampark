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

import org.apache.streampark.console.base.domain.router.RouterMeta;
import org.apache.streampark.console.base.domain.router.VueRouter;
import org.apache.streampark.console.base.util.VueRouterUtils;
import org.apache.streampark.console.core.enums.UserTypeEnum;
import org.apache.streampark.console.system.entity.Menu;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.mapper.MenuMapper;
import org.apache.streampark.console.system.service.MenuService;
import org.apache.streampark.console.system.service.UserService;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class MenuServiceImpl extends ServiceImpl<MenuMapper, Menu> implements MenuService {

    private static final Set<String> REMOVED_PERMISSION_MENU_PATHS = new HashSet<>(Arrays.asList(
        "/system/member", "/system/menu", "/system/role", "/system/team"));

    private static final Set<String> ADMIN_MENU_PATHS = new HashSet<>(Arrays.asList(
        "/setting/system", "/system", "/system/user", "/system/token"));

    @Autowired
    private UserService userService;

    @Override
    public List<Menu> listMenus(Long userId, Long teamId) {
        User user = Optional.ofNullable(userService.getById(userId))
            .orElseThrow(
                () -> new IllegalArgumentException(
                    String.format("The userId:[%s] not found", userId)));
        List<Menu> menus = this.lambdaQuery().eq(Menu::getType, "0")
            .orderByAsc(Menu::getOrderNum).list();
        menus.removeIf(menu -> REMOVED_PERMISSION_MENU_PATHS.contains(menu.getPath()));
        if (UserTypeEnum.ADMIN != user.getUserType()) {
            menus.removeIf(menu -> ADMIN_MENU_PATHS.contains(menu.getPath()));
        }
        return menus;
    }

    @Override
    public List<VueRouter<Menu>> listRouters(Long userId, Long teamId) {
        List<VueRouter<Menu>> routes = new ArrayList<>();
        // The query type is the menu type
        List<Menu> menus = this.listMenus(userId, teamId);
        menus.forEach(
            menu -> {
                VueRouter<Menu> route = new VueRouter<>();
                route.setId(menu.getMenuId().toString());
                route.setParentId(menu.getParentId().toString());
                route.setPath(menu.getPath());
                route.setComponent(menu.getComponent());
                route.setName(menu.getMenuName());
                route.setMeta(new RouterMeta(true, !menu.isDisplay(), true, menu.getIcon()));
                routes.add(route);
            });
        return VueRouterUtils.buildVueRouter(routes);
    }
}
