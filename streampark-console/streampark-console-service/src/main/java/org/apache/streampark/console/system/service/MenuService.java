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

package org.apache.streampark.console.system.service;

import org.apache.streampark.console.base.domain.router.VueRouter;
import org.apache.streampark.console.system.entity.Menu;

import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/** This interface is used to control the menu of the page */
public interface MenuService extends IService<Menu> {

    /**
     * List menus for one of the two built-in user roles.
     *
     * @param userId user id
     * @param teamId ignored legacy parameter
     * @return List of Menu
     */
    List<Menu> listMenus(Long userId, Long teamId);

    /**
     * List Menu Routers based on user id and team id
     *
     * @param userId user id
     * @param teamId team id
     * @return List of Menu VueRouter
     */
    List<VueRouter<Menu>> listRouters(Long userId, Long teamId);
}
