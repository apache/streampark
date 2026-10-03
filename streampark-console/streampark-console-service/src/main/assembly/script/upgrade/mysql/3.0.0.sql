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

use streampark;

-- Map legacy team-admin membership to the built-in ADMIN user type before dropping role tables.
update `t_user` u
    inner join `t_member` m on u.`user_id` = m.`user_id`
set u.`user_type` = 1
where m.`role_id` = 100002;

delete from `t_menu`
where `type` = '1'
   or `path` in ('/system/menu', '/system/role', '/system/team', '/system/member');

update `t_menu`
set `component` = 'system/token/View'
where `path` = '/system/token';

drop table if exists `t_role_menu`;
drop table if exists `t_member`;
drop table if exists `t_role`;
