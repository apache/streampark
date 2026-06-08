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

set names utf8mb4;
set foreign_key_checks = 0;

drop table if exists `t_external_link`;
CREATE TABLE `t_external_link` (
  `id` bigint not null auto_increment primary key,
  `badge_label` varchar(64) collate utf8mb4_general_ci default null,
  `badge_name` varchar(64) collate utf8mb4_general_ci default null,
  `badge_color` varchar(64) collate utf8mb4_general_ci default null,
  `link_url` varchar(255) collate utf8mb4_general_ci default null,
  `create_time` datetime default null COMMENT 'create time',
  `modify_time` datetime default null COMMENT 'modify time'
) engine = innodb default charset=utf8mb4 collate=utf8mb4_general_ci;

drop table if exists `t_yarn_queue`;
create table `t_yarn_queue` (
  `id` bigint not null primary key auto_increment comment 'queue id',
  `team_id` bigint not null comment 'team id',
  `queue_label` varchar(128) collate utf8mb4_general_ci not null comment 'queue and label expression',
  `description` varchar(255) collate utf8mb4_general_ci default null comment 'description of the queue label',
  `create_time` datetime default null comment 'create time',
  `modify_time` datetime default null comment 'modify time',
  unique key `unq_team_id_queue_label` (`team_id`, `queue_label`) using btree
) engine = innodb default charset = utf8mb4 collate = utf8mb4_general_ci;

drop table if exists t_flink_tutorial;

-- type change
alter table `t_app_backup` modify column `path` varchar(128) collate utf8mb4_general_ci default null;

alter table `t_flink_app`
    change column `launch` `release` tinyint default 1,
    modify column `project_id` bigint default null,
    modify column `app_id` varchar(64) collate utf8mb4_general_ci default null,
    modify column `cluster_id` varchar(45) collate utf8mb4_general_ci default null,
    modify column `k8s_namespace` varchar(63) collate utf8mb4_general_ci default null,
    modify column `flink_image` varchar(128) collate utf8mb4_general_ci default null,
    modify column `state` int default null,
    drop index `inx_state`;

alter table `t_flink_env`
    modify column `version` varchar(64) collate utf8mb4_general_ci not null comment 'flink version',
    modify column `scala_version` varchar(64) collate utf8mb4_general_ci not null comment 'scala version of flink';

alter table `t_flink_log`
    modify column `yarn_app_id` varchar(64) collate utf8mb4_general_ci default null,
    add column `option_name` tinyint default null;

alter table `t_flink_project`
    modify column `url` varchar(255) collate utf8mb4_general_ci default null,
    modify column `branches` varchar(64) collate utf8mb4_general_ci default null,
    modify column `user_name` varchar(64) collate utf8mb4_general_ci default null,
    modify column `password` varchar(64) collate utf8mb4_general_ci default null,
    modify column `prvkey_path` varchar(128) collate utf8mb4_general_ci default null;

alter table `t_flink_savepoint`
    modify column `path` varchar(255) collate utf8mb4_general_ci default null;


alter table `t_menu`
    modify column `menu_name` varchar(64) collate utf8mb4_general_ci not null comment 'menu button name',
    modify column `path` varchar(64) collate utf8mb4_general_ci default null comment 'routing path',
    modify column `component` varchar(64) collate utf8mb4_general_ci default null comment 'routing component component',
    modify column `perms` varchar(64) collate utf8mb4_general_ci default null comment 'authority id',
    modify column `icon` varchar(64) collate utf8mb4_general_ci default null comment 'icon';

alter table `t_team`
    modify column `team_name` varchar(64) collate utf8mb4_general_ci not null comment 'team name';

alter table `t_variable`
    modify column `variable_code` varchar(128) collate utf8mb4_general_ci not null comment 'Variable code is used for parameter names passed to the program or as placeholders';

alter table `t_role`
    modify column `role_name` varchar(64) collate utf8mb4_general_ci not null comment 'role name',
    change column `remark` `description` varchar(255) collate utf8mb4_general_ci default null comment 'description',
    drop column `role_code`;

alter table `t_setting`
    modify column `setting_key` varchar(64) collate utf8mb4_general_ci not null;

alter table `t_user`
    modify column `username` varchar(64) collate utf8mb4_general_ci not null comment 'user name',
    modify column `nick_name` varchar(64) collate utf8mb4_general_ci not null comment 'nick name',
    modify column `salt` varchar(26) collate utf8mb4_general_ci default null comment 'salt',
    modify column `password` varchar(64) collate utf8mb4_general_ci not null comment 'password',
    modify column `email` varchar(64) collate utf8mb4_general_ci default null comment 'email',
    modify column `description` varchar(255) collate utf8mb4_general_ci default null comment 'description',
    add column `login_type` tinyint default 0 after `user_type`,
    drop column `avatar`;

alter table `t_flink_cluster`
    modify column `cluster_id` varchar(45) default null comment 'clusterid of session mode(yarn-session:application-id,k8s-session:cluster-id)',
    modify column `cluster_name` varchar(128) not null comment 'cluster name',
    modify column `options` text comment 'json form of parameter collection ',
    modify column `yarn_queue` varchar(128) default null comment 'the yarn queue where the task is located',
    modify column `k8s_namespace` varchar(63) default 'default' comment 'k8s namespace',
    modify column `service_account` varchar(64) default null comment 'k8s service account',
    modify column `description` varchar(255) default null,
    modify column `user_id` bigint default null,
    modify column `flink_image` varchar(128) default null comment 'flink image';

alter table `t_access_token`
    modify column `description` varchar(255) character set utf8mb4 collate utf8mb4_general_ci default null comment 'description';


-- menu script
delete from `t_menu`;
-- menu level 1
insert into `t_menu` values (110000, 0, 'menu.system', '/system', 'PageView', null, 'desktop', '0', 1, 1, now(), now());
insert into `t_menu` values (120000, 0, 'StreamPark', '/flink', 'PageView', null, 'build', '0', 1, 2, now(), now());
insert into `t_menu` values (130000, 0, 'menu.setting', '/setting', 'PageView', null, 'setting', '0', 1, 5, now(), now());
-- menu level 2
insert into `t_menu` values (110100, 110000, 'menu.userManagement', '/system/user', 'system/user/User', null, 'user', '0', 1, 1, now(), now());
insert into `t_menu` values (110200, 110000, 'menu.roleManagement', '/system/role', 'system/role/Role', null, 'smile', '0', 1, 2, now(), now());
insert into `t_menu` values (110300, 110000, 'menu.menuManagement', '/system/menu', 'system/menu/Menu', 'menu:view', 'bars', '0', 1, 3, now(), now());
insert into `t_menu` values (110400, 110000, 'menu.tokenManagement', '/system/token', 'system/token/Token', null, 'lock', '0', 1, 1, now(), now());
insert into `t_menu` values (110500, 110000, 'menu.teamManagement', '/system/team', 'system/team/Team', null, 'team', '0', 1, 2, now(), now());
insert into `t_menu` values (110600, 110000, 'menu.memberManagement', '/system/member', 'system/member/Member', null, 'usergroup-add', '0', 1, 2, now(), now());
insert into `t_menu` values (120100, 120000, 'menu.project', '/flink/project', 'flink/project/View', null, 'github', '0', 1, 1, now(), now());
insert into `t_menu` values (120200, 120000, 'menu.application', '/flink/app', 'flink/app/View', null, 'mobile', '0', 1, 2, now(), now());
insert into `t_menu` values (120300, 120000, 'menu.variable', '/flink/variable', 'flink/variable/View', null, 'code', '0', 1, 3, now(), now());
insert into `t_menu` values (130100, 130000, 'setting.system', '/setting/system', 'setting/System/index', null, 'database', '0', 1, 1, now(), now());
insert into `t_menu` values (130200, 130000, 'setting.alarm', '/setting/alarm', 'setting/Alarm/index', null, 'alert', '0', 1, 2, now(), now());
insert into `t_menu` values (130300, 130000, 'setting.flinkHome', '/setting/flinkHome', 'setting/FlinkHome/index', null, 'desktop', '0', 1, 3, now(), now());
insert into `t_menu` values (130400, 130000, 'setting.flinkCluster', '/setting/flinkCluster', 'setting/FlinkCluster/index', 'menu:view', 'cluster', '0', 1, 4, now(), now());
insert into `t_menu` values (130500, 130000, 'setting.externalLink', '/setting/externalLink', 'setting/ExternalLink/index', 'menu:view', 'link', '0', 1, 5, now(), now());
insert into `t_menu` values (130600, 130000, 'setting.yarnQueue', '/setting/yarnQueue', 'setting/YarnQueue/index', 'menu:view', 'bars', '0', 1, 6, now(), now());
-- menu level 3
insert into `t_menu` values (110101, 110100, 'add', null, null, 'user:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (110102, 110100, 'update', null, null, 'user:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (110103, 110100, 'delete', null, null, 'user:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110104, 110100, 'reset', null, null, 'user:reset', null, '1', 1, null, now(), now());
insert into `t_menu` values (110105, 110100, 'types', null, null, 'user:types', null, '1', 1, null, now(), now());
insert into `t_menu` values (110106, 110100, 'view', null, null, 'user:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (110201, 110200, 'add', null, null, 'role:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (110202, 110200, 'update', null, null, 'role:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (110203, 110200, 'delete', null, null, 'role:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110204, 110200, 'view', null, null, 'role:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (110401, 110400, 'add', null, null, 'token:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (110402, 110400, 'delete', null, null, 'token:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110403, 110400, 'view', null, null, 'token:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (110501, 110500, 'add', null, null, 'team:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (110502, 110500, 'update', null, null, 'team:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (110503, 110500, 'delete', null, null, 'team:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110504, 110500, 'view', null, null, 'team:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (110601, 110600, 'add', null, null, 'member:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (110602, 110600, 'update', null, null, 'member:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (110603, 110600, 'delete', null, null, 'member:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110604, 110600, 'role view', null, null, 'role:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (110605, 110600, 'view', null, null, 'member:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (120101, 120100, 'add', '/flink/project/add', 'flink/project/Add', 'project:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (120102, 120100, 'build', null, null, 'project:build', null, '1', 1, null, now(), now());
insert into `t_menu` values (120103, 120100, 'delete', null, null, 'project:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (120104, 120100, 'edit', '/flink/project/edit', 'flink/project/Edit', 'project:update', null, '0', 0, null, now(), now());
insert into `t_menu` values (120105, 120100, 'view', null, null, 'project:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (120201, 120200, 'add', '/flink/app/add', 'flink/app/Add', 'app:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (120202, 120200, 'detail app', '/flink/app/detail', 'flink/app/Detail', 'app:detail', '', '0', 0, null, now(), now());
insert into `t_menu` values (120203, 120200, 'edit flink', '/flink/app/edit_flink', 'flink/app/EditFlink', 'app:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (120204, 120200, 'edit streampark', '/flink/app/edit_streampark', 'flink/app/EditStreamPark', 'app:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (120205, 120200, 'mapping', null, null, 'app:mapping', null, '1', 1, null, now(), now());
insert into `t_menu` values (120206, 120200, 'release', null, null, 'app:release', null, '1', 1, null, now(), now());
insert into `t_menu` values (120207, 120200, 'start', null, null, 'app:start', null, '1', 1, null, now(), now());
insert into `t_menu` values (120208, 120200, 'clean', null, null, 'app:clean', null, '1', 1, null, now(), now());
insert into `t_menu` values (120209, 120200, 'cancel', null, null, 'app:cancel', null, '1', 1, null, now(), now());
insert into `t_menu` values (120210, 120200, 'savepoint delete', null, null, 'savepoint:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (120211, 120200, 'backup rollback', null, null, 'backup:rollback', null, '1', 1, null, now(), now());
insert into `t_menu` values (120212, 120200, 'backup delete', null, null, 'backup:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (120213, 120200, 'conf delete', null, null, 'conf:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (120214, 120200, 'delete', null, null, 'app:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (120215, 120200, 'copy', null, null, 'app:copy', null, '1', 1, null, now(), now());
insert into `t_menu` values (120216, 120200, 'view', null, null, 'app:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (120217, 120200, 'savepoint trigger', null, null, 'savepoint:trigger', null, '1', 1, null, now(), now());
insert into `t_menu` values (120218, 120200, 'sql delete', null, null, 'sql:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (120301, 120300, 'add', NULL, NULL, 'variable:add', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (120302, 120300, 'update', NULL, NULL, 'variable:update', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (120303, 120300, 'delete', NULL, NULL, 'variable:delete', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (120304, 120300, 'depend apps', '/flink/variable/depend_apps', 'flink/variable/DependApps', 'variable:depend_apps', '', '0', 0, NULL, now(), now());
insert into `t_menu` values (120305, 120300, 'show original', NULL, NULL, 'variable:show_original', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (120306, 120300, 'view', NULL, NULL, 'variable:view', NULL, '1', 1, null, now(), now());
insert into `t_menu` values (120307, 120300, 'depend view', null, null, 'variable:depend_apps', null, '1', 1, NULL, now(), now());
insert into `t_menu` values (130101, 130100, 'view', null, null, 'setting:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (130102, 130100, 'setting update', null, null, 'setting:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (130401, 130400, 'add cluster', '/setting/add_cluster', 'setting/FlinkCluster/AddCluster', 'cluster:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (130402, 130400, 'edit cluster', '/setting/edit_cluster', 'setting/FlinkCluster/EditCluster', 'cluster:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (130501, 130500, 'link view', null, null, 'externalLink:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (130502, 130500, 'link create', null, null, 'externalLink:create', null, '1', 1, null, now(), now());
insert into `t_menu` values (130503, 130500, 'link update', null, null, 'externalLink:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (130504, 130500, 'link delete', null, null, 'externalLink:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (130601, 130600, 'add yarn queue', null, null, 'yarnQueue:create', '', '1', 0, null, now(), now());
insert into `t_menu` values (130602, 130600, 'edit yarn queue', null, null, 'yarnQueue:update', '', '1', 0, null, now(), now());
insert into `t_menu` values (130603, 130600, 'delete yarn queue', null, null, 'yarnQueue:delete', '', '1', 0, null, now(), now());

-- role menu script
update `t_role_menu` set menu_id=110000 where menu_id=100000;
update `t_role_menu` set menu_id=110100 where menu_id=100001;
update `t_role_menu` set menu_id=110101 where menu_id=100004;
update `t_role_menu` set menu_id=110102 where menu_id=100005;
update `t_role_menu` set menu_id=110103 where menu_id=100006;
update `t_role_menu` set menu_id=110104 where menu_id=100012;
update `t_role_menu` set menu_id=110105 where menu_id=100052;
update `t_role_menu` set menu_id=110106 where menu_id=100059;
update `t_role_menu` set menu_id=110200 where menu_id=100002;
update `t_role_menu` set menu_id=110201 where menu_id=100007;
update `t_role_menu` set menu_id=110202 where menu_id=100008;
update `t_role_menu` set menu_id=110203 where menu_id=100009;
update `t_role_menu` set menu_id=110204 where menu_id=100061;
update `t_role_menu` set menu_id=110300 where menu_id=100003;
update `t_role_menu` set menu_id=110400 where menu_id=100037;
update `t_role_menu` set menu_id=110401 where menu_id=100038;
update `t_role_menu` set menu_id=110402 where menu_id=100039;
update `t_role_menu` set menu_id=110403 where menu_id=100060;
update `t_role_menu` set menu_id=110500 where menu_id=100043;
update `t_role_menu` set menu_id=110501 where menu_id=100044;
update `t_role_menu` set menu_id=110502 where menu_id=100045;
update `t_role_menu` set menu_id=110503 where menu_id=100046;
update `t_role_menu` set menu_id=110504 where menu_id=100062;
update `t_role_menu` set menu_id=110600 where menu_id=100047;
update `t_role_menu` set menu_id=110601 where menu_id=100048;
update `t_role_menu` set menu_id=110602 where menu_id=100049;
update `t_role_menu` set menu_id=110603 where menu_id=100050;
update `t_role_menu` set menu_id=110604 where menu_id=100051;
update `t_role_menu` set menu_id=110605 where menu_id=100063;
update `t_role_menu` set menu_id=120000 where menu_id=100013;
update `t_role_menu` set menu_id=120100 where menu_id=100014;
update `t_role_menu` set menu_id=120101 where menu_id=100017;
update `t_role_menu` set menu_id=120102 where menu_id=100022;
update `t_role_menu` set menu_id=120103 where menu_id=100023;
update `t_role_menu` set menu_id=120104 where menu_id=100035;
update `t_role_menu` set menu_id=120105 where menu_id=100065;
update `t_role_menu` set menu_id=120200 where menu_id=100015;
update `t_role_menu` set menu_id=120201 where menu_id=100016;
update `t_role_menu` set menu_id=120202 where menu_id=100018;
update `t_role_menu` set menu_id=120203 where menu_id=100020;
update `t_role_menu` set menu_id=120204 where menu_id=100021;
update `t_role_menu` set menu_id=120205 where menu_id=100024;
update `t_role_menu` set menu_id=120207 where menu_id=100026;
update `t_role_menu` set menu_id=120208 where menu_id=100027;
update `t_role_menu` set menu_id=120209 where menu_id=100028;
update `t_role_menu` set menu_id=120210 where menu_id=100029;
update `t_role_menu` set menu_id=120211 where menu_id=100030;
update `t_role_menu` set menu_id=120212 where menu_id=100031;
update `t_role_menu` set menu_id=120213 where menu_id=100032;
update `t_role_menu` set menu_id=120214 where menu_id=100036;
update `t_role_menu` set menu_id=120215 where menu_id=100042;
update `t_role_menu` set menu_id=120216 where menu_id=100066;
update `t_role_menu` set menu_id=120300 where menu_id=100053;
update `t_role_menu` set menu_id=120301 where menu_id=100054;
update `t_role_menu` set menu_id=120302 where menu_id=100055;
update `t_role_menu` set menu_id=120303 where menu_id=100056;
update `t_role_menu` set menu_id=120304 where menu_id=100057;
update `t_role_menu` set menu_id=120305 where menu_id=100058;
update `t_role_menu` set menu_id=120306 where menu_id=100067;
update `t_role_menu` set menu_id=120307 where menu_id=100069;
update `t_role_menu` set menu_id=130101 where menu_id=100068;
update `t_role_menu` set menu_id=130102 where menu_id=100034;
update `t_role_menu` set menu_id=120206 where menu_id=100025;
update `t_role_menu` set menu_id=130000 where menu_id=100033;
update `t_role_menu` set menu_id=130401 where menu_id=100040;
update `t_role_menu` set menu_id=130402 where menu_id=100041;

delete from `t_role_menu` where role_id=100001 and menu_id=120000;
delete from `t_role_menu` where role_id=100001 and menu_id=120100;
delete from `t_role_menu` where role_id=100001 and menu_id=120101;
delete from `t_role_menu` where role_id=100001 and menu_id=120102;
delete from `t_role_menu` where role_id=100001 and menu_id=120104;
delete from `t_role_menu` where role_id=100001 and menu_id=120105;
delete from `t_role_menu` where role_id=100001 and menu_id=120200;
delete from `t_role_menu` where role_id=100001 and menu_id=120201;
delete from `t_role_menu` where role_id=100001 and menu_id=120202;
delete from `t_role_menu` where role_id=100001 and menu_id=120203;
delete from `t_role_menu` where role_id=100001 and menu_id=120204;
delete from `t_role_menu` where role_id=100001 and menu_id=120206;
delete from `t_role_menu` where role_id=100001 and menu_id=120207;
delete from `t_role_menu` where role_id=100001 and menu_id=120208;
delete from `t_role_menu` where role_id=100001 and menu_id=120209;
delete from `t_role_menu` where role_id=100001 and menu_id=120210;
delete from `t_role_menu` where role_id=100001 and menu_id=120211;
delete from `t_role_menu` where role_id=100001 and menu_id=120212;
delete from `t_role_menu` where role_id=100001 and menu_id=120213;
delete from `t_role_menu` where role_id=100001 and menu_id=120215;
delete from `t_role_menu` where role_id=100001 and menu_id=120216;
delete from `t_role_menu` where role_id=100001 and menu_id=120217;
delete from `t_role_menu` where role_id=100001 and menu_id=120300;
delete from `t_role_menu` where role_id=100001 and menu_id=120304;
delete from `t_role_menu` where role_id=100001 and menu_id=120306;
delete from `t_role_menu` where role_id=100001 and menu_id=120307;
delete from `t_role_menu` where role_id=100001 and menu_id=130000;
delete from `t_role_menu` where role_id=100001 and menu_id=130100;
delete from `t_role_menu` where role_id=100001 and menu_id=130101;
delete from `t_role_menu` where role_id=100002 and menu_id=110000;
delete from `t_role_menu` where role_id=100002 and menu_id=110600;
delete from `t_role_menu` where role_id=100002 and menu_id=110601;
delete from `t_role_menu` where role_id=100002 and menu_id=110602;
delete from `t_role_menu` where role_id=100002 and menu_id=110603;
delete from `t_role_menu` where role_id=100002 and menu_id=110604;
delete from `t_role_menu` where role_id=100002 and menu_id=110605;
delete from `t_role_menu` where role_id=100002 and menu_id=120000;
delete from `t_role_menu` where role_id=100002 and menu_id=120100;
delete from `t_role_menu` where role_id=100002 and menu_id=120101;
delete from `t_role_menu` where role_id=100002 and menu_id=120102;
delete from `t_role_menu` where role_id=100002 and menu_id=120103;
delete from `t_role_menu` where role_id=100002 and menu_id=120104;
delete from `t_role_menu` where role_id=100002 and menu_id=120105;
delete from `t_role_menu` where role_id=100002 and menu_id=120200;
delete from `t_role_menu` where role_id=100002 and menu_id=120201;
delete from `t_role_menu` where role_id=100002 and menu_id=120202;
delete from `t_role_menu` where role_id=100002 and menu_id=120203;
delete from `t_role_menu` where role_id=100002 and menu_id=120204;
delete from `t_role_menu` where role_id=100002 and menu_id=120205;
delete from `t_role_menu` where role_id=100002 and menu_id=120206;
delete from `t_role_menu` where role_id=100002 and menu_id=120207;
delete from `t_role_menu` where role_id=100002 and menu_id=120208;
delete from `t_role_menu` where role_id=100002 and menu_id=120209;
delete from `t_role_menu` where role_id=100002 and menu_id=120210;
delete from `t_role_menu` where role_id=100002 and menu_id=120211;
delete from `t_role_menu` where role_id=100002 and menu_id=120212;
delete from `t_role_menu` where role_id=100002 and menu_id=120213;
delete from `t_role_menu` where role_id=100002 and menu_id=120214;
delete from `t_role_menu` where role_id=100002 and menu_id=120215;
delete from `t_role_menu` where role_id=100002 and menu_id=120216;
delete from `t_role_menu` where role_id=100002 and menu_id=120217;
delete from `t_role_menu` where role_id=100002 and menu_id=120218;
delete from `t_role_menu` where role_id=100002 and menu_id=120300;
delete from `t_role_menu` where role_id=100002 and menu_id=120301;
delete from `t_role_menu` where role_id=100002 and menu_id=120302;
delete from `t_role_menu` where role_id=100002 and menu_id=120303;
delete from `t_role_menu` where role_id=100002 and menu_id=120304;
delete from `t_role_menu` where role_id=100002 and menu_id=120305;
delete from `t_role_menu` where role_id=100002 and menu_id=120306;
delete from `t_role_menu` where role_id=100002 and menu_id=120307;
delete from `t_role_menu` where role_id=100002 and menu_id=130000;
delete from `t_role_menu` where role_id=100002 and menu_id=130100;
delete from `t_role_menu` where role_id=100002 and menu_id=130101;
delete from `t_role_menu` where role_id=100002 and menu_id=130200;
delete from `t_role_menu` where role_id=100002 and menu_id=130300;
delete from `t_role_menu` where role_id=100002 and menu_id=130400;
delete from `t_role_menu` where role_id=100002 and menu_id=130401;
delete from `t_role_menu` where role_id=100002 and menu_id=130402;
delete from `t_role_menu` where role_id=100002 and menu_id=130500;
delete from `t_role_menu` where role_id=100002 and menu_id=130501;
delete from `t_role_menu` where role_id=100002 and menu_id=130502;
delete from `t_role_menu` where role_id=100002 and menu_id=130503;
delete from `t_role_menu` where role_id=100002 and menu_id=130504;
delete from `t_role_menu` where role_id=100002 and menu_id=130600;
delete from `t_role_menu` where role_id=100002 and menu_id=130601;
delete from `t_role_menu` where role_id=100002 and menu_id=130602;
delete from `t_role_menu` where role_id=100002 and menu_id=130603;

insert into `t_role_menu` (role_id, menu_id) values (100001, 120000);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120100);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120101);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120102);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120104);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120105);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120200);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120201);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120202);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120203);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120204);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120206);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120207);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120208);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120209);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120210);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120211);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120212);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120213);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120215);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120216);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120217);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120300);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120304);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120306);
insert into `t_role_menu` (role_id, menu_id) values (100001, 120307);
insert into `t_role_menu` (role_id, menu_id) values (100001, 130000);
insert into `t_role_menu` (role_id, menu_id) values (100001, 130100);
insert into `t_role_menu` (role_id, menu_id) values (100001, 130101);
insert into `t_role_menu` (role_id, menu_id) values (100002, 110000);
insert into `t_role_menu` (role_id, menu_id) values (100002, 110600);
insert into `t_role_menu` (role_id, menu_id) values (100002, 110601);
insert into `t_role_menu` (role_id, menu_id) values (100002, 110602);
insert into `t_role_menu` (role_id, menu_id) values (100002, 110603);
insert into `t_role_menu` (role_id, menu_id) values (100002, 110604);
insert into `t_role_menu` (role_id, menu_id) values (100002, 110605);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120000);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120100);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120101);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120102);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120103);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120104);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120105);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120200);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120201);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120202);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120203);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120204);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120205);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120206);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120207);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120208);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120209);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120210);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120211);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120212);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120213);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120214);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120215);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120216);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120217);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120218);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120300);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120301);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120302);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120303);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120304);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120305);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120306);
insert into `t_role_menu` (role_id, menu_id) values (100002, 120307);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130000);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130100);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130101);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130200);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130300);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130400);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130401);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130402);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130500);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130501);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130502);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130503);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130504);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130600);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130601);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130602);
insert into `t_role_menu` (role_id, menu_id) values (100002, 130603);

delete from `t_menu` where menu_id < 110000;
delete from `t_role_menu` where menu_id < 110000;

set foreign_key_checks = 1;
