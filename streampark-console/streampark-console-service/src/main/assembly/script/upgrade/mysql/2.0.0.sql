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

drop table if exists `t_alert_config`;
create table `t_alert_config` (
  `id` bigint   not null auto_increment primary key,
  `user_id` bigint  default null,
  `alert_name` varchar(128) collate utf8mb4_general_ci default null comment 'alert group name',
  `alert_type` int default 0 comment 'alert type',
  `email_params` varchar(255) collate utf8mb4_general_ci comment 'email params',
  `sms_params` text collate utf8mb4_general_ci comment 'sms params',
  `ding_talk_params` text collate utf8mb4_general_ci comment 'ding talk params',
  `we_com_params` varchar(255) collate utf8mb4_general_ci comment 'wechat params',
  `http_callback_params` text collate utf8mb4_general_ci comment 'http callback params',
  `lark_params` text collate utf8mb4_general_ci comment 'lark params',
  `create_time` datetime default null comment 'create time',
  `modify_time` datetime default null comment 'modify time',
  index `inx_user_id` (`user_id`) using btree
) engine = innodb default charset = utf8mb4 collate = utf8mb4_general_ci;

-- add alert_id field
alter table t_flink_app add column alert_id bigint after end_time;

-- dump history email alarm configuration
insert into t_alert_config(user_id, alert_name, alert_type, email_params)
select a.user_id, concat('emailAlertConf-', (@rownum := @rownum + 1)) AS alert_name, 1 as alert_type, a.alert_email
from (select user_id, alert_email from t_flink_app where alert_email is not null group by user_id, alert_email) a,
     (select @rownum := 0) t;

-- update the original table mail configuration id
update t_flink_app a inner join t_alert_config b on a.alert_email = b.email_params
    set a.alert_id = b.id
where a.alert_email = b.email_params;

-- Adjust the content of the alarm configuration table params
update t_alert_config
set email_params = concat('{"contacts":"', email_params, '"}'),
    ding_talk_params = '{}',
    we_com_params='{}',
    lark_params='{}'
where alert_type = 1;

-- t_flink_app
alter table `t_flink_app`
    drop column alert_email,
    change column dynamic_options dynamic_properties text comment 'allows specifying multiple generic configuration options',
    add column `job_manager_url` varchar(255) default null after `job_id`,
    add column `option_time` datetime default null after `create_time`,
    add column `ingress_template` text collate utf8mb4_general_ci comment 'ingress模版文件',
    add column `default_mode_ingress` text collate utf8mb4_general_ci comment '配置ingress的域名',
    add column `modify_time` datetime default null after `create_time`,
    add column `tags` varchar(500) default null;

alter table `t_flink_log` add column `job_manager_url` varchar(255) default null after `yarn_app_id`;

-- t_flink_project
alter table `t_flink_project`
    change column `date` `create_time` datetime default current_timestamp not null,
    add column `team_id` bigint not null comment 'team id' default 100000 after `id`,
    add column `git_credential` tinyint not null default 1 after `id`,
    add column `prvkey_path` varchar(255) collate utf8mb4_general_ci default null after `password`,
    add column `modify_time` datetime default null after `create_time`,
    add index `inx_team` (`team_id`) using btree;

update `t_flink_project` set git_credential=1 where url like 'http%';
update `t_flink_project` set git_credential=2 where url like 'git@%';

alter table `t_flink_cluster`
    drop column `flame_graph`,
    change column `dynamic_options` `dynamic_properties` text comment 'allows specifying multiple generic configuration options';

-- change `update_time` to `modify_time`
alter table `t_app_build_pipe` change column `update_time` `modify_time` datetime default null on update current_timestamp;


-- change `readed` to `is_read`
alter table `t_message` change column `readed` `is_read` tinyint default 0;


-- add chk_id field
alter table `t_flink_savepoint` add column `chk_id` bigint after `app_id`;

-- change create_time field and modify_time field
update `t_access_token` set `modify_time` = current_timestamp where `modify_time` is null;
update `t_role` set `modify_time` = current_timestamp where `modify_time` is null;
update `t_user` set `modify_time` = current_timestamp where `modify_time` is null;
update `t_menu` set `modify_time` = current_timestamp where `modify_time` is null;

alter table `t_app_backup` modify `create_time` datetime not null default current_timestamp;
alter table `t_flink_app` modify `create_time` datetime not null default current_timestamp;
alter table `t_flink_config` modify `create_time` datetime not null default current_timestamp;
alter table `t_flink_effective` modify `create_time` datetime not null default current_timestamp;
alter table `t_flink_env` modify `create_time` datetime not null default current_timestamp;
alter table `t_flink_savepoint` modify `create_time` datetime not null default current_timestamp;
alter table `t_flink_sql` modify `create_time` datetime not null default current_timestamp;
alter table `t_flink_tutorial` modify `create_time` datetime not null default current_timestamp;
alter table `t_message` modify `create_time` datetime not null default current_timestamp;
alter table `t_role` modify `create_time` datetime not null default current_timestamp;
alter table `t_user` modify `create_time` datetime not null default current_timestamp;
alter table `t_flink_cluster` modify `create_time` datetime not null default current_timestamp;
alter table `t_access_token` modify `create_time` datetime not null default current_timestamp;

alter table `t_access_token` modify `modify_time` datetime default null on update current_timestamp;
alter table `t_menu` modify `modify_time` datetime default null on update current_timestamp;
alter table `t_role` modify `modify_time` datetime default null on update current_timestamp;
alter table `t_user` modify `modify_time` datetime default null on update current_timestamp;

-- add new modules to the menu
delete from `t_menu`;
insert into `t_menu` values (100000, 0, 'menu.system', '/system', 'PageView', null, 'desktop', '0', 1, 1, now(), now());
insert into `t_menu` values (100001, 100000, 'menu.userManagement', '/system/user', 'system/user/User', null, 'user', '0', 1, 1, now(), now());
insert into `t_menu` values (100002, 100000, 'menu.roleManagement', '/system/role', 'system/role/Role', null, 'smile', '0', 1, 2, now(), now());
insert into `t_menu` values (100003, 100000, 'menu.menuManagement', '/system/menu', 'system/menu/Menu', 'menu:view', 'bars', '0', 1, 3, now(), now());
insert into `t_menu` values (100004, 100001, 'add', null, null, 'user:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (100005, 100001, 'update', null, null, 'user:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (100006, 100001, 'delete', null, null, 'user:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100007, 100002, 'add', null, null, 'role:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (100008, 100002, 'update', null, null, 'role:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (100009, 100002, 'delete', null, null, 'role:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100012, 100001, 'reset', null, null, 'user:reset', null, '1', 1, null, now(), now());
insert into `t_menu` values (100013, 0, 'StreamPark', '/flink', 'PageView', null, 'build', '0', 1, 2, now(), now());
insert into `t_menu` values (100014, 100013, 'menu.project', '/flink/project', 'flink/project/View', null, 'github', '0', 1, 1, now(), now());
insert into `t_menu` values (100015, 100013, 'menu.application', '/flink/app', 'flink/app/View', null, 'mobile', '0', 1, 2, now(), now());
insert into `t_menu` values (100016, 100015, 'add', '/flink/app/add', 'flink/app/Add', 'app:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (100017, 100014, 'add', '/flink/project/add', 'flink/project/Add', 'project:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (100018, 100015, 'detail app', '/flink/app/detail', 'flink/app/Detail', 'app:detail', '', '0', 0, null, now(), now());
-- insert into `t_menu` values (100019, 100013, 'Notebook', '/flink/notebook/view', 'flink/notebook/Submit', 'notebook:submit', 'read', '0', 1, 4, now(), now());
insert into `t_menu` values (100020, 100015, 'edit flink', '/flink/app/edit_flink', 'flink/app/EditFlink', 'app:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (100021, 100015, 'edit streampark', '/flink/app/edit_streampark', 'flink/app/EditStreamPark', 'app:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (100022, 100014, 'build', null, null, 'project:build', null, '1', 1, null, now(), now());
insert into `t_menu` values (100023, 100014, 'delete', null, null, 'project:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100024, 100015, 'mapping', null, null, 'app:mapping', null, '1', 1, null, now(), now());
insert into `t_menu` values (100025, 100015, 'launch', null, null, 'app:launch', null, '1', 1, null, now(), now());
insert into `t_menu` values (100026, 100015, 'start', null, null, 'app:start', null, '1', 1, null, now(), now());
insert into `t_menu` values (100027, 100015, 'clean', null, null, 'app:clean', null, '1', 1, null, now(), now());
insert into `t_menu` values (100028, 100015, 'cancel', null, null, 'app:cancel', null, '1', 1, null, now(), now());
insert into `t_menu` values (100029, 100015, 'savepoint delete', null, null, 'savepoint:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100030, 100015, 'backup rollback', null, null, 'backup:rollback', null, '1', 1, null, now(), now());
insert into `t_menu` values (100031, 100015, 'backup delete', null, null, 'backup:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100032, 100015, 'conf delete', null, null, 'conf:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100033, 100013, 'menu.setting', '/flink/setting', 'flink/setting/View', null, 'setting', '0', 1, 5, now(), now());
insert into `t_menu` values (100034, 100033, 'setting update', null, null, 'setting:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (100035, 100014, 'edit', '/flink/project/edit', 'flink/project/Edit', 'project:update', null, '0', 0, null, now(), now());
insert into `t_menu` values (100036, 100015, 'delete', null, null, 'app:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100037, 100000, 'menu.tokenManagement', '/system/token', 'system/token/Token', null, 'lock', '0', 1, 1, now(), now());
insert into `t_menu` values (100038, 100037, 'add', null, null, 'token:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (100039, 100037, 'delete', null, null, 'token:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100040, 100033, 'add cluster', '/flink/setting/add_cluster', 'flink/setting/AddCluster', 'cluster:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (100041, 100033, 'edit cluster', '/flink/setting/edit_cluster', 'flink/setting/EditCluster', 'cluster:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (100042, 100015, 'copy', null, null, 'app:copy', null, '1', 1, null, now(), now());
insert into `t_menu` values (100043, 100000, 'menu.teamManagement', '/system/team', 'system/team/Team', null, 'team', '0', 1, 2, now(), now());
insert into `t_menu` values (100044, 100043, 'add', null, null, 'team:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (100045, 100043, 'update', null, null, 'team:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (100046, 100043, 'delete', null, null, 'team:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100047, 100000, 'menu.memberManagement', '/system/member', 'system/member/Member', null, 'usergroup-add', '0', 1, 2, now(), now());
insert into `t_menu` values (100048, 100047, 'add', null, null, 'member:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (100049, 100047, 'update', null, null, 'member:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (100050, 100047, 'delete', null, null, 'member:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (100051, 100047, 'role view', null, null, 'role:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100052, 100001, 'types', null, null, 'user:types', null, '1', 1, null, now(), now());
insert into `t_menu` values (100053, 100013, 'menu.variable', '/flink/variable', 'flink/variable/View', null, 'code', '0', 1, 3, now(), now());
insert into `t_menu` values (100054, 100053, 'add', NULL, NULL, 'variable:add', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (100055, 100053, 'update', NULL, NULL, 'variable:update', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (100056, 100053, 'delete', NULL, NULL, 'variable:delete', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (100057, 100053, 'depend apps', '/flink/variable/depend_apps', 'flink/variable/DependApps', 'variable:depend_apps', '', '0', 0, NULL, now(), now());
insert into `t_menu` values (100058, 100053, 'show original', NULL, NULL, 'variable:show_original', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (100059, 100001, 'view', null, null, 'user:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100060, 100037, 'view', null, null, 'token:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100061, 100002, 'view', null, null, 'role:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100062, 100043, 'view', null, null, 'team:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100063, 100047, 'view', null, null, 'member:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100065, 100014, 'view', null, null, 'project:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100066, 100015, 'view', null, null, 'app:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100067, 100053, 'view', NULL, NULL, 'variable:view', NULL, '1', 1, null, now(), now());
insert into `t_menu` values (100068, 100033, 'view', null, null, 'setting:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (100069, 100053, 'depend view', null, null, 'variable:depend_apps', null, '1', 1, NULL, now(), now());

-- reset t_role_menu
delete from t_role_menu;
insert into `t_role_menu` values (100001, 100001, 100014);
insert into `t_role_menu` values (100002, 100001, 100016);
insert into `t_role_menu` values (100003, 100001, 100017);
insert into `t_role_menu` values (100004, 100001, 100018);
insert into `t_role_menu` values (100005, 100001, 100020);
insert into `t_role_menu` values (100006, 100001, 100021);
insert into `t_role_menu` values (100007, 100001, 100022);
insert into `t_role_menu` values (100008, 100001, 100025);
insert into `t_role_menu` values (100009, 100001, 100026);
insert into `t_role_menu` values (100010, 100001, 100027);
insert into `t_role_menu` values (100011, 100001, 100028);
insert into `t_role_menu` values (100012, 100001, 100029);
insert into `t_role_menu` values (100013, 100001, 100030);
insert into `t_role_menu` values (100014, 100001, 100031);
insert into `t_role_menu` values (100015, 100001, 100032);
insert into `t_role_menu` values (100016, 100001, 100013);
insert into `t_role_menu` values (100017, 100001, 100015);
insert into `t_role_menu` values (100018, 100002, 100014);
insert into `t_role_menu` values (100019, 100002, 100016);
insert into `t_role_menu` values (100020, 100002, 100017);
insert into `t_role_menu` values (100021, 100002, 100018);
insert into `t_role_menu` values (100022, 100002, 100020);
insert into `t_role_menu` values (100023, 100002, 100021);
insert into `t_role_menu` values (100024, 100002, 100022);
insert into `t_role_menu` values (100025, 100002, 100025);
insert into `t_role_menu` values (100026, 100002, 100026);
insert into `t_role_menu` values (100027, 100002, 100027);
insert into `t_role_menu` values (100028, 100002, 100028);
insert into `t_role_menu` values (100029, 100002, 100029);
insert into `t_role_menu` values (100030, 100002, 100030);
insert into `t_role_menu` values (100031, 100002, 100031);
insert into `t_role_menu` values (100032, 100002, 100032);
insert into `t_role_menu` values (100033, 100002, 100013);
insert into `t_role_menu` values (100034, 100002, 100015);
insert into `t_role_menu` values (100035, 100002, 100000);
insert into `t_role_menu` values (100036, 100002, 100036);
insert into `t_role_menu` values (100037, 100002, 100047);
insert into `t_role_menu` values (100038, 100002, 100048);
insert into `t_role_menu` values (100039, 100002, 100049);
insert into `t_role_menu` values (100040, 100002, 100050);
insert into `t_role_menu` values (100041, 100002, 100051);
insert into `t_role_menu` values (100042, 100002, 100052);
insert into `t_role_menu` values (100043, 100002, 100053);
insert into `t_role_menu` values (100044, 100002, 100054);
insert into `t_role_menu` values (100045, 100002, 100055);
insert into `t_role_menu` values (100046, 100002, 100056);
insert into `t_role_menu` values (100047, 100001, 100065);
insert into `t_role_menu` values (100048, 100002, 100065);
insert into `t_role_menu` values (100049, 100001, 100066);
insert into `t_role_menu` values (100050, 100002, 100066);
insert into `t_role_menu` values (100051, 100002, 100063);
insert into `t_role_menu` values (100052, 100002, 100059);
insert into `t_role_menu` values (100053, 100002, 100067);
insert into `t_role_menu` values (100054, 100001, 100053);
insert into `t_role_menu` values (100055, 100001, 100057);
insert into `t_role_menu` values (100056, 100001, 100067);
insert into `t_role_menu` values (100057, 100001, 100069);
insert into `t_role_menu` values (100058, 100002, 100057);
insert into `t_role_menu` values (100059, 100002, 100058);
insert into `t_role_menu` values (100060, 100002, 100069);

-- Add team related sql
create table `t_team` (
  `id` bigint not null auto_increment comment 'team id',
  `team_name` varchar(50) collate utf8mb4_general_ci not null comment 'team name',
  `description` varchar(255) collate utf8mb4_general_ci default null,
  `create_time` datetime default null comment 'create time',
  `modify_time` datetime default null comment 'modify time',
  primary key (`id`) using btree,
  unique key `team_name_idx` (`team_name`) using btree
) engine = innodb default charset = utf8mb4 collate = utf8mb4_general_ci;

insert into `t_team` values (100000, 'default', null, now(), now());

alter table `t_flink_app`
add column `team_id` bigint not null comment 'team id' default 100000 after `id`,
add index `inx_team` (`team_id`) using btree;


-- Update user
alter table `t_user`
add column `user_type` int  not null default 2 comment 'user type 1:admin 2:user' after `email`,
add column `last_team_id` bigint default null comment 'last team id' after `user_type`;

-- after adding team module, admin has all permissions by default, so admin does not need to add permissions separately, so delete admin related roles and associations
update `t_user` set `user_type` = 1
where `user_id` in (select user_id from `t_user_role` where role_id = 100000);

delete from t_role where role_id = 100000;
delete from `t_user_role` where role_id = 100000;

-- Add team admin
insert into `t_role` values (100002, 'team admin', 'Team Admin has all permissions inside the team.', now(), now(), null);

-- alter t_role_user to t_member and update the schema
alter table `t_user_role` rename `t_member`;

alter table `t_member`
add column `team_id` bigint not null comment 'team id' default 100000 after `id`,
modify column `user_id` bigint not null comment 'user id',
modify column `role_id` bigint not null comment 'role id',
add column  `create_time` datetime default null comment 'create time',
add column  `modify_time` datetime default null comment 'modify time',
drop index `UN_INX`,
add unique key `un_user_team_role_inx` (`user_id`,`team_id`,`role_id`) using btree;

-- remove user table contact phone field
alter table `t_user` drop column `mobile`;

-- t_setting
alter table `t_setting` drop primary key;
alter table `t_setting`
change column `NUM` `order_num` int default null,
change column `KEY` `setting_key` varchar(50) collate utf8mb4_general_ci not null,
change column `VALUE` `setting_value` text collate utf8mb4_general_ci default null,
change column `TITLE` `setting_name` varchar(255) collate utf8mb4_general_ci default null,
change column `DESCRIPTION` `description` varchar(255) collate utf8mb4_general_ci default null,
change column `TYPE` `type` tinyint not null comment '1: input 2: boolean 3: number',
add primary key (`setting_key`);

insert into `t_setting` values (14, 'docker.register.namespace', null, 'Docker namespace', 'Namespace for docker image used in docker building env and target image register', 1);
insert into `t_setting` values (15, 'streampark.maven.settings', null, 'Maven Settings File Path', 'Maven Settings.xml full path', 1);
insert into `t_setting` values (16, 'ingress.mode.default', null, 'Ingress domain address', 'Automatically generate an nginx-based ingress by passing in a domain name', 1);

update t_setting set setting_key = replace(setting_key, 'streamx', 'streampark') where setting_key like 'streamx%';

delete from `t_setting` where setting_key like '%console.webapp.address';

-- t_user
alter table `t_user`
    drop index `un_username`,
    modify column `username` varchar(255) collate utf8mb4_general_ci not null comment 'user name',
    add unique key `un_username` (`username`) using btree;

drop table if exists `t_variable`;
create table `t_variable` (
  `id` bigint not null auto_increment,
  `variable_code` varchar(100) collate utf8mb4_general_ci not null comment 'Variable code is used for parameter names passed to the program or as placeholders',
  `variable_value` text collate utf8mb4_general_ci not null comment 'The specific value corresponding to the variable',
  `description` text collate utf8mb4_general_ci default null comment 'More detailed description of variables',
  `creator_id` bigint collate utf8mb4_general_ci not null comment 'user id of creator',
  `team_id` bigint collate utf8mb4_general_ci not null comment 'team id',
  `desensitization` tinyint not null default 0 comment '0 is no desensitization, 1 is desensitization, if set to desensitization, it will be replaced by * when displayed',
  `create_time` datetime default null comment 'create time',
  `modify_time` datetime default null comment 'modify time',
  primary key (`id`) using btree,
  unique key `un_team_vcode_inx` (`team_id`,`variable_code`) using btree
) engine=innodb auto_increment=100000 default charset=utf8mb4 collate=utf8mb4_general_ci;


drop table if exists `t_flame_graph`;

set foreign_key_checks = 1;

