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

-- ----------------------------
-- Records of t_team
-- ----------------------------
insert into `t_team` values (100000, 'default', 'The default team', now(), now());
insert into `t_team` values (100001, 'test', 'The test team', now(), now());

-- ----------------------------
-- Records of flink-app
-- ----------------------------
INSERT INTO `t_app` (`id`, `job_type`, `create_time`, `modify_time`)
VALUES (100000, 1, now(), now());

-- ----------------------------
-- Records of t_flink_app
-- ----------------------------
INSERT INTO `t_flink_app` (
    `id`, `team_id`, `job_type`, `deploy_mode`, `job_name`, `user_id`, `app_type`, `state`, `restart_size`,
    `description`,`resolve_order`,`option_state`,`tracking`,`create_time`, `modify_time`,`release`, `build`,
    `k8s_hadoop_integration`,`tags`
) VALUES (100000, 100000, 2, 4, 'Flink SQL Demo', 100000, 1, '0', 0, 'Flink SQL Demo', 0, 0, 0, now(), now(), 1, 1, 0, 'streampark,test');

-- ----------------------------
-- Records of t_flink_effective
-- ----------------------------
insert into `t_flink_effective` values (100000, 100000, 2, 100000, now());

-- ----------------------------
-- Records of t_flink_project
-- ----------------------------
insert into `t_flink_project` values (100000, 100000, 'streampark-quickstart', 'https://github.com/apache/streampark-quickstart', 'dev', null, null, null, null, null, null, 1, 1, null, 'streampark-quickstart', -1, now(), now());

-- ----------------------------
-- Records of t_flink_sql
-- ----------------------------
insert into `t_flink_sql` values (100000, 100000, 'eNqlUUtPhDAQvu+vmFs1AYIHT5s94AaVqGxSSPZIKgxrY2mxrdGfb4GS3c0+LnJo6Mz36syapkmZQpk8vKbQMMt2KOFmAe5rK4Nf3yhrhCwvA1/TTDaqO61UxmooSprlT1PDGkgKEKpmwvIOjWVdP3W2zpG+JfQFHjfU46xxrVvYZuWztye1khJrqzSBFRCfjUwSYQiqt1xJJvyPcbWJp9WPCXvUoUEn0ZAVufcs0nIUjYn2L4s++YiY75eBLr+2Dnl3GYKTWRyfQKYRRR2XZxXmNvu9yh9GHAmUO/sxyMRkGNly4c714RZ7zaWtLHsX+N9NjvVrWxm99jmyvEhpOUhujmIYFI5zkCOYzYIj11a7QH7Tyz+nE8bw', null, null, 1, 1, now());


-- ----------------------------
-- Records of spark-app
-- ----------------------------
INSERT INTO `t_app` (`id`, `job_type`, `create_time`, `modify_time`)
VALUES (100001, 2, now(), now());

-- ----------------------------
-- Records of t_spark_app
-- ----------------------------
insert into `t_spark_app` (
    `id`, `team_id`, `job_type`, `app_type`, `app_name`, `deploy_mode`, `resource_from`, `main_class`,
    `yarn_queue`, `k8s_image_pull_policy`, `k8s_namespace`, `state`, `option_state`, `user_id`,
    `description`, `tracking`, `release`, `build`, `create_time`, `modify_time`, `tags`)
values (100001, 100000, 2, 4, 'Spark SQL Demo', 2, 2, 'org.apache.streampark.spark.cli.SqlClient', 'default', 0, 'default', 0, 0, 100000, 'Spark SQL Demo', 0, 1, 1, now(), now(), 'streampark,test');

-- ----------------------------
-- Records of t_spark_effective
-- ----------------------------
insert into `t_spark_effective` values (100000, 100001, 4, 100000, now());

-- ----------------------------
-- Records of t_spark_sql
-- ----------------------------
insert into `t_spark_sql` values (100000, 100001, 'eNq1jr0OgjAURnee4m4FY/oCTJVUg/KT9F7cK2kQiy2W+P6KMQ6yuDh9+YZzcjIlBUkgsSkkXCbv0N9Da0ifBgOx01cDSCqvdmsIpuu9e98kavA54EPH9ajbs+HTqIPl023gsyeN8gqlIsgrqhfmoygaiTEre2vYGliDgiW/IXvd2hdymIls0d87+5f6jxdlITOCFWxVXX5npg92MWtB', null, null, 1, 1, now());

-- ----------------------------
-- Records of t_menu
-- ----------------------------
insert into `t_menu` values (110000, 0, 'Apache Flink', '/flink', 'PageView', null, null, '0', 1, 1, now(), now());
insert into `t_menu` values (120000, 0, 'Apache Spark', '/spark', 'PageView', null, null, '0', 1, 2, now(), now());
insert into `t_menu` values (130000, 0, 'resource.menu', '/resource', 'PageView', null, 0, '0', 1, 3, now(), now());
insert into `t_menu` values (140000, 0, 'setting.menu', '/setting', 'PageView', null, 0, '0', 1, 4, now(), now());
insert into `t_menu` values (150000, 0, 'system.menu', '/system', 'PageView', null, 0, '0', 1, 5, now(), now());

insert into `t_menu` values (110100, 110000, 'flink.application', '/flink/app', 'flink/app/View', null, null, '0', 1, 2, now(), now());
insert into `t_menu` values (110200, 110000, 'flink.flinkHome', '/flink/home', 'flink/home/View', null, null, '0', 1, 3, now(), now());
insert into `t_menu` values (110300, 110000, 'flink.flinkCluster', '/flink/cluster', 'flink/cluster/View', 'menu:view', null, '0', 1, 4, now(), now());

insert into `t_menu` values (110101, 110100, 'app view', null, null, 'app:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (110102, 110100, 'app add', '/flink/app/add', 'flink/app/Add', 'app:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (110103, 110100, 'app detail', '/flink/app/detail', 'flink/app/Detail', 'app:detail', '', '0', 0, null, now(), now());
insert into `t_menu` values (110104, 110100, 'app edit flink', '/flink/app/edit_flink', 'flink/app/EditFlink', 'app:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (110105, 110100, 'app edit streampark', '/flink/app/edit_streampark', 'flink/app/EditStreamPark', 'app:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (110106, 110100, 'app mapping', null, null, 'app:mapping', null, '1', 1, null, now(), now());
insert into `t_menu` values (110107, 110100, 'app release', null, null, 'app:release', null, '1', 1, null, now(), now());
insert into `t_menu` values (110108, 110100, 'app start', null, null, 'app:start', null, '1', 1, null, now(), now());
insert into `t_menu` values (110109, 110100, 'app clean', null, null, 'app:clean', null, '1', 1, null, now(), now());
insert into `t_menu` values (110110, 110100, 'app cancel', null, null, 'app:cancel', null, '1', 1, null, now(), now());
insert into `t_menu` values (110111, 110100, 'app savepoint delete', null, null, 'savepoint:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110112, 110100, 'app backup rollback', null, null, 'backup:rollback', null, '1', 1, null, now(), now());
insert into `t_menu` values (110113, 110100, 'app backup delete', null, null, 'backup:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110114, 110100, 'app conf delete', null, null, 'conf:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110115, 110100, 'app delete', null, null, 'app:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (110116, 110100, 'app copy', null, null, 'app:copy', null, '1', 1, null, now(), now());
insert into `t_menu` values (110117, 110100, 'app savepoint trigger', null, null, 'savepoint:trigger', null, '1', 1, null, now(), now());
insert into `t_menu` values (110118, 110100, 'app sql delete', null, null, 'sql:delete', null, '1', 1, null, now(), now());

insert into `t_menu` values (110301, 110300, 'cluster add', '/flink/add_cluster', 'flink/cluster/Add', 'cluster:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (110302, 110300, 'cluster edit', '/flink/edit_cluster', 'flink/cluster/Edit', 'cluster:update', '', '0', 0, null, now(), now());

insert into `t_menu` values (120100, 120000, 'spark.application', '/spark/app', 'spark/app/index', null, null, '0', 1, 2, now(), now());
insert into `t_menu` values (120200, 120000, 'spark.sparkHome', '/spark/home', 'spark/home/index', null, null, '0', 1, 3, now(), now());
insert into `t_menu` values (120300, 120000, 'spark.createApplication', '/spark/app/add', 'spark/app/create', 'app:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (120400, 120000, 'spark.updateApplication', '/spark/app/edit', 'spark/app/edit', 'app:update', '', '0', 0, null, now(), now());
insert into `t_menu` values (120500, 120000, 'spark.applicationDetail', '/spark/app/detail', 'spark/app/detail', 'app:detail', '', '0', 0, null, now(), now());

insert into `t_menu` values (130100, 130000, 'resource.project', '/resource/project', 'resource/project/View', null, 'github', '0', 1, 2, now(), now());
insert into `t_menu` values (130200, 130000, 'resource.variable', '/resource/variable', 'resource/variable/View', null, null, '0', 1, 3, now(), now());
insert into `t_menu` values (130300, 130000, 'resource.upload', '/resource/upload', 'resource/upload/View', null, null, '0', 1, 1, now(), now());

insert into `t_menu` values (130101, 130100, 'project view', null, null, 'project:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (130102, 130100, 'project add', '/project/add', 'resource/project/Add', 'project:create', '', '0', 0, null, now(), now());
insert into `t_menu` values (130103, 130100, 'project build', null, null, 'project:build', null, '1', 1, null, now(), now());
insert into `t_menu` values (130104, 130100, 'project delete', null, null, 'project:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (130105, 130100, 'project edit', '/project/edit', 'resource/project/Edit', 'project:update', null, '0', 0, null, now(), now());

insert into `t_menu` values (130201, 130200, 'variable view', NULL, NULL, 'variable:view', NULL, '1', 1, null, now(), now());
insert into `t_menu` values (130202, 130200, 'variable depend view', null, null, 'variable:depend_apps', null, '1', 1, NULL, now(), now());
insert into `t_menu` values (130203, 130200, 'variable add', NULL, NULL, 'variable:add', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (130204, 130200, 'variable update', NULL, NULL, 'variable:update', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (130205, 130200, 'variable delete', NULL, NULL, 'variable:delete', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (130206, 130200, 'variable depend apps', '/resource/variable/depend_apps', 'resource/variable/DependApps', 'variable:depend_apps', '', '0', 0, NULL, now(), now());
insert into `t_menu` values (130207, 130200, 'variable show original', NULL, NULL, 'variable:show_original', NULL, '1', 1, NULL, now(), now());

insert into `t_menu` values (130301, 130300, 'resource add', NULL, NULL, 'resource:add', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (130302, 130300, 'resource update', NULL, NULL, 'resource:update', NULL, '1', 1, NULL, now(), now());
insert into `t_menu` values (130303, 130300, 'resource delete', NULL, NULL, 'resource:delete', NULL, '1', 1, NULL, now(), now());

insert into `t_menu` values (140100, 140000, 'setting.system', '/setting/system', 'setting/system/View', null, null, '0', 1, 1, now(), now());
insert into `t_menu` values (140200, 140000, 'setting.alarm', '/setting/alarm', 'setting/alarm/View', null, null, '0', 1, 2, now(), now());
insert into `t_menu` values (140300, 140000, 'setting.externalLink', '/setting/extlink', 'setting/extlink/View', 'menu:view', null, '0', 1, 5, now(), now());
insert into `t_menu` values (140400, 140000, 'setting.yarnQueue', '/setting/yarn-queue', 'setting/yarn-queue/View', 'menu:view', null, '0', 1, 6, now(), now());
insert into `t_menu` values (140101, 140100, 'setting view', null, null, 'setting:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (140102, 140100, 'setting update', null, null, 'setting:update', null, '1', 1, null, now(), now());

insert into `t_menu` values (140301, 140300, 'link view', null, null, 'externalLink:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (140302, 140300, 'link create', null, null, 'externalLink:create', null, '1', 1, null, now(), now());
insert into `t_menu` values (140303, 140300, 'link update', null, null, 'externalLink:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (140304, 140300, 'link delete', null, null, 'externalLink:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (140401, 140400, 'yarn queue add', null, null, 'yarnQueue:create', '', '1', 0, null, now(), now());
insert into `t_menu` values (140402, 140400, 'yarn queue edit', null, null, 'yarnQueue:update', '', '1', 0, null, now(), now());
insert into `t_menu` values (140403, 140400, 'yarn queue delete', null, null, 'yarnQueue:delete', '', '1', 0, null, now(), now());

insert into `t_menu` values (150100, 150000, 'system.user', '/system/user', 'system/user/View', null, null, '0', 1, 1, now(), now());
insert into `t_menu` values (150200, 150000, 'system.role', '/system/role', 'system/role/View', null, null, '0', 1, 2, now(), now());
insert into `t_menu` values (150300, 150000, 'system.token', '/system/token', 'system/token/View', null, null, '0', 1, 1, now(), now());
insert into `t_menu` values (150400, 150000, 'system.team', '/system/team', 'system/team/View', null, null, '0', 1, 2, now(), now());
insert into `t_menu` values (150500, 150000, 'system.member', '/system/member', 'system/member/View', null, null, '0', 1, 2, now(), now());

insert into `t_menu` values (150101, 150100, 'user view', null, null, 'user:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (150102, 150100, 'user add', null, null, 'user:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (150103, 150100, 'user update', null, null, 'user:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (150104, 150100, 'user delete', null, null, 'user:delete', null, '1', 1, null, now(), now());
insert into `t_menu` values (150105, 150100, 'user reset', null, null, 'user:reset', null, '1', 1, null, now(), now());
insert into `t_menu` values (150106, 150100, 'user types', null, null, 'user:types', null, '1', 1, null, now(), now());

insert into `t_menu` values (150201, 150200, 'role view', null, null, 'role:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (150202, 150200, 'role add', null, null, 'role:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (150203, 150200, 'role update', null, null, 'role:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (150204, 150200, 'role delete', null, null, 'role:delete', null, '1', 1, null, now(), now());

insert into `t_menu` values (150301, 150300, 'token view', null, null, 'token:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (150302, 150300, 'token add', null, null, 'token:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (150303, 150300, 'token delete', null, null, 'token:delete', null, '1', 1, null, now(), now());

insert into `t_menu` values (150401, 150400, 'team view', null, null, 'team:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (150402, 150400, 'team add', null, null, 'team:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (150403, 150400, 'team update', null, null, 'team:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (150404, 150400, 'team delete', null, null, 'team:delete', null, '1', 1, null, now(), now());

insert into `t_menu` values (150501, 150500, 'member view', null, null, 'member:view', null, '1', 1, null, now(), now());
insert into `t_menu` values (150502, 150500, 'member add', null, null, 'member:add', null, '1', 1, null, now(), now());
insert into `t_menu` values (150503, 150500, 'member update', null, null, 'member:update', null, '1', 1, null, now(), now());
insert into `t_menu` values (150504, 150500, 'member delete', null, null, 'member:delete', null, '1', 1, null, now(), now());

-- ----------------------------
-- Records of t_setting
-- ----------------------------
delete from `t_menu`
where `type` = '1'
   or `path` in ('/system/menu', '/system/role', '/system/team', '/system/member');

insert into `t_setting` values (1, 'streampark.maven.settings', null, 'Maven Settings File Path', 'Maven Settings.xml full path', 1);
insert into `t_setting` values (2, 'streampark.maven.central.repository', null, 'Maven Central Repository', 'Maven private server address', 1);
insert into `t_setting` values (3, 'streampark.maven.auth.user', null, 'Maven Central Repository Auth User', 'Maven private server authentication username', 1);
insert into `t_setting` values (4, 'streampark.maven.auth.password', null, 'Maven Central Repository Auth Password', 'Maven private server authentication password', 1);
insert into `t_setting` values (5, 'alert.email.host', null, 'Alert Email Smtp Host', 'Alert Mailbox Smtp Host', 1);
insert into `t_setting` values (6, 'alert.email.port', null, 'Alert Email Smtp Port', 'Smtp Port of the alarm mailbox', 1);
insert into `t_setting` values (7, 'alert.email.from', null, 'Alert  Email From', 'Email to send alerts', 1);
insert into `t_setting` values (8, 'alert.email.userName', null, 'Alert  Email User', 'Authentication username used to send alert emails', 1);
insert into `t_setting` values (9, 'alert.email.password', null, 'Alert Email Password', 'Authentication password used to send alarm email', 1);
insert into `t_setting` values (10, 'alert.email.ssl', 'false', 'Alert Email Is SSL', 'Whether to enable SSL in the mailbox that sends the alert', 2);
insert into `t_setting` values (11, 'docker.register.address', null, 'Docker Register Address', 'Docker container service address', 1);
insert into `t_setting` values (12, 'docker.register.user', null, 'Docker Register User', 'Docker container service authentication username', 1);
insert into `t_setting` values (13, 'docker.register.password', null, 'Docker Register Password', 'Docker container service authentication password', 1);
insert into `t_setting` values (14, 'docker.register.namespace', null, 'Docker namespace', 'Namespace for docker image used in docker building env and target image register', 1);
insert into `t_setting` values (15, 'ingress.mode.default', null, 'Ingress domain address', 'Automatically generate an nginx-based ingress by passing in a domain name', 1);

-- ----------------------------
-- Records of t_user
-- ----------------------------
insert into `t_user` values (100000, 'admin', '', 'rh8b1ojwog777yrg0daesf04gk', '2513f3748847298ea324dffbf67fe68681dd92315bda830065facd8efe08f54f', null, 1, 0, 100000, '1', now(), now(),null,'0',null,null);
insert into `t_user` values (100001, 'test1', '', 'rh8b1ojwog777yrg0daesf04gk', '2513f3748847298ea324dffbf67fe68681dd92315bda830065facd8efe08f54f', null, 2, 0, 100000, '1', now(), now(),null,'0',null,null);
insert into `t_user` values (100002, 'test2', '', 'rh8b1ojwog777yrg0daesf04gk', '2513f3748847298ea324dffbf67fe68681dd92315bda830065facd8efe08f54f', null, 2, 0, 100000, '1', now(), now(),null,'0',null,null);
insert into `t_user` values (100003, 'test3', '', 'rh8b1ojwog777yrg0daesf04gk', '2513f3748847298ea324dffbf67fe68681dd92315bda830065facd8efe08f54f', null, 2, 0, 100001, '1', now(), now(),null,'0',null,null);
