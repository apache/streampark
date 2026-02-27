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

-- ----------------------------
-- Table of t_access_token definition
-- ----------------------------
drop table if exists `t_access_token`;
create table `t_access_token` (
`id` int not null auto_increment comment 'key',
`user_id` bigint,
`token` varchar(1024) character set utf8mb4 collate utf8mb4_general_ci default null comment 'token',
`expire_time` datetime default null comment 'expiration',
`description` varchar(512) character set utf8mb4 collate utf8mb4_general_ci default null comment 'description',
`status` tinyint default null comment '1:enable,0:disable',
`create_time` datetime default null comment 'create time',
`modify_time` datetime default null comment 'modify time',
primary key (`id`)
) engine=innodb auto_increment=100000 default charset=utf8mb4 collate=utf8mb4_general_ci;

-- ----------------------------
-- Table of t_flink_cluster
-- ----------------------------
alter table `t_flink_cluster` change column `address` `address` varchar(255) character set utf8mb4 not null,
add column  `cluster_id` varchar(255) default null comment 'clusterid of session mode(yarn-session:application-id,k8s-session:cluster-id)',
add column  `options` text comment 'json form of parameter collection ',
add column  `yarn_queue` varchar(100) default null comment 'the yarn queue where the task is located',
add column  `execution_mode` tinyint not null default '1' comment 'k8s execution session mode(1:remote,3:yarn-session,5:kubernetes-session)',
add column  `version_id` bigint not null comment 'flink version id',
add column  `k8s_namespace` varchar(255) default 'default' comment 'k8s namespace',
add column  `service_account` varchar(50) default null comment 'k8s service account',
add column  `user_id` bigint default null,
add column  `flink_image` varchar(255) default null comment 'flink image',
add column  `dynamic_options` text comment 'dynamic parameters',
add column  `k8s_rest_exposed_type` tinyint default '2' comment 'k8s export(0:loadbalancer,1:clusterip,2:nodeport)',
add column  `k8s_hadoop_integration` tinyint default '0',
add column  `flame_graph` tinyint default '0' comment 'flameGraph enable，default disable',
add column  `k8s_conf` varchar(255) default null comment 'the path where the k 8 s configuration file is located',
add column  `resolve_order` int(11) default null,
add column  `exception` text comment 'exception information',
add column  `cluster_state` tinyint default '0' comment 'cluster status (0: created but not started, 1: started, 2: stopped)',
add unique index `inx_name`(`cluster_name`),
add unique index `inx_cluster`(`cluster_id`, `address`, `execution_mode`);

insert into `t_menu` values (100038, 100000, 'Token Management', '/system/token', 'system/token/Token', 'token:view', 'lock', '0', '1', 1.0, now(), now());
insert into `t_menu` values (100039, 100038, 'add', null, null, 'token:add', null, '1', '1', null, now(), null);
insert into `t_menu` values (100040, 100038, 'delete', null, null, 'token:delete', null, '1', '1', null, now(), null);
insert into `t_menu` values (100041, 100013, 'Add Cluster', '/flink/setting/add_cluster', 'flink/setting/AddCluster', 'cluster:create', '', '0', '0', null, now(), now());
insert into `t_menu` values (100042, 100013, 'Edit Cluster', '/flink/setting/edit_cluster', 'flink/setting/EditCluster', 'cluster:update', '', '0', '0', null, now(), now());

insert into `t_role_menu` values (100057, 100000, 100038);
insert into `t_role_menu` values (100058, 100000, 100039);
insert into `t_role_menu` values (100059, 100000, 100040);
insert into `t_role_menu` values (100060, 100000, 100041);
insert into `t_role_menu` values (100061, 100000, 100042);

set foreign_key_checks = 1;
