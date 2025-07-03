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
-- Table of t_resource
-- ----------------------------
create sequence "public"."streampark_t_resource_id_seq"
    increment 1 start 10000 cache 1 minvalue 10000 maxvalue 9223372036854775807;

create table "public"."t_resource" (
"id" int8 not null default nextval('streampark_t_resource_id_seq'::regclass),
"resource_name" varchar(128) collate "pg_catalog"."default" not null,
"resource_type" int4,
"resource_path" varchar(255) default null,
"resource" text collate "pg_catalog"."default",
"engine_type" int4,
"main_class" varchar(255) collate "pg_catalog"."default",
"description" text collate "pg_catalog"."default" default null,
"creator_id" int8  not null,
"connector_required_options" text default null,
"connector_optional_options" text default null,
"team_id" int8  not null,
"create_time" timestamp(6),
"modify_time" timestamp(6)
);

comment on column "public"."t_resource"."id" is 'Resource id';
comment on column "public"."t_resource"."resource_name" is 'Resource name';
comment on column "public"."t_resource"."resource_type" is '0:app 1:common 2:connector 3:format 4:udf';
comment on column "public"."t_resource"."resource" is 'resource content, including jars and poms';
comment on column "public"."t_resource"."engine_type" is 'compute engine type, 0:apache flink 1:apache spark';
comment on column "public"."t_resource"."main_class" is 'The program main class';
comment on column "public"."t_resource"."description" is 'More detailed description of resource';
comment on column "public"."t_resource"."creator_id" is 'user id of creator';
comment on column "public"."t_resource"."team_id" is 'team id';
comment on column "public"."t_resource"."create_time" is 'creation time';
comment on column "public"."t_resource"."modify_time" is 'modify time';

alter table "public"."t_resource" add constraint "t_resource_pkey" primary key ("id");
create index "un_team_dname_inx" on "public"."t_resource" using btree (
    "team_id" "pg_catalog"."int8_ops" asc nulls last,
    "resource_name" collate "pg_catalog"."default" "pg_catalog"."text_ops" asc nulls last
    );

alter table "public"."t_flink_sql"
    add column "team_resource" varchar(64) default null;

alter table "public"."t_flink_cluster"
    add column "job_manager_url" varchar(150) collate "pg_catalog"."default",
add column "start_time" timestamp(6) collate "pg_catalog"."default",
add column "end_time" timestamp(6) collate "pg_catalog"."default",
add column "alert_id" int8 collate "pg_catalog"."default";

insert into "public"."t_menu" values (120400, 120000, 'menu.resource', '/flink/resource', 'flink/resource/View', null, 'apartment', '0', '1', 3, now(), now());
insert into "public"."t_menu" values (110401, 110400, 'add', null, null, 'token:add', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (110402, 110400, 'delete', null, null, 'token:delete', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (110403, 110400, 'view', null, null, 'token:view', null, '1', '1', null, now(), now());

insert into "public"."t_role_menu" (role_id, menu_id) values (100001, 120400);
insert into "public"."t_role_menu" (role_id, menu_id) values (100001, 120401);
insert into "public"."t_role_menu" (role_id, menu_id) values (100001, 120402);
insert into "public"."t_role_menu" (role_id, menu_id) values (100001, 120403);

insert into "public"."t_role_menu" (role_id, menu_id) values (100002, 120400);
insert into "public"."t_role_menu" (role_id, menu_id) values (100002, 120401);
insert into "public"."t_role_menu" (role_id, menu_id) values (100002, 120402);
insert into "public"."t_role_menu" (role_id, menu_id) values (100002, 120403);

-- add sso as login type
alter table "public"."t_user" alter column "password" TYPE varchar(64) collate "pg_catalog"."default";
comment on column "public"."t_user"."login_type" is 'login type 0:password 1:ldap 2:sso';

insert into "public"."t_menu" values (120500, 130000, 'setting.flinkGateway', '/setting/FlinkGateway', 'setting/FlinkGateway/index', null, 'apartment', '0', '1', 3, now(), now());
insert into "public"."t_menu" values (110501, 110500, 'add', null, null, 'gateway:add', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (110502, 110500, 'update', null, null, 'gateway:update', null, '1', '1', null, now(), now());
insert into "public"."t_menu" values (110503, 110500, 'delete', null, null, 'gateway:delete', null, '1', '1', null, now(), now());

insert into "public"."t_role_menu" (role_id, menu_id) values (100001, 120500);
insert into "public"."t_role_menu" (role_id, menu_id) values (100001, 120501);
insert into "public"."t_role_menu" (role_id, menu_id) values (100001, 120502);
insert into "public"."t_role_menu" (role_id, menu_id) values (100001, 120503);

insert into "public"."t_role_menu" (role_id, menu_id) values (100002, 120500);
insert into "public"."t_role_menu" (role_id, menu_id) values (100002, 120501);
insert into "public"."t_role_menu" (role_id, menu_id) values (100002, 120502);
insert into "public"."t_role_menu" (role_id, menu_id) values (100002, 120503);
