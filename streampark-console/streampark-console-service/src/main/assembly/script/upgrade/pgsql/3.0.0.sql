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
-- lineage: t_setting
-- ----------------------------
insert into "public"."t_setting" values (16, 'lineage.gravitino.address', null, 'Gravitino Address', 'Base URL of the Gravitino server lineage events are reported to, e.g. http://host:8090', 1);
insert into "public"."t_setting" values (17, 'lineage.gravitino.token', null, 'Gravitino Auth Token', 'Bearer token forwarded to Gravitino, required once Gravitino oauth authentication is enabled', 1);
insert into "public"."t_setting" values (18, 'lineage.gravitino.namespace', null, 'Gravitino Lineage Namespace', 'OpenLineage job/dataset namespace StreamPark reports under', 1);
insert into "public"."t_setting" values (19, 'lineage.flink.native.listener.enable', 'true', 'Enable Flink Native OpenLineage Listener', 'Whether to also inject the official openlineage-flink job-status-changed-listener config; only takes effect once Gravitino Address is set', 2);

-- ----------------------------
-- lineage: t_flink_app
-- ----------------------------
alter table "public"."t_flink_app"
add column "lineage_enable" boolean default false;

-- Note: t_spark_app has no PostgreSQL schema definition anywhere in this repository (pre-existing
-- gap, confirmed absent from pgsql-schema.sql; not introduced by this change and not fixed here,
-- out of scope for a lineage feature migration). The corresponding "lineage_enable" column for
-- t_spark_app is added only in upgrade/mysql/3.0.0.sql; add it here once that gap is fixed.
