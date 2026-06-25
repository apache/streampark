/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
export default {
  title: 'Flink Home',
  conf: 'Flink Conf',
  sync: 'Sync Conf',
  edit: 'Edit Flink Home',
  delete: 'Are you sure delete this flink home ?',
  flinkName: 'Flink Name',
  flinkNamePlaceholder: 'Please enter flink name',
  flinkHome: 'Flink Home',
  flinkHomePlaceholder: 'Please enter flink home',
  description: 'Description',
  descriptionPlaceholder: 'Please enter description',
  flinkVersion: 'Flink Version',
  searchByName: 'Search by flink name',
  operateMessage: {
    flinkNameTips: 'The flink name, e.g: flink-1.12',
    flinkNameIsRepeated: 'Flink name already exists',
    flinkNameIsRequired: 'Flink name is required',
    flinkHomeTips:
      'The absolute path of FLINK_HOME. StreamPark Console can keep running on JDK 8; configure JAVA_HOME (JDK 11+) in $FLINK_HOME/conf/flink-env.sh for Flink 2.x runtime. See script/FLINK_JDK_GUIDE.md.',
    flinkHomeIsRequired: 'Flink home is required',
    flinkHomePathIsInvalid: 'Flink home path is invalid',
    flinkDistNotFound: 'Can not find flink-dist in FLINK_HOME/lib',
    flinkDistIsRepeated: 'Found multiple flink-dist in FLINK_HOME/lib, must be only one',
    createFlinkHomeSuccessful: ' create successful!',
    updateFlinkHomeSuccessful: ' update successful!',
  },
};
