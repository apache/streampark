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

import type { AppStateEnum, OptionStateEnum } from '/@/enums/sparkEnum';

// dashboard
export interface DashboardResponse {
  numTasks: number;
  numStages: number;
  usedMemory: number;
  usedVCores: number;
  numCompletedTasks: number;
  runningApplication: number;
  numCompletedStages: number;
}

// The list of data
export interface AppListResponse {
  total: string;
  records: SparkApplication[];
}

export interface SparkApplication {
  createTime?: string;
  modifyTime?: string;
  id?: string;
  teamId?: string;
  jobType?: number;
  appType?: number;
  versionId?: string;
  appName?: string;
  deployMode?: number;
  resourceFrom?: number;
  projectId?: number;
  module?: string;
  mainClass?: string;
  jar?: string;
  jarCheckSum?: number;
  appProperties?: string;
  appArgs?: string;
  appId?: string;
  clusterId?: string;
  yarnQueue?: string;
  yarnQueueName?: string;
  yarnQueueLabel?: string;
  k8sMasterUrl?: string;
  k8sContainerImage?: string;
  k8sImagePullPolicy?: number;
  k8sServiceAccount?: number;
  k8sNamespace?: string;
  hadoopUser?: string;
  restartSize?: number;
  restartCount?: number;
  state?: AppStateEnum;
  options?: string;
  optionState?: OptionStateEnum;
  optionTime?: string;
  userId?: string;
  description?: string;
  tracking?: number;
  release?: number;
  build?: boolean;
  alertId?: number;
  startTime?: string;
  endTime?: string;
  duration?: number;
  tags?: string;
  driverCores?: string;
  driverMemory?: string;
  executorCores?: string;
  executorMemory?: string;
  executorMaxNums?: string;
  numTasks?: number;
  numCompletedTasks?: number;
  numStages?: number;
  numCompletedStages?: number;
  usedMemory?: number;
  usedVCores?: number;
  teamResource?: number;
  dependency?: string;
  sqlId?: number;
  sparkSql?: string;
  backUp?: boolean;
  restart?: boolean;
  config?: string;
  configId?: number;
  sparkVersion?: string;
  confPath?: string;
  format?: string;
  backUpDescription?: string;
  sparkRestUrl?: string;
  buildStatus?: number;
  appControl?: AppControl;
  canBeStart?: boolean;
  streamParkJob?: boolean;
}
interface AppControl {
  allowStart: boolean;
  allowStop: boolean;
  allowBuild: boolean;
  allowView: boolean;
}

// create Params
export interface CreateParams {
  jobType: number;
  deployMode: number;
  versionId: string;
  flinkSql: string;
  appType: number;
  config?: any;
  format?: any;
  jobName: string;
  tags: string;
  args?: any;
  dependency: string;
  options: string;
  cpMaxFailureInterval: number;
  cpFailureRateInterval: number;
  cpFailureAction: number;
  dynamicProperties: string;
  resolveOrder: number;
  restartSize: number;
  alertId: string;
  description: string;
  k8sNamespace?: any;
  clusterId: string;
  flinkClusterId: string;
  flinkImage?: any;
}
