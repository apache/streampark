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
import { AxiosResponse } from 'axios';
import { AppListResponse, CancelParam, CreateParams, DashboardResponse } from './app.type';
import { Result } from '/#/axios';
import { ContentTypeEnum } from '/@/enums/httpEnum';
import { defHttp } from '/@/utils/http/axios';

enum APP_API {
  READ_CONF = '/flink/app/read_conf',
  UPDATE = '/flink/app/update',
  COPY = '/flink/app/copy',
  UPLOAD = '/flink/app/upload',
  K8S_LOG = '/flink/app/k8s_log',
  MAPPING = '/flink/app/mapping',
  YARN = '/flink/app/yarn',
  LIST = '/flink/app/list',
  GET = '/flink/app/get',
  DASHBOARD = '/flink/app/dashboard',
  MAIN = '/flink/app/main',
  NAME = '/flink/app/name',
  CHECK_NAME = '/flink/app/check/name',
  CANCEL = '/flink/app/cancel',
  ABORT = '/flink/app/abort',
  DELETE = '/flink/app/delete',
  DELETE_BAK = '/flink/app/delete/backup',
  CREATE = '/flink/app/create',
  CHECK_START = '/flink/app/check/start',
  START = '/flink/app/start',
  BACKUPS = '/flink/app/backups',
  ROLLBACK = '/flink/app/rollback',
  REVOKE = '/flink/app/revoke',
  OPTION_LOG = '/flink/app/opt_log',
  DELETE_LOG = '/flink/app/delete/opt_log',
  CHECK_SAVEPOINT_PATH = '/flink/app/check/savepoint_path',
  DOWNLOAD_ARTIFACT = '/flink/app/downloadArtifact',
}

/**
 * read configuration file
 * @returns Promise<any>
 */
export function fetchAppConf(params?: { config: any }) {
  return defHttp.post<any>({
    url: APP_API.READ_CONF,
    params,
  });
}

/**
 * Dashboard data
 * @returns Promise<DashboardResponse>
 */
export function fetchDashboard() {
  return defHttp.post<DashboardResponse>({
    url: APP_API.DASHBOARD,
    params: {},
  });
}

/**
 * Get app list data
 * @returns {Promise<AppListResponse>}
 */
export function fetchAppRecord(data): Promise<AppListResponse> {
  return defHttp.post({ url: APP_API.LIST, data });
}
/**
 * remove the app
 * @returns {Promise<boolean>}
 */
export function fetchAppRemove(id: string): Promise<boolean> {
  return defHttp.post({ url: APP_API.DELETE, data: { id } });
}

export function fetchRemoveBackup(id: string): Promise<boolean> {
  return defHttp.post({ url: APP_API.DELETE_BAK, data: { id } });
}

/**
 * get yarn address
 * @returns {Promise<string>}
 */
export function fetchYarn(): Promise<string> {
  return defHttp.post({ url: APP_API.YARN });
}

/**
 * get item
 * @returns {Promise<number>}
 */
export function fetchCheckName(data: { id?: string; jobName: string }): Promise<number> {
  return defHttp.post({ url: APP_API.CHECK_NAME, data });
}

export function fetchMain(data): Promise<string> {
  return defHttp.post({ url: APP_API.MAIN, data });
}
/**
 * upload
 * @param params
 * @returns {String} file path
 */
export function fetchUpload(params): Promise<string> {
  return defHttp.post<string>({
    url: APP_API.UPLOAD,
    params,
    headers: {
      'Content-Type': ContentTypeEnum.FORM_DATA,
    },
    timeout: 1000 * 60 * 10, // Uploading files timed out for 10 minutes
  });
}

/**
 * create
 * @param params Create parameters
 * @returns {Promise<AxiosResponse<Result>>} Whether the data creation was successful message: error message
 */
export function fetchCreate(data: CreateParams): Promise<AxiosResponse<Result>> {
  return defHttp.post({ url: APP_API.CREATE, data }, { isReturnNativeResponse: true });
}
/**
 * update
 * @param params update parameters
 * @returns {Promise<AxiosResponse<Result>>} Whether the data update is successful message: error message
 */
export function fetchUpdate(data): Promise<AxiosResponse<Result>> {
  return defHttp.post({ url: APP_API.UPDATE, data }, { isReturnNativeResponse: true });
}

/**
 * Get application information by id
 * @param params get parameters
 * @returns {Promise<AxiosResponse<Result>>} Whether the data get is successful message: error message
 */
export function fetchGet(data: { id: string }): Promise<any> {
  return defHttp.post({ url: APP_API.GET, data });
}

export function fetchBackUps(data) {
  return defHttp.post({ url: APP_API.BACKUPS, data });
}
export function fetchOptionLog(data) {
  return defHttp.post({ url: APP_API.OPTION_LOG, data });
}

export function fetchDeleteOperationLog(id: string) {
  return defHttp.post({ url: APP_API.DELETE_LOG, data: { id } });
}

/**
 * abort job
 * @returns
 * @param data
 */
export function fetchAbort(data: { id: string }): Promise<boolean> {
  return defHttp.post({ url: APP_API.ABORT, data });
}

export function fetchStart(data): Promise<AxiosResponse<Result>> {
  return defHttp.post({ url: APP_API.START, data }, { isReturnNativeResponse: true });
}

export function fetchCopy(data): Promise<AxiosResponse<any>> {
  return defHttp.post(
    { url: APP_API.COPY, data },
    { isReturnNativeResponse: true, errorMessageMode: 'none' },
  );
}
/**
 * mapping
 * @param params {id:string,appId:string,jobId:string}
 * @returns {Promise<Boolean>}
 */
export function fetchMapping(data): Promise<boolean> {
  return defHttp.post({ url: APP_API.MAPPING, data });
}

/**
 * log
 * @param params
 * @returns {Promise<AxiosResponse<any>>}
 */
export function fetchK8sStartLog(data): Promise<AxiosResponse<any>> {
  return defHttp.post({ url: APP_API.K8S_LOG, data }, { isReturnNativeResponse: true });
}
/**
 * SavepointPath
 * @param {Object} data app Id
 * @returns {Promise<AxiosResponse<Result>>}
 */
export function fetchCheckSavepointPath(data: {
  id?: string;
  savepointPath?: string;
}): Promise<AxiosResponse<Result>> {
  return defHttp.post(
    { url: APP_API.CHECK_SAVEPOINT_PATH, data },
    { isReturnNativeResponse: true },
  );
}

/**
 * Cancel
 * @param {CancelParam} data
 * @returns {Promise<Boolean>}
 */
export function fetchCancel(data: CancelParam): Promise<boolean> {
  return defHttp.post({ url: APP_API.CANCEL, data });
}

export function fetchName(data: { config: string }) {
  return defHttp.post({ url: APP_API.NAME, data });
}

export function fetchCheckStart(data): Promise<AxiosResponse<number>> {
  return defHttp.post({ url: APP_API.CHECK_START, data }, { isReturnNativeResponse: true });
}

/**
 * Download the latest successfully built application artifact.
 */
export function fetchDownloadArtifact(id: string): Promise<AxiosResponse<Blob>> {
  return defHttp.post(
    {
      url: APP_API.DOWNLOAD_ARTIFACT,
      data: { id },
      responseType: 'blob',
      timeout: 1000 * 60 * 10,
      validateStatus: (status) => (status >= 200 && status < 300) || status === 500,
    },
    { isReturnNativeResponse: true, errorMessageMode: 'none' },
  );
}
