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

export const APP_LIST_POLL_IDLE_MS = 5000;

export const APP_LIST_POLL_ACTIVE_MS = 2000;

export type AppOptionApps = {
  starting: Map<unknown, unknown>;
  stopping: Map<unknown, unknown>;
  release: Map<unknown, unknown>;
  savepointing?: Map<unknown, unknown>;
};

export function getAppListPollInterval(optionApps: AppOptionApps): number {
  const active =
    optionApps.starting.size > 0 ||
    optionApps.stopping.size > 0 ||
    optionApps.release.size > 0 ||
    (optionApps.savepointing?.size ?? 0) > 0;
  return active ? APP_LIST_POLL_ACTIVE_MS : APP_LIST_POLL_IDLE_MS;
}
