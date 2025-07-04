<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<script lang="ts" setup name="AppView">
  import { PlusOutlined } from '@ant-design/icons-vue';
  import { nextTick, ref, onUnmounted, onMounted } from 'vue';
  import { useAppTableAction } from './hooks/useAppTableAction';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { JobTypeEnum, OptionStateEnum, ReleaseStateEnum } from '/@/enums/flinkEnum';
  import { useDebounceFn, useTimeoutFn } from '@vueuse/core';
  import {
    Form,
    Button,
    Select,
    Input,
    Tooltip,
    Badge,
    Tag,
    Popover,
    Row,
    Col,
  } from 'ant-design-vue';
  import { fetchAppRecord } from '/@/api/flink/app';
  import { useTable } from '/@/components/Table';
  import { PageWrapper } from '/@/components/Page';
  import { BasicTable, TableAction } from '/@/components/Table';
  import { AppListRecord } from '/@/api/flink/app.type';
  import { releaseTitleMap } from './data';
  import { useDrawer } from '/@/components/Drawer';
  import { useModal } from '/@/components/Modal';

  import StartApplicationModal from './components/AppView/StartApplicationModal.vue';
  import StopApplicationModal from './components/AppView/StopApplicationModal.vue';
  import LogModal from './components/AppView/LogModal.vue';
  import BuildDrawer from './components/AppView/BuildDrawer.vue';
  import AppDashboard from './components/AppView/AppDashboard.vue';
  import State, {
    buildStatusMap,
    optionStateMap,
    releaseStateMap,
    stateMap,
  } from './components/State';
  import { useSavepoint } from './hooks/useSavepoint';
  import { useAppTableColumns } from './hooks/useAppTableColumns';
  import AppTableResize from './components/AppResize.vue';
  import { useRouter } from 'vue-router';

  defineOptions({
    name: 'AppView',
  });
  const { t } = useI18n();
  const searchRef = ref<Recordable>({
    tags: undefined,
    owner: undefined,
    jobType: undefined,
  });
  const optionApps = {
    starting: new Map(),
    stopping: new Map(),
    release: new Map(),
    savepointing: new Map(),
  };

  const titleLenRef = ref({
    maxState: '',
    maxRelease: '',
    maxBuild: '',
  });

  const appDashboardRef = ref<any>();
  const noData = ref<boolean>();

  const currentTablePage = ref(1);
  const router = useRouter();
  const { onTableColumnResize, tableColumnWidth, getAppColumns } = useAppTableColumns();
  const { openSavepoint } = useSavepoint(handleOptionApp);
  const [registerStartModal, { openModal: openStartModal }] = useModal();
  const [registerStopModal, { openModal: openStopModal }] = useModal();
  const [registerLogModal, { openModal: openLogModal }] = useModal();
  const [registerBuildDrawer, { openDrawer: openBuildDrawer }] = useDrawer();

  const [registerTable, { reload, getLoading, setPagination }] = useTable({
    rowKey: 'id',
    api: fetchAppRecord,
    beforeFetch: (params) => {
      if (Reflect.has(params, 'state')) {
        if (params.state && params.state.length > 0) {
          params['stateArray'] = [...params.state];
        }
        delete params.state;
      }
      currentTablePage.value = params.pageNum;
      Object.assign(params, searchRef.value);
      sessionStorage.setItem('appPageNo', params.pageNum);
      return params;
    },
    afterFetch: (dataSource) => {
      const timestamp = new Date().getTime();
      noData.value = dataSource.length == 0;
      dataSource.forEach((x) => {
        x.expanded = [
          {
            jmMemory: x.jmMemory,
            tmMemory: x.tmMemory,
            totalTM: x.totalTM,
            totalSlot: x.totalSlot,
            availableSlot: x.availableSlot,
          },
        ];
        if (x['optionState'] === OptionStateEnum.NONE) {
          if (optionApps.starting.get(x.id)) {
            if (timestamp - optionApps.starting.get(x.id) > 2000 * 2) {
              optionApps.starting.delete(x.id);
            }
          }
          if (optionApps.stopping.get(x.id)) {
            if (timestamp - optionApps.stopping.get(x.id) > 2000) {
              optionApps.stopping.delete(x.id);
            }
          }
          if (optionApps.release.get(x.id)) {
            if (timestamp - optionApps.release.get(x.id) > 2000) {
              optionApps.release.delete(x.id);
            }
          }
          if (optionApps.savepointing.get(x.id)) {
            if (timestamp - optionApps.savepointing.get(x.id) > 2000) {
              optionApps.savepointing.delete(x.id);
            }
          }
        }
      });
      const stateLenMap = dataSource.reduce(
        (
          prev: {
            maxState: string;
            maxRelease: string;
            maxBuild: string;
          },
          cur: any,
        ) => {
          const { state, optionState, release, buildStatus } = cur;
          // state title len
          if (optionState === OptionStateEnum.NONE) {
            const stateStr = stateMap[state]?.title;
            if (stateStr && stateStr.length > prev.maxState.length) {
              prev.maxState = stateStr;
            }
          } else {
            const stateStr = optionStateMap[optionState]?.title;
            if (stateStr && stateStr.length > prev.maxState.length) {
              prev.maxState = stateStr;
            }
          }
          //release title len
          const releaseStr = releaseStateMap[release]?.title;
          if (releaseStr && releaseStr.length > prev.maxRelease.length) {
            prev.maxRelease = releaseStr;
          }
          //build title len
          const buildStr = buildStatusMap[buildStatus]?.title;
          if (buildStr && buildStr.length > prev.maxBuild.length) {
            prev.maxBuild = buildStr;
          }
          return prev;
        },
        {
          maxState: '',
          maxRelease: '',
          maxBuild: '',
        },
      );
      Object.assign(titleLenRef.value, stateLenMap);
      return dataSource;
    },
    fetchSetting: { listField: 'records' },
    immediate: true,
    canResize: false,
    showIndexColumn: false,
    showTableSetting: false,
    useSearchForm: false,
    tableSetting: { fullScreen: true, redo: false },
    actionColumn: {
      dataIndex: 'operation',
      title: t('component.table.operation'),
      width: 180,
    },
  });

  const { getTableActions, tagsOptions, users } = useAppTableAction(
    openStartModal,
    openStopModal,
    openSavepoint,
    openLogModal,
    openBuildDrawer,
    handlePageDataReload,
    optionApps,
  );

  /* view */
  function handleJobView(app: AppListRecord) {
    router.push({ path: '/flink/app/detail', query: { appId: app.id } });
  }

  /* Update options data */
  function handleOptionApp(data: {
    type: 'starting' | 'stopping' | 'release' | 'savepointing';
    key: any;
    value: any;
  }) {
    optionApps[data.type].set(data.key, data.value);
  }

  function handlePageDataReload(polling = false) {
    nextTick(() => {
      appDashboardRef.value?.handleDashboard(false);
      reload({ polling });
    });
  }

  const handleResetReload = useDebounceFn(() => {
    setPagination({
      current: 1,
    });
    reload();
  }, 500);

  const { start, stop } = useTimeoutFn(() => {
    if (!getLoading()) {
      handlePageDataReload(true);
    }
    start();
  }, 2000);

  onMounted(() => {
    // If there is a page, jump to the page number of the record
    const currentPage = sessionStorage.getItem('appPageNo');
    if (currentPage) {
      setPagination({
        current: Number(currentPage) || 1,
      });
      sessionStorage.removeItem('appPageNo');
    }
  });

  onUnmounted(() => {
    stop();
  });
</script>
<template>
  <PageWrapper contentFullHeight content-class="flex flex-col">
    <AppDashboard ref="appDashboardRef" />
    <div class="flex-1 bg-white mt-15px">
      <BasicTable
        @register="registerTable"
        :columns="getAppColumns"
        @resize-column="onTableColumnResize"
        class="app_list !px-0 table-searchbar flex-1 !px-0"
      >
        <template #tableTitle>
          <div class="flex justify-between" style="width: 100%">
            <Form name="appTableForm" :model="searchRef" layout="inline" class="flex-1 search-bar">
              <Row :gutter="4" class="w-full">
                <Col :span="5">
                  <Form.Item>
                    <Input
                      :placeholder="t('flink.app.searchName')"
                      allow-clear
                      v-model:value="searchRef.jobName"
                      @change="() => handleResetReload()"
                      @search="() => handleResetReload()"
                    />
                  </Form.Item>
                </Col>
                <Col :span="4">
                  <Form.Item>
                    <Select
                      :placeholder="t('flink.app.tags')"
                      show-search
                      allow-clear
                      v-model:value="searchRef.tags"
                      @change="() => handleResetReload()"
                      :options="
                        (tagsOptions || []).map((t: Recordable) => ({ label: t, value: t }))
                      "
                    />
                  </Form.Item>
                </Col>
                <Col :span="4">
                  <Form.Item>
                    <Select
                      :placeholder="t('flink.app.jobType')"
                      show-search
                      allow-clear
                      v-model:value="searchRef.jobType"
                      @change="() => handleResetReload()"
                      :options="[
                        { label: 'JAR', value: JobTypeEnum.JAR },
                        { label: 'SQL', value: JobTypeEnum.SQL },
                      ]"
                    />
                  </Form.Item>
                </Col>
                <Col :span="4">
                  <Form.Item>
                    <Select
                      :placeholder="t('flink.app.owner')"
                      show-search
                      allow-clear
                      v-model:value="searchRef.userId"
                      @change="() => handleResetReload()"
                      :options="
                        (users || []).map((u: Recordable) => ({
                          label: u.nickName || u.username,
                          value: u.userId,
                        }))
                      "
                    />
                  </Form.Item>
                </Col>
              </Row>
            </Form>
            <div v-auth="'app:create'">
              <Button
                id="e2e-flinkapp-create-btn"
                type="primary"
                @click="() => router.push({ path: '/flink/app/add' })"
              >
                <PlusOutlined />
                {{ t('common.add') }}
              </Button>
            </div>
          </div>
        </template>
        <template #bodyCell="{ column, record }">
          <template v-if="column.dataIndex === 'jobName'">
            <span class="app_type app_jar" v-if="record['jobType'] === JobTypeEnum.JAR"> JAR </span>
            <span class="app_type app_sql" v-if="record['jobType'] === JobTypeEnum.SQL"> SQL </span>
            <span class="link cursor-pointer" @click="handleJobView(record)">
              <Popover :title="t('common.detailText')">
                <template #content>
                  <div class="flex">
                    <span class="pr-6px font-bold">{{ t('flink.app.appName') }}:</span>
                    <div class="max-w-300px break-words">{{ record.jobName }}</div>
                  </div>
                  <div class="pt-2px">
                    <span class="pr-6px font-bold">{{ t('flink.app.jobType') }}:</span>
                    <Tag color="blue">
                      <span v-if="record['jobType'] == JobTypeEnum.JAR"> JAR </span>
                      <span v-if="record['jobType'] == JobTypeEnum.SQL"> SQL </span>
                    </Tag>
                  </div>
                  <div class="pt-2px flex">
                    <span class="pr-6px font-bold">{{ t('common.description') }}:</span>
                    <div class="max-w-300px break-words">{{ record.description }}</div>
                  </div>
                </template>
                {{ record.jobName }}
              </Popover>
            </span>

            <template v-if="record['jobType'] === JobTypeEnum.JAR">
              <Badge
                v-if="record.release === ReleaseStateEnum.NEED_CHECK"
                class="build-badge"
                count="NEW"
                :title="t('flink.app.view.recheck')"
              />
              <Badge
                v-else-if="record.release >= ReleaseStateEnum.RELEASING"
                class="build-badge"
                count="NEW"
                :title="t('flink.app.view.changed')"
              />
            </template>
          </template>
          <template v-if="column.dataIndex === 'tags'">
            <Tooltip v-if="record.tags" :title="record.tags">
              <span
                v-for="(tag, index) in record.tags.split(',')"
                :key="'tag-'.concat(index.toString())"
                class="pl-4px"
              >
                <Tag color="blue">{{ tag }}</Tag>
              </span>
            </Tooltip>
          </template>
          <template v-if="column.dataIndex === 'task'">
            <State option="task" :data="record" />
          </template>
          <template v-if="column.dataIndex === 'state'">
            <State option="state" :data="record" :maxTitle="titleLenRef.maxState" />
          </template>
          <template v-if="column.dataIndex === 'release'">
            <State
              option="release"
              :maxTitle="titleLenRef.maxRelease"
              :title="releaseTitleMap[record.release] || ''"
              :data="record"
            />
          </template>
          <template v-if="column.dataIndex === 'operation'">
            <TableAction v-bind="getTableActions(record, currentTablePage)" />
          </template>
        </template>
        <template #insertTable="{ tableContainer }">
          <AppTableResize
            v-if="!noData"
            :table-container="tableContainer"
            :resize-min="200"
            v-model:left="tableColumnWidth.jobName"
          />
        </template>
      </BasicTable>
    </div>
    <StartApplicationModal @register="registerStartModal" @update-option="handleOptionApp" />
    <StopApplicationModal @register="registerStopModal" @update-option="handleOptionApp" />
    <LogModal @register="registerLogModal" />
    <BuildDrawer @register="registerBuildDrawer" />
  </PageWrapper>
</template>
<style lang="less">
  @import url('./styles/View.less');
</style>
