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
<template>
  <PageWrapper
    contentFullHeight
    fixed-height
    content-background
    contentClass="px-10px"
    class="sp-project"
  >
    <a-card class="header" :bordered="false">
      <template #title>
        <div class="flex items-center justify-between">
          <div>
            <a-radio-group v-model:value="queryParams.buildState">
              <a-radio-button
                v-for="item in buttonList"
                @click="handleQuery(item.key)"
                :value="item.key"
                :key="item.key"
                >{{ item.label }}</a-radio-button
              >
            </a-radio-group>
            <a-input
              v-model:value="queryParams.name"
              allow-clear
              @search="() => reload()"
              :placeholder="t('flink.project.searchPlaceholder')"
              class="search-input"
            />
          </div>
          <div class="operate bg-white" v-auth="'project:create'">
            <a-button id="e2e-project-create-btn" type="primary" class="w-full" @click="onAdd">
              <Icon icon="ant-design:plus-outlined" />
              {{ t('common.add') }}
            </a-button>
          </div>
        </div>
      </template>
    </a-card>
    <BasicTable @register="registerTable">
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'branches'">
          <a-tag
            v-if="record.refs.startsWith('refs/tags/') > 0"
            color="green"
            style="border-radius: 4px"
          >
            {{ record.refs.replace('refs/tags/', '') }}
          </a-tag>
          <a-tag v-else color="blue" style="border-radius: 4px">
            {{ record.refs.replace('refs/heads/', '') }}
          </a-tag>
        </template>
        <template v-if="column.dataIndex === 'type'">
          <a-badge
            class="build-badge"
            v-if="record.buildState == BuildStateEnum.NEED_REBUILD"
            count="NEW"
            title="this project has changed, need rebuild"
          >
            <svg-icon class="avatar" :name="projectMap[record.type]" :size="20" />
          </a-badge>
          <svg-icon v-else class="avatar" :name="projectMap[record.type]" :size="20" />
          {{ projectMap[record.type].toUpperCase() }}
        </template>
        <template v-if="column.dataIndex === 'buildState'">
          <a-badge
            status="success"
            title="installing"
            class="mr-10px"
            v-if="record.buildState == BuildStateEnum.BUILDING"
          />
          <a-tag
            class="bold-tag"
            :color="buildStateMap[record.buildState]?.color || '#f5222d'"
            :class="buildStateMap[record.buildState]?.className"
          >
            {{ buildStateMap[record.buildState]?.label || t('flink.project.projectStatus.failed') }}
          </a-tag>
        </template>
        <template v-if="column.dataIndex === 'action'">
          <TableAction
            :actions="[
              {
                icon: 'ant-design:code-outlined',
                auth: 'project:build',
                tooltip: t('flink.project.operationTips.seeBuildLog'),
                onClick: handleViewLog.bind(null, record),
              },
              {
                class: 'e2e-project-build-btn',
                icon: 'ant-design:thunderbolt-outlined',
                auth: 'project:build',
                tooltip: t('flink.project.operationTips.buildProject'),
                ifShow: record.buildState !== BuildStateEnum.BUILDING,
                popConfirm: {
                  okButtonProps: {
                    class: 'e2e-project-build-confirm',
                  },
                  title: t('flink.project.operationTips.buildProjectMessage'),
                  placement: 'left',
                  confirm: handleBuild.bind(null, record),
                },
              },
              {
                class: 'e2e-project-edit-btn',
                icon: 'clarity:note-edit-line',
                ifShow: record.buildState !== BuildStateEnum.BUILDING,
                auth: 'project:update',
                tooltip: t('common.edit'),
                onClick: handleEdit.bind(null, record),
              },
              {
                class: 'e2e-project-delete-btn',
                icon: 'ant-design:delete-outlined',
                color: 'error',
                tooltip: t('common.delText'),
                ifShow: record.buildState !== BuildStateEnum.BUILDING,
                auth: 'project:delete',
                popConfirm: {
                  okButtonProps: {
                    class: 'e2e-project-delete-confirm',
                  },
                  title: t('flink.project.operationTips.deleteProjectMessage'),
                  placement: 'left',
                  confirm: handleDelete.bind(null, record),
                },
              },
            ]"
          />
        </template>
      </template>
    </BasicTable>
    <LogModal @register="registerLogModal" />
  </PageWrapper>
</template>
<script lang="ts">
  import { defineComponent, nextTick, onUnmounted, reactive, ref, watch } from 'vue';

  import { PageWrapper } from '/@/components/Page';
  import { statusList } from './project.data';
  import { RadioGroup, Radio, Card, Tag, Badge } from 'ant-design-vue';
  import { buildStateMap } from './project.data';
  import { buildProject, deleteProject, getList } from '/@/api/resource/project';
  import { ProjectRecord } from '/@/api/resource/project/model/projectModel';
  import Icon, { SvgIcon } from '/@/components/Icon';
  import { useGo } from '/@/hooks/web/usePage';
  import { useTimeoutFn } from '@vueuse/core';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useModal } from '/@/components/Modal';
  import LogModal from './components/LogModal.vue';
  import { useUserStoreWithOut } from '/@/store/modules/user';
  import { BasicTable, TableAction, useTable } from '/@/components/Table';
  import { BuildStateEnum } from '/@/enums/flinkEnum';
  import { buildUUID } from '/@/utils/uuid';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { useRouter } from 'vue-router';
  import { ProjectTypeEnum } from '/@/enums/projectEnum';

  export default defineComponent({
    name: 'ProjectView',
    components: {
      PageWrapper,
      ARadioGroup: RadioGroup,
      ARadioButton: Radio.Button,
      ACard: Card,
      ATag: Tag,
      ABadge: Badge,
      Icon,
      SvgIcon,
      LogModal,
      TableAction,
      BasicTable,
    },
    setup() {
      const go = useGo();
      const userStore = useUserStoreWithOut();
      const { t } = useI18n();
      const router = useRouter();
      const { Swal, createMessage } = useMessage();
      const [registerLogModal, { openModal: openLogModal }] = useModal();
      const buttonList = reactive(statusList);
      const loading = ref(false);
      const buildState = ref('');
      const searchValue = ref('');
      const pageInfo = reactive({
        currentPage: 1,
        pageSize: 10,
        total: 0,
      });

      const queryParams = reactive<{ buildState: string; name?: string }>({
        buildState: '',
        name: '',
      });

      let projectDataSource = ref<Array<ProjectRecord>>([]);
      const projectMap = {
        [ProjectTypeEnum.FLINK]: 'flink',
        [ProjectTypeEnum.SPARK]: 'spark',
      };

      function onAdd() {
        go(`/project/add`);
      }

      async function getRequestList(params: Recordable) {
        return getList({
          ...queryParams,
          pageNum: params.pageNum,
          pageSize: params.pageSize,
        });
      }

      const [registerTable, { reload, getLoading, setPagination }] = useTable({
        rowKey: 'id',
        api: getRequestList,
        columns: [
          { dataIndex: 'name', title: t('flink.project.form.projectName') },
          { dataIndex: 'type', title: t('flink.project.form.projectType') },
          { dataIndex: 'branches', title: t('flink.project.form.branches') },
          { dataIndex: 'lastBuild', title: t('flink.project.form.lastBuild') },
          { dataIndex: 'buildState', title: t('flink.project.form.buildState') },
        ],
        useSearchForm: false,
        striped: false,
        canResize: false,
        bordered: false,
        showIndexColumn: false,
        actionColumn: {
          width: 200,
          title: t('component.table.operation'),
          dataIndex: 'action',
        },
      });
      function handlePageDataReload(polling = false) {
        nextTick(() => {
          reload({ polling });
        });
      }
      async function handleBuild(record: ProjectRecord) {
        try {
          await buildProject({
            id: record.id,
            socketId: buildUUID(),
          });
          Swal.fire({
            icon: 'success',
            title: t('flink.project.operationTips.projectIsbuildingMessage'),
            showConfirmButton: false,
            timer: 2000,
          });
        } catch (e) {
          createMessage.error(t('flink.project.operationTips.projectIsbuildFailedMessage'));
        }
      }
      const handleEdit = function (record: ProjectRecord) {
        router.push({ path: '/project/edit', query: { id: record.id } });
      };
      async function handleDelete(record: ProjectRecord) {
        try {
          const res = await deleteProject({ id: record.id });
          if (res.data) {
            Swal.fire({
              icon: 'success',
              title: t('flink.project.operationTips.deleteProjectSuccessMessage'),
              showConfirmButton: false,
              timer: 2000,
            });
            reload();
          } else {
            Swal.fire(
              'Failed',
              t('flink.project.operationTips.deleteProjectFailedDetailMessage'),
              'error',
            );
          }
        } catch (e) {
          createMessage.error(t('flink.project.operationTips.deleteProjectFailedMessage'));
        }
      }

      const handleQuery = function (val: string | undefined) {
        setPagination({ current: 1 });
        queryParams.buildState = val!;
        reload();
      };

      const { start, stop } = useTimeoutFn(() => {
        if (!getLoading()) {
          handlePageDataReload(true);
        }
        start();
      }, 2000);

      /* View log */
      function handleViewLog(value: Recordable) {
        openLogModal(true, { project: value });
      }
      // teamId update
      watch(
        () => userStore.getTeamId,
        (val) => {
          if (val) {
            setPagination({ current: 1 });
            reload();
          }
        },
      );

      onUnmounted(() => {
        stop();
      });

      return {
        t,
        BuildStateEnum,
        buildStateMap,
        registerTable,
        reload,
        searchValue,
        pageInfo,
        buildState,
        buttonList,
        handleQuery,
        queryParams,
        projectDataSource,
        loading,
        onAdd,
        registerLogModal,
        handleViewLog,
        handleBuild,
        handleEdit,
        handleDelete,
        projectMap,
      };
    },
  });
</script>
<style lang="less">
  .sp-project {
    .search-input {
      width: 272px;
      margin-left: 16px;
    }
    .add-btn {
      margin-left: 30px;
    }
    .ant-card-head {
      padding: 0 2px !important;
    }
  }
  .status-processing-running {
    animation: running-color 800ms ease-out infinite alternate;
  }
</style>
