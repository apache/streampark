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
<script lang="ts" setup name="FlinkClusterSetting">
  import { nextTick, onUnmounted } from 'vue';
  import { useTimeoutFn } from '@vueuse/core';
  import { SvgIcon } from '/@/components/Icon';
  import { Col, Tag } from 'ant-design-vue';
  import { ClusterStateEnum, DeployMode } from '/@/enums/flinkEnum';
  import { PlusOutlined } from '@ant-design/icons-vue';
  import { useMessage } from '/@/hooks/web/useMessage';
  import {
    fetchClusterRemove,
    fetchClusterShutdown,
    fetchClusterStart,
    fetchFlinkClusterPage,
  } from '/@/api/flink/flinkCluster';
  import { FlinkCluster } from '/@/api/flink/flinkCluster.type';
  import { useGo } from '/@/hooks/web/usePage';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { PageWrapper } from '/@/components/Page';
  import { BasicTable, TableAction, useTable } from '/@/components/Table';
  import State from './State';
  defineOptions({
    name: 'FlinkClusterSetting',
  });
  const deployModeMap = {
    [DeployMode.STANDALONE]: {
      color: '#2db7f5',
      text: 'standalone',
    },
    [DeployMode.YARN_SESSION]: {
      color: '#87d068',
      text: 'yarn session',
    },
    [DeployMode.KUBERNETES_SESSION]: {
      color: '#108ee9',
      text: 'k8s session',
    },
  };

  const go = useGo();
  const { t } = useI18n();
  const { Swal, createMessage } = useMessage();
  const [registerTable, { reload, getLoading }] = useTable({
    rowKey: 'id',
    api: fetchFlinkClusterPage,
    columns: [
      { dataIndex: 'clusterName', title: t('setting.flinkCluster.form.clusterName') },
      { dataIndex: 'deployMode', title: t('setting.flinkCluster.form.deployMode') },
      { dataIndex: 'address', title: t('setting.flinkCluster.form.address') },
      { dataIndex: 'clusterState', title: t('setting.flinkCluster.form.runState') },
      { dataIndex: 'description', title: t('setting.flinkHome.description') },
    ],

    formConfig: {
      schemas: [
        {
          field: 'clusterName',
          label: '',
          component: 'Input',
          componentProps: {
            placeholder: t('setting.flinkCluster.searchByName'),
            allowClear: true,
          },
          colProps: { span: 6 },
        },
      ],
      rowProps: {
        gutter: 14,
      },
      submitOnChange: true,
      showActionButtonGroup: false,
    },
    pagination: true,
    useSearchForm: true,
    showTableSetting: false,
    showIndexColumn: false,
    canResize: false,
    actionColumn: {
      width: 200,
      title: t('component.table.operation'),
      dataIndex: 'action',
    },
  });

  function handleIsStart(item) {
    return item.clusterState === ClusterStateEnum.RUNNING;
  }

  /* Go to edit cluster */
  function handleEditCluster(item: FlinkCluster) {
    go(`/flink/edit_cluster?clusterId=${item.id}`);
  }
  /* deploy */
  async function handleDeployCluster(item: FlinkCluster) {
    const hide = createMessage.loading(
      t('setting.flinkCluster.operateMessage.flinkClusterIsStarting'),
      0,
    );
    try {
      await fetchClusterStart(item.id);
      await Swal.fire({
        icon: 'success',
        title: t('setting.flinkCluster.operateMessage.flinkClusterHasStartedSuccessful'),
        showConfirmButton: false,
        timer: 2000,
      });
    } catch (error) {
      console.error(error);
    } finally {
      hide();
    }
  }
  /* delete */
  async function handleDelete(item: FlinkCluster) {
    await fetchClusterRemove(item.id);
    handlePageDataReload(true);
    createMessage.success('The current cluster is remove');
  }
  /* shutdown */
  async function handleShutdownCluster(item: FlinkCluster) {
    const hide = createMessage.loading('The current cluster is canceling', 0);
    try {
      await fetchClusterShutdown(item.id);
      createMessage.success('The current cluster is shutdown');
    } catch (error) {
      console.error(error);
    } finally {
      hide();
    }
  }

  function handlePageDataReload(polling = false) {
    nextTick(() => {
      reload({ polling });
    });
  }

  const { start, stop } = useTimeoutFn(() => {
    // Prevent another request from being initiated while the previous request is pending
    if (!getLoading()) {
      handlePageDataReload(true);
    }
    start();
  }, 1000 * 3);

  onUnmounted(() => {
    stop();
  });
</script>
<template>
  <PageWrapper contentFullHeight fixed-height content-class="flex flex-col">
    <BasicTable @register="registerTable" class="flex flex-col">
      <template #form-formFooter>
        <Col :span="5" :offset="13" class="text-right">
          <a-button
            id="e2e-flinkcluster-create-btn"
            type="primary"
            @click="() => go('/flink/add_cluster')"
          >
            <PlusOutlined />
            {{ t('common.add') }}
          </a-button>
        </Col>
      </template>
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'clusterName'">
          <svg-icon class="avatar" name="flink" :size="20" />
          {{ record.clusterName }}
        </template>
        <template v-if="column.dataIndex === 'deployMode'">
          <Tag
            v-if="deployModeMap[record.deployMode]"
            :color="deployModeMap[record.deployMode]?.color"
          >
            {{ deployModeMap[record.deployMode]?.text }}
          </Tag>
        </template>
        <template v-if="column.dataIndex === 'address'">
          <a
            :href="`/proxy/flink_cluster/${record.id}/`"
            target="_blank"
            v-if="
              record.deployMode === DeployMode.STANDALONE ||
              record.deployMode === DeployMode.YARN_SESSION ||
              record.deployMode === DeployMode.KUBERNETES_SESSION
            "
          >
            {{ record.address }}
          </a>
          <span v-else> - </span>
        </template>
        <template v-if="column.dataIndex === 'clusterState'">
          <State :data="{ clusterState: record.clusterState }" />
        </template>
        <template v-if="column.dataIndex === 'action'">
          <TableAction
            :actions="[
              {
                class: 'e2e-flinkcluster-edit-btn',
                icon: 'clarity:note-edit-line',
                auth: 'cluster:update',
                tooltip: t('setting.flinkCluster.edit'),
                disabled: handleIsStart(record),
                onClick: handleEditCluster.bind(null, record),
              },
              {
                class: 'e2e-flinkcluster-shutdown-btn',
                icon: 'ant-design:pause-circle-outlined',
                auth: 'cluster:create',
                ifShow: handleIsStart(record),
                disabled: record.deployMode === DeployMode.STANDALONE,
                tooltip: t('setting.flinkCluster.stop'),
                onClick: handleShutdownCluster.bind(null, record),
              },
              {
                class: 'e2e-flinkcluster-start-btn',
                icon: 'ant-design:play-circle-outlined',
                auth: 'cluster:create',
                ifShow: !handleIsStart(record),
                disabled: record.deployMode === DeployMode.STANDALONE,
                tooltip: t('setting.flinkCluster.start'),
                onClick: handleDeployCluster.bind(null, record),
              },
              {
                icon: 'ant-design:eye-outlined',
                auth: 'app:detail',
                disabled: !handleIsStart(record),
                tooltip: t('setting.flinkCluster.detail'),
                href: `/proxy/flink_cluster/${record.id}/`,
                target: '_blank',
              },
              {
                class: 'e2e-flinkcluster-delete-btn',
                icon: 'ant-design:delete-outlined',
                color: 'error',
                tooltip: t('common.delText'),
                popConfirm: {
                  okButtonProps: {
                    class: 'e2e-flinkcluster-delete-confirm',
                  },
                  title: t('setting.flinkCluster.delete'),
                  placement: 'left',
                  confirm: handleDelete.bind(null, record),
                },
              },
            ]"
          />
        </template>
      </template>
    </BasicTable>
  </PageWrapper>
</template>
<style lang="less" scoped>
  .cluster-card-list {
    background-color: @component-background;
    height: 100%;
  }
</style>
