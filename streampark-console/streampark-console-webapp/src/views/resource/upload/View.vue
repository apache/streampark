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
  <PageWrapper contentFullHeight fixed-height>
    <BasicTable @register="registerTable" class="flex flex-col">
      <template #form-formFooter>
        <Col :span="4" :offset="14" class="text-right">
          <a-button
            id="e2e-upload-create-btn"
            type="primary"
            @click="handleCreate"
            v-auth="'resource:add'"
          >
            <Icon icon="ant-design:plus-outlined" />
            {{ t('common.add') }}
          </a-button>
        </Col>
      </template>
      <template #bodyCell="{ column, record }">
        <template v-if="column.dataIndex === 'engineType'">
          <span v-if="record.engineType === EngineTypeEnum.FLINK">
            <SvgIcon name="flink" /> Apache Flink
          </span>
          <span v-if="record.engineType === EngineTypeEnum.SPARK">
            <SvgIcon name="spark2" /> Apache Spark
          </span>
        </template>
        <template v-if="column.dataIndex === 'resourceType'">
          <Tag color="processing" v-if="record.resourceType === ResourceTypeEnum.APP">
            <span v-if="record.engineType === EngineTypeEnum.FLINK">
              <img :src="flinkAppSvg" class="svg-icon" alt="Flink App" />
              Flink App
            </span>
            <span v-else>
              <img :src="sparkAppSvg" class="svg-icon" alt="Spark App" />
              Spark App
            </span>
          </Tag>

          <Tag color="processing" v-if="record.resourceType === ResourceTypeEnum.CONNECTOR">
            <template #icon>
              <img :src="connectorSvg" class="svg-icon" alt="Connector" />
            </template>
            Connector
          </Tag>

          <Tag color="processing" v-if="record.resourceType === ResourceTypeEnum.UDXF">
            <template #icon>
              <img :src="udxfSvg" class="svg-icon" alt="UDXF" />
            </template>
            UDXF
          </Tag>

          <Tag color="processing" v-if="record.resourceType === ResourceTypeEnum.JAR_LIBRARY">
            <template #icon>
              <img :src="jarSvg" class="svg-icon" alt="Jar Library" />
            </template>
            Jar Library
          </Tag>

          <Tag color="processing" v-if="record.resourceType === ResourceTypeEnum.GROUP">
            <template #icon>
              <img :src="groupSvg" class="svg-icon" alt="GROUP" />
            </template>
            GROUP
          </Tag>
        </template>
        <template v-if="column.dataIndex === 'action'">
          <TableAction
            :actions="[
              {
                class: 'e2e-upload-edit-btn',
                icon: 'clarity:note-edit-line',
                auth: 'resource:update',
                tooltip: t('flink.resource.modifyResource'),
                onClick: handleEdit.bind(null, record),
              },
              {
                class: 'e2e-upload-delete-btn',
                icon: 'ant-design:delete-outlined',
                color: 'error',
                tooltip: t('flink.resource.deleteResource'),
                auth: 'resource:delete',
                popConfirm: {
                  okButtonProps: {
                    class: 'e2e-upload-delete-confirm',
                  },
                  title: t('flink.resource.deletePopConfirm'),
                  confirm: handleDelete.bind(null, record),
                },
              },
            ]"
          />
        </template>
      </template>
    </BasicTable>
    <UploadDrawer
      :teamResource="teamResource"
      @register="registerDrawer"
      @success="handleSuccess"
    />
  </PageWrapper>
</template>
<script lang="ts">
  export default defineComponent({
    name: 'Resource',
  });
</script>

<script lang="ts" setup>
  import { defineComponent, onMounted, ref } from 'vue';
  import { BasicTable, useTable, TableAction, SorterResult } from '/@/components/Table';
  import UploadDrawer from './components/UploadDrawer.vue';
  import { useDrawer } from '/@/components/Drawer';
  import { columns, searchFormSchema, EngineTypeEnum, ResourceTypeEnum } from './upload.data';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { useI18n } from '/@/hooks/web/useI18n';
  import Icon from '/@/components/Icon';
  import {
    fetchResourceDelete,
    fetchResourceList,
    fetchTeamResource,
  } from '/@/api/resource/upload';
  import { Col, Tag } from 'ant-design-vue';
  import SvgIcon from '/@/components/Icon/src/SvgIcon.vue';

  import flinkAppSvg from '/@/assets/icons/flink2.svg';
  import sparkAppSvg from '/@/assets/icons/spark.svg';

  import connectorSvg from '/@/assets/icons/connector.svg';
  import udxfSvg from '/@/assets/icons/fx.svg';
  import jarSvg from '/@/assets/icons/jar.svg';
  import groupSvg from '/@/assets/icons/group.svg';
  import { PageWrapper } from '/@/components/Page';

  const teamResource = ref<Array<any>>([]);
  const [registerDrawer, { openDrawer }] = useDrawer();
  const { createMessage } = useMessage();
  const { t } = useI18n();
  const [registerTable, { reload }] = useTable({
    api: fetchResourceList,
    columns,
    formConfig: {
      schemas: searchFormSchema,
      rowProps: {
        gutter: 14,
      },
      submitOnChange: true,
      showActionButtonGroup: false,
    },
    sortFn: (sortInfo: SorterResult) => {
      const { field, order } = sortInfo;
      if (field && order) {
        return {
          // The sort field passed to the backend you
          sortField: field,
          // Sorting method passed to the background asc/desc
          sortOrder: order === 'ascend' ? 'asc' : 'desc',
        };
      } else {
        return {};
      }
    },
    rowKey: 'id',
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

  function handleCreate() {
    openDrawer(true, {
      isUpdate: false,
    });
  }

  function handleEdit(record: Recordable) {
    openDrawer(true, {
      record,
      isUpdate: true,
    });
  }

  /* Delete the resource */
  async function handleDelete(record: Recordable) {
    const { data } = await fetchResourceDelete({
      id: record.id,
      teamId: record.teamId,
      resourceName: record.resourceName,
    });
    if (data.status === 'success') {
      createMessage.success(t('flink.resource.deleteResource') + t('flink.resource.success'));
      await reload();
      updateTeamResource();
    } else {
      createMessage.error(t('flink.resource.deleteResource') + t('flink.resource.fail'));
    }
  }

  function handleSuccess(isUpdate: boolean) {
    createMessage.success(
      `${isUpdate ? t('common.edit') : t('flink.resource.add')}${t('flink.resource.success')}`,
    );
    reload();
    updateTeamResource();
  }

  function updateTeamResource() {
    /* Get team dependencies */
    fetchTeamResource({}).then((res) => {
      teamResource.value = res;
    });
  }

  onMounted(async () => {
    updateTeamResource();
  });
</script>

<style lang="less" scoped>
  .svg-icon {
    display: inline-block;
    width: 14px;
    height: 14px;

    .svg-connector {
      svg path {
        fill: #fff0f6 !important;
      }
    }
  }
</style>
