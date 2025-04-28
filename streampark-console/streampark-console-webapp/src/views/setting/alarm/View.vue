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
<script setup lang="ts" name="AlertSetting">
  import { onMounted, ref } from 'vue';
  import { List, Popconfirm, Tooltip, Card, Tag } from 'ant-design-vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { AlertTypeEnum } from '/@/enums/flinkEnum';
  import {
    EyeOutlined,
    ThunderboltOutlined,
    EditOutlined,
    DeleteOutlined,
    PlusOutlined,
  } from '@ant-design/icons-vue';
  import { useModal } from '/@/components/Modal';
  import { fetchAlertSetting, fetchSendAlert, fetchAlertDelete } from '/@/api/setting/alert';
  import { AlertSetting } from '/@/api/setting/types/alert.type';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { AlertTypeInfo, DetailModal, AlertModal } from './components';
  import { PageWrapper } from '/@/components/Page';
  defineOptions({
    name: 'AlertSetting',
  });
  const ListItem = List.Item;

  const { t } = useI18n();
  const { Swal, createMessage } = useMessage();
  const [registerAlertModal, { openModal: openAlertModal }] = useModal();
  const [registerAlertDetailModal, { openModal: openAlertDetailModal }] = useModal();
  const alerts = ref<AlertSetting[]>([]);

  /* Get alert configuration */
  async function getAlertSetting() {
    const res = await fetchAlertSetting();
    res.map((a) => (a.alertTypeTags = computeAlertType(a.alertType)));
    alerts.value = res;
  }
  const alertTypeMap = {
    [AlertTypeEnum.MAIL]: 'mail',
    [AlertTypeEnum.DINGTALK]: 'dingtalk',
    [AlertTypeEnum.WECOM]: 'wecom',
    [AlertTypeEnum.MESSAGE]: 'message',
    [AlertTypeEnum.LARK]: 'lark',
  };
  /* compute type */
  function computeAlertType(level: number) {
    if (level === null) level = 0;
    const result: string[] = [];
    while (level != 0) {
      // Get the lowest 1
      const code = level & -level;
      result.push(String(code));
      // Set the lowest position to 0
      level ^= code;
    }
    return result;
  }
  /* test connection */
  async function handleTestAlarm(item) {
    const hide = createMessage.loading(' Testing', 0);
    try {
      await fetchSendAlert({ id: item.id });
      Swal.fire({
        icon: 'success',
        title: 'Test Alert Config  successful!',
        showConfirmButton: false,
        timer: 2000,
      });
      getAlertSetting();
    } catch (error: any) {
      console.error(error);
    } finally {
      hide();
    }
  }
  /* Click the edit button */
  function handleEditAlertConf(item: AlertSetting) {
    let emailParams: Recordable<any> = {};
    let dingTalkParams: Recordable<any> = {};
    let weComParams: Recordable<any> = {};
    let larkParams: Recordable<any> = {};
    if (item.alertTypeTags?.includes('1')) {
      emailParams = JSON.parse(item.emailParams);
    }
    if (item.alertTypeTags?.includes(String(AlertTypeEnum.DINGTALK))) {
      dingTalkParams = JSON.parse(item.dingTalkParams);
      // dingtalkIsAtAll = dingTalkParams.isAtAll;
      // dingtalkSecretEnable = dingTalkParams.secretEnable;
    }
    if (item.alertTypeTags?.includes(String(AlertTypeEnum.WECOM))) {
      weComParams = JSON.parse(item.weComParams) as Recordable;
    }
    if (item.alertTypeTags?.includes(String(AlertTypeEnum.LARK))) {
      larkParams = JSON.parse(item.larkParams) as Recordable;
      // larkIsAtAll = larkParams.isAtAll;
      // larkSecretEnable = larkParams.secretEnable;
    }

    openAlertModal(true, {
      alertId: item.id,
      alertName: item.alertName,
      alertType: item.alertTypeTags,
      alertEmail: emailParams.contacts,
      alertDingURL: dingTalkParams.alertDingURL,
      dingtalkToken: dingTalkParams.token,
      dingtalkSecretToken: dingTalkParams.secretToken,
      alertDingUser: dingTalkParams.contacts,
      dingtalkIsAtAll: dingTalkParams.isAtAll,
      dingtalkSecretEnable: dingTalkParams.secretEnable,
      weToken: weComParams.token,
      larkToken: larkParams.token,
      larkIsAtAll: larkParams.isAtAll,
      larkSecretEnable: larkParams.secretEnable,
      larkSecretToken: larkParams.secretToken,
    });
  }

  /* delete configuration */
  async function handleDeleteAlertConf(item: AlertSetting) {
    try {
      const { data } = await fetchAlertDelete({ id: item.id });
      if (data.data) {
        Swal.fire({
          icon: 'success',
          title: 'Delete Alert Config  successful!',
          showConfirmButton: false,
          timer: 2000,
        });
      } else {
        Swal.fire(
          'Failed delete AlertConfig',
          data['message'].replaceAll(/\[StreamPark]/g, ''),
          'error',
        );
      }

      getAlertSetting();
    } catch (error) {
      console.error(error);
    }
  }

  function getAlertTypeName(type: number) {
    return alertTypeMap[type] || '';
  }

  onMounted(() => {
    getAlertSetting();
  });
</script>

<template>
  <PageWrapper contentFullHeight content-class="flex flex-col">
    <div class="bg-white py-16px px-24px">
      <span class="alarm-title">{{ t('setting.alarm.alertSetting') }}</span>
      <div v-auth="'project:create'">
        <a-button type="dashed" class="w-full mt-10px" @click="openAlertModal(true, {})">
          <PlusOutlined />
          {{ t('common.add') }}
        </a-button>
      </div>
    </div>

    <div class="flex-1">
      <List
        class="alert-card-list !mt-10px"
        :grid="{ gutter: 40, xs: 1, sm: 2, md: 4, lg: 4, xl: 4, xxl: 4, xxxl: 4 }"
        :data-source="alerts"
        :pagination="false"
      >
        <template #renderItem="{ item }">
          <ListItem>
            <Card
              class="shadow-xl alert-card"
              :bordered="false"
              :bodyStyle="{ height: '240px', padding: '15px', overflowY: 'auto' }"
            >
              <template #title>
                {{ item.alertName }}
                <div class="tag-list mt-4px">
                  <Tag
                    color="blue"
                    size="small"
                    v-for="type in item.alertTypeTags"
                    :key="type"
                    class="!leading-15px"
                  >
                    {{ getAlertTypeName(type) }}
                  </Tag>
                </div>
              </template>
              <template #actions>
                <Tooltip :title="t('setting.alarm.tooltip.test')">
                  <a-button
                    @click="handleTestAlarm(item)"
                    shape="circle"
                    size="large"
                    style="margin-left: 3px"
                    class="control-button ctl-btn-color"
                  >
                    <ThunderboltOutlined />
                  </a-button>
                </Tooltip>
                <Tooltip :title="t('setting.alarm.tooltip.detail')">
                  <a-button
                    @click="openAlertDetailModal(true, item)"
                    shape="circle"
                    size="large"
                    style="margin-left: 3px"
                    class="control-button ctl-btn-color"
                  >
                    <EyeOutlined />
                  </a-button>
                </Tooltip>
                <Tooltip :title="t('setting.alarm.tooltip.edit')">
                  <a-button
                    @click="handleEditAlertConf(item)"
                    shape="circle"
                    size="large"
                    style="margin-left: 3px"
                    class="control-button ctl-btn-color"
                  >
                    <EditOutlined />
                  </a-button>
                </Tooltip>
                <Popconfirm
                  :title="t('setting.alarm.tooltip.delete')"
                  :cancel-text="t('common.no')"
                  :ok-text="t('common.yes')"
                  @confirm="handleDeleteAlertConf(item)"
                >
                  <a-button
                    type="danger"
                    shape="circle"
                    size="large"
                    style="margin-left: 3px"
                    class="control-button"
                  >
                    <DeleteOutlined />
                  </a-button>
                </Popconfirm>
              </template>

              <AlertTypeInfo
                :alertType="String(AlertTypeEnum.MAIL)"
                :alertSource="item"
                v-if="item.alertTypeTags.includes(String(AlertTypeEnum.MAIL))"
              />
              <AlertTypeInfo
                :alertType="String(AlertTypeEnum.WECOM)"
                :alertSource="item"
                v-if="item.alertTypeTags.includes(String(AlertTypeEnum.WECOM))"
              />
              <AlertTypeInfo
                :alertType="String(AlertTypeEnum.DINGTALK)"
                :alertSource="item"
                v-if="item.alertTypeTags.includes(String(AlertTypeEnum.DINGTALK))"
              />
              <AlertTypeInfo
                :alertType="String(AlertTypeEnum.MESSAGE)"
                :alertSource="item"
                v-if="item.alertTypeTags.includes(String(AlertTypeEnum.MESSAGE))"
              />
              <AlertTypeInfo
                :alertType="String(AlertTypeEnum.LARK)"
                :alertSource="item"
                v-if="item.alertTypeTags.includes(String(AlertTypeEnum.LARK))"
              />
            </Card>
          </ListItem>
        </template>
      </List>
    </div>
    <AlertModal @register="registerAlertModal" @reload="getAlertSetting" />
    <DetailModal @register="registerAlertDetailModal" />
  </PageWrapper>
</template>
<style lang="less">
  .alert-card {
    .ant-card-head-title {
      padding: 8px 0;
    }
  }

  .alarm-title {
    background-color: @background-color-base;
    font-size: 14px;
    display: table;
    font-weight: normal;
    padding: 8px 12px;
  }

  .alert-card-list {
    background-color: @component-background;
    height: 100%;
  }
</style>
