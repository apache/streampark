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
<script setup lang="ts" name="SystemSetting">
  import { Collapse, Card } from 'ant-design-vue';
  import { ref, onMounted, computed } from 'vue';
  import { fetchSystemSetting } from '/@/api/setting';
  import { SystemSetting } from '/@/api/setting/types/setting.type';
  import { fetchSystemSettingUpdate } from '/@/api/setting';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { useI18n } from '/@/hooks/web/useI18n';
  import SettingList from './SettingList.vue';
  import { PageWrapper } from '/@/components/Page';
  defineOptions({
    name: 'SystemSetting',
  });
  const { t } = useI18n();
  const CollapsePane = Collapse.Panel;
  const { createMessage } = useMessage();
  const settings = ref<SystemSetting[]>([]);

  const settingsList = computed(() => {
    const filterValue = (key: string) => {
      return settings.value.filter((i) => i.settingKey.indexOf(key) > -1);
    };
    return [
      {
        key: 1,
        title: t('setting.system.systemSettingItems.mavenSetting.name'),
        isPassword: (item: SystemSetting) => item.settingKey === 'streampark.maven.auth.password',
        data: filterValue('streampark.maven'),
      },
      {
        key: 2,
        title: t('setting.system.systemSettingItems.dockerSetting.name'),
        isPassword: (item: SystemSetting) => item.settingKey === 'docker.register.password',
        data: filterValue('docker.register'),
      },
      {
        key: 3,
        title: t('setting.system.systemSettingItems.emailSetting.name'),
        isPassword: (item: SystemSetting) => item.settingKey === 'alert.email.password',
        data: filterValue('alert.email'),
      },
      {
        key: 4,
        title: t('setting.system.systemSettingItems.ingressSetting.name'),
        isPassword: () => false,
        data: filterValue('ingress.mode'),
      },
      {
        key: 5,
        title: t('setting.system.systemSettingItems.lineageSetting.name'),
        isPassword: (item: SystemSetting) => item.settingKey === 'lineage.gravitino.token',
        data: filterValue('lineage.'),
      },
    ];
  });
  const collapseActive = ref(['1', '2', '3', '4', '5']);
  /* Get all system settings */
  async function getSettingAll() {
    const res = await fetchSystemSetting();
    settings.value = res;
  }

  /* Update setting value */
  async function handleSettingUpdate(record: SystemSetting) {
    await fetchSystemSettingUpdate({
      settingKey: record.settingKey,
      settingValue: record.settingValue !== 'true',
    });
    createMessage.success(t('setting.system.update.success'));
    getSettingAll();
  }

  onMounted(() => {
    getSettingAll();
  });
</script>

<template>
  <PageWrapper contentFullHeight>
    <Card :bordered="false" class="system-setting">
      <span class="streampark-basic-title">{{ t('setting.system.systemSetting') }}</span>
      <Collapse class="collapse" v-model:activeKey="collapseActive">
        <CollapsePane v-for="item in settingsList" :key="item.key" :header="item.title">
          <div class="bg-white">
            <SettingList
              :data="item.data"
              :isPassword="item.isPassword"
              @update-value="handleSettingUpdate"
              @reload="getSettingAll"
            />
          </div>
        </CollapsePane>
      </Collapse>
    </Card>
  </PageWrapper>
</template>
<style lang="less">
  .system-setting {
    .ant-card-body {
      padding: 16px 24px;
    }

    .streampark-basic-title {
      background-color: @background-color-base;
      height: 100%;
      font-size: 14px !important;
      display: table !important;
      font-weight: normal;
      margin-top: 24px !important;
    }

    .collapse {
      margin-top: 20px;
      margin-bottom: 20px;
    }

    .ant-collapse {
      border: unset;
    }

    .ant-tabs-bar {
      margin: 0 !important;
    }

    .ant-list-split .ant-list-item {
      padding-top: 20px;
      padding-bottom: 20px;
    }

    .list-content-item {
      display: inline-block;
      color: @text-color-secondary;
      vertical-align: middle;
      font-size: 14px;
      margin-left: 20px;

      span {
        line-height: 10px;
      }

      p {
        margin-top: 4px;
        margin-bottom: 0;
        line-height: 15px;
        word-break: break-all;
      }

      .ant-list-item-action {
        .ctl-btn-color {
          background: none !important;
        }
      }
    }

    .ant-tag {
      line-height: 25px;
    }

    .avatar {
      width: 70px;
      height: 70px;
      text-align: center;
      line-height: 70px;
      border-radius: 50%;
      background-color: @border-color-base;

      svg {
        width: 40px !important;
        height: 40px !important;
        vertical-align: middle;
        // padding: 10px;
      }
    }
  }
</style>
