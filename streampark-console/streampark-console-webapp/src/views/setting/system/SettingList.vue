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
<script lang="ts">
  import { defineComponent } from 'vue';
  import { useModal } from '/@/components/Modal';
  export default defineComponent({
    name: 'MavenSetting',
  });
</script>
<script setup lang="ts" name="MavenSetting">
  import { List, Input, Switch } from 'ant-design-vue';
  import { SvgIcon } from '/@/components/Icon';
  import { SystemSetting } from '/@/api/setting/types/setting.type';
  import { fetchSystemSettingUpdate } from '/@/api/setting';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { useI18n } from '/@/hooks/web/useI18n';
  import SettingForm from './SettingForm.vue';

  const { t } = useI18n();

  const AvatarMap = {
    'streampark.maven.settings': 'settings2',
    'streampark.maven.central.repository': 'maven',
    'streampark.maven.auth.user': 'user',
    'streampark.maven.auth.password': 'mvnpass',
    'docker.register.address': 'docker',
    'alert.email.from': 'mail',
    'ingress.mode.default': 'nginx',
    'lineage.gravitino.address': 'net',
    'lineage.gravitino.token': 'keys',
    'lineage.gravitino.namespace': 'namespace',
    'lineage.flink.native.listener.enable': 'connector',
  };

  const settingTitles = {
    'streampark.maven.settings': t('setting.system.title.mavenSettings'),
    'streampark.maven.central.repository': t('setting.system.title.mavenRepository'),
    'streampark.maven.auth.user': t('setting.system.title.mavenUser'),
    'streampark.maven.auth.password': t('setting.system.title.mavenPassword'),
    'docker.register.address': t('setting.system.title.docker'),
    'alert.email.from': t('setting.system.title.email'),
    'ingress.mode.default': t('setting.system.title.ingress'),
    'lineage.gravitino.address': t('setting.system.lineage.address.label'),
    'lineage.gravitino.token': t('setting.system.lineage.token.label'),
    'lineage.gravitino.namespace': t('setting.system.lineage.namespace.label'),
    'lineage.flink.native.listener.enable': t('setting.system.lineage.nativeListenerEnable.label'),
  };

  const settingDesc = {
    'streampark.maven.settings': t('setting.system.desc.mavenSettings'),
    'streampark.maven.central.repository': t('setting.system.desc.mavenRepository'),
    'streampark.maven.auth.user': t('setting.system.desc.mavenUser'),
    'streampark.maven.auth.password': t('setting.system.desc.mavenPassword'),
    'docker.register.address': t('setting.system.desc.docker'),
    'alert.email.from': t('setting.system.desc.email'),
    'ingress.mode.default': t('setting.system.desc.ingress'),
    'lineage.gravitino.address': t('setting.system.lineage.address.desc'),
    'lineage.gravitino.token': t('setting.system.lineage.token.desc'),
    'lineage.gravitino.namespace': t('setting.system.lineage.namespace.desc'),
    'lineage.flink.native.listener.enable': t('setting.system.lineage.nativeListenerEnable.desc'),
  };

  const ListItem = List.Item;
  const ListItemMeta = ListItem.Meta;

  const emits = defineEmits(['updateValue', 'reload']);
  defineProps({
    data: {
      type: Array as PropType<SystemSetting[]>,
      required: true,
    },
    isPassword: {
      type: Function as PropType<(item: Recordable) => boolean>,
      required: true,
    },
  });

  const { createMessage } = useMessage();
  const [registerModal, { openModal }] = useModal();
  function handleSwitch(record: SystemSetting) {
    emits('updateValue', record);
  }
  /* edit input */
  function handleEdit(record: SystemSetting) {
    if (record.settingKey.startsWith('docker.register')) {
      openModal(true, {
        type: 'docker',
      });
    } else if (record.settingKey.startsWith('alert.email')) {
      openModal(true, {
        type: 'email',
      });
    } else {
      if (!record.editable) {
        record.submitting = true;
      }
      record.editable = !record.editable;
    }
  }
  /* edit commit */
  async function handleSubmit(record: SystemSetting) {
    record.submitting = false;
    record.editable = false;
    await fetchSystemSettingUpdate({
      settingKey: record.settingKey,
      settingValue: record.settingValue,
    });
    createMessage.success(t('setting.system.update.success'));
    emits('reload');
  }
</script>

<template>
  <List>
    <template v-for="item in data">
      <ListItem v-if="AvatarMap[item.settingKey]" :key="item.settingKey">
        <ListItemMeta style="width: 50%">
          <template #title>
            {{ settingTitles[item.settingKey] }}
          </template>
          <template #description>
            {{ settingDesc[item.settingKey] }}
          </template>
          <template #avatar>
            <div class="avatar">
              <SvgIcon :name="AvatarMap[item.settingKey]" />
            </div>
          </template>
        </ListItemMeta>
        <div class="list-content" style="width: 50%">
          <div class="list-content-item" style="width: 100%">
            <template v-if="item.type === 1">
              <Input
                :type="isPassword(item) ? 'password' : 'text'"
                v-if="item.editable"
                v-model:value="item.settingValue"
                :class="item.settingKey.replace(/\./g, '_')"
                :placeholder="t('common.inputText')"
                class="ant-input"
              />
              <div v-else style="width: 100%">
                <span v-if="isPassword(item) && item.settingValue !== null"> ******** </span>
                <span v-else>{{ item.settingValue }}</span>
              </div>
            </template>
            <template v-else>
              <Switch
                checked-children="ON"
                un-checked-children="OFF"
                style="float: right; margin-right: 30px"
                :checked="item.settingValue === 'true'"
                @change="handleSwitch(item)"
              />
            </template>
          </div>
        </div>
        <template #actions>
          <div v-if="item.type === 1" v-auth="'setting:update'">
            <a v-if="!item.submitting" @click="handleEdit(item)">
              <a-button type="primary" shape="circle">
                <SvgIcon name="edit" />
              </a-button>
            </a>
            <a v-else @click="handleSubmit(item)">
              <a-button type="primary" shape="circle">
                <SvgIcon name="save" />
              </a-button>
            </a>
          </div>
        </template>
      </ListItem>
    </template>
  </List>
  <SettingForm @register="registerModal" @success="emits('reload')" />
</template>
