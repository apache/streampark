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
  export default {
    name: 'FlinkSql',
    components: { SvgIcon },
  };
</script>

<script setup lang="ts" name="FlinkSql">
  import { computed, reactive, ref, watchEffect } from 'vue';
  import { Tooltip } from 'ant-design-vue';
  import { FullscreenExitOutlined } from '@ant-design/icons-vue';
  import { getMonacoOptions } from '../data';
  import { Icon, SvgIcon } from '/@/components/Icon';
  import { useMonaco } from '/@/hooks/web/useMonaco';
  import { Button } from 'ant-design-vue';
  import { isEmpty } from '/@/utils/is';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { fetchFlinkSqlVerify } from '/@/api/flink/flinkSql';
  import { format } from '../FlinkSqlFormatter';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useFullContent } from '/@/hooks/event/useFullscreen';
  const ButtonGroup = Button.Group;
  const { t } = useI18n();

  const flinkSql = ref();
  const verifyRes = reactive({
    errorMsg: '',
    verified: false,
    errorStart: 0,
    errorEnd: 0,
  });

  const { toggle, fullContentClass, fullEditorClass, fullScreenStatus } = useFullContent();
  const emit = defineEmits(['update:value', 'preview']);
  const { createMessage } = useMessage();

  const props = defineProps({
    value: {
      type: String,
      default: '',
    },
    appId: {
      type: String as PropType<Nullable<string>>,
    },
    versionId: {
      type: String as PropType<Nullable<string>>,
    },
    suggestions: {
      type: Array as PropType<Array<{ text: string; description: string }>>,
      default: () => [],
    },
  });
  const defaultValue = '';

  /* verify */
  async function handleVerifySql() {
    if (isEmpty(props.value)) {
      verifyRes.errorMsg = 'empty sql';
      return false;
    }

    if (!props.versionId) {
      createMessage.error(t('flink.app.dependencyError'));
      return false;
    } else {
      try {
        const { data } = await fetchFlinkSqlVerify({
          sql: props.value,
          versionId: props.versionId,
        });
        const success = data.data === true || data.data === 'true';
        if (success) {
          verifyRes.verified = true;
          verifyRes.errorMsg = '';
          syntaxError();
          return true;
        } else {
          verifyRes.errorStart = parseInt(data.start);
          verifyRes.errorEnd = parseInt(data.end);
          switch (data.type) {
            case 4:
              verifyRes.errorMsg = 'Unsupported sql';
              break;
            case 5:
              verifyRes.errorMsg = "SQL is not endWith ';'";
              break;
            default:
              verifyRes.errorMsg = data.message;
              break;
          }
          syntaxError();
          return false;
        }
      } catch (error) {
        console.error(error);
        return false;
      }
    }
  }

  async function syntaxError() {
    const editor = await getInstance();
    if (editor) {
      const model = editor.getModel();
      const monaco = await getMonacoInstance();
      if (verifyRes.errorMsg) {
        try {
          monaco.editor.setModelMarkers(model, 'sql', [
            {
              startLineNumber: verifyRes.errorStart,
              endLineNumber: verifyRes.errorEnd,
              severity: monaco.MarkerSeverity.Error,
              message: verifyRes.errorMsg,
            },
          ]);
        } catch (e) {
          console.log(e);
        }
      } else {
        monaco.editor.setModelMarkers(model, 'sql', []);
      }
    }
  }
  /* format */
  function handleFormatSql() {
    if (isEmpty(props.value)) return;
    const formatSql = format(props.value);
    setContent(formatSql);
  }
  /* full screen */
  // function handleBigScreen() {
  //   toggle();
  //   unref(flinkSql).style.width = '0';
  //   setTimeout(() => {
  //     unref(flinkSql).style.width = '100%';
  //     unref(flinkSql).style.height = isFullscreen.value ? 'calc(100vh - 50px)' : '550px';
  //   }, 500);
  // }
  const { onChange, setContent, getInstance, getMonacoInstance, setMonacoSuggest } = useMonaco(
    flinkSql,
    {
      language: 'sql',
      code: props.value || defaultValue,
      options: {
        minimap: { enabled: true },
        ...(getMonacoOptions(false) as any),
        autoClosingBrackets: 'never',
      },
    },
  );

  watchEffect(() => {
    if (props.suggestions.length > 0) {
      setMonacoSuggest(props.suggestions);
    }
  });
  const canPreview = computed(() => {
    return /\${.+}/.test(props.value);
  });
  const flinkEditorClass = computed(() => {
    return {
      ...fullEditorClass.value,
      ['syntax-' + (verifyRes.errorMsg ? 'false' : 'true')]: true,
    };
  });

  onChange((data) => {
    emit('update:value', data);
  });

  defineExpose({ handleVerifySql, setContent });
</script>

<template>
  <div style="height: 550px" class="w-full" :class="fullContentClass">
    <div
      class="full-content-tool flex justify-between px-20px pb-10px mb-10px"
      v-if="fullScreenStatus"
    >
      <div class="flex items-center">
        <SvgIcon name="fql" />
        <div class="basic-title ml-10px">Flink Sql</div>
      </div>
      <Tooltip :title="t('component.modal.restore')" placement="bottom">
        <FullscreenExitOutlined role="full" @click="toggle" style="font-size: 18px" />
      </Tooltip>
    </div>

    <div ref="flinkSql" class="overflow-hidden w-full mt-5px" :class="flinkEditorClass"></div>
    <ButtonGroup class="flinksql-tool" v-if="!fullScreenStatus">
      <a-button size="small" class="flinksql-tool-item" type="primary" @click="handleVerifySql">
        <Icon icon="ant-design:check-outlined" />
        {{ t('flink.app.flinkSql.verify') }}
      </a-button>
      <a-button
        class="flinksql-tool-item"
        size="small"
        type="default"
        v-if="canPreview"
        @click="emit('preview', value)"
      >
        <Icon icon="ant-design:eye-outlined" />
        {{ t('flink.app.flinkSql.preview') }}
      </a-button>
      <a-button class="flinksql-tool-item" size="small" type="default" @click="handleFormatSql">
        <Icon icon="ant-design:thunderbolt-outlined" />
        {{ t('flink.app.flinkSql.format') }}
      </a-button>
      <a-button class="flinksql-tool-item" size="small" type="default" @click="toggle">
        <Icon icon="ant-design:fullscreen-outlined" />
        {{ t('flink.app.flinkSql.fullScreen') }}
      </a-button>
    </ButtonGroup>
    <div class="flex items-center justify-between" v-else>
      <div class="mt-10px flex-1 mr-10px overflow-hidden whitespace-nowrap">
        <div class="text-red-600 overflow-ellipsis overflow-hidden" v-if="verifyRes.errorMsg">
          {{ verifyRes.errorMsg }}
        </div>
        <div v-else class="text-green-700">
          <span v-if="verifyRes.verified"> {{ t('flink.app.flinkSql.successful') }} </span>
        </div>
      </div>
      <div class="flinksql-tool">
        <a-button type="primary" @click="handleVerifySql">
          <div class="flex items-center">
            <Icon icon="ant-design:check-outlined" />
            {{ t('flink.app.flinkSql.verify') }}
          </div>
        </a-button>
        <a-button v-if="canPreview" @click="emit('preview', value)" class="ml-10px">
          <div class="flex items-center">
            <Icon icon="ant-design:eye-outlined" />
            {{ t('flink.app.flinkSql.preview') }}
          </div>
        </a-button>
        <a-button type="default" @click="handleFormatSql" class="ml-10px">
          <div class="flex items-center">
            <Icon icon="ant-design:thunderbolt-outlined" />
            {{ t('flink.app.flinkSql.format') }}
          </div>
        </a-button>
        <a-button type="default" @click="toggle" class="ml-10px">
          <div class="flex items-center">
            <Icon icon="ant-design:fullscreen-exit-outlined" />
            {{ t('layout.header.tooltipExitFull') }}
          </div>
        </a-button>
      </div>
    </div>
  </div>
  <p class="conf-desc mt-10px" v-if="!fullScreenStatus">
    <span class="text-red-600" v-if="verifyRes.errorMsg"> {{ verifyRes.errorMsg }} </span>
    <span v-else class="text-green-700">
      <span v-if="verifyRes.verified"> {{ t('flink.app.flinkSql.successful') }} </span>
    </span>
  </p>
</template>
