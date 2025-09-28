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
    name: 'EditFlink',
  };
</script>
<script setup lang="ts" name="EditFlink">
  import { PageWrapper } from '/@/components/Page';
  import { BasicForm, useForm } from '/@/components/Form';
  import { onMounted, reactive, ref, nextTick, unref } from 'vue';
  import { Alert } from 'ant-design-vue';
  import { fetchUpdate } from '/@/api/flink/app';
  import { fetchUpload } from '/@/api/resource/upload';
  import { handleSubmitParams } from './utils';
  import PomTemplateTab from './components/PodTemplate/PomTemplateTab.vue';
  import { fetchListJars } from '/@/api/resource/project';
  import { useEditFlinkSchema } from './hooks/useEditFlinkSchema';
  import { useEdit } from './hooks/useEdit';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useRoute } from 'vue-router';
  import { useGo } from '/@/hooks/web/usePage';
  import UploadJobJar from './components/UploadJobJar.vue';
  import ProgramArgs from './components/ProgramArgs.vue';
  import VariableReview from './components/VariableReview.vue';
  import { useDrawer } from '/@/components/Drawer';
  import { DeployMode, ResourceFromEnum } from '/@/enums/flinkEnum';

  const route = useRoute();
  const { t } = useI18n();
  const { createMessage } = useMessage();
  const go = useGo();

  const submitLoading = ref<boolean>(false);
  const jars = ref<string[]>([]);

  const uploadLoading = ref(false);
  const uploadJar = ref('');
  const programArgRef = ref();
  const podTemplateRef = ref();

  const k8sTemplate = reactive({
    podTemplate: '',
    jmPodTemplate: '',
    tmPodTemplate: '',
  });

  const [registerReviewDrawer, { openDrawer: openReviewDrawer }] = useDrawer();

  const { getEditFlinkFormSchema, alerts, suggestions } = useEditFlinkSchema(jars);
  const { handleGetApplication, app, handleResetApplication } = useEdit();
  const [registerForm, { setFieldsValue, submit }] = useForm({
    labelWidth: 120,
    colon: true,
    baseColProps: { span: 24 },
    labelCol: { lg: { span: 5, offset: 0 }, sm: { span: 7, offset: 0 } },
    wrapperCol: { lg: { span: 16, offset: 0 }, sm: { span: 17, offset: 0 } },
    showActionButtonGroup: false,
  });

  /* Form reset */
  function handleReset(deployMode?: string) {
    nextTick(async () => {
      let selectAlertId: string | undefined;
      if (app.alertId) {
        selectAlertId = unref(alerts)?.filter((t) => t.id == app.alertId)[0]?.id;
      }
      const resetParams = handleResetApplication();
      const defaultParams = {
        jobName: app.jobName,
        tags: app.tags,
        mainClass: app.mainClass,
        args: app.args || '',
        jar: app.jar,
        description: app.description,
        hadoopUser: app.hadoopUser,
        dynamicProperties: app.dynamicProperties,
        resolveOrder: app.resolveOrder,
        deployMode: app.deployMode,
        yarnQueue: app.yarnQueue,
        restartSize: app.restartSize,
        checkPointFailure: {
          cpMaxFailureInterval: app.cpMaxFailureInterval,
          cpFailureRateInterval: app.cpFailureRateInterval,
          cpFailureAction: app.cpFailureAction,
        },
        versionId: app.versionId || null,
        k8sRestExposedType: app.k8sRestExposedType,
        flinkImage: app.flinkImage,
        k8sNamespace: app.k8sNamespace,
        serviceAccount: app.serviceAccount,
        alertId: selectAlertId,
        projectName: app.projectName,
        module: app.module,
        ...resetParams,
      };
      if (!deployMode) {
        Object.assign(defaultParams, { deployMode: app.deployMode });
      }
      switch (app.deployMode) {
        case DeployMode.STANDALONE:
          defaultParams['remoteClusterId'] = app.flinkClusterId;
          break;
        case DeployMode.YARN_SESSION:
          defaultParams['yarnSessionClusterId'] = app.flinkClusterId;
          break;
        case DeployMode.KUBERNETES_SESSION:
          defaultParams['k8sSessionClusterId'] = app.flinkClusterId;
          break;
        default:
          break;
      }
      setFieldsValue(defaultParams);
      app.args && programArgRef.value?.setContent(app.args);
      setTimeout(() => {
        unref(podTemplateRef)?.handleChoicePodTemplate('ptVisual', app.k8sPodTemplate);
        unref(podTemplateRef)?.handleChoicePodTemplate('jmPtVisual', app.k8sJmPodTemplate);
        unref(podTemplateRef)?.handleChoicePodTemplate('tmPtVisual', app.k8sTmPodTemplate);
      }, 1000);
    });
  }
  /* Custom job upload */
  async function handleCustomJobRequest(data) {
    const formData = new FormData();
    formData.append('file', data.file);
    try {
      const response = await fetchUpload(formData);
      uploadJar.value = data.file.name;
      uploadLoading.value = false;
      setFieldsValue({ mainClass: response.mainClass, jar: unref(uploadJar) });
    } catch (error) {
      console.error(error);
      uploadLoading.value = false;
    }
  }

  /* Handling update parameters */
  function handleAppUpdate(values: Recordable) {
    submitLoading.value = true;
    try {
      const params = {
        id: app.id,
        jar: values.jar,
        mainClass: values.mainClass,
      };
      handleSubmitParams(params, values, k8sTemplate);

      handleUpdateApp(params);
    } catch (error) {
      submitLoading.value = false;
    }
  }

  /* Submit an update */
  async function handleUpdateApp(params: Recordable) {
    const updated = await fetchUpdate(params);
    if (updated) {
      createMessage.success(t('flink.app.editStreamPark.success'));
      go('/flink/app');
    }
  }

  onMounted(async () => {
    if (!route?.query?.appId) {
      go('/flink/app');
      createMessage.warning(t('flink.app.editStreamPark.appidCheck'));
      return;
    }
    const value = await handleGetApplication();
    setFieldsValue(value);
    if (app.resourceFrom == ResourceFromEnum.PROJECT) {
      jars.value = await fetchListJars({
        id: app.projectId,
        module: app.module,
      });
    }
    handleReset();
  });
</script>
<template>
  <PageWrapper contentBackground content-class="p-26px app_controller">
    <BasicForm @register="registerForm" @submit="handleAppUpdate" :schemas="getEditFlinkFormSchema">
      <template #podTemplate>
        <PomTemplateTab
          ref="podTemplateRef"
          v-model:podTemplate="k8sTemplate.podTemplate"
          v-model:jmPodTemplate="k8sTemplate.jmPodTemplate"
          v-model:tmPodTemplate="k8sTemplate.tmPodTemplate"
        />
      </template>

      <template #uploadJobJar>
        <UploadJobJar :custom-request="handleCustomJobRequest" v-model:loading="uploadLoading">
          <template #uploadInfo>
            <Alert v-if="uploadJar" class="uploadjar-box" type="info">
              <template #message>
                <span class="tag-dependency-pom">
                  {{ uploadJar }}
                </span>
              </template>
            </Alert>
          </template>
        </UploadJobJar>
      </template>

      <template #args="{ model }">
        <ProgramArgs
          ref="programArgRef"
          v-if="model.args != null && model.args != undefined"
          v-model:value="model.args"
          :suggestions="suggestions"
          @preview="(value) => openReviewDrawer(true, { value, suggestions })"
        />
      </template>

      <template #formFooter>
        <div class="flex items-center w-full justify-center">
          <a-button @click="go('/flink/app')">
            {{ t('common.cancelText') }}
          </a-button>
          <a-button class="ml-4" :loading="submitLoading" type="primary" @click="submit()">
            {{ t('common.submitText') }}
          </a-button>
        </div>
      </template>
    </BasicForm>
    <VariableReview @register="registerReviewDrawer" />
  </PageWrapper>
</template>
<style lang="less">
  @import url('./styles/Add.less');
</style>
