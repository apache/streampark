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
<script setup lang="ts">
  import { useGo } from '/@/hooks/web/usePage';
  import AppForm from './components/AppForm.vue';
  import { onMounted, ref } from 'vue';
  import { PageWrapper } from '/@/components/Page';

  import { useMessage } from '/@/hooks/web/useMessage';
  import { createLocalStorage } from '/@/utils/cache';
  import { buildUUID } from '/@/utils/uuid';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { decodeByBase64, encryptByBase64 } from '/@/utils/cipher';
  import { AppTypeEnum, JobTypeEnum, ResourceFromEnum } from '/@/enums/flinkEnum';
  import { fetchGetSparkApp, fetchUpdateSparkApp } from '/@/api/spark/app';
  import { SparkApplication } from '/@/api/spark/app.type';
  import { fetchSparkEnvList } from '/@/api/spark/home';
  import { SparkEnv } from '/@/api/spark/home.type';
  import { useRoute } from 'vue-router';

  defineOptions({
    name: 'SparkApplicationAction',
  });
  const go = useGo();
  const appFormRef = ref<{
    sparkSql: any;
  } | null>(null);

  const { t } = useI18n();
  const sparkApp = ref<SparkApplication>({});
  const route = useRoute();
  const { createMessage } = useMessage();
  const ls = createLocalStorage();

  const sparkEnvs = ref<SparkEnv[]>([]);

  async function handleAppFieldValue() {
    const appId = route.query.appId;

    const res = await fetchGetSparkApp({ id: appId as string });
    let isSetConfig = false;
    let configOverride = '';
    if (res.config && res.config.trim() !== '') {
      configOverride = decodeByBase64(res.config);
      isSetConfig = true;
    }
    Object.assign(res, {
      sparkSql: res.sparkSql ? decodeByBase64(res.sparkSql) : '',
      isSetConfig,
      configOverride,
    });
    sparkApp.value = res;
    return res;
  }

  /* custom mode */
  async function handleCustomJobMode(values: Recordable) {
    const params = {
      jobType: JobTypeEnum.JAR,
      deployMode: values.deployMode,
      appType: AppTypeEnum.APACHE_SPARK,
      versionId: values.versionId,
      sparkSql: null,
      jar: values.jar,
      mainClass: values.mainClass,
      appName: values.appName,
      tags: values.tags,
      yarnQueue: values.yarnQueue,
      resourceFrom: ResourceFromEnum.UPLOAD,
      config: values.config,
      appProperties: values.appProperties,
      appArgs: values.args,
      hadoopUser: values.hadoopUser,
      lineageEnable: values.lineageEnable ?? false,
      description: values.description,
    };
    await handleUpdateAction(params);
  }
  /* spark sql mode */
  async function handleSQLMode(values: Recordable) {
    await handleUpdateAction({
      jobType: JobTypeEnum.SQL,
      deployMode: values.deployMode,
      appType: AppTypeEnum.APACHE_SPARK,
      versionId: values.versionId,
      sparkSql: values.sparkSql,
      sqlId: sparkApp.value.sqlId,
      jar: null,
      mainClass: null,
      appName: values.appName,
      tags: values.tags,
      yarnQueue: values.yarnQueue,
      resourceFrom: ResourceFromEnum.UPLOAD,
      config: values.config,
      appProperties: values.appProperties,
      appArgs: values.args,
      hadoopUser: values.hadoopUser,
      lineageEnable: values.lineageEnable ?? false,
      description: values.description,
    });
  }
  /* Submit to create */
  async function handleAppSubmit(formValue: Recordable) {
    const { configOverride } = formValue;
    if (configOverride != null && configOverride.trim() != '') {
      formValue.config = encryptByBase64(configOverride);
    } else {
      formValue.config = null;
    }

    if (formValue.jobType == JobTypeEnum.SQL) {
      if (formValue.sparkSql == null || formValue.sparkSql.trim() === '') {
        createMessage.warning(t('spark.app.addAppTips.sparkSqlIsRequiredMessage'));
      } else {
        const access = await appFormRef?.value?.sparkSql?.handleVerifySql();
        if (!access) {
          createMessage.warning(t('spark.app.addAppTips.sqlCheck'));
          throw new Error(t('spark.app.addAppTips.sqlCheck'));
        }
      }
      await handleSQLMode(formValue);
    } else {
      await handleCustomJobMode(formValue);
    }
  }
  /* send create request */
  async function handleUpdateAction(params: Recordable) {
    const fetchParams: SparkApplication = {};
    for (const k in params) {
      const v = params[k];
      if (v != null) {
        fetchParams[k] = v;
      }
    }
    const socketId = buildUUID();
    ls.set('DOWN_SOCKET_ID', socketId);
    Object.assign(fetchParams, {
      socketId,
      id: route.query.appId,
    });
    const updated = await fetchUpdateSparkApp(fetchParams);
    if (updated) {
      createMessage.success(t('spark.app.success'));
      go('/spark/app');
    }
  }
  onMounted(() => {
    if (!route?.query?.appId) {
      go('/spark/app');
      createMessage.warning(t('spark.app.appidCheck'));
      return;
    }
    //get flinkEnv
    fetchSparkEnvList().then((res) => {
      sparkEnvs.value = res;
    });
  });
</script>

<template>
  <PageWrapper contentFullHeight contentBackground contentClass="p-26px app_controller">
    <AppForm
      ref="appFormRef"
      :initFormFn="handleAppFieldValue"
      :submit="handleAppSubmit"
      :spark-envs="sparkEnvs"
    />
  </PageWrapper>
</template>

<style lang="less">
  @import url('./styles/spark.less');
</style>
