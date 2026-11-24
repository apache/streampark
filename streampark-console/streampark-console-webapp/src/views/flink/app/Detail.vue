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
<script setup lang="ts" name="ApplicationDetail">
  import { DeployMode, PipelineStepEnum, ReleaseStateEnum } from '/@/enums/flinkEnum';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { fetchAppExternalLink } from '/@/api/setting/externalLink';
  import { ExternalLink } from '/@/api/setting/types/externalLink.type';
  import { useModal } from '/@/components/Modal';
  import { PageWrapper } from '/@/components/Page';
  import { Description, useDescription } from '/@/components/Description';
  import { Icon } from '/@/components/Icon';
  import { useRoute, useRouter } from 'vue-router';
  import { fetchDownloadArtifact, fetchGet, fetchOptionLog, fetchYarn } from '/@/api/flink/app';
  import { computed, onUnmounted, reactive, h, ref, onMounted } from 'vue';
  import { useIntervalFn } from '@vueuse/core';
  import { AppListRecord } from '/@/api/flink/app.type';
  import { Tooltip, Divider, Space } from 'ant-design-vue';
  import { handleView } from './utils';
  import { Button } from '/@/components/Button';
  import { getDescSchema } from './data/detail.data';
  import { fetchListVer } from '/@/api/flink/config';
  import { fetchSavePointHistory } from '/@/api/flink/savepoint';
  import Mergely from './components/Mergely.vue';
  import DetailTab from './components/AppDetail/DetailTab.vue';
  import RequestModal from './components/RequestModal';
  import { createDetailProviderContext } from './hooks/useDetailContext';
  import { useDrawer } from '/@/components/Drawer';
  import { LinkBadge } from '/@/components/LinkBadge';
  import { downloadByData } from '/@/utils/file/download';
  import Swal from 'sweetalert2';

  defineOptions({
    name: 'ApplicationDetail',
  });
  const route = useRoute();
  const router = useRouter();

  const { t } = useI18n();

  const appNotRunning = ref(true);

  const yarn = ref('');
  const externalLinks = ref<ExternalLink[]>([]);
  const app = reactive<Partial<AppListRecord>>({});
  const detailTabs = reactive({
    showConf: false,
    showSaveOption: false,
    showBackup: false,
    showOptionLog: false,
  });

  createDetailProviderContext({ app });

  const [registerDescription] = useDescription({
    schema: [
      ...(getDescSchema() as any),
      {
        field: 'resetApi',
        label: h('div', null, [
          t('flink.app.detail.resetApi'),
          h(Tooltip, { title: t('flink.app.detail.resetApiToolTip'), placement: 'top' }, () =>
            h(Icon, { icon: 'ant-design:question-circle-outlined', class: 'pl-5px', color: 'red' }),
          ),
        ]),
        render: () => [
          h(
            Button,
            {
              type: 'primary',
              size: 'small',
              class: 'mx-3px px-5px',
              onClick: () =>
                openApiModal(true, {
                  name: 'flinkStart',
                  app,
                }),
            },
            () => [t('flink.app.detail.copyStartcURL')],
          ),
          h(
            Button,
            {
              type: 'primary',
              size: 'small',
              class: 'mx-3px px-5px',
              onClick: () => {
                openApiModal(true, {
                  name: 'flinkCancel',
                  app,
                });
              },
            },
            () => [t('flink.app.detail.copyCancelcURL')],
          ),
        ],
      },
    ],
    data: app,
    layout: 'horizontal',
    column: 2,
    size: 'small',
  });

  const [registerConfDrawer] = useDrawer();
  const [registerOpenApi, { openModal: openApiModal }] = useModal();

  /* Flink Web UI */
  function handleFlinkView() {
    handleView(app as any);
  }

  const { pause } = useIntervalFn(
    () => {
      handleGetAppInfo();
    },
    5000,
    { immediateCallback: true },
  );

  async function handleGetAppInfo() {
    if (!route.query.appId) router.back();
    const res = await fetchGet({
      id: route.query.appId as string,
    });
    // Get data for the first time
    if (Object.keys(app).length == 0) {
      if (
        [DeployMode.YARN_PER_JOB, DeployMode.YARN_SESSION, DeployMode.YARN_APPLICATION].includes(
          res.deployMode,
        )
      ) {
        await handleYarn();
      }
      await handleDetailTabs();
    }
    Object.assign(app, res);
    appNotRunning.value = !app.appControl.allowView;
  }

  async function handleDetailTabs() {
    const commonParams = {
      appId: route.query.appId as string,
      pageNum: 1,
      pageSize: 10,
    };

    const confList = await fetchListVer(commonParams);
    const pointHistory = await fetchSavePointHistory(commonParams);
    const optionList = await fetchOptionLog(commonParams);

    if (confList.records.length > 0) detailTabs.showConf = true;
    if (pointHistory.records.length > 0) detailTabs.showSaveOption = true;
    if (optionList.records.length > 0) detailTabs.showOptionLog = true;
  }

  /* Get yarn data */
  async function handleYarn() {
    yarn.value = await fetchYarn();
  }

  async function getExternalLinks() {
    const { data: links } = await fetchAppExternalLink({ appId: route.query.appId as string });
    externalLinks.value = links.data;
  }

  const downloadingArtifact = ref(false);
  const artifactBuildUnavailable = computed(
    () =>
      app.build === true ||
      app.release === ReleaseStateEnum.RELEASING ||
      app.buildStatus !== PipelineStepEnum.SUCCESS,
  );
  let artifactDownloadPopup: HTMLElement | null = null;
  let artifactDownloadRequest = 0;

  async function handleArtifactDownload() {
    const id = app.id;
    if (!id || artifactBuildUnavailable.value || downloadingArtifact.value) return;

    const requestId = ++artifactDownloadRequest;
    downloadingArtifact.value = true;
    try {
      await new Promise<void>((resolve) => {
        void Swal.fire({
          width: 450,
          titleText: t('flink.app.detail.artifactDownloading'),
          text: t('flink.app.detail.artifactDownloadWait'),
          customClass: { popup: 'artifact-download-wait' },
          showConfirmButton: false,
          allowOutsideClick: false,
          allowEscapeKey: false,
          heightAuto: false,
          didOpen(popup) {
            if (requestId !== artifactDownloadRequest) {
              if (Swal.getPopup() === popup) Swal.close();
              resolve();
              return;
            }
            artifactDownloadPopup = popup;
            if (Swal.getPopup() === popup) {
              Swal.showLoading();
              const loader = popup.querySelector('.swal2-loader');
              if (loader) popup.prepend(loader);
            }
            resolve();
          },
        });
      });
      if (requestId !== artifactDownloadRequest) return;

      const response = await fetchDownloadArtifact(String(id));
      const contentType = String(
        response.headers?.['content-type'] || response.data?.type || '',
      ).toLowerCase();
      if (
        response.status < 200 ||
        response.status >= 300 ||
        !contentType.startsWith('application/zip') ||
        response.data.size === 0
      ) {
        throw new Error('The artifact response is not a ZIP archive.');
      }
      downloadByData(response.data, `app-${id}-dependencies.zip`, 'application/zip');
    } catch (error: any) {
      const status = error?.response?.status;
      if (status === 401 || status === 403) return;
      if (
        requestId !== artifactDownloadRequest ||
        !artifactDownloadPopup ||
        Swal.getPopup() !== artifactDownloadPopup
      ) {
        return;
      }
      Swal.close();
      artifactDownloadPopup = null;
      void Swal.fire({
        width: 450,
        icon: 'error',
        titleText: t('flink.app.detail.artifactDownloadErrorTitle'),
        text: t('flink.app.detail.artifactDownloadFailed'),
        confirmButtonText: t('common.okText'),
        heightAuto: false,
      });
    } finally {
      if (requestId !== artifactDownloadRequest) return;
      if (artifactDownloadPopup && Swal.getPopup() === artifactDownloadPopup) {
        Swal.close();
      }
      artifactDownloadPopup = null;
      downloadingArtifact.value = false;
    }
  }

  onMounted(() => {
    getExternalLinks();
  });

  onUnmounted(() => {
    pause();
    artifactDownloadRequest += 1;
    if (artifactDownloadPopup && Swal.getPopup() === artifactDownloadPopup) {
      Swal.close();
    }
    artifactDownloadPopup = null;
  });
</script>
<template>
  <PageWrapper content-full-height content-background>
    <div class="detail-pad">
      <div class="mb-15px">
        <span class="app-bar">{{ t('flink.app.detail.detailTitle') }}</span>
        <Space class="-mt-8px">
          <div v-for="link in externalLinks" :key="link.id">
            <LinkBadge
              :label="link.badgeLabel"
              :redirect="link.renderedLinkUrl"
              :color="link.badgeColor"
              :message="link.badgeName"
              :disabled="appNotRunning"
            />
          </div>
        </Space>
        <a-button type="primary" shape="circle" @click="router.back()" class="float-right -mt-8px">
          <Icon icon="ant-design:arrow-left-outlined" />
        </a-button>
        <a-button
          type="primary"
          @click="handleFlinkView"
          :disabled="appNotRunning"
          class="float-right -mt-8px mr-20px"
        >
          <Icon icon="ant-design:cloud-outlined" />
          {{ t('flink.app.detail.flinkWebUi') }}
        </a-button>
        <a-button
          v-if="app.id"
          v-auth="'app:detail'"
          type="primary"
          :disabled="artifactBuildUnavailable"
          :loading="downloadingArtifact"
          @click="handleArtifactDownload"
          class="float-right -mt-8px mr-20px"
        >
          <Icon icon="ant-design:download-outlined" />
          {{ t('flink.app.detail.artifactDependencies') }}
        </a-button>
      </div>
      <Description @register="registerDescription" />
      <Divider class="mt-20px -mb-17px" />
      <DetailTab :app="app" :tabConf="detailTabs" />

      <Mergely @register="registerConfDrawer" :readOnly="true" />
      <RequestModal @register="registerOpenApi" />
    </div>
  </PageWrapper>
</template>
<style lang="less">
  @import url('./styles/Detail.less');
</style>
