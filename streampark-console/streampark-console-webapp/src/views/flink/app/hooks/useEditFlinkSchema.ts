/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { FormSchema } from '/@/components/Table';
import { computed, h, Ref, ref, unref } from 'vue';

import { useCreateAndEditSchema } from './useCreateAndEditSchema';
import { getAlertSvgIcon } from './useFlinkRender';
import { Alert } from 'ant-design-vue';
import { useRoute } from 'vue-router';
import { fetchMain } from '/@/api/flink/app';
import { ResourceFromEnum } from '/@/enums/flinkEnum';

export const useEditFlinkSchema = (jars: Ref) => {
  const flinkSql = ref();
  const route = useRoute();
  const {
    alerts,
    flinkEnvs,
    flinkClusters,
    getFlinkClusterSchemas,
    getFlinkFormOtherSchemas,
    getFlinkTypeSchema,
    getDeployModeSchema,
    suggestions,
  } = useCreateAndEditSchema(null, { appId: route.query.appId as string, mode: 'streampark' });

  const getEditFlinkFormSchema = computed((): FormSchema[] => {
    return [
      ...getFlinkTypeSchema.value,
      ...getDeployModeSchema.value,
      {
        field: 'resourceFrom',
        label: 'Resource From',
        component: 'Input',
        render: ({ model }) => {
          if (model.resourceFrom == ResourceFromEnum.PROJECT)
            return getAlertSvgIcon('github', 'Project (build from CSV)');
          else if (model.resourceFrom == ResourceFromEnum.UPLOAD)
            return getAlertSvgIcon('upload', 'Upload (upload local job)');
          else if (model.resourceFrom == ResourceFromEnum.IMAGE)
            return getAlertSvgIcon('docker', 'Image (From Flink Base Image)');
          else return '';
        },
      },
      ...getFlinkClusterSchemas.value,
      {
        field: 'projectName',
        label: 'Project',
        component: 'Input',
        render: ({ model }) => h(Alert, { message: model.projectName, type: 'info' }),
        ifShow: ({ model }) => model.resourceFrom == ResourceFromEnum.PROJECT && model.projectName,
      },
      {
        field: 'module',
        label: 'Module',
        component: 'Input',
        render: ({ model }) => h(Alert, { message: model.module, type: 'info' }),
        ifShow: ({ model }) => model.resourceFrom == ResourceFromEnum.PROJECT && model.module,
      },
      {
        field: 'jar',
        label: 'Program Jar',
        component: 'Select',
        componentProps: ({ formModel }) => {
          return {
            placeholder: 'Please select jar',
            options: unref(jars).map((i) => ({ label: i, value: i })),
            onChange: (value: string) => {
              fetchMain({
                projectId: formModel.projectId,
                module: formModel.module,
                jar: value,
              }).then((res) => {
                formModel.mainClass = res;
              });
            },
          };
        },
        ifShow: ({ model }) => model.resourceFrom == ResourceFromEnum.PROJECT,
        rules: [{ required: true, message: 'Please select jar' }],
      },
      {
        field: 'uploadJobJar',
        label: 'Upload Job Jar',
        component: 'Select',
        slot: 'uploadJobJar',
        ifShow: ({ model }) => model.resourceFrom != ResourceFromEnum.PROJECT && model.resourceFrom != ResourceFromEnum.IMAGE,
      },
      {
        field: 'jar',
        label: 'Program Jar',
        component: 'Input',
        dynamicDisabled: ({ model }) => model.resourceFrom != ResourceFromEnum.IMAGE,
        ifShow: ({ model }) => model.resourceFrom != ResourceFromEnum.PROJECT,
      },
      {
        field: 'mainClass',
        label: 'Program Main',
        component: 'Input',
        componentProps: {
          allowClear: true,
          placeholder: 'Please enter Main class',
        },
        rules: [{ required: true, message: 'Program Main is required' }],
      },
      ...getFlinkFormOtherSchemas.value,
    ];
  });
  return {
    getEditFlinkFormSchema,
    flinkEnvs,
    flinkClusters,
    flinkSql,
    alerts,
    suggestions,
  };
};
