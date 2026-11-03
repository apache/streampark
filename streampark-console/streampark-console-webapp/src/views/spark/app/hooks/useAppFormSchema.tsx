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
import { computed, h, onMounted, ref, unref, type Ref } from 'vue';
import type { FormSchema } from '/@/components/Form';
import { useI18n } from '/@/hooks/web/useI18n';
import { AppExistsStateEnum, JobTypeEnum, DeployMode } from '/@/enums/sparkEnum';
import { ResourceFromEnum } from '/@/enums/flinkEnum';
import type { SparkEnv } from '/@/api/spark/home.type';
import type { RuleObject } from 'ant-design-vue/lib/form';
import type { StoreValue } from 'ant-design-vue/lib/form/interface';
import { renderIsSetConfig, renderStreamParkResource, sparkJobTypeMap } from './useSparkRender';
import { deployModes } from '../data';
import { useDrawer } from '/@/components/Drawer';
import { fetchVariableAll } from '/@/api/resource/variable';
import { fetchTeamResource } from '/@/api/resource/upload';
import { fetchCheckSparkName } from '/@/api/spark/app';
import { useRoute } from 'vue-router';
import { Alert, Select, Tag } from 'ant-design-vue';
import { fetchYarnQueueList } from '/@/api/setting/yarnQueue';

export function useSparkSchema(sparkEnvs: Ref<SparkEnv[]>) {
  const { t } = useI18n();
  const route = useRoute();
  const teamResource = ref<Array<any>>([]);
  const yarnQueue = ref<Array<any>>([]);
  const suggestions = ref<Array<{ text: string; description: string; value: string }>>([]);

  const [registerConfDrawer, { openDrawer: openConfDrawer }] = useDrawer();
  /* Detect job name field */
  async function getJobNameCheck(_rule: RuleObject, value: StoreValue, _model: Recordable) {
    if (value === null || value === undefined || value === '') {
      return Promise.reject(t('spark.app.addAppTips.appNameIsRequiredMessage'));
    }
    const params = { appName: value };
    if (route.query?.appId) {
      Object.assign(params, { id: route.query?.appId });
    }
    const res = await fetchCheckSparkName(params);
    switch (parseInt(res)) {
      case AppExistsStateEnum.NO:
        return Promise.resolve();
      case AppExistsStateEnum.IN_DB:
        return Promise.reject(t('spark.app.addAppTips.appNameNotUniqueMessage'));
      case AppExistsStateEnum.IN_YARN:
        return Promise.reject(t('spark.app.addAppTips.appNameExistsInYarnMessage'));
      case AppExistsStateEnum.IN_KUBERNETES:
        return Promise.reject(t('spark.app.addAppTips.appNameExistsInK8sMessage'));
      default:
        return Promise.reject(t('spark.app.addAppTips.appNameValid'));
    }
  }
  const getJobTypeOptions = () => {
    return Object.values(sparkJobTypeMap);
  };

  const getJobTypeSchema = computed((): FormSchema[] => {
    if (route.query.appId) {
      return [
        {
          field: 'jobType',
          label: t('spark.app.jobType'),
          component: 'InputNumber',
          render: ({ model }) => {
            const jobOptions = getJobTypeOptions();
            return (
              <Alert
                type="info"
                v-slots={{
                  message: () => jobOptions.find((v) => v.value == model.jobType)?.label ?? '',
                }}
              ></Alert>
            );
          },
        },
      ];
    } else {
      return [
        {
          field: 'jobType',
          label: t('spark.app.jobType'),
          component: 'Select',
          componentProps: ({ formModel }) => {
            return {
              placeholder: t('spark.app.addAppTips.jobTypePlaceholder'),
              options: getJobTypeOptions(),
              onChange: (value) => {
                if (value != JobTypeEnum.SQL) {
                  formModel.resourceFrom = String(ResourceFromEnum.PROJECT);
                }
              },
            };
          },
          defaultValue: JobTypeEnum.SQL,
          rules: [
            {
              required: true,
              message: t('spark.app.addAppTips.jobTypeIsRequiredMessage'),
              type: 'number',
            },
          ],
        },
      ];
    }
  });
  const formSchema = computed((): FormSchema[] => {
    return [
      ...getJobTypeSchema.value,
      {
        field: 'deployMode',
        label: t('spark.app.deployMode'),
        component: 'Select',
        itemProps: {
          autoLink: false, //Resolve multiple trigger validators with null value ·
        },
        componentProps: {
          placeholder: t('spark.app.addAppTips.deployModePlaceholder'),
          options: deployModes,
        },
        rules: [
          {
            required: true,
            validator: async (_rule, value) => {
              if (value === null || value === undefined || value === '') {
                return Promise.reject(t('spark.app.addAppTips.deployModeIsRequiredMessage'));
              } else {
                return Promise.resolve();
              }
            },
          },
        ],
      },
      {
        field: 'versionId',
        label: 'Spark Version',
        component: 'Select',
        componentProps: {
          placeholder: 'Spark Version',
          options: unref(sparkEnvs),
          fieldNames: { label: 'sparkName', value: 'id', options: 'options' },
        },
        rules: [
          { required: true, message: t('spark.app.addAppTips.sparkVersionIsRequiredMessage') },
        ],
      },
      {
        field: 'sparkSql',
        label: 'Spark SQL',
        component: 'Input',
        slot: 'sparkSql',
        ifShow: ({ values }) => values?.jobType == JobTypeEnum.SQL,
        rules: [{ required: true, message: t('spark.app.addAppTips.sparkSqlIsRequiredMessage') }],
      },
      {
        field: 'jar',
        label: t('spark.app.resource'),
        component: 'Select',
        render: ({ model }) => renderStreamParkResource({ model, resources: unref(teamResource) }),
        ifShow: ({ values }) => values.jobType == JobTypeEnum.JAR,
        rules: [{ required: true, message: t('spark.app.addAppTips.sparkAppRequire') }],
      },
      {
        field: 'mainClass',
        label: t('spark.app.mainClass'),
        component: 'Input',
        componentProps: { placeholder: t('spark.app.addAppTips.mainClassPlaceholder') },
        ifShow: ({ values }) => values?.jobType == JobTypeEnum.JAR,
        rules: [{ required: true, message: t('spark.app.addAppTips.mainClassIsRequiredMessage') }],
      },
      {
        field: 'appName',
        label: t('spark.app.appName'),
        component: 'Input',
        componentProps: { placeholder: t('spark.app.addAppTips.appNamePlaceholder') },
        dynamicRules: ({ model }) => {
          return [
            {
              required: true,
              trigger: 'blur',
              validator: (rule: RuleObject, value: StoreValue) =>
                getJobNameCheck(rule, value, model),
            },
          ];
        },
      },
      {
        field: 'args',
        label: t('spark.app.programArgs'),
        component: 'InputTextArea',
        defaultValue: '',
        slot: 'args',
        ifShow: ({ model }) => [JobTypeEnum.JAR, JobTypeEnum.PYSPARK].includes(model?.jobType),
      },
      {
        field: 'appProperties',
        label: 'Spark Properties',
        component: 'InputTextArea',
        componentProps: {
          rows: 4,
          placeholder: '--conf, -c PROP=VALUE Arbitrary Spark configuration property.',
        },
      },
      { field: 'configOverride', label: '', component: 'Input', show: false },
      {
        field: 'isSetConfig',
        label: t('spark.app.appConf'),
        component: 'Switch',
        render({ model, field }) {
          return renderIsSetConfig(model, field, registerConfDrawer, openConfDrawer);
        },
      },
      {
        field: 'tags',
        label: t('spark.app.tags'),
        component: 'Input',
        componentProps: {
          placeholder: t('spark.app.addAppTips.tagsPlaceholder'),
        },
      },
      {
        field: 'hadoopUser',
        label: t('spark.app.hadoopUser'),
        component: 'Input',
        ifShow: ({ values }) =>
          values?.deployMode == DeployMode.YARN_CLIENT ||
          values?.deployMode == DeployMode.YARN_CLUSTER,
      },
      {
        field: 'lineageEnable',
        label: t('spark.app.lineageEnable'),
        component: 'Switch',
        defaultValue: false,
        componentProps: { checkedChildren: 'ON', unCheckedChildren: 'OFF' },
        afterItem: () => h('span', { class: 'pop-tip' }, t('spark.app.lineageEnableTip')),
      },
      {
        field: 'yarnQueue',
        label: t('spark.app.yarnQueue'),
        component: 'Input',
        ifShow: ({ values }) =>
          values?.deployMode == DeployMode.YARN_CLIENT ||
          values?.deployMode == DeployMode.YARN_CLUSTER,
        render: ({ model, field }) => {
          return (
            <div>
              <Select
                name="yarnQueue"
                placeholder={t('setting.yarnQueue.placeholder.yarnQueueLabelExpression')}
                fieldNames={{ label: 'queueLabel', value: 'queueLabel' }}
                v-model={[model[field], 'value']}
                showSearch={true}
              />
              <p class="conf-desc mt-10px">
                <span class="note-info">
                  <Tag color="#2db7f5" class="tag-note">
                    {t('spark.app.noteInfo.note')}
                  </Tag>
                  {t('setting.yarnQueue.selectionHint')}
                </span>
              </p>
            </div>
          );
        },
      },
      {
        field: 'description',
        label: t('common.description'),
        component: 'InputTextArea',
        componentProps: { rows: 4, placeholder: t('spark.app.addAppTips.descriptionPlaceholder') },
      },
    ];
  });
  onMounted(async () => {
    /* Get team dependencies */
    fetchTeamResource({}).then((res) => {
      teamResource.value = res;
    });
    fetchYarnQueueList({
      page: 1,
      pageSize: 9999,
    }).then((res) => {
      yarnQueue.value = res.records;
    });
    fetchVariableAll().then((res) => {
      suggestions.value = res.map((v) => {
        return {
          text: v.variableCode,
          description: v.description,
          value: v.variableValue,
        };
      });
    });
  });
  return {
    formSchema,
    suggestions,
  };
}
