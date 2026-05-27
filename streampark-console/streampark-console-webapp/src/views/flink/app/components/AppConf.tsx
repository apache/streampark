/*
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
*/

import { defineComponent, toRefs, unref } from 'vue';
import { Form, Input, Select } from 'ant-design-vue';
import { renderCompareSelectTag } from '../hooks/useFlinkRender';
import { Button } from '/@/components/Button';
import { decodeByBase64 } from '/@/utils/cipher';
import { fetchGetVer } from '/@/api/flink/config';

import { ApiTreeSelect } from '/@/components/Form';
import { fetchAppConf, fetchName } from '/@/api/flink/app/app';
import { SettingTwoTone } from '@ant-design/icons-vue';
import { fetchListConf } from '/@/api/flink/project';
import { UseStrategyEnum } from '/@/enums/flinkEnum';
import { useI18n } from '/@/hooks/web/useI18n';
const { t } = useI18n();

export default defineComponent({
  name: 'AppConf',
  props: {
    model: {
      type: Object as PropType<Recordable<any>>,
      required: true,
    },
    configVersions: {
      type: Array as PropType<Array<any>>,
      default: () => [],
    },
  },
  emit: ['openMergely'],
  setup(props, { emit }) {
    const { model } = toRefs(props);

    if (!model.value.strategy) {
      model.value.strategy = UseStrategyEnum.USE_EXIST;
    }

    async function handleChangeStrategy(strategy: number) {
      model.value.strategy = strategy;
    }

    async function handleChangeConfig(v: string) {
      const res = await fetchGetVer({ id: v });
      model.value.configId = res.id;
      model.value.configOverride = decodeByBase64(res.content);
    }

    async function handleChangeNewConfig(confFile: string) {
      const appName = await fetchName({
        projectId: unref(model).project,
        module: unref(model).module,
        config: confFile,
      });
      const appConf = await fetchAppConf({
        projectId: unref(model).project,
        module: unref(model).module,
        config: confFile,
      });
      model.value.config = confFile;
      model.value.jobName = appName;
      model.value.configOverride = decodeByBase64(appConf);
    }
    return () => {
      return (
        <div>
          <Input.Group compact>
            <Select
              class="mr-10px"
              style="width: 25%"
              value={unref(model).strategy}
              onChange={(value: any) => handleChangeStrategy(value)}
            >
              <Select.Option value={UseStrategyEnum.USE_EXIST}>
                {t('flink.app.useExisting')}
              </Select.Option>
              <Select.Option value={UseStrategyEnum.RESELECT}>
                {t('flink.app.reselect')}
              </Select.Option>
            </Select>
            {unref(model).strategy == UseStrategyEnum.USE_EXIST && (
              <Form.Item style="width: calc(75% - 75px);margin-left:10px;">
                <Select
                  class="!w-full"
                  onChange={(value: any) => handleChangeConfig(value)}
                  value={unref(model).configId}
                  placeholder="please select tag"
                >
                  {props.configVersions.map((ver) => {
                    return (
                      <Select.Option key={ver.id}>{renderCompareSelectTag(ver)}</Select.Option>
                    );
                  })}
                </Select>
              </Form.Item>
            )}
            {unref(model).strategy == UseStrategyEnum.RESELECT && (
              <Form.Item style="width: calc(75% - 60px)">
                <ApiTreeSelect
                  class="!w-full"
                  dropdownStyle={{ maxHeight: '400px', overflow: 'auto' }}
                  api={fetchListConf}
                  params={{ id: props.model.project, module: props.model.module }}
                  placeholder="Please select config"
                  tree-default-expand-all
                  onChange={(value: string) => handleChangeNewConfig(value)}
                ></ApiTreeSelect>
              </Form.Item>
            )}

            <Button
              disabled={
                unref(model).strategy == UseStrategyEnum.USE_EXIST ? false : !unref(model).config
              }
              type="primary"
              class="ml-10px w-50px"
              onClick={() => emit('openMergely', unref(model).configOverride)}
            >
              <SettingTwoTone two-tone-color="#4a9ff5" />
            </Button>
          </Input.Group>
        </div>
      );
    };
  },
});
