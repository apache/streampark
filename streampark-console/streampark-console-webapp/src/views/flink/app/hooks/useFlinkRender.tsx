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
import { RenderCallbackParams } from '/@/components/Form/src/types/form';
import { Icon, SvgIcon } from '/@/components/Icon';
import options from '../data/option';

import Mergely from '../components/Mergely.vue';
import CustomForm from '../components/CustomForm';
import {
  Alert,
  Dropdown,
  Form,
  Input,
  InputNumber,
  Menu,
  Select,
  Switch,
  Tag,
} from 'ant-design-vue';
import { Button } from '/@/components/Button';
import { descriptionFilter } from '../utils';
import { SettingTwoTone } from '@ant-design/icons-vue';
import { ref, unref } from 'vue';
import { handleConfTemplate } from '/@/api/flink/config';
import { decodeByBase64 } from '/@/utils/cipher';
import { useMessage } from '/@/hooks/web/useMessage';
import { SelectValue } from 'ant-design-vue/lib/select';
import {
  CandidateTypeEnum,
  FailoverStrategyEnum,
  RestoreModeEnum,
  ClusterStateEnum,
  DeployMode,
  JobTypeEnum,
} from '/@/enums/flinkEnum';
import { useI18n } from '/@/hooks/web/useI18n';
import { fetchYarnQueueList } from '/@/api/setting/yarnQueue';
import { ApiSelect } from '/@/components/Form';
import { ResourceTypeEnum } from '/@/views/resource/upload/upload.data';

const { t } = useI18n();
/* render input dropdown component */
export const renderInputDropdown = (
  formModel: Recordable,
  field: string,
  componentProps: { placeholder: string; options: Array<string> },
) => {
  return (
    <Input
      type="text"
      placeholder={componentProps.placeholder}
      allowClear
      value={formModel[field]}
      onInput={(e) => (formModel[field] = e.target.value)}
    >
      {{
        addonAfter: () => (
          <Dropdown placement="bottomRight">
            {{
              overlay: () => (
                <div>
                  <Menu trigger="['click', 'hover']">
                    {componentProps.options.map((item) => {
                      return (
                        <Menu.Item
                          key={item}
                          onClick={() => (formModel[field] = item)}
                          class="pr-60px"
                        >
                          <Icon icon="ant-design:plus-circle-outlined" />
                          {{ item }}
                        </Menu.Item>
                      );
                    })}
                  </Menu>
                  <Icon icon="ant-design:history-outlined" />
                </div>
              ),
            }}
          </Dropdown>
        ),
      }}
    </Input>
  );
};

export function handleCheckCheckpoint(values: Recordable) {
  const { cpMaxFailureInterval, cpFailureRateInterval, cpFailureAction } = values.checkPointFailure;
  if (cpMaxFailureInterval != null && cpFailureRateInterval != null && cpFailureAction != null) {
    if (cpFailureAction === FailoverStrategyEnum.ALERT) {
      if (values.alertId == null) {
        // this.form.setFields({
        //   alertEmail: {
        //     errors: [new Error('checkPoint failure trigger is alert,email must not be empty')],
        //   },
        // });
        return Promise.reject('trigger action is alert,Fault Alert Template must not be empty');
      } else {
        return Promise.resolve();
      }
    } else {
      return Promise.resolve();
    }
  } else if (
    cpMaxFailureInterval == null &&
    cpFailureRateInterval == null &&
    cpFailureAction == null
  ) {
    return Promise.resolve();
  } else {
    return Promise.reject('options all required or all empty');
  }
}

/* render input Group component */
export const renderInputGroup = ({ model }) => {
  if (!Reflect.has(model, 'checkPointFailure')) {
    model.checkPointFailure = {};
  }
  const handleUpdate = (value: any) => {
    Object.assign(model, {
      checkPointFailure: value,
    });
  };
  return (
    <Form.Item
      label={t('flink.app.noteInfo.checkPointFailureOptions')}
      name="checkPointFailure"
      rules={[{ validator: () => handleCheckCheckpoint(model) }]}
    >
      <CustomForm
        value={{
          cpMaxFailureInterval: model.checkPointFailure.cpMaxFailureInterval,
          cpFailureRateInterval: model.checkPointFailure.cpFailureRateInterval,
          cpFailureAction: model.checkPointFailure.cpFailureAction,
        }}
        onUpdateValue={handleUpdate}
      ></CustomForm>
    </Form.Item>
  );
};
/*Gets the selection of totalOptions */
function getTotalOptions() {
  return [
    {
      name: 'process memory(进程总内存)',
      options: options.filter((x) => x.group === 'process-memory'),
    },
    {
      name: 'total memory(Flink 总内存)',
      options: options.filter((x) => x.group === 'total-memory'),
    },
  ];
}
/* render total memory component */
export const renderTotalMemory = ({ model, field }: RenderCallbackParams) => {
  return (
    <div>
      <Select
        show-search
        allow-clear
        mode="multiple"
        max-tag-count={2}
        options={getTotalOptions()}
        field-names={{ label: 'name', value: 'key', options: 'options' }}
        value={model[field]}
        onChange={(value) => (model[field] = value)}
        placeholder="Please select the resource parameters to set"
      />
      <p class="conf-desc mt-10px">
        <span class="note-info">
          <Tag color="#2db7f5" class="tag-note">
            {t('flink.app.noteInfo.note')}
          </Tag>
          <span>{t('flink.app.noteInfo.totalMemoryNote')}</span>
        </span>
      </p>
    </div>
  );
};

function hasOptions(items) {
  return options.filter((x) => items.includes(x.key)) as any;
}
/* render memory option */
export const renderOptionsItems = (
  model: Recordable,
  parentField: string,
  field: string,
  reg: string,
  replace = false,
) => {
  return hasOptions(model[parentField] || []).map((conf: Recordable) => {
    let label = conf.name.replace(new RegExp(reg, 'g'), '');
    if (replace) label = label.replace(/\./g, ' ');
    if (!Reflect.has(model, field)) {
      model[field] = {};
    }
    if (!model[field][conf.key]) {
      model[field][conf.key] = conf.defaultValue || null;
    }
    if (!Reflect.has(model, conf.key)) {
      model[conf.key] = model[field][conf.key] || conf.defaultValue || null;
    }

    function handleValueChange(value: string) {
      model[conf.key] = value;
      model[field][conf.key] = value;
    }
    return (
      <Form.Item label={label} name={conf.key} key={`${field}.${conf.key}`}>
        {conf.type === 'number' && (
          <InputNumber
            class="!w-full"
            min={conf.min}
            max={conf.max}
            step={conf.step}
            value={model[field][conf.key]}
            onChange={handleValueChange}
            rules={[{ validator: conf.validator }]}
          />
        )}
        {conf.type === 'switch' && <span class="tip-info">({conf.placeholder})</span>}
        <p class="conf-desc"> {descriptionFilter(conf)} </p>
      </Form.Item>
    );
  });
};

/* render Yarn Queue */
export const renderYarnQueue = ({ model, field }: RenderCallbackParams) => {
  return (
    <div>
      <ApiSelect
        name="yarnQueue"
        placeholder={t('setting.yarnQueue.placeholder.yarnQueueLabelExpression')}
        api={fetchYarnQueueList}
        params={{ page: 1, pageSize: 9999 }}
        resultField={'records'}
        labelField={'queueLabel'}
        valueField={'queueLabel'}
        showSearch={true}
        value={model[field]}
        onChange={(value: string) => (model[field] = value)}
      />
      <p class="conf-desc mt-10px">
        <span class="note-info">
          <Tag color="#2db7f5" class="tag-note">
            {t('flink.app.noteInfo.note')}
          </Tag>
          {t('setting.yarnQueue.selectionHint')}
        </span>
      </p>
    </div>
  );
};

export const renderFlinkCluster = (clusters, { model, field }: RenderCallbackParams) => {
  return (
    <Select
      placeholder={t('flink.app.flinkCluster')}
      value={model[field]}
      onChange={(value: any) => (model[field] = value)}
      codeField={field}
    >
      {clusters.map((item) => {
        return (
          <Select.Option key={item.id}>
            {item.label}
            <span style="margin-left: 50px;">
              {item.state == ClusterStateEnum.CREATED && (
                <Tag color="#108ee9">{t('flink.app.clusterState.created')}</Tag>
              )}
              {item.state == ClusterStateEnum.RUNNING && (
                <Tag color="#52c41a">{t('flink.app.clusterState.started')}</Tag>
              )}
              {item.state == ClusterStateEnum.CANCELED && (
                <Tag color="#fa8c16">{t('flink.app.clusterState.canceled')}</Tag>
              )}
              {item.state == ClusterStateEnum.LOST && (
                <Tag color="#333333">{t('flink.app.clusterState.lost')}</Tag>
              )}
            </span>
          </Select.Option>
        );
      })}
    </Select>
  );
};

export const renderJobName = ({ model, field }: RenderCallbackParams) => {
  return (
    <div>
      <Input
        name="jobName"
        placeholder={t('flink.app.addAppTips.appNamePlaceholder')}
        value={model[field]}
        onInput={(e: ChangeEvent) => (model[field] = e?.target?.value)}
      />
      <p class="conf-desc mt-10px">
        <span class="note-info">
          <Tag color="#2db7f5" class="tag-note">
            {t('flink.app.noteInfo.note')}
          </Tag>
          {model.deployMode == DeployMode.KUBERNETES_APPLICATION && (
            <span>
              {t('flink.app.addAppTips.appNameK8sClusterIdRole')}
              <div>
                <Tag color="orange"> 1.</Tag>
                {t('flink.app.addAppTips.appNameK8sClusterIdRoleLength')}
              </div>
              <div>
                <Tag color="orange"> 2.</Tag>
                {t('flink.app.addAppTips.appNameK8sClusterIdRoleRegexp')}
              </div>
            </span>
          )}
          {model.deployMode != DeployMode.KUBERNETES_APPLICATION && (
            <span>
              <span>{t('flink.app.addAppTips.appNameRole')}</span>
              <span>{t('flink.app.addAppTips.appNameRoleContent')}</span>
            </span>
          )}
        </span>
      </p>
    </div>
  );
};

/* render memory option */
export const renderDynamicProperties = ({ model, field }: RenderCallbackParams) => {
  return (
    <div>
      <Input.TextArea
        rows={8}
        name="dynamicProperties"
        placeholder="$key=$value,If there are multiple parameters,you can new line enter them (-D <arg>)"
        value={model[field]}
        onInput={(e: ChangeEvent) => (model[field] = e?.target?.value)}
      />
      <p class="conf-desc mt-10px">
        <span class="note-info">
          <Tag color="#2db7f5" class="tag-note">
            {t('flink.app.noteInfo.note')}
          </Tag>
          <a
            href="https://ci.apache.org/projects/flink/flink-docs-stable/ops/config.html"
            target="_blank"
            class="pl-5px"
          >
            Flink {t('flink.app.noteInfo.officialDoc')}
          </a>
        </span>
      </p>
    </div>
  );
};

export const getAlertSvgIcon = (name: string, text: string) => {
  return (
    <Alert type="info">
      {{
        message: () => (
          <div>
            <SvgIcon class="mr-8px" name={name} style={{ color: '#108ee9' }} />
            <span>{text}</span>
          </div>
        ),
      }}
    </Alert>
  );
};
/* render application conf */
export const renderIsSetConfig = (
  model: Recordable,
  field: string,
  registerConfDrawer: Fn,
  openConfDrawer: Fn,
) => {
  /* Open the sqlConf drawer */
  async function handleSQLConf(checked: boolean) {
    if (checked) {
      if (unref(model.configOverride)) {
        openConfDrawer(true, {
          configOverride: unref(model.configOverride),
        });
      } else {
        const res = await handleConfTemplate();
        openConfDrawer(true, {
          configOverride: decodeByBase64(res),
        });
      }
    } else {
      openConfDrawer(false);
      Object.assign(model, {
        configOverride: null,
        isSetConfig: false,
      });
    }
  }
  function handleEditConfClose() {
    if (!model.configOverride) {
      model.isSetConfig = false;
    }
  }
  function handleMergeSubmit(data: { configOverride: string; isSetConfig: boolean }) {
    if (data.configOverride == null || !data.configOverride.replace(/^\s+|\s+$/gm, '')) {
      Object.assign(model, {
        configOverride: null,
        isSetConfig: false,
      });
    } else {
      Object.assign(model, {
        configOverride: data.configOverride,
        isSetConfig: true,
      });
    }
  }
  function handleConfChange(checked: boolean) {
    model[field] = checked;
    if (checked) {
      handleSQLConf(true);
    }
  }
  return (
    <div>
      <Switch
        checked-children="ON"
        un-checked-children="OFF"
        checked={model[field]}
        onChange={handleConfChange}
      />
      {model[field] && (
        <SettingTwoTone
          class="ml-10px"
          theme="twoTone"
          two-tone-color="#4a9ff5"
          onClick={() => handleSQLConf(true)}
        />
      )}

      <Mergely
        onOk={handleMergeSubmit}
        onClose={() => handleEditConfClose()}
        onRegister={registerConfDrawer}
      />
    </div>
  );
};

// render history version form item
export const renderSqlHistory = (
  { model, flinkSqlHistory },
  { handleChangeSQL, handleCompareOk }: { handleChangeSQL: Fn; handleCompareOk: Fn },
) => {
  const { createConfirm } = useMessage();
  const compareSQL = ref<string[]>([]);

  function handleSelectChange(value: SelectValue) {
    model.flinkSqlHistory = value;
    handleChangeSQL(value);
  }

  //version compact
  function handleCompactSQL() {
    createConfirm({
      title: () => (
        <div>
          <Icon icon="ant-design:swap-outlined" style="color: #4a9ff5" />
          <span class="pl-10px"> {t('flink.app.flinkSql.compare')} </span>
        </div>
      ),
      okText: t('flink.app.flinkSql.compare'),
      width: 600,
      content: () => {
        return (
          <Form class="!pt-30px">
            <Form.Item
              label={t('flink.app.flinkSql.version')}
              label-col={{ lg: { span: 5 }, sm: { span: 7 } }}
              wrapper-col={{ lg: { span: 16 }, sm: { span: 17 } }}
            >
              <Select
                placeholder={t('flink.app.flinkSql.compareFlinkSQL')}
                value={unref(compareSQL)}
                onChange={(value: any) => (compareSQL.value = value)}
                mode="multiple"
                maxTagCount={2}
              >
                {renderSelectOptions()}
              </Select>
            </Form.Item>
          </Form>
        );
      },
      onOk: () => handleCompareOk(compareSQL.value),
      onCancel: () => {
        compareSQL.value.length = 0;
      },
    });
  }

  const renderSelectOptions = (isCompareSelect = false) => {
    const isDisabled = (ver: Recordable) => {
      if (!isCompareSelect) return false;
      return compareSQL.value.length == 2 && compareSQL.value.findIndex((i) => i === ver.id) === -1;
    };
    console.log('flinkSqlHistory', flinkSqlHistory);
    return (flinkSqlHistory || []).map((ver) => {
      return (
        <Select.Option key={ver.id} disabled={isDisabled(ver)}>
          <div>
            <Button type="primary" shape="circle" size="small">
              {ver.version}
            </Button>
            {ver.effective && (
              <Tag color="green" style=";margin-left: 5px;" size="small">
                {t('flink.app.flinkSql.effectiveVersion')}
              </Tag>
            )}

            {[CandidateTypeEnum.NEW, CandidateTypeEnum.HISTORY].includes(ver.candidate) && (
              <Tag color="cyan" style=";margin-left: 5px;" size="small">
                {t('flink.app.flinkSql.candidateVersion')}
              </Tag>
            )}

            <span style="color: darkgrey">
              <Icon icon="ant-design:clock-circle-outlined" />
              {ver.createTime}
            </span>
          </div>
        </Select.Option>
      );
    });
  };

  return (
    <div>
      <Select
        onChange={(value: SelectValue) => handleSelectChange(value)}
        value={model.flinkSqlHistory}
        style="width: calc(100% - 60px)"
      >
        {renderSelectOptions()}
      </Select>
      <Button type="primary" class="ml-10px w-50px" onClick={() => handleCompactSQL()}>
        <Icon icon="ant-design:swap-outlined" />
      </Button>
    </div>
  );
};

export const renderCompareSelectTag = (ver: any) => {
  return (
    <div>
      <Button type="primary" shape="circle" size="small">
        {ver.version}
      </Button>
      {ver.effective && (
        <Tag color="green" style=";margin-left: 5px;" size="small">
          {t('flink.app.flinkSql.effectiveVersion')}
        </Tag>
      )}

      {[CandidateTypeEnum.NEW, CandidateTypeEnum.HISTORY].includes(ver.candidate) && (
        <Tag color="cyan" style=";margin-left: 5px;" size="small">
          {t('flink.app.flinkSql.candidateVersion')}
        </Tag>
      )}
    </div>
  );
};

export const renderResourceFrom = (model: Recordable) => {
  return (
    <Select
      onChange={(value: string) => (model.resourceFrom = value)}
      value={model.resourceFrom}
      placeholder="Please select resource from">
      <Select.Option value="1">
        <SvgIcon name="github" />
        <span class="pl-10px">Project</span>
        <span class="gray"> (build from Project)</span>
      </Select.Option>
      <Select.Option value="2">
        <SvgIcon name="upload" />
        <span class="pl-10px">Upload</span>
        <span class="gray"> (upload local job)</span>
      </Select.Option>
      {model.deployMode == DeployMode.KUBERNETES_APPLICATION && model.jobType == JobTypeEnum.JAR && (
      <Select.Option value="3">
          <SvgIcon name="docker" />
          <span class="pl-10px">Image</span>
          <span class="gray"> (From Flink Base Image)</span>
      </Select.Option>
       )}
    </Select>
  );
};

export const renderStreamParkResource = ({ model, resources }) => {
  const renderOptions = () => {
    return (resources || [])
      .filter((item) => item.resourceType !== ResourceTypeEnum.APP)
      .map((resource) => {
        return (
          <Select.Option
            key={resource.id}
            label={resource.resourceType + '-' + resource.resourceName}
          >
            <div>
              <Tag color="green" style=";margin-left: 5px;" size="small">
                {resource.resourceType}
              </Tag>
              <span style="color: darkgrey">{resource.resourceName}</span>
            </div>
          </Select.Option>
        );
      });
  };

  return (
    <div>
      <Select
        show-search
        allow-clear
        optionFilterProp="label"
        mode="multiple"
        max-tag-count={3}
        onChange={(value) => (model.teamResource = value)}
        value={model.teamResource}
        placeholder={t('flink.app.resourcePlaceHolder')}
      >
        {renderOptions()}
      </Select>
    </div>
  );
};

export const renderStreamParkJarApp = ({ model, resources }) => {
  function handleAppChange(value: SelectValue) {
    const res = resources.filter((item) => item.id == value)[0];
    model.mainClass = res.mainClass;
    model.uploadJobJar = res.resourceName;
  }

  const renderOptions = () => {
    console.log('resources', resources);
    return (resources || [])
      .filter((item) => item.resourceType == ResourceTypeEnum.APP)
      .map((resource) => {
        return (
          <Select.Option key={resource.id} label={resource.resourceName}>
            <div>
              <Tag color="green" style=";margin-left: 5px;" size="small">
                {resource.resourceType}
              </Tag>
              <span style="color: darkgrey">{resource.resourceName}</span>
            </div>
          </Select.Option>
        );
      });
  };

  return (
    <div>
      <Select
        show-search
        allow-clear
        optionFilterProp="label"
        onChange={handleAppChange}
        value={model.uploadJobJar}
        placeholder={t('flink.app.selectAppPlaceHolder')}
      >
        {renderOptions()}
      </Select>
    </div>
  );
};

export const renderFlinkAppRestoreMode = ({ model, field }: RenderCallbackParams) => {
  return (
    <div>
      <Select
        value={model[field]}
        onChange={(value) => (model[field] = value)}
        placeholder="Please select restore mode"
      >
        <Select.Option key="no_claim" value={RestoreModeEnum.NO_CLAIM}>
          <Tag color="gray" style=";margin-left: 5px;" size="small">
            NO_CLAIM
          </Tag>
        </Select.Option>
        <Select.Option key="claim" value={RestoreModeEnum.CLAIM}>
          <Tag color="#108ee9" style=";margin-left: 5px;" size="small">
            CLAIM
          </Tag>
        </Select.Option>
        <Select.Option key="legacy" value={RestoreModeEnum.LEGACY}>
          <Tag color="#8E50FF" style=";margin-left: 5px;" size="small">
            LEGACY
          </Tag>
        </Select.Option>
      </Select>
      <p class="mt-10px">
        <span class="note-info">
          <Tag color="#2db7f5" class="tag-note" size="small">
            {t('flink.app.noteInfo.note')}
          </Tag>
          <span class="tip-info">{t('flink.app.restoreModeTip')}</span>
        </span>
      </p>
    </div>
  );
};
