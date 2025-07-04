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
<template>
  <BasicModal :width="600" @register="registerModal" showFooter>
    <template #title>
      <SvgIcon name="variable" />
      {{ getTitle }}
    </template>
    <div class="mt-3">
      <BasicForm @register="registerForm" :schemas="getTeamFormSchema" />
    </div>
    <template #footer>
      <div>
        <a-button class="e2e-var-cancel-btn" @click="closeModal">
          {{ t('common.cancelText') }}
        </a-button>
        <a-button class="e2e-var-submit-btn" type="primary" @click="handleSubmit()">
          {{ t('common.submitText') }}
        </a-button>
      </div>
    </template>
  </BasicModal>
</template>

<script lang="ts" setup>
  import { ref, h, computed, unref, reactive } from 'vue';
  import { BasicForm, FormSchema, useForm } from '/@/components/Form';
  import { BasicModal, useModalInner } from '/@/components/Modal';
  import { SvgIcon } from '/@/components/Icon';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { useFormValidate } from '/@/hooks/web/useFormValidate';
  import {
    fetchAddVariable,
    fetchCheckVariableCode,
    fetchUpdateVariable,
    fetchVariableInfo,
  } from '/@/api/resource/variable';
  import { VariableListRecord } from '/@/api/resource/variable/model/variableModel';

  const emit = defineEmits(['success', 'register']);

  const { t } = useI18n();

  const isUpdate = ref(false);
  const { getItemProp, setValidateStatus, setHelp } = useFormValidate();
  const variableId = ref<Nullable<number>>(null);
  const variableInfo = reactive<Partial<VariableListRecord>>({});

  async function handleVariableCodeBlur(e) {
    const value = (e && e.target.value) || '';
    if (value.length) {
      setValidateStatus('validating');
      if (value.length < 3 || value.length > 50) {
        setValidateStatus('error');
        setHelp(t('flink.variable.form.len'));
        return Promise.reject();
      } else if (!new RegExp(/^([A-Za-z])+([A-Za-z0-9._-])+$/).test(value)) {
        setValidateStatus('error');
        setHelp(t('flink.variable.form.regExp'));
      } else {
        const { data } = await fetchCheckVariableCode({
          variableCode: value,
        });
        if (data.status !== 'success') {
          setValidateStatus('error');
          setHelp(data.message);
          return Promise.reject();
        } else if (data.data) {
          setValidateStatus('success');
          setHelp('');
          return Promise.resolve();
        } else {
          setValidateStatus('error');
          setHelp(t('flink.variable.form.exists'));
          return Promise.reject();
        }
      }
    } else {
      setValidateStatus('error');
      setHelp(t('flink.variable.form.empty'));
      return Promise.reject();
    }
  }

  const getTeamFormSchema = computed((): FormSchema[] => {
    return [
      {
        field: 'variableCode',
        label: t('flink.variable.table.variableCode'),
        component: 'Input',
        componentProps: {
          placeholder: t('flink.variable.table.variableCode'),
          disabled: unref(isUpdate),
          onblur: handleVariableCodeBlur,
        },
        itemProps: getItemProp.value,
        rules: unref(isUpdate) ? [] : [{ required: true }],
      },
      {
        field: 'variableValue',
        label: t('flink.variable.table.variableValue'),
        component: 'InputTextArea',
        componentProps: {
          rows: 2,
          placeholder: t('flink.variable.table.variableValuePlaceholder'),
        },
        rules: [{ required: true, message: t('flink.variable.table.variableValuePlaceholder') }],
      },
      {
        field: 'desensitization',
        label: t('flink.variable.form.desensitization'),
        component: 'Switch',
        componentProps: {
          checkedChildren: 'ON',
          unCheckedChildren: 'OFF',
        },
        defaultValue: false,
        afterItem: () =>
          h('span', { class: 'pop-tip' }, t('flink.variable.form.desensitizationDesc')),
      },
      {
        field: 'description',
        label: t('common.description'),
        component: 'InputTextArea',
        componentProps: { rows: 2 },
        rules: [{ max: 100, message: t('flink.variable.form.descriptionMessage') }],
      },
    ];
  });

  const [registerForm, { resetFields, setFieldsValue, validate }] = useForm({
    name: 'VariableForm',
    colon: true,
    showActionButtonGroup: false,
    layout: 'vertical',
    baseColProps: { span: 22, offset: 1 },
  });

  const [registerModal, { setModalProps, changeLoading, closeModal }] = useModalInner(
    async (data: Recordable) => {
      try {
        variableId.value = null;
        resetFields();
        setValidateStatus('');
        setHelp('');
        setModalProps({ confirmLoading: false });
        isUpdate.value = !!data?.isUpdate;
        if (unref(isUpdate)) {
          // is desensitization variable
          if (data.record.desensitization) {
            changeLoading(true);
            const res = await fetchVariableInfo({
              id: data.record.id,
            });
            Object.assign(variableInfo, res);
            changeLoading(false);
          } else {
            Object.assign(variableInfo, data.record);
          }
          variableId.value = data.record.id;
          setFieldsValue(variableInfo);
        }
      } catch (error) {
        changeLoading(false);
      }
    },
  );

  const getTitle = computed(() =>
    !unref(isUpdate) ? t('flink.variable.addVariable') : t('flink.variable.modifyVariable'),
  );
  // form submit
  async function handleSubmit() {
    try {
      const values = await validate();
      setModalProps({ confirmLoading: true });
      await (isUpdate.value
        ? fetchUpdateVariable({ id: variableId.value, ...values })
        : fetchAddVariable(values));
      closeModal();
      emit('success', isUpdate.value);
    } finally {
      setModalProps({ confirmLoading: false });
    }
  }
</script>
