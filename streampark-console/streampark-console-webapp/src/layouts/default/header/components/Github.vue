<script lang="ts" setup name="Github">
  import { Tooltip } from 'ant-design-vue';
  import Icon from '/@/components/Icon';
  import { GITHUB_URL } from '/@/settings/siteSetting';
</script>
<template>
  <Tooltip title="Github" placement="bottom" :mouseEnterDelay="0.5">
    <a :href="GITHUB_URL" target="_blank" class="text-black hover:text-black github">
      <Icon icon="ant-design:github-outlined" class="!pl-8px" style="font-size: 19px !important" />
      <img
        src="https://img.shields.io/github/stars/apache/streampark.svg?sanitize=true"
        class="px-5px hidden md:block"
      />

      <img
        src="https://img.shields.io/github/forks/apache/streampark.svg?sanitize=true"
        class="pr-8px hidden md:block"
      />
    </a>
  </Tooltip>
</template>
<style lang="less">
  [data-theme='dark'] {
    .github,
    .github:hover {
      color: #fff;
    }
  }
</style>
