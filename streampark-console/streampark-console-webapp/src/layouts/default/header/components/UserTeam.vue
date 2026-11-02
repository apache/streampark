<!--
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
 -->
<template>
  <div v-if="teams.length > 1" class="flex items-center min-w-160px">
    <span class="text-blue-500 pr-10px">{{ t('system.team.team') }}:</span>
    <Select
      :value="String(userStore.getTeamId)"
      :options="teams"
      :loading="switching"
      :disabled="switching"
      :allow-clear="false"
      class="flex-1"
      size="small"
      @change="switchTeam"
    />
  </div>
</template>
<script setup lang="ts">
  import { onMounted, ref } from 'vue';
  import { Select } from 'ant-design-vue';
  import { fetchUserTeams, setUserTeam } from '/@/api/system/user';
  import { useUserStore } from '/@/store/modules/user';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { APP_TEAMID_KEY_, MULTIPLE_TABS_KEY } from '/@/enums/cacheEnum';
  import { PageEnum } from '/@/enums/pageEnum';
  import { Persistent } from '/@/utils/cache/persistent';
  import { router } from '/@/router';

  const { t } = useI18n();
  const userStore = useUserStore();
  const teams = ref<{ label: string; value: string }[]>([]);
  const switching = ref(false);

  onMounted(async () => {
    const result = await fetchUserTeams();
    teams.value = result.map((team) => ({ label: team.teamName, value: String(team.id) }));
  });

  async function switchTeam(value: string) {
    if (value === String(userStore.getTeamId)) return;
    switching.value = true;
    try {
      await setUserTeam(value);
      userStore.teamId = value;
      userStore.setUserInfo({ ...userStore.getUserInfo, lastTeamId: value });
      sessionStorage.setItem(APP_TEAMID_KEY_, value);
      localStorage.setItem(APP_TEAMID_KEY_, value);
      Persistent.removeLocal(MULTIPLE_TABS_KEY, true);
      sessionStorage.removeItem('appPageNo');
      // Reload the home page to discard cached views belonging to the previous team.
      window.location.assign(router.resolve(PageEnum.BASE_HOME).href);
    } finally {
      switching.value = false;
    }
  }
</script>
