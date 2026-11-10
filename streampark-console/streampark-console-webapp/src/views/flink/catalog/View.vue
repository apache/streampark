<script lang="ts" setup name="CatalogView">
  import { ref, onMounted } from 'vue';
  import { PageWrapper } from '/@/components/Page';
  import { BasicTable, useTable } from '/@/components/Table';
  import { Card, Empty } from 'ant-design-vue';
  import { BasicTree } from '/@/components/Tree';
  import { fetchCatalogList, fetchDatabaseList, fetchTableList } from '/@/api/flink/catalog';
  import { useI18n } from '/@/hooks/web/useI18n';

  const { t } = useI18n();
  const treeData = ref<any[]>([]);
  const currentCatalogId = ref<string>('');
  const currentDatabaseName = ref<string>('');

  const [registerTable, { reload, setColumns }] = useTable({
    title: t('flink.catalog.tableList'),
    api: fetchTableList,
    columns: [
      { dataIndex: 'name', title: t('flink.catalog.tableName') },
      { dataIndex: 'description', title: t('common.description') },
    ],
    beforeFetch: (params) => {
      params.catalogId = currentCatalogId.value;
      params.databaseName = currentDatabaseName.value;
      return params;
    },
    immediate: false,
    useSearchForm: false,
    showTableSetting: true,
    canResize: true,
  });

  async function loadTree() {
    const catalogs = await fetchCatalogList();
    if (!catalogs) return;

    treeData.value = await Promise.all(
      catalogs.map(async (catalog) => {
        const dbs = await fetchDatabaseList({ catalogId: catalog.id });
        return {
          title: catalog.catalogName,
          key: catalog.id,
          children: dbs?.map((db) => ({
            title: db.name,
            key: `${catalog.id}:${db.name}`,
            isLeaf: true,
            catalogId: catalog.id,
            dbName: db.name,
          })),
        };
      }),
    );
  }

  function handleSelect(keys: string[], { node }) {
    if (keys.length === 0) return;
    if (node.dataRef.children) {
      // It's a catalog, maybe expand?
      return;
    }
    // It's a database
    currentCatalogId.value = node.dataRef.catalogId;
    currentDatabaseName.value = node.dataRef.dbName;
    reload();
  }

  onMounted(() => {
    loadTree();
  });
</script>

<template>
  <PageWrapper contentFullHeight class="flex p-4">
    <Card class="w-1/4 mr-4 !h-full overflow-auto" :title="t('flink.catalog.browser')">
      <BasicTree :treeData="treeData" @select="handleSelect" defaultExpandAll />
    </Card>
    <Card class="w-3/4 !h-full" :title="t('flink.catalog.tableInfo')">
      <div v-if="!currentDatabaseName" class="flex justify-center items-center h-full">
        <Empty :description="t('flink.catalog.selectDbHint')" />
      </div>
      <BasicTable v-else @register="registerTable" />
    </Card>
  </PageWrapper>
</template>
