import { defHttp } from '/@/utils/http/axios';
import { CatalogParams, DatabaseParams, TableParams } from './catalog.type';

enum API {
  CATALOG_LIST = '/flink/catalog/list',
  CATALOG_CREATE = '/flink/catalog/create',
  CATALOG_DELETE = '/flink/catalog/delete',
  DATABASE_LIST = '/flink/database/list',
  DATABASE_CREATE = '/flink/database/create',
  DATABASE_DELETE = '/flink/database/delete',
  TABLE_LIST = '/flink/table/list',
  TABLE_CREATE = '/flink/table/create',
  TABLE_DELETE = '/flink/table/delete',
}


export function fetchCatalogList(params?: any) {
  return defHttp.post({ url: API.CATALOG_LIST, params });
}

export function fetchCreateCatalog(data: CatalogParams) {
  return defHttp.post({ url: API.CATALOG_CREATE, data });
}

export function fetchRemoveCatalog(id: string) {
  return defHttp.post({ url: API.CATALOG_DELETE, data: { id } });
}


export function fetchDatabaseList(params: { catalogId: string }) {
  return defHttp.post({ url: API.DATABASE_LIST, params });
}

export function fetchCreateDatabase(data: DatabaseParams) {
  return defHttp.post({ url: API.DATABASE_CREATE, data });
}

export function fetchRemoveDatabase(data: { catalogId: string; name: string }) {
  return defHttp.post({ url: API.DATABASE_DELETE, data });
}


export function fetchTableList(params: { catalogId: string; databaseName: string }) {
  return defHttp.post({ url: API.TABLE_LIST, params });
}

export function fetchCreateTable(data: TableParams) {
  return defHttp.post({ url: API.TABLE_CREATE, data });
}

export function fetchRemoveTable(data: { catalogId: string; databaseName: string; name: string }) {
  return defHttp.post({ url: API.TABLE_DELETE, data });
}
