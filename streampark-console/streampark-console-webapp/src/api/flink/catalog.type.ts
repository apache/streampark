export interface CatalogParams {
  catalogName: string;
  catalogType: string;
  description?: string;
  config?: Record<string, string>;
}

export interface CatalogRecord {
  id: string;
  catalogName: string;
  catalogType: string;
  createTime: string;
  updateTime: string;
}

export interface DatabaseParams {
  name: string;
  catalogId: string;
  catalogName?: string;
  description?: string;
  ignoreIfExits?: boolean;
}

export interface DatabaseRecord {
  name: string;
  catalogId: string;
  catalogName: string;
  description?: string;
}

export interface TableColumn {
  name: string;
  type: string;
  comment?: string;
}

export interface TableParams {
  catalogId: string;
  catalogName?: string;
  databaseName: string;
  name: string;
  description?: string;
  tableColumns?: TableColumn[];
  partitionKey?: string[];
  tableOptions?: Record<string, string>;
}

export interface TableRecord {
  name: string;
  catalogId: string;
  catalogName: string;
  databaseName: string;
  description?: string;
}
