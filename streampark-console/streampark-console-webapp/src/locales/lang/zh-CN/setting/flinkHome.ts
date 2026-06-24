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
export default {
  title: 'Flink Home',
  conf: 'Flink 配置',
  sync: '配置同步',
  edit: '编辑 Flink Home',
  delete: '确定要删除此 Flink home ?',
  flinkName: 'Flink名称',
  flinkNamePlaceholder: '请输入Flink别名',
  flinkHome: '安装路径',
  flinkHomePlaceholder: '请输入Flink安装路径',
  description: '描述',
  descriptionPlaceholder: 'Flink描述',
  flinkVersion: 'Flink 版本',
  searchByName: '根据 Flink 名称搜索',
  operateMessage: {
    flinkNameTips: 'Flink别名,举例: Flink-1.12',
    flinkNameIsRepeated: 'Flink名称已存在',
    flinkNameIsRequired: 'Flink名称必填',
    flinkHomeTips:
      'Flink所在服务器的绝对路径,举例: /usr/local/flink。Flink 2.x 运行需 JDK 11+；Flink 2.x shims 需 JDK 11+ 构建 StreamPark 才会打包，详见 script/FLINK_JDK_GUIDE.zh.md',
    flinkHomeIsRequired: 'Flink Home必填',
    flinkHomePathIsInvalid: 'Flink Home路径无效',
    flinkDistNotFound: 'flink/lib 路径下未找到 flink-dist jar文件',
    flinkDistIsRepeated: 'flink/lib 路径下存在多个flink-dist jar文件, 必须只能有一个!',
    createFlinkHomeSuccessful: ' 创建成功!',
    updateFlinkHomeSuccessful: ' 更新成功!',
  },
};
