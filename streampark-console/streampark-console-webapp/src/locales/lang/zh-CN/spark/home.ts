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
  title: 'Spark Home',
  sparkVersion: 'Spark 版本',
  searchByName: '按Spark名称搜索',
  conf: 'Spark Conf',
  sync: '同步 Conf',
  edit: '编辑 Spark Home',
  delete: '确定删除此 Spark home?',
  tips: {
    remove: '当前的 spark home 已被成功删除。',
    setDefault: '成功设置默认spark home',
    sparkName: 'Spark别名,举例: Spark-1.12',
    sparkHome: 'Spark所在服务器的绝对路径,举例: /usr/local/spark。Spark 4.x 需 JDK 17+，通常无需修改 StreamPark 服务 JDK，详见安装包 script/SPARK_JDK_GUIDE.zh.md',
    sparkNameIsRequired: 'Spark名称必填',
    sparkHomeIsRequired: 'Spark Home 不能为空',
    sparkNameIsRepeated: 'Spark名称已存在',
    sparkHomePathIsInvalid: 'Spark Home路径无效',
    sparkDistNotFound: 'spark/lib 路径下未找到 spark-dist jar文件',
    sparkDistIsRepeated: 'spark/lib 路径下存在多个spark-dist jar文件, 必须只能有一个!',
    createSparkHomeSuccessful: '创建成功!',
    updateSparkHomeSuccessful: '更新成功!',
  },
  form: {
    sparkName: 'Spark 名称',
    sparkHome: '安装路径',
    description: '描述',
  },
  placeholder: {
    sparkName: '请输入Spark别名',
    sparkHome: '请输入Spark安装路径',
    description: 'Spark描述',
  },
};
