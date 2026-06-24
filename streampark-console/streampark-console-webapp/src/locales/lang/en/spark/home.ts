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
  sparkVersion: 'Spark Version',
  searchByName: 'Search by Spark Name',
  conf: 'Spark Conf',
  sync: 'Sync Conf',
  edit: 'Edit Spark Home',
  delete: 'Are you sure delete this spark home ?',
  tips: {
    remove: 'The current spark home has been successfully deleted.',
    setDefault: 'Successfully set the default spark home.',
    sparkName: 'Spark alias, for example: Spark-1.12',
    sparkHome:
      'The absolute path of the server where Spark is located, for example: /usr/local/spark. Spark 4.x requires JDK 17+. StreamPark service JDK upgrade is usually not required. See script/SPARK_JDK_GUIDE.md in the installation package.',
    sparkNameIsRequired: 'Spark name is required',
    sparkHomeIsRequired: 'Spark Home is required',
    sparkNameIsRepeated: 'Spark name already exists',
    sparkHomePathIsInvalid: 'Spark Home path is invalid',
    sparkDistNotFound: 'spark-dist jar file not found in spark/lib path',
    sparkDistIsRepeated:
      'Multiple spark-dist jar files exist in spark/lib path, there must be only one!',
    createSparkHomeSuccessful: 'Creation successful!',
    updateSparkHomeSuccessful: 'Update successful!',
  },
  form: {
    sparkName: 'Spark Name',
    sparkHome: 'Spark Home',
    description: 'Description',
  },
  placeholder: {
    sparkName: 'Please enter Spark alias',
    sparkHome: 'Please enter Spark installation path',
    description: 'Spark description',
  },
};
