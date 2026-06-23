/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.flink.core

import org.apache.streampark.common.util.Implicits.JavaList

import org.apache.flink.table.api._
import org.apache.flink.table.api.ModelDescriptor
import org.apache.flink.table.catalog.CatalogDescriptor
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.module.ModuleEntry
import org.apache.flink.table.resource.ResourceUri
import org.apache.flink.util.ParameterTool

class TableContext(override val parameter: ParameterTool, private val tableEnv: TableEnvironment)
  extends FlinkTableTrait(parameter, tableEnv) {

  def this(args: (ParameterTool, TableEnvironment)) = this(args._1, args._2)

  def this(args: TableEnvConfig) = this(FlinkTableInitializerV2.initialize(args))

  override def useModules(strings: String*): Unit =
    tableEnv.useModules(strings: _*)

  override def createTemporaryTable(path: String, descriptor: TableDescriptor): Unit = {
    tableEnv.createTemporaryTable(path, descriptor)
  }

  override def createTable(path: String, descriptor: TableDescriptor): Unit = {
    tableEnv.createTable(path, descriptor)
  }

  override def from(tableDescriptor: TableDescriptor): Table = {
    tableEnv.from(tableDescriptor)
  }

  override def listFullModules(): Array[ModuleEntry] =
    tableEnv.listFullModules()

  /** @since 1.15 */
  override def listTables(catalogName: String, databaseName: String): Array[String] =
    tableEnv.listTables(catalogName, databaseName)

  /** @since 1.15 */
  override def loadPlan(planReference: PlanReference): CompiledPlan =
    tableEnv.loadPlan(planReference)

  /** @since 1.15 */
  override def compilePlanSql(stmt: String): CompiledPlan =
    tableEnv.compilePlanSql(stmt)

  /** @since 1.17 */
  override def createFunction(
      path: String,
      className: String,
      resourceUris: JavaList[ResourceUri]): Unit =
    tableEnv.createFunction(path, className, resourceUris)

  /** @since 1.17 */
  override def createFunction(
      path: String,
      className: String,
      resourceUris: JavaList[ResourceUri],
      ignoreIfExists: Boolean): Unit =
    tableEnv.createFunction(path, className, resourceUris, ignoreIfExists)

  /** @since 1.17 */
  override def createTemporaryFunction(
      path: String,
      className: String,
      resourceUris: JavaList[ResourceUri]): Unit =
    tableEnv.createTemporaryFunction(path, className, resourceUris)

  /** @since 1.17 */
  override def createTemporarySystemFunction(
      name: String,
      className: String,
      resourceUris: JavaList[ResourceUri]): Unit =
    tableEnv.createTemporarySystemFunction(name, className, resourceUris)

  /** @since 1.17 */
  override def explainSql(
      statement: String,
      format: ExplainFormat,
      extraDetails: ExplainDetail*): String =
    tableEnv.explainSql(statement, format, extraDetails: _*)

  /** @since 1.18 */
  override def createCatalog(catalog: String, catalogDescriptor: CatalogDescriptor): Unit = {
    tableEnv.createCatalog(catalog, catalogDescriptor)
  }

  /** since Flink 2.0 */
  override def createTable(
      path: String,
      descriptor: TableDescriptor,
      ignoreIfExists: Boolean): Boolean =
    tableEnv.createTable(path, descriptor, ignoreIfExists)

  /** since Flink 2.0 */
  override def createTemporaryTable(
      path: String,
      descriptor: TableDescriptor,
      ignoreIfExists: Boolean): Unit =
    tableEnv.createTemporaryTable(path, descriptor, ignoreIfExists)

  /** since Flink 2.0 */
  override def createView(path: String, view: Table, ignoreIfExists: Boolean): Boolean =
    tableEnv.createView(path, view, ignoreIfExists)

  /** since Flink 2.0 */
  override def createView(path: String, view: Table): Unit =
    tableEnv.createView(path, view)

  /** since Flink 2.0 */
  override def dropTable(path: String, ignoreIfNotExists: Boolean): Boolean =
    tableEnv.dropTable(path, ignoreIfNotExists)

  /** since Flink 2.0 */
  override def dropTable(path: String): Boolean =
    tableEnv.dropTable(path)

  /** since Flink 2.0 */
  override def dropView(path: String, ignoreIfNotExists: Boolean): Boolean =
    tableEnv.dropView(path, ignoreIfNotExists)

  /** since Flink 2.0 */
  override def dropView(path: String): Boolean =
    tableEnv.dropView(path)

  /** since Flink 2.1 */
  override def createModel(path: String, descriptor: ModelDescriptor, ignoreIfExists: Boolean): Unit =
    tableEnv.createModel(path, descriptor, ignoreIfExists)

  /** since Flink 2.1 */
  override def createModel(path: String, descriptor: ModelDescriptor): Unit =
    tableEnv.createModel(path, descriptor)

  /** since Flink 2.1 */
  override def createTemporaryModel(path: String, descriptor: ModelDescriptor, ignoreIfExists: Boolean): Unit =
    tableEnv.createTemporaryModel(path, descriptor, ignoreIfExists)

  /** since Flink 2.1 */
  override def createTemporaryModel(path: String, descriptor: ModelDescriptor): Unit =
    tableEnv.createTemporaryModel(path, descriptor)

  /** since Flink 2.1 */
  override def dropModel(path: String, ignoreIfNotExists: Boolean): Boolean =
    tableEnv.dropModel(path, ignoreIfNotExists)

  /** since Flink 2.1 */
  override def dropModel(path: String): Boolean =
    tableEnv.dropModel(path)

  /** since Flink 2.1 */
  override def dropTemporaryModel(path: String): Boolean =
    tableEnv.dropTemporaryModel(path)

  /** since Flink 2.1 */
  override def fromCall(functionClass: Class[_ <: UserDefinedFunction], arguments: Object*): Table =
    tableEnv.fromCall(functionClass, arguments: _*)

  /** since Flink 2.1 */
  override def fromCall(functionName: String, arguments: Object*): Table =
    tableEnv.fromCall(functionName, arguments: _*)

  /** since Flink 2.1 */
  override def listModels(): Array[String] =
    tableEnv.listModels()

  /** since Flink 2.1 */
  override def listTemporaryModels(): Array[String] =
    tableEnv.listTemporaryModels()

  /** since Flink 2.2 */
  override def createFunction(
      path: String,
      descriptor: FunctionDescriptor,
      ignoreIfExists: Boolean): Unit =
    tableEnv.createFunction(path, descriptor, ignoreIfExists)

  /** since Flink 2.2 */
  override def createFunction(path: String, descriptor: FunctionDescriptor): Unit =
    tableEnv.createFunction(path, descriptor)

  /** since Flink 2.2 */
  override def createTemporaryFunction(path: String, descriptor: FunctionDescriptor): Unit =
    tableEnv.createTemporaryFunction(path, descriptor)

  /** since Flink 2.2 */
  override def createTemporarySystemFunction(name: String, descriptor: FunctionDescriptor): Unit =
    tableEnv.createTemporarySystemFunction(name, descriptor)

  /** since Flink 2.2 */
  override def fromModel(descriptor: ModelDescriptor): Model =
    tableEnv.fromModel(descriptor)

  /** since Flink 2.2 */
  override def fromModel(path: String): Model =
    tableEnv.fromModel(path)

  /** since Flink 2.2 */
  override def listMaterializedTables(): Array[String] =
    tableEnv.listMaterializedTables()

}
