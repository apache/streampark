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

import org.apache.streampark.common.util.Utils
import org.apache.streampark.flink.core.EnhancerImplicit._

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions._
import org.apache.flink.table.module.Module
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.util.ParameterTool

import java.lang
import java.util.Optional

abstract class FlinkTableTrait(val parameter: ParameterTool, private val tableEnv: TableEnvironment)
  extends TableEnvironment {

  def start(): JobExecutionResult = {
    val appName = parameter.getAppName(required = true)
    execute(appName)
  }

  def execute(jobName: String): JobExecutionResult = {
    Utils.printLogo(s"FlinkTable $jobName Starting...")
    null
  }

  def sql(sql: String = null): Unit =
    FlinkSqlExecutor.executeSql(sql, parameter, this)

  override def fromValues(values: Expression*): Table =
    tableEnv.fromValues(values)

  override def fromValues(rowType: AbstractDataType[_], values: Expression*): Table =
    tableEnv.fromValues(rowType, values: _*)

  override def fromValues(values: lang.Iterable[_]): Table =
    tableEnv.fromValues(values)

  override def fromValues(rowType: AbstractDataType[_], values: lang.Iterable[_]): Table =
    tableEnv.fromValues(rowType, values)

  override def registerCatalog(catalogName: String, catalog: Catalog): Unit =
    tableEnv.registerCatalog(catalogName, catalog)

  override def getCatalog(catalogName: String): Optional[Catalog] =
    tableEnv.getCatalog(catalogName)

  override def loadModule(moduleName: String, module: Module): Unit =
    tableEnv.loadModule(moduleName, module)

  override def unloadModule(moduleName: String): Unit =
    tableEnv.unloadModule(moduleName)

  override def createTemporarySystemFunction(
      name: String,
      functionClass: Class[_ <: UserDefinedFunction]): Unit =
    tableEnv.createTemporarySystemFunction(name, functionClass)

  override def createTemporarySystemFunction(
      name: String,
      functionInstance: UserDefinedFunction): Unit =
    tableEnv.createTemporarySystemFunction(name, functionInstance)

  override def dropTemporarySystemFunction(name: String): Boolean =
    tableEnv.dropTemporarySystemFunction(name)

  override def createFunction(path: String, functionClass: Class[_ <: UserDefinedFunction]): Unit =
    tableEnv.createFunction(path, functionClass)

  override def createFunction(
      path: String,
      functionClass: Class[_ <: UserDefinedFunction],
      ignoreIfExists: Boolean): Unit =
    tableEnv.createFunction(path, functionClass)

  override def dropFunction(path: String): Boolean = tableEnv.dropFunction(path)

  override def createTemporaryFunction(
      path: String,
      functionClass: Class[_ <: UserDefinedFunction]): Unit =
    tableEnv.createTemporaryFunction(path, functionClass)

  override def createTemporaryFunction(path: String, functionInstance: UserDefinedFunction): Unit =
    tableEnv.createTemporaryFunction(path, functionInstance)

  override def dropTemporaryFunction(path: String): Boolean =
    tableEnv.dropTemporaryFunction(path)

  override def createTemporaryView(path: String, view: Table): Unit =
    tableEnv.createTemporaryView(path, view)

  override def from(path: String): Table = tableEnv.from(path)

  override def listCatalogs(): Array[String] = tableEnv.listCatalogs()

  override def listModules(): Array[String] = tableEnv.listModules()

  override def listDatabases(): Array[String] = tableEnv.listDatabases()

  override def listTables(): Array[String] = tableEnv.listTables()

  override def listViews(): Array[String] = tableEnv.listViews()

  override def listTemporaryTables(): Array[String] =
    tableEnv.listTemporaryTables

  override def listTemporaryViews(): Array[String] =
    tableEnv.listTemporaryViews()

  override def listUserDefinedFunctions(): Array[String] =
    tableEnv.listUserDefinedFunctions()

  override def listFunctions(): Array[String] = tableEnv.listFunctions()

  override def dropTemporaryTable(path: String): Boolean =
    tableEnv.dropTemporaryTable(path)

  override def dropTemporaryView(path: String): Boolean =
    tableEnv.dropTemporaryView(path)

  override def explainSql(statement: String, extraDetails: ExplainDetail*): String =
    tableEnv.explainSql(statement, extraDetails: _*)

  override def sqlQuery(query: String): Table = tableEnv.sqlQuery(query)

  override def executeSql(statement: String): TableResult =
    tableEnv.executeSql(statement)

  override def getCurrentCatalog: String = tableEnv.getCurrentCatalog

  override def useCatalog(catalogName: String): Unit =
    tableEnv.useCatalog(catalogName)

  override def getCurrentDatabase: String = tableEnv.getCurrentDatabase

  override def useDatabase(databaseName: String): Unit =
    tableEnv.useDatabase(databaseName)

  override def getConfig: TableConfig = tableEnv.getConfig

  override def createStatementSet(): StatementSet =
    tableEnv.createStatementSet()

  @deprecated override def registerFunction(name: String, function: ScalarFunction): Unit =
    tableEnv.registerFunction(name, function)

  @deprecated override def registerTable(name: String, table: Table): Unit =
    tableEnv.registerTable(name, table)

  @deprecated override def scan(tablePath: String*): Table =
    tableEnv.scan(tablePath: _*)

  @deprecated override def getCompletionHints(statement: String, position: Int): Array[String] =
    tableEnv.getCompletionHints(statement, position)
}
