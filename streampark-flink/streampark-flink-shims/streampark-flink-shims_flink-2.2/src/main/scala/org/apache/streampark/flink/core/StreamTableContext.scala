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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.streaming.api.datastream.{DataStream => JavaDataStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.ModelDescriptor
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.catalog.CatalogDescriptor
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.module.ModuleEntry
import org.apache.flink.table.resource.ResourceUri
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.types.Row
import org.apache.flink.util.ParameterTool

class StreamTableContext(
    override val parameter: ParameterTool,
    private val streamEnv: StreamExecutionEnvironment,
    private val tableEnv: StreamTableEnvironment)
  extends FlinkStreamTableTraitV2(parameter, streamEnv, tableEnv) {

  def this(args: (ParameterTool, StreamExecutionEnvironment, StreamTableEnvironment)) =
    this(args._1, args._2, args._3)

  def this(args: StreamTableEnvConfig) =
    this(FlinkTableInitializerV2.initialize(args))

  override def fromDataStream[T](dataStream: JavaDataStream[T], schema: Schema): Table =
    tableEnv.fromDataStream[T](dataStream, schema)

  /** @deprecated old API */
  override def fromDataStream[T](dataStream: JavaDataStream[T], expressions: Expression*): Table =
    tableEnv.fromDataStream(dataStream, expressions: _*)

  override def fromChangelogStream(dataStream: JavaDataStream[Row]): Table =
    tableEnv.fromChangelogStream(dataStream)

  override def fromChangelogStream(dataStream: JavaDataStream[Row], schema: Schema): Table =
    tableEnv.fromChangelogStream(dataStream, schema)

  override def fromChangelogStream(
      dataStream: JavaDataStream[Row],
      schema: Schema,
      changelogMode: ChangelogMode): Table =
    tableEnv.fromChangelogStream(dataStream, schema, changelogMode)

  override def createTemporaryView[T](
      path: String,
      dataStream: JavaDataStream[T],
      schema: Schema): Unit =
    tableEnv.createTemporaryView[T](path, dataStream, schema)

  /** @deprecated old API */
  @deprecated override def createTemporaryView[T](
      path: String,
      dataStream: JavaDataStream[T],
      expressions: Expression*): Unit =
    tableEnv.createTemporaryView(path, dataStream, expressions: _*)

  override def toDataStream(table: Table): JavaDataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream(table)
  }

  override def toDataStream[T](table: Table, targetClass: Class[T]): JavaDataStream[T] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream[T](table, targetClass)
  }

  override def toDataStream[T](table: Table, targetDataType: AbstractDataType[_]): JavaDataStream[T] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream[T](table, targetDataType)
  }

  override def toChangelogStream(table: Table): JavaDataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toChangelogStream(table)
  }

  override def toChangelogStream(table: Table, targetSchema: Schema): JavaDataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toChangelogStream(table, targetSchema)
  }

  override def toChangelogStream(
      table: Table,
      targetSchema: Schema,
      changelogMode: ChangelogMode): JavaDataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toChangelogStream(table, targetSchema, changelogMode)
  }

  override def createStatementSet(): org.apache.flink.table.api.bridge.java.StreamStatementSet =
    tableEnv.createStatementSet()

  override def useModules(strings: String*): Unit =
    tableEnv.useModules(strings: _*)

  override def createTemporaryTable(path: String, descriptor: TableDescriptor): Unit =
    tableEnv.createTemporaryTable(path, descriptor)

  override def createTable(path: String, descriptor: TableDescriptor): Unit =
    tableEnv.createTable(path, descriptor)

  override def from(descriptor: TableDescriptor): Table =
    tableEnv.from(descriptor)

  override def listFullModules(): Array[ModuleEntry] =
    tableEnv.listFullModules()

  /** @since 1.15 */
  override def listTables(s: String, s1: String): Array[String] =
    tableEnv.listTables(s, s1)

  /** @since 1.15 */
  override def loadPlan(planReference: PlanReference): CompiledPlan =
    tableEnv.loadPlan(planReference)

  /** @since 1.15 */
  override def compilePlanSql(s: String): CompiledPlan =
    tableEnv.compilePlanSql(s)

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

  /** @deprecated old API */
  @deprecated override def toAppendStream[T](
      table: Table,
      typeInformation: TypeInformation[T]): JavaDataStream[T] =
    tableEnv.toAppendStream(table, typeInformation)

  /** @deprecated old API */
  @deprecated override def toRetractStream[T](
      table: Table,
      typeInformation: TypeInformation[T]): JavaDataStream[tuple.Tuple2[java.lang.Boolean, T]] =
    tableEnv.toRetractStream(table, typeInformation)

  /** since Flink 2.0 */
  override def toAppendStream[T](table: Table, clazz: Class[T]): JavaDataStream[T] =
    tableEnv.toAppendStream(table, clazz)

  /** since Flink 2.0 */
  override def toRetractStream[T](table: Table, clazz: Class[T]): JavaDataStream[tuple.Tuple2[java.lang.Boolean, T]] =
    tableEnv.toRetractStream(table, clazz)

  /** since Flink 2.0 */
  override def createTable(path: String, descriptor: TableDescriptor, ignoreIfExists: Boolean): Boolean =
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
