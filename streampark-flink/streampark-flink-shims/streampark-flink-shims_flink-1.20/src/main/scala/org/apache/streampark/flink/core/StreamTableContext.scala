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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamStatementSet, StreamTableEnvironment}
import org.apache.flink.table.catalog.CatalogDescriptor
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.module.ModuleEntry
import org.apache.flink.table.resource.ResourceUri
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.types.Row

class StreamTableContext(
    override val parameter: ParameterTool,
    private val streamEnv: StreamExecutionEnvironment,
    private val tableEnv: StreamTableEnvironment)
  extends FlinkStreamTableTrait(parameter, streamEnv, tableEnv) {

  def this(args: (ParameterTool, StreamExecutionEnvironment, StreamTableEnvironment)) =
    this(args._1, args._2, args._3)

  def this(args: StreamTableEnvConfig) =
    this(FlinkTableInitializer.initialize(args))

  override def fromDataStream[T](dataStream: DataStream[T], schema: Schema): Table =
    tableEnv.fromDataStream[T](dataStream, schema)

  override def fromChangelogStream(dataStream: DataStream[Row]): Table =
    tableEnv.fromChangelogStream(dataStream)

  override def fromChangelogStream(dataStream: DataStream[Row], schema: Schema): Table =
    tableEnv.fromChangelogStream(dataStream, schema)

  override def fromChangelogStream(
      dataStream: DataStream[Row],
      schema: Schema,
      changelogMode: ChangelogMode): Table =
    tableEnv.fromChangelogStream(dataStream, schema, changelogMode)

  override def createTemporaryView[T](
      path: String,
      dataStream: DataStream[T],
      schema: Schema): Unit =
    tableEnv.createTemporaryView[T](path, dataStream, schema)

  override def toDataStream(table: Table): DataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream(table)
  }

  override def toDataStream[T](table: Table, targetClass: Class[T]): DataStream[T] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream[T](table, targetClass)
  }

  override def toDataStream[T](table: Table, targetDataType: AbstractDataType[_]): DataStream[T] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream[T](table, targetDataType)
  }

  override def toChangelogStream(table: Table): DataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toChangelogStream(table)
  }

  override def toChangelogStream(table: Table, targetSchema: Schema): DataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toChangelogStream(table, targetSchema)
  }

  override def toChangelogStream(
      table: Table,
      targetSchema: Schema,
      changelogMode: ChangelogMode): DataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toChangelogStream(table, targetSchema, changelogMode)
  }

  override def createStatementSet(): StreamStatementSet =
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
}
