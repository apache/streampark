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
import org.apache.streampark.common.util.Utils
import org.apache.streampark.flink.core.EnhancerImplicit._

import org.apache.flink.api.common.{JobExecutionResult, RuntimeExecutionMode}
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.io.{FileInputFormat, FilePathFilter, InputFormat}
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.api.java.tuple
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.core.execution.{JobClient, JobListener}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.functions._
import org.apache.flink.table.module.Module
import org.apache.flink.types.Row
import org.apache.flink.util.{ParameterTool, SplittableIterator}

import java.util.Optional

/**
 * Integration api of stream and table (Flink 2.0 - Java DataStream API)
 *
 * @param parameter
 *   parameter
 * @param streamEnv
 *   streamEnv
 * @param tableEnv
 *   tableEnv
 */
abstract class FlinkStreamTableTraitV2(
    val parameter: ParameterTool,
    private val streamEnv: StreamExecutionEnvironment,
    private val tableEnv: StreamTableEnvironment)
  extends StreamTableEnvironment {

  /**
   * Once a Table has been converted to a DataStream, the DataStream job must be executed using the
   * execute method of the StreamExecutionEnvironment.
   */
  var isConvertedToDataStream: Boolean = false

  /** Recommended to use this Api to start tasks */
  def start(name: String = null): JobExecutionResult = {
    val appName = parameter.getAppName(name, true)
    execute(appName)
  }

  @deprecated def execute(jobName: String): JobExecutionResult = {
    Utils.printLogo(s"FlinkStreamTable $jobName Starting...")
    if (isConvertedToDataStream) {
      streamEnv.execute(jobName)
    } else null
  }

  def sql(sql: String = null)(implicit callback: String => Unit = null): Unit =
    FlinkSqlExecutor.executeSql(sql, parameter, this)

  // ...streamEnv api start...

  def $getCachedFiles: JavaList[tuple.Tuple2[String, DistributedCache.DistributedCacheEntry]] =
    this.streamEnv.getCachedFiles

  def $getJobListeners: JavaList[JobListener] = this.streamEnv.getJobListeners

  def $setParallelism(parallelism: Int): Unit =
    this.streamEnv.setParallelism(parallelism)

  def $setRuntimeMode(deployMode: RuntimeExecutionMode): StreamExecutionEnvironment =
    this.streamEnv.setRuntimeMode(deployMode)

  def $setMaxParallelism(maxParallelism: Int): Unit =
    this.streamEnv.setMaxParallelism(maxParallelism)

  def $getParallelism: Int = this.streamEnv.getParallelism

  def $getMaxParallelism: Int = this.streamEnv.getMaxParallelism

  def $setBufferTimeout(timeoutMillis: Long): StreamExecutionEnvironment =
    this.streamEnv.setBufferTimeout(timeoutMillis)

  def $getBufferTimeout: Long = this.streamEnv.getBufferTimeout

  def $disableOperatorChaining(): StreamExecutionEnvironment =
    this.streamEnv.disableOperatorChaining()

  def $getCheckpointConfig: CheckpointConfig =
    this.streamEnv.getCheckpointConfig

  def $enableCheckpointing(interval: Long, mode: CheckpointingMode): StreamExecutionEnvironment =
    this.streamEnv.enableCheckpointing(interval, mode)

  def $enableCheckpointing(interval: Long): StreamExecutionEnvironment =
    this.streamEnv.enableCheckpointing(interval)

  def $getCheckpointingMode: CheckpointingMode =
    this.streamEnv.getCheckpointingMode

  def $configure(configuration: ReadableConfig, classLoader: ClassLoader): Unit =
    this.streamEnv.configure(configuration, classLoader)

  def $fromSequence(from: Long, to: Long): DataStream[java.lang.Long] =
    this.streamEnv.fromSequence(from, to)

  // fromData with varargs removed in Flink 2.0
  def $fromData[T](data: T): DataStream[T] =
    this.streamEnv.fromData(data)

  def $fromCollection[T](data: java.util.Collection[T]): DataStream[T] =
    this.streamEnv.fromCollection(data)

  def $fromParallelCollection[T](data: SplittableIterator[T], clazz: Class[T]): DataStream[T] =
    this.streamEnv.fromParallelCollection(data, clazz)

  def $readFile[T](inputFormat: FileInputFormat[T], filePath: String): DataStream[T] =
    this.streamEnv.readFile(inputFormat, filePath)

  def $readFile[T](
      inputFormat: FileInputFormat[T],
      filePath: String,
      watchType: FileProcessingMode,
      interval: Long): DataStream[T] =
    this.streamEnv.readFile(inputFormat, filePath, watchType, interval)

  def $socketTextStream(
      hostname: String,
      port: Int,
      delimiter: Char,
      maxRetry: Long): DataStream[String] =
    this.streamEnv.socketTextStream(hostname, port, delimiter, maxRetry)

  def $createInput[T](inputFormat: InputFormat[T, _]): DataStream[T] =
    this.streamEnv.createInput(inputFormat)

  def $fromSource[T](
      source: Source[T, _ <: SourceSplit, _],
      watermarkStrategy: WatermarkStrategy[T],
      sourceName: String): DataStream[T] =
    this.streamEnv.fromSource(source, watermarkStrategy, sourceName)

  def $registerJobListener(jobListener: JobListener): Unit =
    this.streamEnv.registerJobListener(jobListener)

  def $clearJobListeners(): Unit = this.streamEnv.clearJobListeners()

  def $executeAsync(): JobClient = this.streamEnv.executeAsync()

  def $executeAsync(jobName: String): JobClient =
    this.streamEnv.executeAsync(jobName)

  def $getExecutionPlan: String = this.streamEnv.getExecutionPlan

  def $getStreamGraph: StreamGraph = this.streamEnv.getStreamGraph

  def $registerCachedFile(filePath: String, name: String): Unit =
    this.streamEnv.registerCachedFile(filePath, name)

  def $registerCachedFile(filePath: String, name: String, executable: Boolean): Unit =
    this.streamEnv.registerCachedFile(filePath, name, executable)

  def $isUnalignedCheckpointsEnabled: Boolean =
    this.streamEnv.isUnalignedCheckpointsEnabled

  def $isForceUnalignedCheckpoints: Boolean =
    this.streamEnv.isForceUnalignedCheckpoints

  @deprecated def $readFile[T](
      inputFormat: FileInputFormat[T],
      filePath: String,
      watchType: FileProcessingMode,
      interval: Long,
      filter: FilePathFilter): DataStream[T] =
    this.streamEnv.readFile(inputFormat, filePath, watchType, interval, filter)

  // ...streamEnv api end...

  override def fromDataStream[T](dataStream: DataStream[T]): Table =
    tableEnv.fromDataStream(dataStream)

  override def fromDataStream[T](dataStream: DataStream[T], schema: Schema): Table =
    tableEnv.fromDataStream(dataStream, schema)

  override def fromChangelogStream(dataStream: DataStream[Row]): Table =
    tableEnv.fromChangelogStream(dataStream)

  override def fromChangelogStream(dataStream: DataStream[Row], schema: Schema): Table =
    tableEnv.fromChangelogStream(dataStream, schema)

  override def fromChangelogStream(
      dataStream: DataStream[Row],
      schema: Schema,
      changelogMode: org.apache.flink.table.connector.ChangelogMode): Table =
    tableEnv.fromChangelogStream(dataStream, schema, changelogMode)

  override def createTemporaryView[T](path: String, dataStream: DataStream[T]): Unit =
    tableEnv.createTemporaryView(path, dataStream)

  override def createTemporaryView[T](
      path: String,
      dataStream: DataStream[T],
      schema: Schema): Unit =
    tableEnv.createTemporaryView(path, dataStream, schema)

  override def toDataStream(table: Table): DataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream(table)
  }

  override def toDataStream[T](table: Table, targetClass: Class[T]): DataStream[T] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream(table, targetClass)
  }

  override def toDataStream[T](
      table: Table,
      targetDataType: org.apache.flink.table.types.AbstractDataType[_]): DataStream[T] = {
    isConvertedToDataStream = true
    tableEnv.toDataStream(table, targetDataType)
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
      changelogMode: org.apache.flink.table.connector.ChangelogMode): DataStream[Row] = {
    isConvertedToDataStream = true
    tableEnv.toChangelogStream(table, targetSchema, changelogMode)
  }

  override def createStatementSet(): org.apache.flink.table.api.bridge.java.StreamStatementSet =
    tableEnv.createStatementSet()

  // ...table env delegation...

  override def fromValues(values: org.apache.flink.table.expressions.Expression*): Table =
    tableEnv.fromValues(values)

  override def fromValues(
      rowType: org.apache.flink.table.types.AbstractDataType[_],
      values: org.apache.flink.table.expressions.Expression*): Table =
    tableEnv.fromValues(rowType, values: _*)

  override def fromValues(values: java.lang.Iterable[_]): Table =
    tableEnv.fromValues(values)

  override def fromValues(
      rowType: org.apache.flink.table.types.AbstractDataType[_],
      values: java.lang.Iterable[_]): Table =
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

  @deprecated override def registerFunction(name: String, function: ScalarFunction): Unit =
    tableEnv.registerFunction(name, function)

  @deprecated override def registerTable(name: String, table: Table): Unit =
    tableEnv.registerTable(name, table)

  @deprecated override def scan(tablePath: String*): Table =
    tableEnv.scan(tablePath: _*)

  @deprecated override def getCompletionHints(statement: String, position: Int): Array[String] =
    tableEnv.getCompletionHints(statement, position)
}
