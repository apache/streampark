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

import org.apache.streampark.common.enums.FlinkSqlValidationFailedType
import org.apache.streampark.common.util.{ExceptionUtils, Logger}
import org.apache.streampark.flink.core.SqlCommand._

import org.apache.calcite.config.Lex
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.SqlParser.Config
import org.apache.flink.sql.parser.validate.FlinkSqlConformance
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.api.SqlDialect.{DEFAULT, HIVE}
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories

import scala.language.existentials
import scala.util.{Failure, Try}

object FlinkSqlValidator extends Logger {

  private[this] val FLINK112_CALCITE_PARSER_CLASS =
    "org.apache.flink.table.planner.calcite.CalciteParser"

  private[this] val FLINK113_PLUS_CALCITE_PARSER_CLASS =
    "org.apache.flink.table.planner.parse.CalciteParser"

  private[this] val SYNTAX_ERROR_REGEXP =
    ".*at\\sline\\s(\\d+),\\scolumn\\s(\\d+).*".r

  private[this] lazy val sqlParserConfigMap: Map[String, SqlParser.Config] = {
    def getConfig(sqlDialect: SqlDialect): Config = {
      val conformance = sqlDialect match {
        case HIVE =>
          try {
            FlinkSqlConformance.DEFAULT
          } catch {
            // for flink 1.18+
            case _: NoSuchFieldError => FlinkSqlConformance.DEFAULT
            case e: Throwable =>
              throw new IllegalArgumentException("Init Flink sql Dialect error: ", e)
          }
        case DEFAULT => FlinkSqlConformance.DEFAULT
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported sqlDialect: $sqlDialect")
      }
      SqlParser.config
        .withParserFactory(FlinkSqlParserFactories.create(conformance))
        .withConformance(conformance)
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256)
    }

    Map(
      SqlDialect.DEFAULT.name() -> getConfig(SqlDialect.DEFAULT),
      SqlDialect.HIVE.name() -> getConfig(SqlDialect.HIVE))
  }

  def verifySql(sql: String): FlinkSqlValidationResult = {
    val sqlCommands = SqlCommandParser.parseSQL(sql, r => return r)
    var sqlDialect = SqlDialect.DEFAULT.name().toLowerCase()
    var hasInsert = false
    for (call <- sqlCommands) {
      val args = call.operands.head
      val command = call.command
      command match {
        case SET | RESET =>
          if (command == SET && args == TableConfigOptions.TABLE_SQL_DIALECT.key()) {
            sqlDialect = call.operands.last
          }
        case BEGIN_STATEMENT_SET | END_STATEMENT_SET =>
          logWarn(s"SQL Client Syntax: ${call.command.name} ")
        case _ =>
          if (command == INSERT) {
            hasInsert = true
          }
          Try {
            val calciteClass = Try(Class.forName(FLINK112_CALCITE_PARSER_CLASS))
              .getOrElse(Class.forName(FLINK113_PLUS_CALCITE_PARSER_CLASS))
            sqlDialect.toUpperCase() match {
              case "HIVE" =>
              case "DEFAULT" =>
                val parser = calciteClass
                  .getConstructor(Array(classOf[Config]): _*)
                  .newInstance(sqlParserConfigMap(sqlDialect.toUpperCase()))
                val method =
                  parser.getClass.getDeclaredMethod("parse", classOf[String])
                method.setAccessible(true)
                method.invoke(parser, call.originSql)
              case _ =>
                throw new UnsupportedOperationException(s"unsupported dialect: $sqlDialect")
            }
          } match {
            case Failure(e) =>
              val exception = ExceptionUtils.stringifyException(e)
              val causedBy = exception.drop(exception.indexOf("Caused by:"))
              val cleanUpError = exception.replaceAll("[\r\n]", "")
              if (SYNTAX_ERROR_REGEXP.findAllMatchIn(cleanUpError).nonEmpty) {
                val SYNTAX_ERROR_REGEXP(line, column) = cleanUpError
                val errorLine = call.lineStart + line.toInt - 1
                return FlinkSqlValidationResult(
                  success = false,
                  failedType = FlinkSqlValidationFailedType.SYNTAX_ERROR,
                  lineStart = call.lineStart,
                  lineEnd = call.lineEnd,
                  errorLine = errorLine,
                  errorColumn = column.toInt,
                  sql = call.originSql,
                  exception = causedBy.replaceAll(s"at\\sline\\s$line", s"at line $errorLine"))
              } else {
                return FlinkSqlValidationResult(
                  success = false,
                  failedType = FlinkSqlValidationFailedType.SYNTAX_ERROR,
                  lineStart = call.lineStart,
                  lineEnd = call.lineEnd,
                  sql = call.originSql,
                  exception = causedBy)
              }
            case _ =>
          }
      }
    }

    if (hasInsert) {
      FlinkSqlValidationResult()
    } else {
      FlinkSqlValidationResult(
        success = false,
        failedType = FlinkSqlValidationFailedType.SYNTAX_ERROR,
        lineStart = sqlCommands.head.lineStart,
        lineEnd = sqlCommands.last.lineEnd,
        exception = "No 'INSERT' statement to trigger the execution of the Flink job.")
    }
  }

}
