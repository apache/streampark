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

import org.apache.streampark.common.conf.ConfigKeys.PARAM_PREFIX
import org.apache.streampark.common.enums.FlinkSqlValidationFailedType
import org.apache.streampark.common.util.Logger

import enumeratum.EnumEntry
import org.apache.commons.lang3.StringUtils

import java.lang.{Boolean => JavaBool}
import java.util.Scanner
import java.util.regex.{Matcher, Pattern}

import scala.annotation.tailrec
import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object SqlCommandParser extends Logger {

  def parseSQL(
      sql: String,
      validationCallback: FlinkSqlValidationResult => Unit = null): List[SqlCommandCall] = {
    val sqlEmptyError = "verify failed: flink sql cannot be empty."
    require(StringUtils.isNotBlank(sql), sqlEmptyError)
    val sqlSegments = SqlSplitter.splitSql(sql)
    sqlSegments match {
      case s if s.isEmpty =>
        if (validationCallback != null) {
          validationCallback(
            FlinkSqlValidationResult(
              success = false,
              failedType = FlinkSqlValidationFailedType.VERIFY_FAILED,
              exception = sqlEmptyError))
          null
        } else {
          throw new IllegalArgumentException(sqlEmptyError)
        }
      case segments =>
        val calls = new ListBuffer[SqlCommandCall]
        for (segment <- segments) {
          parseLine(segment) match {
            case Some(x) => calls += x
            case _ =>
              if (validationCallback != null) {
                validationCallback(
                  FlinkSqlValidationResult(
                    success = false,
                    failedType = FlinkSqlValidationFailedType.UNSUPPORTED_SQL,
                    lineStart = segment.start,
                    lineEnd = segment.end,
                    exception = s"unsupported sql",
                    sql = segment.sql))
              } else {
                throw new UnsupportedOperationException(s"unsupported sql: ${segment.sql}")
              }
          }
        }

        calls.toList match {
          case c if c.isEmpty =>
            if (validationCallback != null) {
              validationCallback(
                FlinkSqlValidationResult(
                  success = false,
                  failedType = FlinkSqlValidationFailedType.VERIFY_FAILED,
                  exception = "flink sql syntax error, no executable sql"))
              null
            } else {
              throw new UnsupportedOperationException("flink sql syntax error, no executable sql")
            }
          case r => r
        }
    }
  }

  private[this] def parseLine(sqlSegment: SqlSegment): Option[SqlCommandCall] = {
    val sqlCommand = SqlCommand.get(sqlSegment.sql.trim)
    if (sqlCommand == null) None
    else {
      val matcher = sqlCommand.matcher
      val groups = new Array[String](matcher.groupCount)
      for (i <- groups.indices) {
        groups(i) = matcher.group(i + 1)
      }
      sqlCommand
        .converter(groups)
        .map(x =>
          SqlCommandCall(sqlSegment.start, sqlSegment.end, sqlCommand, x, sqlSegment.sql.trim))
    }
  }

}

object Converters {
  val NO_OPERANDS = (_: Array[String]) => Some(Array.empty[String])
}

sealed abstract class SqlCommand(
    val name: String,
    private val regex: String,
    val converter: Array[String] => Option[Array[String]] = (x: Array[String]) =>
      Some(Array[String](x.head)))
  extends EnumEntry {
  var matcher: Matcher = _

  def matches(input: String): Boolean = {
    if (StringUtils.isBlank(regex)) false
    else {
      val pattern =
        Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL)
      matcher = pattern.matcher(input)
      matcher.matches()
    }
  }
}

object SqlCommand extends enumeratum.Enum[SqlCommand] {

  def get(stmt: String): SqlCommand = {
    var cmd: SqlCommand = null
    breakable {
      this.values.foreach(x => {
        if (x.matches(stmt)) {
          cmd = x
          break()
        }
      })
    }
    cmd
  }

  val values: immutable.IndexedSeq[SqlCommand] = findValues

  // ---- SELECT Statements--------------------------------------------------------------------------------------------------------------------------------
  case object SELECT extends SqlCommand("select", "(SELECT\\s+.+)")

  // ----CREATE Statements--------------------------------------------------------------------------------------------------------------------------------

  /**
   * <pre> CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name ( {
   * <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ ,
   * ...n] [ <watermark_definition> ] [ <table_constraint> ][ , ...n] ) [COMMENT table_comment]
   * [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)] WITH (key1=val1,
   * key2=val2, ...) [ LIKE source_table [( <like_options> )] ] </pre
   */
  case object CREATE_TABLE
    extends SqlCommand("create table", "(CREATE\\s+(TEMPORARY\\s+|)TABLE\\s+.+)")

  /** <pre> CREATE CATALOG catalog_name WITH (key1=val1, key2=val2, ...) </pre> */
  case object CREATE_CATALOG extends SqlCommand("create catalog", "(CREATE\\s+CATALOG\\s+.+)")

  /**
   * <pre> CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name<br> [COMMENT database_comment]<br>
   * WITH (key1=val1, key2=val2, ...)<br> </pre>
   */
  case object CREATE_DATABASE extends SqlCommand("create database", "(CREATE\\s+DATABASE\\s+.+)")

  /**
   * <pre> CREATE [TEMPORARY] VIEW [IF NOT EXISTS] [catalog_name.][db_name.]view_name [( columnName
   * [, columnName ]* )] [COMMENT view_comment] AS query_expression< </pre
   */
  case object CREATE_VIEW
    extends SqlCommand(
      "create view",
      "(CREATE\\s+(TEMPORARY\\s+|)VIEW\\s+(IF\\s+NOT\\s+EXISTS\\s+|)(\\S+)\\s+AS\\s+SELECT\\s+.+)")

  /**
   * <pre> CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF NOT EXISTS]
   * [catalog_name.][db_name.]function_name AS identifier [LANGUAGE JAVA|SCALA|PYTHON] </pre
   */
  case object CREATE_FUNCTION
    extends SqlCommand(
      "create function",
      "(CREATE\\s+(TEMPORARY\\s+|TEMPORARY\\s+SYSTEM\\s+|)FUNCTION\\s+(IF\\s+NOT\\s+EXISTS\\s+|)(\\S+)\\s+AS\\s+.*)")

  // ----DROP Statements--------------------------------------------------------------------------------------------------------------------------------

  /**
   * <pre> DROP statements are used to remove a catalog with the given catalog name or to remove a
   * registered table/view/function from the current or specified Catalog.
   *
   * Flink SQL supports the following DROP statements for now: * DROP CATALOG * DROP TABLE * DROP
   * DATABASE * DROP VIEW * DROP FUNCTION </pre>
   */

  /** <strong>DROP CATALOG [IF EXISTS] catalog_name</strong> */
  case object DROP_CATALOG extends SqlCommand("drop catalog", "(DROP\\s+CATALOG\\s+.+)")

  /** <strong>DROP [TEMPORARY] TABLE [IF EXISTS] [catalog_name.][db_name.]table_name</strong> */
  case object DROP_TABLE extends SqlCommand("drop table", "(DROP\\s+(TEMPORARY\\s+|)TABLE\\s+.+)")

  /** <strong>DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]</strong> */
  case object DROP_DATABASE extends SqlCommand("drop database", "(DROP\\s+DATABASE\\s+.+)")

  /** <strong>DROP [TEMPORARY] VIEW [IF EXISTS] [catalog_name.][db_name.]view_name</strong> */
  case object DROP_VIEW extends SqlCommand("drop view", "(DROP\\s+(TEMPORARY\\s+|)VIEW\\s+.+)")

  /**
   * <strong>DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS]
   * [catalog_name.][db_name.]function_name</strong>
   */
  case object DROP_FUNCTION
    extends SqlCommand(
      "drop function",
      "(DROP\\s+(TEMPORARY\\s+|TEMPORARY\\s+SYSTEM\\s+|)FUNCTION\\s+.+)")

  // ----ALTER Statements--------------------------------------------------------------------------------------------------------------------------------

  /**
   * <strong>ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name</strong>
   *
   * <strong>ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2,
   * ...)</strong>
   */
  case object ALTER_TABLE extends SqlCommand("alter table", "(ALTER\\s+TABLE\\s+.+)")

  /**
   * <strong>ALTER VIEW [catalog_name.][db_name.]view_name RENAME TO new_view_name</strong>
   *
   * <strong>ALTER VIEW [catalog_name.][db_name.]view_name AS new_query_expression</strong>
   */
  case object ALTER_VIEW extends SqlCommand("alter view", "(ALTER\\s+VIEW\\s+.+)")

  /** <strong>ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)</strong> */
  case object ALTER_DATABASE extends SqlCommand("alter database", "(ALTER\\s+DATABASE\\s+.+)")

  /**
   * <strong> ALTER [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS]
   * [catalog_name.][db_name.]function_name AS identifier [LANGUAGE JAVA|SCALA|PYTHON] </strong>
   */
  case object ALTER_FUNCTION
    extends SqlCommand(
      "alter function",
      "(ALTER\\s+(TEMPORARY\\s+|TEMPORARY\\s+SYSTEM\\s+|)FUNCTION\\s+.+)")

  // ---- INSERT Statement--------------------------------------------------------------------------------------------------------------------------------

  /**
   * INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name [PARTITION part_spec]
   * [column_list] select_statement INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name
   * VALUES values_row [, values_row ...]
   */
  case object INSERT extends SqlCommand("insert", "(INSERT\\s+(INTO|OVERWRITE)\\s+.+)")

  // ---- DESCRIBE Statement--------------------------------------------------------------------------------------------------------------------------------

  /** { DESCRIBE | DESC } [catalog_name.][db_name.]table_name */
  case object DESC extends SqlCommand("desc", "(DESC\\s+.+)")

  /** { DESCRIBE | DESC } [catalog_name.][db_name.]table_name */
  case object DESCRIBE extends SqlCommand("describe", "(DESCRIBE\\s+.+)")

  // ---- EXPLAIN Statement--------------------------------------------------------------------------------------------------------------------------------

  /**
   * For flink-1.13.x: EXPLAIN PLAN FOR `<query_statement_or_insert_statement>` <br> For
   * flink-1.14.x: EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN
   * `<query_statement_or_insert_statement>`<br> For flink-1.15.x: <br> <pre> EXPLAIN
   * [([ExplainDetail[, ExplainDetail]*]) | PLAN FOR]
   * <query_statement_or_insert_statement_or_statement_set>
   *
   * statement_set: EXECUTE STATEMENT SET BEGIN insert_statement; ... insert_statement; END; </pre>
   * Recommended not to use the form of flink-1.15.x
   */
  case object EXPLAIN extends SqlCommand("explain", "(EXPLAIN\\s+.+)")

  // ---- USE Statements--------------------------------------------------------------------------------------------------------------------------------

  /** USE CATALOG catalog_name */
  case object USE_CATALOG extends SqlCommand("use catalog", "(USE\\s+CATALOG\\s+.+)")

  /** USE MODULES module_name1[, module_name2, ...] */
  case object USE_MODULES extends SqlCommand("use modules", "(USE\\s+MODULES\\s+.+)")

  /** USE [catalog_name.]database_name */
  case object USE_DATABASE extends SqlCommand("use database", "(USE\\s+(?!(CATALOG|MODULES)).+)")

  // ----SHOW Statements--------------------------------------------------------------------------------------------------------------------------------

  /** SHOW CATALOGS */
  case object SHOW_CATALOGS extends SqlCommand("show catalogs", "(SHOW\\s+CATALOGS\\s*)")

  /** SHOW CURRENT CATALOG */
  case object SHOW_CURRENT_CATALOG
    extends SqlCommand("show current catalog", "(SHOW\\s+CURRENT\\s+CATALOG\\s*)")

  /** SHOW DATABASES */
  case object SHOW_DATABASES extends SqlCommand("show databases", "(SHOW\\s+DATABASES\\s*)")

  /** SHOW CURRENT DATABASE */
  case object SHOW_CURRENT_DATABASE
    extends SqlCommand("show current database", "(SHOW\\s+CURRENT\\s+DATABASE\\s*)")

  /**
   * SHOW TABLES,support from flink-1.13.x<br> SHOW TABLES [ ( FROM | IN )
   * [catalog_name.]database_name ] [ [NOT] LIKE `<sql_like_pattern`> ], support from flink-1.15.x
   */
  case object SHOW_TABLES extends SqlCommand("show tables", "(SHOW\\s+TABLES.*)")

  /** SHOW CREATE TABLE, flink-1.14.x support. */
  case object SHOW_CREATE_TABLE
    extends SqlCommand("show create table", "(SHOW\\s+CREATE\\s+TABLE\\s+.+)")

  /**
   * SHOW COLUMNS ( FROM | IN ) [`[`catalog_name.]database.]`<table_name>` [ [NOT] LIKE
   * `<sql_like_pattern>`],flink-1.15.x support.
   */
  case object SHOW_COLUMNS extends SqlCommand("show columns", "(SHOW\\s+COLUMNS\\s+.+)")

  /** SHOW VIEWS */
  case object SHOW_VIEWS extends SqlCommand("show views", "(SHOW\\s+VIEWS\\s*)")

  /** SHOW CREATE VIEW */
  case object SHOW_CREATE_VIEW
    extends SqlCommand("show create view", "(SHOW\\s+CREATE\\s+VIEW\\s+.+)")

  /** SHOW [USER] FUNCTIONS */
  case object SHOW_FUNCTIONS
    extends SqlCommand("show functions", "(SHOW\\s+(USER\\s+|)FUNCTIONS\\s*)")

  /** SHOW [FULL] MODULES */
  case object SHOW_MODULES extends SqlCommand("show modules", "(SHOW\\s+(FULL\\s+|)MODULES\\s*)")

  // ----LOAD Statements--------------------------------------------------------------------------------------------------------------------------------

  /** LOAD MODULE module_name [WITH ('key1' = 'val1', 'key2' = 'val2', ...)] */
  case object LOAD_MODULE extends SqlCommand("load module", "(LOAD\\s+MODULE\\s+.+)")

  // ----UNLOAD Statements--------------------------------------------------------------------------------------------------------------------------------

  /** UNLOAD MODULE module_name */
  case object UNLOAD_MODULE extends SqlCommand("unload module", "(UNLOAD\\s+MODULE\\s+.+)")

  // ----SET Statements--------------------------------------------------------------------------------------------------------------------------------

  /** SET ('key' = 'value') */
  case object SET
    extends SqlCommand(
      "set",
      "SET(\\s+(\\S+)\\s*=(.*))?",
      {
        case a if a.length < 3 => None
        case a if a.head == null => Some(Array[String](cleanUp(a.head)))
        case a => Some(Array[String](cleanUp(a(1)), cleanUp(a(2))))
      })

  // ----RESET Statements--------------------------------------------------------------------------------------------------------------------------------

  /** RESET ('key') */
  case object RESET extends SqlCommand("reset", "RESET\\s+'(.*)'")

  /** RESET */
  case object RESET_ALL extends SqlCommand("reset all", "RESET", _ => Some(Array[String]("ALL")))

  // ----INSERT SET Statements--------------------------------------------------------------------------------------------------------------------------------
  /*
   * <pre>
   * SQL Client execute each INSERT INTO statement as a single Flink job. However,
   * this is sometimes not optimal because some part of the pipeline can be reused.
   * SQL Client supports STATEMENT SET syntax to execute a set of SQL statements.
   * This is an equivalent feature with StatementSet in Table API.
   * The STATEMENT SET syntax encloses one or more INSERT INTO statements.
   * All statements in a STATEMENT SET block are holistically optimized and executed as a single Flink job.
   * Joint optimization and execution allows for reusing common intermediate results and can therefore significantly
   * improve the efficiency of executing multiple queries.
   * </pre>
   */
  /** This is SQL Client's syntax, don't use in our platform. */
  @deprecated
  case object BEGIN_STATEMENT_SET
    extends SqlCommand("begin statement set", "BEGIN\\s+STATEMENT\\s+SET", Converters.NO_OPERANDS)

  /** This is SQL Client's syntax, don't use in our platform. */
  @deprecated
  case object END_STATEMENT_SET
    extends SqlCommand("end statement set", "END", Converters.NO_OPERANDS)

  // Since: 2.1.2 for flink 1.18
  case object DELETE extends SqlCommand("delete", "(DELETE\\s+FROM\\s+.+)")

  // Since: 2.1.2 for flink 1.18
  case object UPDATE extends SqlCommand("update", "(UPDATE\\s+.+)")

  private[this] def cleanUp(sql: String): String =
    sql.trim.replaceAll("^(['\"])|(['\"])$", "")

}

/** Call of SQL command with operands and command type. */
case class SqlCommandCall(
    lineStart: Int,
    lineEnd: Int,
    command: SqlCommand,
    operands: Array[String],
    originSql: String) {}

case class FlinkSqlValidationResult(
    success: JavaBool = true,
    failedType: FlinkSqlValidationFailedType = null,
    lineStart: Int = 0,
    lineEnd: Int = 0,
    errorLine: Int = 0,
    errorColumn: Int = 0,
    sql: String = null,
    exception: String = null)

case class SqlSegment(start: Int, end: Int, sql: String)

object SqlSplitter {

  private lazy val singleLineCommentPrefixList = Set[String](PARAM_PREFIX)

  /**
   * Split whole text into multiple sql statements. Two Steps: Step 1, split the whole text into
   * multiple sql statements. Step 2, refine the results. Replace the preceding sql statements with
   * empty lines, so that we can get the correct line number in the parsing error message. e.g:
   * select a from table_1; select a from table_2; select a from table_3; The above text will be
   * splitted into: sql_1: select a from table_1 sql_2: \nselect a from table_2 sql_3: \n\nselect a
   * from table_3
   *
   * @param sql
   * @return
   */
  def splitSql(sql: String): List[SqlSegment] = {
    val queries = ListBuffer[String]()
    val lastIndex = if (StringUtils.isNotBlank(sql)) sql.length - 1 else 0
    var query = new mutable.StringBuilder

    var multiLineComment = false
    var singleLineComment = false
    var singleQuoteString = false
    var doubleQuoteString = false
    var lineNum: Int = 0
    val lineNumMap = new collection.mutable.HashMap[Int, (Int, Int)]()

    // Whether each line of the record is empty. If it is empty, it is false. If it is not empty, it is true
    val lineDescriptor = {
      val scanner = new Scanner(sql)
      val descriptor = new collection.mutable.HashMap[Int, Boolean]
      var lineNumber = 0
      var startComment = false
      var hasComment = false

      while (scanner.hasNextLine) {
        lineNumber += 1
        val line = scanner.nextLine().trim
        val nonEmpty =
          StringUtils.isNotBlank(line) && !line.startsWith(PARAM_PREFIX)
        if (line.startsWith("/*")) {
          startComment = true
          hasComment = true
        }

        descriptor += lineNumber -> (nonEmpty && !hasComment)

        if (startComment && line.endsWith("*/")) {
          startComment = false
          hasComment = false
        }
      }
      descriptor
    }

    @tailrec
    def findStartLine(num: Int): Int =
      if (num >= lineDescriptor.size || lineDescriptor(num)) num
      else findStartLine(num + 1)

    def markLineNumber(): Unit = {
      val line = lineNum + 1
      if (lineNumMap.isEmpty) {
        lineNumMap += (0 -> (findStartLine(1) -> line))
      } else {
        val index = lineNumMap.size
        val start = lineNumMap(lineNumMap.size - 1)._2 + 1
        lineNumMap += (index -> (findStartLine(start) -> line))
      }
    }

    for (idx <- 0 until sql.length) {

      if (sql.charAt(idx) == '\n') lineNum += 1

      breakable {
        val ch = sql.charAt(idx)

        // end of single line comment
        if (singleLineComment && (ch == '\n')) {
          singleLineComment = false
          query += ch
          if (idx == lastIndex && query.toString.trim.nonEmpty) {
            // add query when it is the end of sql.
            queries += query.toString
          }
          break()
        }

        // end of multiple line comment
        if (multiLineComment && (idx - 1) >= 0 && sql.charAt(idx - 1) == '/'
          && (idx - 2) >= 0 && sql.charAt(idx - 2) == '*') {
          multiLineComment = false
        }

        // single quote start or end mark
        if (ch == '\'' && !(singleLineComment || multiLineComment)) {
          if (singleQuoteString) {
            singleQuoteString = false
          } else if (!doubleQuoteString) {
            singleQuoteString = true
          }
        }

        // double quote start or end mark
        if (ch == '"' && !(singleLineComment || multiLineComment)) {
          if (doubleQuoteString && idx > 0) {
            doubleQuoteString = false
          } else if (!singleQuoteString) {
            doubleQuoteString = true
          }
        }

        // single line comment or multiple line comment start mark
        if (!singleQuoteString && !doubleQuoteString && !multiLineComment && !singleLineComment && idx < lastIndex) {
          if (isSingleLineComment(sql.charAt(idx), sql.charAt(idx + 1))) {
            singleLineComment = true
          } else if (sql.charAt(idx) == '/' && sql.length > (idx + 2)
            && sql.charAt(idx + 1) == '*' && sql.charAt(idx + 2) != '+') {
            multiLineComment = true
          }
        }

        if (ch == ';' && !singleQuoteString && !doubleQuoteString && !multiLineComment && !singleLineComment) {
          markLineNumber()
          // meet the end of semicolon
          if (query.toString.trim.nonEmpty) {
            queries += query.toString
            query = new mutable.StringBuilder
          }
        } else if (idx == lastIndex) {
          markLineNumber()

          // meet the last character
          if (!singleLineComment && !multiLineComment) {
            query += ch
          }

          if (query.toString.trim.nonEmpty) {
            queries += query.toString
            query = new mutable.StringBuilder
          }
        } else if (!singleLineComment && !multiLineComment) {
          // normal case, not in single line comment and not in multiple line comment
          query += ch
        } else if (ch == '\n') {
          query += ch
        }
      }
    }

    val refinedQueries = new collection.mutable.HashMap[Int, String]()
    for (i <- queries.indices) {
      val currStatement = queries(i)
      if (isSingleLineComment(currStatement) || isMultipleLineComment(currStatement)) {
        // transform comment line as blank lines
        if (refinedQueries.nonEmpty) {
          val lastRefinedQuery = refinedQueries.last
          refinedQueries(refinedQueries.size - 1) =
            lastRefinedQuery + extractLineBreaks(currStatement)
        }
      } else {
        var linesPlaceholder = ""
        if (i > 0) {
          linesPlaceholder = extractLineBreaks(refinedQueries(i - 1))
        }
        // add some blank lines before the statement to keep the original line number
        val refinedQuery = linesPlaceholder + currStatement
        refinedQueries += refinedQueries.size -> refinedQuery
      }
    }

    val set = new ListBuffer[SqlSegment]
    refinedQueries.foreach(x => {
      val line = lineNumMap(x._1)
      set += SqlSegment(line._1, line._2, x._2)
    })
    set.toList.sortWith((a, b) => a.start < b.start)
  }

  /**
   * extract line breaks
   *
   * @param text
   * @return
   */
  private[this] def extractLineBreaks(text: String): String = {
    val builder = new mutable.StringBuilder
    for (i <- 0 until text.length) {
      if (text.charAt(i) == '\n') {
        builder.append('\n')
      }
    }
    builder.toString
  }

  private[this] def isSingleLineComment(text: String) =
    text.trim.startsWith(PARAM_PREFIX)

  private[this] def isMultipleLineComment(text: String) =
    text.trim.startsWith("/*") && text.trim.endsWith("*/")

  /**
   * check single-line comment
   *
   * @param curChar
   * @param nextChar
   * @return
   */
  private[this] def isSingleLineComment(curChar: Char, nextChar: Char): Boolean = {
    var flag = false
    for (singleCommentPrefix <- singleLineCommentPrefixList) {
      singleCommentPrefix.length match {
        case 1 if curChar == singleCommentPrefix.charAt(0) => flag = true
        case 2
            if curChar == singleCommentPrefix.charAt(0) && nextChar == singleCommentPrefix.charAt(
              1) =>
          flag = true
        case _ =>
      }
    }
    flag
  }

}
