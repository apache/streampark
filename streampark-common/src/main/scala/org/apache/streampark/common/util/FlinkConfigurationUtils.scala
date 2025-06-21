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

package org.apache.streampark.common.util

import org.apache.streampark.common.util.Implicits._

import org.apache.commons.lang3.StringUtils

import javax.annotation.Nonnull

import java.io.File
import java.util.Scanner
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object FlinkConfigurationUtils extends Logger {

  private[this] val PROPERTY_PATTERN = Pattern.compile("(.*?)=(.*?)")

  private[this] val MULTI_PROPERTY_REGEXP = "-D(.*?)\\s*=\\s*[\\\"|'](.*)[\\\"|']"

  private[this] val MULTI_PROPERTY_PATTERN = Pattern.compile(MULTI_PROPERTY_REGEXP)

  /**
   * @param file
   * @return
   */
  def loadFlinkConf(file: File): JavaMap[String, String] = {
    AssertUtils.required(
      file != null && file.exists() && file.isFile,
      "[StreamPark] loadFlinkConfYaml: file must not be null")
    loadFlinkConf(org.apache.commons.io.FileUtils.readFileToString(file))
  }

  def loadFlinkConf(yaml: String): JavaMap[String, String] = {
    AssertUtils.required(yaml != null && yaml.nonEmpty, "[StreamPark] loadFlinkConfYaml: yaml must not be null")
    PropertiesUtils.fromYamlText(yaml)
  }

  def loadLegacyFlinkConf(file: File): JavaMap[String, String] = {
    AssertUtils.required(
      file != null && file.exists() && file.isFile,
      "[StreamPark] loadFlinkConfYaml: file must not be null")
    loadLegacyFlinkConf(org.apache.commons.io.FileUtils.readFileToString(file))
  }

  def loadLegacyFlinkConf(yaml: String): JavaMap[String, String] = {
    AssertUtils.required(yaml != null && yaml.nonEmpty, "[StreamPark] loadFlinkConfYaml: yaml must not be null")
    val flinkConf = new JavaHashMap[String, String]()
    val scanner: Scanner = new Scanner(yaml)
    val lineNo: AtomicInteger = new AtomicInteger(0)
    while (scanner.hasNextLine) {
      val line = scanner.nextLine()
      lineNo.incrementAndGet()
      // 1. check for comments
      // [FLINK-27299] flink parsing parameter bug fixed.
      val comments = line.split("^#|\\s+#", 2)
      val conf = comments(0).trim
      // 2. get key and value
      if (conf.nonEmpty) {
        val kv = conf.split(": ", 2)
        // skip line with no valid key-value pair
        if (kv.length == 2) {
          val key = kv(0).trim
          val value = kv(1).trim
          // sanity check
          if (key.nonEmpty && value.nonEmpty) {
            flinkConf += key -> value
          } else {
            logWarn(s"Error after splitting key and value in configuration ${lineNo.get()}: $line")
          }
        } else {
          logWarn(s"Error while trying to split key and value in configuration. $lineNo : $line")
        }
      }
    }
    flinkConf
  }

  /** extract flink configuration from application.properties */
  @Nonnull def extractDynamicProperties(properties: String): Map[String, String] = {
    if (StringUtils.isEmpty(properties)) Map.empty[String, String]
    else {
      val map = mutable.Map[String, String]()
      val simple = properties.replaceAll(MULTI_PROPERTY_REGEXP, "")
      simple.split("\\s?-D") match {
        case d if Utils.isNotEmpty(d) =>
          d.foreach(x => {
            if (x.nonEmpty) {
              val p = PROPERTY_PATTERN.matcher(x.trim)
              if (p.matches) {
                map += p.group(1).trim -> p.group(2).trim
              }
            }
          })
        case _ =>
      }
      val matcher = MULTI_PROPERTY_PATTERN.matcher(properties)
      while (matcher.find()) {
        val opts = matcher.group()
        val index = opts.indexOf("=")
        val key = opts.substring(2, index).trim
        val value =
          opts.substring(index + 1).trim.replaceAll("(^[\"|']|[\"|']$)", "")
        map += key -> value
      }
      map.toMap
    }
  }

  @Nonnull def extractArguments(args: String): List[String] = {
    val programArgs = new ArrayBuffer[String]()
    if (StringUtils.isNotEmpty(args)) {
      return extractArguments(args.split("\\s+"))
    }
    programArgs.toList
  }

  def extractArguments(array: Array[String]): List[String] = {
    val programArgs = new ArrayBuffer[String]()
    val iter = array.iterator
    while (iter.hasNext) {
      val v = iter.next()
      val p = v.take(1)
      p match {
        case "'" | "\"" =>
          var value = v
          if (!v.endsWith(p)) {
            while (!value.endsWith(p) && iter.hasNext) {
              value += s" ${iter.next()}"
            }
          }
          programArgs += value.substring(1, value.length - 1)
        case _ =>
          val regexp = "(.*)='(.*)'$"
          if (v.matches(regexp)) {
            programArgs += v.replaceAll(regexp, "$1=$2")
          } else {
            val regexp = "(.*)=\"(.*)\"$"
            if (v.matches(regexp)) {
              programArgs += v.replaceAll(regexp, "$1=$2")
            } else {
              programArgs += v
            }
          }
      }
    }
    programArgs.toList
  }

  def extractMultipleArguments(array: Array[String]): Map[String, Map[String, String]] = {
    val iter = array.iterator
    val map = mutable.Map[String, mutable.Map[String, String]]()
    while (iter.hasNext) {
      val v = iter.next()
      v.take(2) match {
        case "--" =>
          val kv = iter.next()
          val regexp = "(.*)=(.*)"
          if (kv.matches(regexp)) {
            val values = kv.split("=")
            val k1 = values(0).trim
            val v1 = values(1).replaceAll("^['|\"]|['|\"]$", "")
            val k = v.drop(2)
            map.get(k) match {
              case Some(m) => m += k1 -> v1
              case _ => map += k -> mutable.Map(k1 -> v1)
            }
          }
        case _ =>
      }
    }
    map.map(x => x._1 -> x._2.toMap).toMap
  }

  @Nonnull def extractDynamicPropertiesAsJava(properties: String): JavaMap[String, String] =
    new JavaHashMap[String, String](extractDynamicProperties(properties))

  @Nonnull def extractMultipleArgumentsAsJava(args: Array[String]): JavaMap[String, JavaMap[String, String]] = {
    val map =
      extractMultipleArguments(args).map(c => c._1 -> new JavaHashMap[String, String](c._2))
    new JavaHashMap[String, JavaMap[String, String]](map)
  }

}
