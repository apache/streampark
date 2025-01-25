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

import org.apache.streampark.common.conf.ConfigConst._

import java.util.{Map => JavaMap, Properties}

import scala.collection.JavaConversions._
import scala.collection.immutable.{Map => ScalaMap}
import scala.util.Try

private[streampark] object ConfigUtils {

  def getConf(parameter: JavaMap[String, String], prefix: String = "", addfix: String = "")(implicit
      alias: String = ""): Properties = {
    val map = filterParam(parameter, prefix + alias)
    val prop = new Properties()
    map.filter(_._2.nonEmpty).foreach { case (k, v) => prop.put(addfix + k, v) }
    prop
  }

  def getHBaseConfig(parameter: JavaMap[String, String])(implicit alias: String = ""): Properties =
    getConf(parameter, HBASE_PREFIX, HBASE_PREFIX)

  def getInfluxConfig(parameter: JavaMap[String, String])(implicit alias: String = ""): Properties =
    getConf(parameter, INFLUX_PREFIX)

  def getKafkaSinkConf(
      parameter: JavaMap[String, String],
      topic: String = "",
      alias: String = ""): Properties = {
    val prefix = KAFKA_SINK_PREFIX + alias
    val param: ScalaMap[String, String] =
      filterParam(parameter, if (prefix.endsWith(".")) prefix else s"$prefix.")
    if (param.isEmpty) throw new IllegalArgumentException(s"$topic init error...")
    else {
      val kafkaProperty = new Properties()
      param.foreach(x => kafkaProperty.put(x._1, x._2.trim))
      val _topic = topic match {
        case SIGN_EMPTY =>
          val top = kafkaProperty.getOrElse(KEY_KAFKA_TOPIC, null)
          if (top == null || top.split(",|\\s+").length > 1) {
            throw new IllegalArgumentException(
              s"Can't find a unique topic!!!,you must be input a topic")
          } else top
        case t => t
      }
      val hasTopic = !kafkaProperty.toMap.exists(
        x => x._1 == KEY_KAFKA_TOPIC && x._2.split(",|\\s+").toSet.contains(_topic))
      if (hasTopic) {
        throw new IllegalArgumentException(s"Can't find a topic of:${_topic}!!!")
      } else {
        kafkaProperty.put(KEY_KAFKA_TOPIC, _topic)
        kafkaProperty
      }
    }
  }

  /**
   * @param parameter
   * @param dialect
   * @param alias
   * @return
   */
  def getJdbcProperties(parameter: JavaMap[String, String], alias: String = ""): Properties = {
    val prefix = alias match {
      case "" | null => KEY_JDBC_PREFIX
      case other => s"$KEY_JDBC_PREFIX$other".replaceFirst("\\.+$|$", ".")
    }
    val driver = parameter.toMap.getOrDefault(s"$prefix$KEY_JDBC_DRIVER", null)
    val url = parameter.toMap.getOrDefault(s"$prefix$KEY_JDBC_URL", null)
    val user = parameter.toMap.getOrDefault(s"$prefix$KEY_JDBC_USER", null)
    val password = parameter.toMap.getOrDefault(s"$prefix$KEY_JDBC_PASSWORD", null)

    (driver, url, user, password) match {
      case (x, y, _, _) if x == null || y == null =>
        throw new IllegalArgumentException(
          s"Jdbc instance:$prefix error,[driver|url] must not be null")
      case (_, _, x, y) if (x != null && y == null) || (x == null && y != null) =>
        throw new IllegalArgumentException(
          "Jdbc instance:" + prefix + " error, [user|password] must be all null,or all not null ")
      case _ =>
    }
    val param: ScalaMap[String, String] = filterParam(parameter, prefix)
    val properties = new Properties()
    val aliasName = if (alias == null || alias.trim == "") "default" else alias
    properties.put(KEY_ALIAS, aliasName)
    properties.put(KEY_JDBC_DRIVER, driver)
    param.foreach(x => properties.put(x._1, x._2))
    properties
  }

  private[this] def filterParam(
      parameter: JavaMap[String, String],
      fix: String): ScalaMap[String, String] = {
    parameter.toMap
      .filter(x => x._1.startsWith(fix) && Try(x._2 != null).getOrElse(false))
      .flatMap(x => Some(x._1.substring(fix.length).replaceFirst("^\\.", "") -> x._2))
  }

}
