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

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.yaml.snakeyaml.Yaml

import java.io._
import java.util.{Properties, Scanner}

import scala.collection.mutable

object PropertiesUtils extends Logger {

  def readFile(filename: String): String = {
    val file = new File(filename)
    require(file.exists(), s"[StreamPark] readFile: file $file does not exist")
    require(file.isFile, s"[StreamPark] readFile: file $file is not a normal file")
    val scanner = new Scanner(file)
    val buffer = new mutable.StringBuilder
    while (scanner.hasNextLine) {
      buffer.append(scanner.nextLine()).append("\r\n")
    }
    scanner.close()
    buffer.toString()
  }

  def fromYamlText(text: String): Map[String, String] = {
    try {
      val map = new Yaml().load[JavaMap[String, Object]](text)
      flatten(map)
    } catch {
      case e: IOException =>
        throw new IllegalArgumentException(s"Failed when loading conf error:", e)
    }
  }

  def fromHoconText(conf: String): Map[String, String] = {
    require(conf != null, s"[StreamPark] fromHoconText: Hocon content must not be null")
    try parseHoconByReader(new StringReader(conf))
    catch {
      case e: IOException => throw new IllegalArgumentException(s"Failed when loading Hocon ", e)
    }
  }

  def fromPropertiesText(conf: String): Map[String, String] = {
    try {
      val properties = new Properties()
      properties.load(new StringReader(conf))
      properties.stringPropertyNames().map(k => (k, properties.getProperty(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new IllegalArgumentException(s"Failed when loading properties ", e)
    }
  }

  /** Load Yaml present in the given file. */
  def fromYamlFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"[StreamPark] fromYamlFile: Yaml file $file does not exist")
    require(file.isFile, s"[StreamPark] fromYamlFile: Yaml file $file is not a normal file")
    val inputStream: InputStream = new FileInputStream(file)
    fromYamlFile(inputStream)
  }

  def fromHoconFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"[StreamPark] fromHoconFile: file $file does not exist")
    val inputStream = new FileInputStream(file)
    fromHoconFile(inputStream)
  }

  /** Load properties present in the given file. */
  def fromPropertiesFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"[StreamPark] fromPropertiesFile: Properties file $file does not exist")
    require(
      file.isFile,
      s"[StreamPark] fromPropertiesFile: Properties file $file is not a normal file")
    val inputStream = new FileInputStream(file)
    fromPropertiesFile(inputStream)
  }

  /** Load Yaml present in the given file. */
  def fromYamlFile(inputStream: InputStream): Map[String, String] = {
    AssertUtils.required(
      inputStream != null,
      s"[StreamPark] fromYamlFile: Properties inputStream  must not be null")
    try {
      val map = new Yaml().load[JavaMap[String, Object]](inputStream)
      flatten(map)
    } catch {
      case e: IOException =>
        throw new IllegalArgumentException(s"Failed when loading yaml from inputStream", e)
    } finally {
      inputStream.close()
    }
  }

  def fromHoconFile(inputStream: InputStream): Map[String, String] = {
    require(inputStream != null, s"[StreamPark] fromHoconFile: Hocon inputStream  must not be null")
    try
      parseHoconByReader(new InputStreamReader(inputStream))
    catch {
      case e: IOException => throw new IllegalArgumentException(s"Failed when loading Hocon ", e)
    }
  }

  private[this] def parseHoconByReader(reader: Reader): Map[String, String] = {
    try {
      ConfigFactory
        .parseReader(reader)
        .entrySet()
        .map {
          x =>
            val k = x.getKey.trim.replaceAll("\"", "")
            val v = x.getValue.unwrapped().toString.trim
            k -> v
        }
        .toMap
    } catch {
      case e: IOException => throw new IllegalArgumentException(s"Failed when loading Hocon ", e)
    }
  }

  /** Load properties present in the given file. */
  def fromPropertiesFile(inputStream: InputStream): Map[String, String] = {
    require(
      inputStream != null,
      s"[StreamPark] fromPropertiesFile: Properties inputStream  must not be null")
    try {
      val properties = new Properties()
      properties.load(inputStream)
      properties.stringPropertyNames().map(k => (k, properties.getProperty(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new IllegalArgumentException(
          s"[StreamPark] Failed when loading properties from inputStream",
          e)
    }
  }

  def fromYamlTextAsJava(text: String): JavaMap[String, String] =
    new JavaHashMap[String, String](fromYamlText(text))

  def fromHoconTextAsJava(text: String): JavaMap[String, String] =
    new JavaHashMap[String, String](fromHoconText(text))

  def fromPropertiesTextAsJava(text: String): JavaMap[String, String] =
    new JavaHashMap[String, String](fromPropertiesText(text))

  def fromYamlFileAsJava(filename: String): JavaMap[String, String] =
    new JavaHashMap[String, String](fromYamlFile(filename))

  def fromHoconFileAsJava(filename: String): JavaMap[String, String] =
    new JavaHashMap[String, String](fromHoconFile(filename))

  def fromPropertiesFileAsJava(filename: String): JavaMap[String, String] =
    new JavaHashMap[String, String](fromPropertiesFile(filename))

  def fromYamlFileAsJava(inputStream: InputStream): JavaMap[String, String] =
    new JavaHashMap[String, String](fromYamlFile(inputStream))

  def fromHoconFileAsJava(inputStream: InputStream): JavaMap[String, String] =
    new JavaHashMap[String, String](fromHoconFile(inputStream))

  def fromPropertiesFileAsJava(inputStream: InputStream): JavaMap[String, String] =
    new JavaHashMap[String, String](fromPropertiesFile(inputStream))

  private[this] def flatten(map: JavaMap[String, Object], prefix: String = ""): Map[String, String] = {
    map.asScala.flatMap {
      case (k, v: JavaMap[String, Object] @unchecked) => flatten(v, s"$prefix$k.")
      case (k, v: String) =>
        if (StringUtils.isBlank(v)) Map.empty[String, String] else Map(s"$prefix$k" -> v)
      case (k, v: JavaCollection[_]) =>
        if (v.isEmpty) Map.empty[String, String] else Map(s"$prefix$k" -> v.toString)
      case (k, v) =>
        if (v == null) Map.empty[String, String] else Map(s"$prefix$k" -> v.toString)
    }.toMap
  }

}
