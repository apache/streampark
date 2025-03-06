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

import javax.annotation.Nonnull

import java.io._
import java.util.{Properties, Scanner}
import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}

object PropertiesUtils extends Logger {

  private[this] lazy val PROPERTY_PATTERN = Pattern.compile("(.*?)=(.*?)")

  private[this] lazy val SPARK_PROPERTY_COMPLEX_PATTERN = Pattern.compile("^[\"']?(.*?)=(.*?)[\"']?$")

  // scalastyle:off
  private[this] lazy val SPARK_ARGUMENT_REGEXP = "\"?(\\s+|$)(?=(([^\"]*\"){2})*[^\"]*$)\"?"
  // scalastyle:on

  private[this] lazy val MULTI_PROPERTY_REGEXP = "-D(.*?)\\s*=\\s*[\\\"|'](.*)[\\\"|']"

  private[this] lazy val MULTI_PROPERTY_PATTERN = Pattern.compile(MULTI_PROPERTY_REGEXP)

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

  private[this] def eachYamlItem(
      k: String,
      v: Any,
      prefix: String = "",
      proper: MutableMap[String, String] = MutableMap[String, String]()): Map[String, String] = {
    v match {
      case map: JavaLinkedMap[_, _] =>
        map
          .flatMap(x => {
            prefix match {
              case "" => eachYamlItem(x._1.toString, x._2, k, proper)
              case other =>
                eachYamlItem(x._1.toString, x._2, s"$other.$k", proper)
            }
          })
          .toMap
      case text =>
        if (text != null) {
          val value = text.toString.trim
          prefix match {
            case "" => proper += k -> value
            case other => proper += s"$other.$k" -> value
          }
        }
        proper.toMap
    }
  }

  def fromYamlText(text: String): Map[String, String] = {
    try {
      new Yaml()
        .load(text)
        .asInstanceOf[java.util.Map[String, Map[String, Any]]]
        .flatMap(x => eachYamlItem(x._1, x._2))
        .toMap
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
    require(
      inputStream != null,
      s"[StreamPark] fromYamlFile: Properties inputStream  must not be null")
    try {
      new Yaml()
        .load(inputStream)
        .asInstanceOf[java.util.Map[String, Map[String, Any]]]
        .flatMap(x => eachYamlItem(x._1, x._2))
        .toMap
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

  /**
   * @param file
   * @return
   */
  def loadFlinkConfYaml(file: File): JavaMap[String, String] = {
    require(
      file != null && file.exists() && file.isFile,
      "[StreamPark] loadFlinkConfYaml: file must not be null")
    loadFlinkConfYaml(org.apache.commons.io.FileUtils.readFileToString(file))
  }

  def loadFlinkConfYaml(yamlContent: String): JavaMap[String, String] = {
    require(
      yamlContent != null && yamlContent.nonEmpty,
      "[StreamPark] loadFlinkConfYaml: yaml must not be null")

    val yaml = new Yaml()
    val parsedMap = yaml.load[java.util.LinkedHashMap[String, Object]](yamlContent)

    def flatten(map: java.util.Map[String, Object], prefix: String = ""): Map[String, String] = {
      map.asScala.flatMap {
        case (k, v: java.util.Map[String, Object] @unchecked) =>
          flatten(v, s"$prefix$k.")
        case (k, v) =>
          Map(s"$prefix$k" -> v.toString)
      }.toMap
    }

    new java.util.HashMap[String, String](flatten(parsedMap).asJava)
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

  @Nonnull def extractMultipleArgumentsAsJava(
      args: Array[String]): JavaMap[String, JavaMap[String, String]] = {
    val map =
      extractMultipleArguments(args).map(c => c._1 -> new JavaHashMap[String, String](c._2))
    new JavaHashMap[String, JavaMap[String, String]](map)
  }

  /** extract spark configuration from sparkApplication.appProperties */
  @Nonnull def extractSparkPropertiesAsJava(properties: String): JavaMap[String, String] =
    new JavaHashMap[String, String](extractSparkProperties(properties))

  @Nonnull def extractSparkProperties(properties: String): Map[String, String] = {
    if (StringUtils.isEmpty(properties)) Map.empty[String, String]
    else {
      val map = mutable.Map[String, String]()
      properties.split("(\\s)*(--conf|-c)(\\s)+") match {
        case d if Utils.isNotEmpty(d) =>
          d.foreach(x => {
            if (x.nonEmpty) {
              val p = SPARK_PROPERTY_COMPLEX_PATTERN.matcher(x)
              if (p.matches) {
                map += p.group(1).trim -> p.group(2).trim
              }
            }
          })
        case _ =>
      }
      map.toMap
    }
  }

  /** extract spark configuration from sparkApplication.appArgs */
  @Nonnull def extractSparkArgumentsAsJava(arguments: String): JavaList[String] = {
    val list = new JavaArrayList[String]()
    if (StringUtils.isEmpty(arguments)) list
    else {
      arguments.split(SPARK_ARGUMENT_REGEXP) match {
        case d if Utils.isNotEmpty(d) =>
          d.foreach(x => {
            if (x.nonEmpty) {
              list.add(x)
            }
          })
        case _ =>
      }
      list
    }
  }
}
