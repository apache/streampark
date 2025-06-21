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

import com.google.common.collect.Lists
import org.apache.commons.lang3.StringUtils

import javax.annotation.Nonnull

import java.util.regex.Pattern

import scala.collection.mutable

object SparkConfigurationUtils extends Logger {

  private[this] val SPARK_PROPERTY_COMPLEX_PATTERN = Pattern.compile("^[\"']?(.*?)=(.*?)[\"']?$")

  // scalastyle:off
  private[this] val SPARK_ARGUMENT_REGEXP = "\"?(\\s+|$)(?=(([^\"]*\"){2})*[^\"]*$)\"?"
  // scalastyle:on

  /** extract spark configuration from sparkApplication.appProperties */
  @Nonnull def extractPropertiesAsJava(properties: String): JavaMap[String, String] =
    new JavaHashMap[String, String](extractProperties(properties))

  @Nonnull def extractProperties(properties: String): Map[String, String] = {
    if (StringUtils.isEmpty(properties)) {
      Map.empty[String, String]
    } else {
      val map = mutable.Map[String, String]()
      properties.split("(\\s)*(--conf|-c)(\\s)+").filter(Utils.isNotEmpty(_))
        .foreach(x =>
          if (x.nonEmpty) {
            val p = SPARK_PROPERTY_COMPLEX_PATTERN.matcher(x)
            if (p.matches) {
              map += p.group(1).trim -> p.group(2).trim
            }
          })
      map.toMap
    }
  }

  /** extract spark configuration from sparkApplication.appArgs */
  @Nonnull def extractArgumentsAsJava(arguments: String): JavaList[String] = {
    if (StringUtils.isEmpty(arguments)) {
      Lists.newArrayList()
    } else {
      arguments.split(SPARK_ARGUMENT_REGEXP).filter(_.nonEmpty).toList.asJava
    }
  }
}
