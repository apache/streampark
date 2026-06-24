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

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

import scala.util.Try

object SparkEnvUtils extends Logger {

  private[this] lazy val JAVA_HOME_PATTERN =
    Pattern.compile("""(?:^|\n)\s*(?:export\s+)?JAVA_HOME\s*=\s*(?:["']([^"']+)["']|(\S+))""")

  /** Minimum Java major version required by the given Spark version string. */
  def requiredJavaMajorVersion(sparkVersion: String): Int = {
    sparkVersion.split("\\.").headOption.flatMap(v => Try(v.trim.toInt).toOption) match {
      case Some(major) if major >= 4 => 17
      case _ => 8
    }
  }

  /**
   * Resolve JAVA_HOME for Spark CLI and SparkLauncher.
   *
   * Resolution order:
   * 1. `$SPARK_HOME/conf/spark-env.sh`
   * 2. process environment `JAVA_HOME`
   * 3. system auto-detection (macOS `/usr/libexec/java_home`, common Linux paths)
   */
  def resolveJavaHome(sparkHome: String, sparkVersion: String): Option[String] = {
    val minVersion = requiredJavaMajorVersion(sparkVersion)
    parseJavaHomeFromSparkEnv(sparkHome)
      .filter(isValidJavaHome)
      .orElse(Option(System.getenv("JAVA_HOME")).filter(isValidJavaHome))
      .orElse(detectSystemJavaHome(minVersion).filter(isValidJavaHome))
  }

  def parseJavaHomeFromSparkEnv(sparkHome: String): Option[String] = {
    val sparkEnvFile = new File(sparkHome, "conf/spark-env.sh")
    if (!sparkEnvFile.exists()) {
      None
    } else {
      val content = org.apache.commons.io.FileUtils.readFileToString(sparkEnvFile, StandardCharsets.UTF_8)
      extractJavaHome(content)
    }
  }

  private[util] def extractJavaHome(content: String): Option[String] = {
    val matcher = JAVA_HOME_PATTERN.matcher(content)
    var result: Option[String] = None
    while (matcher.find() && result.isEmpty) {
      val value = Option(matcher.group(1)).getOrElse(matcher.group(2))
      if (value != null && value.nonEmpty && !value.startsWith("#")) {
        result = Some(value.trim)
      }
    }
    result
  }

  private def detectSystemJavaHome(minMajor: Int): Option[String] = {
    val os = System.getProperty("os.name", "").toLowerCase
    if (os.contains("mac")) {
      Try {
        val (code, output) = CommandUtils.execute(s"/usr/libexec/java_home -v $minMajor 2>/dev/null")
        if (code == 0 && output.trim.nonEmpty) Some(output.trim) else None
      }.getOrElse(None)
    } else {
      val candidates = List(
        Option(System.getenv(s"JAVA${minMajor}_HOME")),
        Option(s"/usr/lib/jvm/java-$minMajor-openjdk"),
        Option(s"/usr/lib/jvm/java-$minMajor-openjdk-amd64"),
        Option(s"/usr/lib/jvm/java-$minMajor"))
        .flatten
        .filter(isValidJavaHome)
      candidates.headOption
    }
  }

  private def isValidJavaHome(javaHome: String): Boolean = {
    javaHome != null && javaHome.nonEmpty && new File(javaHome, "bin/java").exists()
  }

}
