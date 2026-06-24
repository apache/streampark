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

import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class SparkEnvUtilsTest {

  @TempDir
  private var tempDir: Path = _

  private var sparkHome: File = _

  @AfterEach
  def cleanup(): Unit = {
    if (sparkHome != null && sparkHome.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(sparkHome)
    }
  }

  @Test
  def requiredJavaMajorVersionForSpark4(): Unit = {
    assertEquals(17, SparkEnvUtils.requiredJavaMajorVersion("4.1.2"))
    assertEquals(8, SparkEnvUtils.requiredJavaMajorVersion("3.5.1"))
    assertEquals(8, SparkEnvUtils.requiredJavaMajorVersion("2.4.8"))
  }

  @Test
  def extractJavaHomeFromSparkEnvContent(): Unit = {
    val content =
      """
        |# export other settings
        |export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
        |export HADOOP_CONF_DIR=/etc/hadoop/conf
        |""".stripMargin
    assertEquals(Some("/usr/lib/jvm/java-17-openjdk"), SparkEnvUtils.extractJavaHome(content))
  }

  @Test
  def parseJavaHomeFromSparkEnvFile(): Unit = {
    sparkHome = tempDir.resolve("spark").toFile
    val confDir = new File(sparkHome, "conf")
    confDir.mkdirs()
    org.apache.commons.io.FileUtils.writeStringToFile(
      new File(confDir, "spark-env.sh"),
      "export JAVA_HOME=\"/opt/java/jdk-17\"\n",
      StandardCharsets.UTF_8)

    assertEquals(Some("/opt/java/jdk-17"), SparkEnvUtils.parseJavaHomeFromSparkEnv(sparkHome.getAbsolutePath))
  }

  @Test
  def resolveJavaHomePrefersSparkEnv(): Unit = {
    sparkHome = tempDir.resolve("spark").toFile
    val confDir = new File(sparkHome, "conf")
    confDir.mkdirs()
    val javaHome = tempDir.resolve("jdk-17").toFile
    javaHome.mkdirs()
    new File(javaHome, "bin").mkdirs()
    new File(javaHome, "bin/java").createNewFile()
    org.apache.commons.io.FileUtils.writeStringToFile(
      new File(confDir, "spark-env.sh"),
      s"export JAVA_HOME=${javaHome.getAbsolutePath}\n",
      StandardCharsets.UTF_8)

    assertEquals(
      Some(javaHome.getAbsolutePath),
      SparkEnvUtils.resolveJavaHome(sparkHome.getAbsolutePath, "4.1.2"))
  }

}
