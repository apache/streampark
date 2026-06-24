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

package org.apache.streampark.common.conf

import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class SparkVersionTest {

  @TempDir
  private var tempDir: Path = _

  private var sparkHome: File = _

  @AfterEach
  def cleanup(): Unit = {
    if (sparkHome != null && sparkHome.exists()) {
      FileUtils.deleteDirectory(sparkHome)
    }
  }

  @Test
  def parseSparkVersionFromJarWithoutRunningSparkSubmit(): Unit = {
    sparkHome = tempDir.resolve("spark-4.1.2").toFile
    val jarsDir = new File(sparkHome, "jars")
    jarsDir.mkdirs()
    new File(jarsDir, "spark-core_2.13-4.1.2.jar").createNewFile()
    FileUtils.writeStringToFile(
      new File(sparkHome, "RELEASE"),
      "Spark 4.1.2 (git revision f0bb2e6a47d) built for Hadoop 3.4.2\n",
      StandardCharsets.UTF_8)

    val sparkVersion = new SparkVersion(sparkHome.getAbsolutePath)

    assertEquals("4.1.2", sparkVersion.version)
    assertEquals("2.13", sparkVersion.scalaVersion)
    assertEquals("4.1", sparkVersion.majorVersion)
    assertTrue(sparkVersion.checkVersion(false))
  }

}
