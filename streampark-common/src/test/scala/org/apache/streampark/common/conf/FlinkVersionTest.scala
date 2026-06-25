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

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.file.Path

class FlinkVersionTest {

  @Test
  def testParseFlink2FromDistJar(@TempDir tempDir: Path): Unit = {
    val flinkHome = tempDir.toFile
    val lib = new File(flinkHome, "lib")
    lib.mkdirs()
    new File(lib, "flink-dist-2.2.1.jar").createNewFile()

    val flinkVersion = new FlinkVersion(flinkHome.getAbsolutePath)
    assertEquals("2.2.1", flinkVersion.version)
    assertEquals("2.12", flinkVersion.scalaVersion)
    assertTrue(flinkVersion.checkVersion(false))
  }

  @Test
  def testParseFlink1FromDistJar(@TempDir tempDir: Path): Unit = {
    val flinkHome = tempDir.toFile
    val lib = new File(flinkHome, "lib")
    lib.mkdirs()
    new File(lib, "flink-dist_2.12-1.20.0.jar").createNewFile()

    val flinkVersion = new FlinkVersion(flinkHome.getAbsolutePath)
    assertEquals("1.20.0", flinkVersion.version)
    assertEquals("2.12", flinkVersion.scalaVersion)
    assertTrue(flinkVersion.checkVersion(false))
  }

}
