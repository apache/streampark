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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class FlinkEnvUtilsTest {

  @Test
  def testRequiredJavaMajorVersion(): Unit = {
    assertEquals(11, FlinkEnvUtils.requiredJavaMajorVersion("2.2.1"))
    assertEquals(11, FlinkEnvUtils.requiredJavaMajorVersion("2.0.0"))
    assertEquals(8, FlinkEnvUtils.requiredJavaMajorVersion("1.20.0"))
  }

  @Test
  def testExtractJavaHome(): Unit = {
    val content =
      """
        |# comment
        |export JAVA_HOME="/opt/java/jdk-11"
        |""".stripMargin
    assertEquals("/opt/java/jdk-11", FlinkEnvUtils.extractJavaHome(content).orNull)
  }

  @Test
  def testExtractJavaHomeWithoutExport(): Unit = {
    val content = "JAVA_HOME=/usr/lib/jvm/java-11-openjdk\n"
    assertEquals("/usr/lib/jvm/java-11-openjdk", FlinkEnvUtils.extractJavaHome(content).orNull)
  }

}
