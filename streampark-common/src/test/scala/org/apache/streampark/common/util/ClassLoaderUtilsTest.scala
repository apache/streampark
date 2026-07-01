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

import org.junit.jupiter.api.{Assertions, Test}

import java.io.{File, FileOutputStream}
import java.util.jar.{JarEntry, JarOutputStream}

class ClassLoaderUtilsTest {

  @Test def loadJarShouldAppendJarToSystemClassloader(): Unit = {
    val jarFile = File.createTempFile("streampark-classloader-test", ".jar")
    jarFile.deleteOnExit()
    try {
      val jarOut = new JarOutputStream(new FileOutputStream(jarFile))
      try {
        jarOut.putNextEntry(new JarEntry("META-INF/MANIFEST.MF"))
        jarOut.write("Manifest-Version: 1.0\n".getBytes("UTF-8"))
        jarOut.closeEntry()
      } finally {
        jarOut.close()
      }
      ClassLoaderUtils.loadJar(jarFile.getAbsolutePath)
    } finally {
      jarFile.delete()
    }
  }

  @Test def loadResourceShouldAppendDirectoryToSystemClassloader(): Unit = {
    val dir = FileUtils.createTempDir()
    try {
      ClassLoaderUtils.loadResource(dir.getAbsolutePath)
    } finally {
      Assertions.assertTrue(dir.delete())
    }
  }
}
