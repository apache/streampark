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

import java.io._
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util
import java.util.Scanner

import scala.collection.JavaConversions._
import scala.collection.mutable

object FileUtils {

  private[this] def bytesToHexString(src: Array[Byte]): String = {
    val stringBuilder = new mutable.StringBuilder
    if (src == null || src.length <= 0) return null
    for (i <- src.indices) {
      val v: Int = src(i) & 0xff
      val hv: String = Integer.toHexString(v).toUpperCase
      if (hv.length < 2) {
        stringBuilder.append(0)
      }
      stringBuilder.append(hv)
    }
    stringBuilder.toString
  }

  def isJarFileType(input: InputStream): Boolean = {
    if (input == null) {
      throw new RuntimeException("The inputStream can not be null")
    }
    Utils.using(input) {
      in =>
        val b = new Array[Byte](4)
        in.read(b, 0, b.length)
        bytesToHexString(b)
    } == "504B0304"
  }

  def isJarFileType(file: File): Boolean = {
    if (!file.exists || !file.isFile) {
      throw new RuntimeException("The file does not exist or the path is a directory")
    }
    isJarFileType(new FileInputStream(file))
  }

  def createTempDir(): File = {
    val TEMP_DIR_ATTEMPTS = 10000
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = System.currentTimeMillis + "-"
    for (counter <- 0 until TEMP_DIR_ATTEMPTS) {
      val tempDir = new File(baseDir, baseName + counter)
      if (tempDir.mkdir) {
        return tempDir
      }
    }
    throw new IllegalStateException(
      s"[StreamPark] Failed to create directory within $TEMP_DIR_ATTEMPTS  attempts (tried $baseName 0 to $baseName ${TEMP_DIR_ATTEMPTS - 1})")
  }

  def exists(path: String): Unit = {
    require(
      path != null && path.nonEmpty && new File(path).exists(),
      s"[StreamPark] FileUtils.exists: file $path is not exist!")
  }

  def mkdir(dir: File) = {
    if (dir.exists && !dir.isDirectory) {
      throw new IOException(s"File $dir exists and is not a directory. Unable to create directory.")
    } else if (!dir.mkdirs) {
      // Double-check that some other thread or process hasn't made
      if (!dir.isDirectory) {
        throw new IOException(s"Unable to create directory $dir")
      }
    }
  }

  def getPathFromEnv(env: String): String = {
    val path = System.getenv(env)
    AssertUtils.required(
      AssertUtils.isNotEmpty(path),
      s"[StreamPark] FileUtils.getPathFromEnv: $env is not set on system env")
    val file = new File(path)
    require(file.exists(), s"[StreamPark] FileUtils.getPathFromEnv: $env is not exist!")
    file.getAbsolutePath
  }

  def resolvePath(parent: String, child: String): String = {
    val file = new File(parent, child)
    require(
      file.exists,
      s"[StreamPark] FileUtils.resolvePath: ${file.getAbsolutePath} is not exist!")
    file.getAbsolutePath
  }

  def getSuffix(filename: String): String = {
    require(filename != null)
    filename.drop(filename.lastIndexOf("."))
  }

  def listFileAsURL(dirPath: String): util.List[URL] = {
    new File(dirPath) match {
      case x if x.exists() && x.isDirectory =>
        val files = x.listFiles()
        if (files != null && files.nonEmpty) {
          files.map(f => f.toURI.toURL).toList
        } else {
          util.Collections.emptyList()
        }
      case _ => util.Collections.emptyList()
    }
  }

  def equals(file1: File, file2: File): Boolean = {
    (file1, file2) match {
      case (a, b) if a == null || b == null => false
      case (a, b) if !a.exists() || !b.exists() => false
      case (a, b) if a.getAbsolutePath == b.getAbsolutePath => true
      case (a, b) =>
        val first = new BufferedInputStream(new FileInputStream(a))
        val second = new BufferedInputStream(new FileInputStream(b))
        if (first.available() != second.available()) false;
        else {
          while (true) {
            val firRead = first.read()
            val secRead = second.read()
            if (firRead != secRead) {
              Utils.close(first, second)
              return false
            }
            if (firRead == -1) {
              Utils.close(first, second)
              return true;
            }
          }
          true
        }
    }
  }

  @throws[IOException]
  def readFile(file: File): String = {
    if (file.length >= Int.MaxValue) {
      throw new IOException("Too large file, unexpected!")
    } else {
      val len = file.length
      val array = new Array[Byte](len.toInt)
      val is = Files.newInputStream(file.toPath)
      readInputStream(is, array)
      val content = new String(array, StandardCharsets.UTF_8)
      Utils.close(is)
      content
    }
  }

  @throws[IOException]
  def readInputStream(in: InputStream, array: Array[Byte]): Unit = {
    var toRead = array.length
    var ret = 0
    var off = 0
    while (toRead > 0) {
      ret = in.read(array, off, toRead)
      if (ret < 0) throw new IOException("Bad inputStream, premature EOF")
      toRead -= ret
      off += ret
    }
    Utils.close(in)
  }

  @throws[IOException]
  def writeFile(content: String, file: File): Unit = {
    val outputStream = Files.newOutputStream(file.toPath)
    val channel = Channels.newChannel(outputStream)
    val buffer = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8))
    channel.write(buffer)
    Utils.close(channel, outputStream)
  }

  @throws[IOException]
  def readString(file: File): String = {
    require(file != null && file.isFile)
    val reader = new FileReader(file)
    val scanner = new Scanner(reader)
    val buffer = new mutable.StringBuilder()
    if (scanner.hasNextLine) {
      buffer.append(scanner.nextLine())
    }
    while (scanner.hasNextLine) {
      buffer.append("\r\n")
      buffer.append(scanner.nextLine())
    }
    Utils.close(scanner, reader)
    buffer.toString()
  }

  @throws[IOException]
  def readString(in: InputStream): String = {
    require(in != null)
    val scanner = new Scanner(in)
    val buffer = new mutable.StringBuilder()
    if (scanner.hasNextLine) {
      buffer.append(scanner.nextLine())
    }
    while (scanner.hasNextLine) {
      buffer.append("\r\n")
      buffer.append(scanner.nextLine())
    }
    Utils.close(scanner)
    buffer.toString()
  }

}
