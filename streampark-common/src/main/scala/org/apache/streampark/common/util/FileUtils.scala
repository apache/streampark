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

import java.io._
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.Scanner
import java.util.stream.Collectors

import scala.collection.mutable

object FileUtils {

  private[this] def bytesToHexString(src: Array[Byte]): String = {
    val stringBuilder = new mutable.StringBuilder
    if (src == null || src.length <= 0) return null
    for (i <- src.indices) {
      val v: Int = src(i) & 0xFF
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
    input.using(in => {
      val b = new Array[Byte](4)
      in.read(b, 0, b.length)
      bytesToHexString(b)
    }) == "504B0304"
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

  def mkdir(dir: File): Unit = {
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
    val path = Option(System.getenv(env)).getOrElse(System.getProperty(env))
    AssertUtils.notNull(
      path,
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

  def listFileAsURL(dirPath: String): JavaList[URL] = {
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

  def exists(file: Serializable): Boolean = {
    file match {
      case null => false
      case f: File => f.exists()
      case p => new File(p.toString).exists()
    }
  }

  def directoryNotBlank(file: Serializable): Boolean = {
    file match {
      case null => false
      case f: File => f.isDirectory && f.list().length > 0
      case p =>
        new File(p.toString).isDirectory && new File(p.toString)
          .list()
          .length > 0
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
  def readInputStream(in: InputStream, array: Array[Byte]): Unit = {
    in.using(is => {
      var toRead = array.length
      var ret = 0
      var off = 0
      while (toRead > 0) {
        ret = is.read(array, off, toRead)
        if (ret < 0) throw new IOException("Bad inputStream, premature EOF")
        toRead -= ret
        off += ret
      }
    })
  }

  @throws[IOException]
  def readFile(file: File): String = {
    if (file.length >= Int.MaxValue) {
      throw new IOException("Too large file, unexpected!")
    } else {
      val len = file.length
      val array = new Array[Byte](len.toInt)
      Files
        .newInputStream(file.toPath)
        .using(is => {
          readInputStream(is, array)
          new String(array, StandardCharsets.UTF_8)
        })
    }
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
  def readEndOfFile(file: File, maxSize: Long): Array[Byte] = {
    var readSize = maxSize
    new RandomAccessFile(file, "r").using(raFile => {
      if (raFile.length > maxSize) {
        raFile.seek(raFile.length - maxSize)
      } else if (raFile.length < maxSize) {
        readSize = raFile.length.toInt
      }
      val fileContent = new Array[Byte](readSize.toInt)
      raFile.read(fileContent)
      fileContent
    })
  }

  /**
   * Read the content of a file from a specified offset.
   *
   * @param file
   *   The file to read from
   * @param startOffset
   *   The offset from where to start reading the file
   * @param maxSize
   *   The maximum size of the file to read
   * @return
   *   The content of the file as a byte array
   * @throws IOException
   *   if an I/O error occurs while reading the file
   * @throws IllegalArgumentException
   *   if the startOffset is greater than the file length
   */
  @throws[IOException]
  def readFileFromOffset(file: File, startOffset: Long, maxSize: Long): Array[Byte] = {
    if (file.length < startOffset) {
      throw new IllegalArgumentException(
        s"The startOffset $startOffset is great than the file length ${file.length}")
    }
    new RandomAccessFile(file, "r").using(raFile => {
      val readSize = Math.min(maxSize, file.length - startOffset)
      raFile.seek(startOffset)
      val fileContent = new Array[Byte](readSize.toInt)
      raFile.read(fileContent)
      fileContent
    })
  }

  /**
   * Roll View Log.
   *
   * @param path
   *   The file path.
   * @param offset
   *   The offset.
   * @param limit
   *   The limit.
   * @return
   *   The content of the file.
   */
  def tailOf(path: String, offset: Int, limit: Int): String = {
    val file = new File(path)
    if (file.exists && file.isFile) {
      Files
        .lines(Paths.get(path))
        .using(stream =>
          stream
            .skip(offset)
            .limit(limit)
            .collect(Collectors.joining("\r\n")))
    } else null
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

}
