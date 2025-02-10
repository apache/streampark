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
import org.apache.streampark.common.util.Implicits.AutoCloseImplicits

import org.apache.commons.lang3.StringUtils

import java.io._
import java.net.{HttpURLConnection, URL}
import java.time.{Duration, LocalDateTime}
import java.util.{jar, Properties, UUID}
import java.util.concurrent.locks.LockSupport
import java.util.jar.{JarFile, JarInputStream}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Utils extends Logger {

  private[this] lazy val OS = System.getProperty("os.name").toLowerCase

  def isNotEmpty(elem: Any): Boolean = {
    elem match {
      case null => false
      case x if x.isInstanceOf[Array[_]] => elem.asInstanceOf[Array[_]].nonEmpty
      case x if x.isInstanceOf[CharSequence] => elem.toString.trim.nonEmpty
      case x if x.isInstanceOf[Traversable[_]] => x.asInstanceOf[Traversable[_]].nonEmpty
      case x if x.isInstanceOf[Iterable[_]] => x.asInstanceOf[Iterable[_]].nonEmpty
      case x if x.isInstanceOf[JavaCollection[_]] => !x.asInstanceOf[JavaCollection[_]].isEmpty
      case x if x.isInstanceOf[JavaMap[_, _]] => !x.asInstanceOf[JavaMap[_, _]].isEmpty
      case _ => true
    }
  }

  def isEmpty(elem: Any): Boolean = !isNotEmpty(elem)

  def uuid(): String = UUID.randomUUID().toString.replaceAll("-", "")

  @throws[IOException]
  def requireCheckJarFile(jar: URL): Unit = {
    val jarFile: File = Try(new File(jar.toURI)) match {
      case Success(x) => x
      case Failure(_) => throw new IOException(s"JAR file path is invalid $jar")
    }
    if (!jarFile.exists) {
      throw new IOException(s"JAR file does not exist '${jarFile.getAbsolutePath}'")
    }
    if (!jarFile.canRead) {
      throw new IOException(s"JAR file can't be read '${jarFile.getAbsolutePath}'")
    }
    Try(new JarFile(jarFile)) match {
      case Failure(e) =>
        throw new IOException(s"Error while opening jar file '${jarFile.getAbsolutePath}'", e)
      case Success(x) => x.close()
    }
  }

  def getJarManifest(jarFile: File): jar.Manifest = {
    requireCheckJarFile(jarFile.toURL)
    new JarInputStream(new BufferedInputStream(new FileInputStream(jarFile)))
      .autoClose(_.getManifest)
  }

  def getJarManClass(jarFile: File): String = {
    val manifest = getJarManifest(jarFile)
    manifest.getMainAttributes.getValue("Main-Class") match {
      case null => manifest.getMainAttributes.getValue("program-class")
      case v => v
    }
  }

  def copyProperties(original: Properties, target: Properties): Unit =
    original.foreach(x => target.put(x._1, x._2))

  def isLinux: Boolean = OS.indexOf("linux") >= 0

  def isWindows: Boolean = OS.indexOf("windows") >= 0

  /** if any blank strings exist */
  def isAnyBank(items: String*): Boolean =
    items == null || items.exists(StringUtils.isBlank)

  /**
   * calculate the percentage of num1 / num2, the result range from 0 to 100, with one small digit
   * reserve.
   */
  def calPercent(num1: Long, num2: Long): Double =
    if (num1 == 0 || num2 == 0) 0.0
    else "%.1f".format(num1.toDouble / num2.toDouble * 100).toDouble

  def hashCode(elements: Any*): Int = {
    if (elements == null) return 0
    var result = 1
    for (elem <- elements) {
      val hash = if (elem == null) 0 else elem.hashCode
      result = 31 * result + hash
    }
    result
  }

  def close(closeable: AutoCloseable*)(implicit func: Throwable => Unit = null): Unit = {
    closeable.foreach(c => {
      try {
        if (c != null) {
          c match {
            case flushable: Flushable => flushable.flush()
            case _ =>
          }
          c.close()
        }
      } catch {
        case e: Throwable if func != null => func(e)
      }
    })
  }

  @tailrec
  def retry[R](retryCount: Int, interval: Duration = Duration.ofSeconds(5))(f: => R): Try[R] = {
    require(retryCount >= 0)
    Try(f) match {
      case Success(result) => Success(result)
      case Failure(e) if retryCount > 0 =>
        logWarn(s"Retry failed, execution caused by: ", e)
        logWarn(
          s"$retryCount times retry remaining, the next attempt will be in ${interval.toMillis} ms")
        LockSupport.parkNanos(interval.toNanos)
        retry(retryCount - 1, interval)(f)
      case Failure(e) => Failure(e)
    }
  }

  def checkHttpURL(urlString: String): Boolean = {
    Try {
      val url = new URL(urlString)
      val connection = url.openConnection.asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("HEAD")
      connection.getResponseCode == HttpURLConnection.HTTP_OK
    }.getOrElse(false)
  }

  def printLogo(info: String): Unit = {
    // scalastyle:off println
    println("\n")
    println("        _____ __                                             __       ")
    println("       / ___// /_________  ____ _____ ___  ____  ____ ______/ /__     ")
    println("       \\__ \\/ __/ ___/ _ \\/ __ `/ __ `__ \\/ __ \\  __ `/ ___/ //_/")
    println("      ___/ / /_/ /  /  __/ /_/ / / / / / / /_/ / /_/ / /  / ,<        ")
    println("     /____/\\__/_/   \\___/\\__,_/_/ /_/ /_/ ____/\\__,_/_/  /_/|_|   ")
    println("                                       /_/                        \n\n")
    println("    Version:  2.2.0-SNAPSHOT                                          ")
    println("    WebSite:  https://streampark.apache.org                           ")
    println("    GitHub :  https://github.com/apache/streampark                    ")
    println(s"    Info   :  $info                                 ")
    println(s"    Time   :  ${LocalDateTime.now}              \n\n")
    // scalastyle:on println
  }

}
