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

import org.apache.commons.lang3.StringUtils

import java.io._
import java.lang.{Boolean => JavaBool, Byte => JavaByte, Double => JavaDouble, Float => JavaFloat, Integer => JavaInt, Long => JavaLong, Short => JavaShort}
import java.net.URL
import java.time.Duration
import java.util.{jar, Properties, UUID}
import java.util.concurrent.locks.LockSupport
import java.util.jar.{JarFile, JarInputStream}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object Utils extends Logger {

  private[this] lazy val OS = System.getProperty("os.name").toLowerCase

  def uuid(): String = UUID.randomUUID().toString.replaceAll("-", "")

  @throws[IOException]
  def checkJarFile(jar: URL): Unit = {
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
    checkJarFile(jarFile.toURL)
    new JarInputStream(new BufferedInputStream(new FileInputStream(jarFile))).getManifest
  }

  def getJarManClass(jarFile: File): String = {
    val manifest = getJarManifest(jarFile)
    val mainAttr = manifest.getMainAttributes
    Option(mainAttr.getValue("Main-Class"))
      .getOrElse(Option(mainAttr.getValue("program-class")).orNull)
  }

  def copyProperties(original: Properties, target: Properties): Unit =
    original.foreach(x => target.put(x._1, x._2))

  /** get os name */
  def getOsName: String = OS

  def isLinux: Boolean = OS.indexOf("linux") >= 0

  def isWindows: Boolean = OS.indexOf("windows") >= 0

  /** if any blank strings exist */
  def isAnyBank(items: String*): Boolean = items == null || items.exists(StringUtils.isBlank)

  /*
   * Mimicking the try-with-resource syntax of Java-8+
   */
  def using[R, T <: AutoCloseable](handle: T)(func: T => R)(implicit
      excFunc: Throwable => R = null): R = {
    try {
      func(handle)
    } catch {
      case e: Throwable =>
        if (excFunc != null) {
          excFunc(e)
        } else {
          throw e
        }
    } finally {
      if (handle != null) {
        handle.close()
      }
    }
  }

  def close(closeable: AutoCloseable*)(implicit func: Throwable => Unit = null): Unit = {
    closeable.foreach(
      c => {
        try {
          if (c != null) {
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
        logWarn(s"retry failed, execution caused by: ", e)
        logWarn(
          s"$retryCount times retry remaining, the next attempt will be in ${interval.toMillis} ms")
        LockSupport.parkNanos(interval.toNanos)
        retry(retryCount - 1, interval)(f)
      case Failure(e) => Failure(e)
    }
  }

  /**
   * calculate the percentage of num1 / num2, the result range from 0 to 100, with one small digit
   * reserve.
   */
  def calPercent(num1: Long, num2: Long): Double =
    if (num1 == 0 || num2 == 0) 0.0
    else (num1.toDouble / num2.toDouble * 100).formatted("%.1f").toDouble

  def hashCode(elements: Any*): Int = {
    if (elements == null) return 0
    var result = 1
    for (elem <- elements) {
      val hash = if (elem == null) 0 else elem.hashCode
      result = 31 * result + hash
    }
    result
  }

  def stringifyException(e: Throwable): String = {
    if (e == null) "(null)"
    else {
      try {
        val stm = new StringWriter
        val wrt = new PrintWriter(stm)
        e.printStackTrace(wrt)
        wrt.close()
        stm.toString
      } catch {
        case _: Throwable => e.getClass.getName + " (error while printing stack trace)"
      }
    }
  }

  implicit class StringCasts(v: String) {
    def cast[T](classType: Class[_]): T = {
      classType match {
        case c if c == classOf[String] => v.asInstanceOf[T]
        case c if c == classOf[Byte] => v.toByte.asInstanceOf[T]
        case c if c == classOf[Int] => v.toInt.asInstanceOf[T]
        case c if c == classOf[Long] => v.toLong.asInstanceOf[T]
        case c if c == classOf[Float] => v.toFloat.asInstanceOf[T]
        case c if c == classOf[Double] => v.toDouble.asInstanceOf[T]
        case c if c == classOf[Short] => v.toShort.asInstanceOf[T]
        case c if c == classOf[Boolean] => v.toBoolean.asInstanceOf[T]
        case c if c == classOf[JavaByte] => v.toByte.asInstanceOf[T]
        case c if c == classOf[JavaInt] => JavaInt.valueOf(v).asInstanceOf[T]
        case c if c == classOf[JavaLong] => JavaLong.valueOf(v).asInstanceOf[T]
        case c if c == classOf[JavaFloat] => JavaFloat.valueOf(v).asInstanceOf[T]
        case c if c == classOf[JavaDouble] => JavaDouble.valueOf(v).asInstanceOf[T]
        case c if c == classOf[JavaShort] => JavaShort.valueOf(v).asInstanceOf[T]
        case c if c == classOf[JavaBool] => JavaBool.valueOf(v).asInstanceOf[T]
        case _ =>
          throw new IllegalArgumentException(s"Unsupported type: $classType")
      }
    }
  }

}
