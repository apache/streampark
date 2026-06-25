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

import org.apache.streampark.common.constants.Constants

import java.io.{File, IOException}
import java.net.{URL, URLClassLoader}
import java.util.function.Supplier

import scala.collection.mutable.ArrayBuffer

object ClassLoaderUtils extends Logger {

  private[this] val originalClassLoader: ClassLoader =
    Thread.currentThread().getContextClassLoader

  /**
   * Execute with the specified classloader for scala API
   *
   * @param targetClassLoader
   * @param func
   * @tparam R
   * @return
   */
  def runAsClassLoader[R](targetClassLoader: ClassLoader, func: () => R): R = {
    try {
      Thread.currentThread.setContextClassLoader(targetClassLoader)
      func()
    } catch {
      case e: Exception => throw e
    } finally {
      Thread.currentThread.setContextClassLoader(originalClassLoader)
    }
  }

  /**
   * Execute with the specified classloader for JavaAPI
   *
   * @param targetClassLoader
   * @param supplier
   * @tparam R
   * @return
   */
  def runAsClassLoader[R](targetClassLoader: ClassLoader, supplier: Supplier[R]): R = {
    try {
      Thread.currentThread.setContextClassLoader(targetClassLoader)
      supplier.get()
    } catch {
      case e: Exception => throw e
    } finally {
      Thread.currentThread.setContextClassLoader(originalClassLoader)
    }
  }
  @throws[IOException]
  def cloneClassLoader(): ClassLoader = {
    val urls = originalClassLoader.getResources(".")
    val buffer = ArrayBuffer[URL]()
    while (urls.hasMoreElements) {
      buffer += urls.nextElement()
    }
    new URLClassLoader(buffer.toArray[URL], originalClassLoader)
  }

  def loadJar(jarFilePath: String): Unit = {
    val jarFile = new File(jarFilePath)
    require(
      jarFile.exists,
      s"[StreamPark] ClassLoaderUtils.loadJar: jarFilePath $jarFilePath is not exists")
    require(
      jarFile.isFile,
      s"[StreamPark] ClassLoaderUtils.loadJar: jarFilePath $jarFilePath is not file")
    loadPath(jarFile.getAbsolutePath)
  }

  def loadJars(path: String): Unit = {
    val jarDir = new File(path)
    require(jarDir.exists, s"[StreamPark] ClassLoaderUtils.loadJars: jarPath $path is not exists")
    require(
      jarDir.isDirectory,
      s"[StreamPark] ClassLoaderUtils.loadJars: jarPath $path is not directory")
    require(
      jarDir.listFiles.length > 0,
      s"[StreamPark] ClassLoaderUtils.loadJars: have not jar in path:$path")
    jarDir.listFiles.foreach(x => loadPath(x.getAbsolutePath))
  }

  def loadResource(filepath: String): Unit = {
    val file = new File(filepath)
    addURL(file)
  }

  def loadResourceDir(filepath: String): Unit = {
    val file = new File(filepath)
    loopDirs(file)
  }

  private[this] def loadPath(
      filepath: String,
      ext: List[String] = List(Constants.JAR_SUFFIX, Constants.ZIP_SUFFIX)): Unit = {
    val file = new File(filepath)
    loopFiles(file, ext)
  }

  private[this] def loopDirs(file: File): Unit = {
    if (file.isDirectory) {
      addURL(file)
      file.listFiles.foreach(loopDirs)
    }
  }

  private[this] def loopFiles(file: File, ext: List[String] = List()): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(x => loopFiles(x, ext))
    } else {
      if (ext.isEmpty) {
        addURL(file)
      } else if (ext.exists(x => file.getName.endsWith(x))) {
        Utils.requireCheckJarFile(file.toURI.toURL)
        addURL(file)
      }
    }
  }

  @throws[Exception]
  private[this] def addURL(file: File): Unit = {
    val url = file.toURI.toURL
    val classLoaders = Seq(
      Option(Thread.currentThread().getContextClassLoader),
      Option(ClassLoader.getSystemClassLoader)).flatten.distinct

    var lastError: Exception = null
    classLoaders.foreach { classLoader =>
      try {
        addURLToClasspath(classLoader, url)
        return
      } catch {
        case e: Exception => lastError = e
      }
    }
    throw lastError
  }

  private[this] def addURLToClasspath(classLoader: ClassLoader, url: URL): Unit = {
    classLoader match {
      case urlClassLoader: URLClassLoader =>
        val addURL =
          classOf[URLClassLoader].getDeclaredMethod("addURL", Array(classOf[URL]): _*)
        addURL.setAccessible(true)
        addURL.invoke(urlClassLoader, url)
      case _ =>
        var clazz: Class[_] = classLoader.getClass
        var ucpField: java.lang.reflect.Field = null
        while (clazz != null && ucpField == null) {
          try {
            ucpField = clazz.getDeclaredField("ucp")
          } catch {
            case _: NoSuchFieldException => clazz = clazz.getSuperclass
          }
        }
        if (ucpField == null) {
          throw new NoSuchFieldException("ucp")
        }
        ucpField.setAccessible(true)
        val ucp = ucpField.get(classLoader)
        val addURL =
          ucp.getClass.getDeclaredMethod("addURL", Array(classOf[URL]): _*)
        addURL.setAccessible(true)
        addURL.invoke(ucp, url)
    }
  }
}
