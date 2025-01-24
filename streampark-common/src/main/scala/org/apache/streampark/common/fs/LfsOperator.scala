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

package org.apache.streampark.common.fs

import org.apache.streampark.common.util.{AssertUtils, Logger}
import org.apache.streampark.common.util.Utils.isAnyBank

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.lang3.StringUtils

import java.io.{File, FileInputStream}

/** Local File System (aka LFS) Operator */
//noinspection DuplicatedCode
object LfsOperator extends FsOperator with Logger {

  override def exists(path: String): Boolean = {
    StringUtils.isNotBlank(path) && new File(path).exists()
  }

  override def mkdirs(path: String): Unit = {
    if (!isAnyBank(path)) {
      FileUtils.forceMkdir(new File(path))
    }
  }

  override def delete(path: String): Unit = {
    if (AssertUtils.isNotEmpty(path)) {
      val file = new File(path)
      if (file.exists()) {
        FileUtils.forceDelete(file)
      }
    }
  }

  override def move(srcPath: String, dstPath: String): Unit = {
    if (isAnyBank(srcPath, dstPath)) return
    val srcFile = new File(srcPath)
    val dstFile = new File(dstPath)

    if (!srcFile.exists) return
    if (srcFile.getCanonicalPath == dstFile.getCanonicalPath) return

    FileUtils.moveToDirectory(srcFile, dstFile, true)
  }

  override def upload(
      srcPath: String,
      dstPath: String,
      delSrc: Boolean,
      overwrite: Boolean): Unit = {
    if (new File(srcPath).isDirectory) {
      copyDir(srcPath, dstPath, delSrc, overwrite)
    } else {
      copy(srcPath, dstPath, delSrc, overwrite)
    }
  }

  /**
   * When the suffixes of srcPath and dstPath are the same, or the file names are the same, copy to
   * the file, otherwise copy to the directory.
   */
  override def copy(srcPath: String, dstPath: String, delSrc: Boolean, overwrite: Boolean): Unit = {
    if (isAnyBank(srcPath, dstPath)) return

    val srcFile = new File(srcPath)
    if (!srcFile.exists) return
    require(srcFile.isFile, s"[StreamPark] $srcPath must be a file.")

    val dstFile = {
      new File(dstPath) match {
        case f if f.exists() =>
          if (f.isDirectory) new File(f, srcFile.getName) else f
        case f =>
          require(
            f.getParentFile.exists(),
            "[StreamPark] dstPath is invalid and does not exist. Please check")
          f
      }
    }

    require(srcFile.getCanonicalPath != dstFile.getCanonicalPath)

    val shouldCopy =
      if (overwrite) true;
      else {
        if (!dstFile.exists()) true else dstFile.getName != srcFile.getName
      }

    if (shouldCopy) {
      FileUtils.copyFile(srcFile, dstFile)
      if (delSrc) FileUtils.forceDelete(srcFile)
    }
  }

  override def copyDir(
      srcPath: String,
      dstPath: String,
      delSrc: Boolean,
      overwrite: Boolean): Unit = {
    if (isAnyBank(srcPath, dstPath)) return
    val srcFile = new File(srcPath)
    if (!srcFile.exists) return
    require(srcFile.isDirectory, s"[StreamPark] $srcPath must be a directory.")

    val dstFile = new File(dstPath)
    val shouldCopy = {
      if (overwrite || !dstFile.exists) true
      else srcFile.getCanonicalPath != dstFile.getCanonicalPath
    }
    if (shouldCopy) {
      FileUtils.copyDirectory(srcFile, dstFile)
      if (delSrc) FileUtils.deleteDirectory(srcFile)
    }
  }

  override def fileMd5(path: String): String = {
    require(
      path != null && path.nonEmpty,
      s"[StreamPark] LFsOperator.fileMd5: file must not be null.")
    val file = new File(path)
    require(file.exists, s"[StreamPark] LFsOperator.fileMd5: file must exists.")
    DigestUtils.md5Hex(IOUtils.toByteArray(new FileInputStream(path)))
  }

  /** Force delete directory and recreate it. */
  override def mkCleanDirs(path: String): Unit = {
    delete(path)
    mkdirs(path)
  }

  /** list file under directory, one level of traversal only */
  def listDir(path: String): Array[File] = {
    if (path == null || path.trim.isEmpty) Array.empty
    else
      new File(path) match {
        case f if !f.exists => Array()
        case f if f.isFile => Array(f)
        case f => f.listFiles()
      }
  }

}
