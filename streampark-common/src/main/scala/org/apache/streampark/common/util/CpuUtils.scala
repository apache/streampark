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

import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException

object CpuUtils extends Logger {

  private val cpuCores: Int = CGroupCpuReader.getAvailableCpuCores

  logger.info("Available CPU Cores: " + cpuCores)

  def getCpuCores: Int = {
    cpuCores
  }
}

object CGroupCpuReader extends Logger {
  private val CGROUP_CPU_QUOTA_FILE = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
  private val CGROUP_CPU_PERIOD_FILE = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"
  private val CGROUP_V2_CPU_MAX_FILE = "/sys/fs/cgroup/cpu.max"
  private val CPU_LIMIT_ENV = "CPU_LIMIT"

  def getAvailableCpuCores: Int =
    try {
      if (System.getenv(CPU_LIMIT_ENV) != null) {
        val cpuLimit = System.getenv(CPU_LIMIT_ENV)
        try {
          val cpuLimitInt = cpuLimit.toInt
          if (cpuLimitInt > 0) {
            return cpuLimitInt
          }
        } catch {
          case _: Throwable => logger.error("Invalid CPU_LIMIT value: " + cpuLimit)
        }
      }
      val quota = readCpuQuota
      val period = readCpuPeriod
      if (quota == -1) {
        Runtime.getRuntime.availableProcessors
      } else Math.ceil(quota.toDouble / period).toInt
    } catch {
      case e @ (_: IOException | _: NumberFormatException) =>
        Runtime.getRuntime.availableProcessors
    }

  @throws[IOException]
  private def readCpuQuota = readCgroupValue(CGROUP_CPU_QUOTA_FILE, CGROUP_V2_CPU_MAX_FILE, 0)

  @throws[IOException]
  private def readCpuPeriod = readCgroupValue(CGROUP_CPU_PERIOD_FILE, CGROUP_V2_CPU_MAX_FILE, 1)

  @throws[IOException]
  private def readCgroupValue(v1Path: String, v2Path: String, index: Int): Long = {
    var line = readFirstLine(v1Path)
    if (line != null) return line.trim.toLong
    line = readFirstLine(v2Path)
    if (line != null) {
      val parts = line.split("\\s+")
      if (parts.length >= 2) {
        val value = parts(index).trim
        if ("max" == value) return -1
        return line.trim.toLong
      }
    }
    throw new IOException("CGroup file not found or invalid")
  }

  private def readFirstLine(path: String) = {
    val reader = new BufferedReader(new FileReader(path))
    try reader.readLine
    catch {
      case e: IOException =>
        null
    } finally if (reader != null) reader.close()
  }
}
