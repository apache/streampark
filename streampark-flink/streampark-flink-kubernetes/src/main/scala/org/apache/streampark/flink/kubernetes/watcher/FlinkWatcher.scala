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

package org.apache.streampark.flink.kubernetes.watcher

import org.apache.streampark.common.util.{CpuUtils, ThreadUtils}

import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.atomic.AtomicBoolean

import scala.language.implicitConversions

trait FlinkWatcher extends AutoCloseable {

  private[this] val started: AtomicBoolean = new AtomicBoolean(false)

  private val CPU_CORES = CpuUtils.getCpuCores
  private val CPU_NUM = Math.max(4, CPU_CORES * 2)

  val watchExecutor = new ScheduledThreadPoolExecutor(CPU_NUM, ThreadUtils.threadFactory("flink-watcher-thread-"))

  /**
   * Start watcher process. This method should be a thread-safe implementation of light locking and
   * can be called idempotent.
   */
  def start(): Unit = this.synchronized {
    if (!started.getAndSet(true)) {
      this.doStart()
    }
  }

  /**
   * Stop watcher process. This method should be a thread-safe implementation of light locking and
   * can be called idempotent.
   */
  def stop(): Unit = this.synchronized {
    if (started.getAndSet(false)) {
      doStop()
    }
  }

  override def close(): Unit = this.synchronized {
    if (started.get()) {
      this.doStop()
    }
    doClose()
    watchExecutor.shutdownNow()
  }

  /**
   * This method should be a thread-safe implementation of light locking and can be called
   * idempotent.
   */
  def restart(): Unit = this.synchronized {
    stop()
    start()
  }

  def doStart(): Unit

  def doStop(): Unit

  def doClose(): Unit

  def doWatch(): Unit

  /** Runnable streamline syntax */
  implicit protected def funcToRunnable(fun: () => Unit): Runnable =
    new Runnable() {
      def run(): Unit = fun()
    }

}
