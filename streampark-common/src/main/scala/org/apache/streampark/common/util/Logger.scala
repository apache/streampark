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

import org.apache.streampark.shaded.ch.qos.logback.classic.LoggerContext
import org.apache.streampark.shaded.ch.qos.logback.classic.joran.JoranConfigurator
import org.apache.streampark.shaded.ch.qos.logback.classic.util.{ContextInitializer => LogBackContextInitializer}
import org.apache.streampark.shaded.ch.qos.logback.classic.util.ContextSelectorStaticBinder
import org.apache.streampark.shaded.ch.qos.logback.core.{CoreConstants, LogbackException}
import org.apache.streampark.shaded.ch.qos.logback.core.status.StatusUtil
import org.apache.streampark.shaded.ch.qos.logback.core.util.StatusPrinter
import org.apache.streampark.shaded.org.slf4j.{ILoggerFactory, Logger => Slf4JLogger}
import org.apache.streampark.shaded.org.slf4j.spi.LoggerFactoryBinder

import java.io.{ByteArrayInputStream, File}
import java.net.URL
import java.nio.charset.StandardCharsets

import scala.util.Try

trait Logger {

  @transient private[this] var _logger: Slf4JLogger = _

  private[this] val prefix = "[StreamPark]"

  protected def logName: String = this.getClass.getName.stripSuffix("$")

  protected def logger: Slf4JLogger = {
    if (_logger == null) {
      _logger = LoggerFactory.getLoggerFactory().getLogger(logName)
    }
    _logger
  }

  protected def logInfo(msg: => String) {
    if (logger.isInfoEnabled) logger.info(s"$prefix $msg")
  }

  protected def logInfo(msg: => String, throwable: Throwable) {
    if (logger.isInfoEnabled) logger.info(s"$prefix $msg", throwable)
  }

  protected def logDebug(msg: => String) {
    if (logger.isDebugEnabled) logger.debug(s"$prefix $msg")
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (logger.isDebugEnabled) logger.debug(s"$prefix $msg", throwable)
  }

  def logTrace(msg: => String) {
    if (logger.isTraceEnabled) logger.trace(s"$prefix $msg")
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (logger.isTraceEnabled) logger.trace(s"$prefix $msg", throwable)
  }

  def logWarn(msg: => String) {
    if (logger.isWarnEnabled) logger.warn(s"$prefix $msg")
  }

  protected def logWarn(msg: => String, throwable: Throwable) {
    if (logger.isWarnEnabled) logger.warn(s"$prefix $msg", throwable)
  }

  protected def logError(msg: => String) {
    if (logger.isErrorEnabled) logger.error(s"$prefix $msg")
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (logger.isErrorEnabled) logger.error(s"$prefix $msg", throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    logger.isTraceEnabled
  }

}

private[this] object LoggerFactory extends LoggerFactoryBinder {

  private lazy val contextSelectorBinder: ContextSelectorStaticBinder = {
    val defaultLoggerContext = new LoggerContext

    Try(new ContextInitializer(defaultLoggerContext).autoConfig())
      .recover[Unit] {
        case e =>
          val msg = "Failed to auto configure default logger context"
          // scalastyle:off println
          System.err.println(msg)
          // scalastyle:off println
          System.err.println("Reported exception:")
          e.printStackTrace()
      }

    if (!StatusUtil.contextHasStatusListener(defaultLoggerContext)) {
      StatusPrinter.printInCaseOfErrorsOrWarnings(defaultLoggerContext)
    }

    val selectorBinder = new ContextSelectorStaticBinder()
    selectorBinder.init(defaultLoggerContext, new Object())
    selectorBinder
  }

  override def getLoggerFactory: ILoggerFactory = {
    if (contextSelectorBinder.getContextSelector == null) {
      throw new IllegalStateException(
        "contextSelector cannot be null. See also " + CoreConstants.CODES_URL + "#null_CS")
    }
    contextSelectorBinder.getContextSelector.getLoggerContext
  }

  override def getLoggerFactoryClassStr: String = contextSelectorBinder.getClass.getName

  private class ContextInitializer(loggerContext: LoggerContext)
    extends LogBackContextInitializer(loggerContext) {

    private val shadedPackage = "org.apache.streampark.shaded"

    override def configureByResource(url: URL): Unit = {
      AssertUtils.notNull(url, "URL argument cannot be null")
      val path = url.getPath
      if (path.endsWith("xml")) {
        val configurator = new JoranConfigurator()
        configurator.setContext(loggerContext)
        val text = FileUtils
          .readString(new File(path))
          .replaceAll("org.slf4j", s"$shadedPackage.org.slf4j")
          .replaceAll("ch.qos.logback", s"$shadedPackage.ch.qos.logback")
          .replaceAll("org.apache.log4j", s"$shadedPackage.org.apache.log4j")

        val input = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8))
        configurator.doConfigure(input)
      } else
        throw {
          new LogbackException(
            "Unexpected filename extension of file [" + url.toString + "]. Should be .xml")
        }
    }

  }

}
