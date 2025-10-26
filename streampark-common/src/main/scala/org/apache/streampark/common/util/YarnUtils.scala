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

import org.apache.streampark.common.conf.{CommonConfig, InternalConfigHolder}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.CommonConfigurationKeys
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.records.YarnApplicationState._
import org.apache.hadoop.yarn.conf.{HAUtil, YarnConfiguration}
import org.apache.hadoop.yarn.util.{ConverterUtils, RMHAUtils}
import org.apache.http.client.config.RequestConfig

import java.io.IOException
import java.net.{ConnectException, InetAddress}
import java.security.PrivilegedExceptionAction
import java.util
import java.util.{List => JavaList}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object YarnUtils extends Logger {

  private[this] var rmHttpURL: String = _

  private lazy val PROXY_YARN_URL =
    InternalConfigHolder.get[String](CommonConfig.STREAMPARK_PROXY_YARN_URL)

  /**
   * hadoop.http.authentication.type<br> get yarn http authentication mode.<br> ex: simple, kerberos
   *
   * @return
   */
  lazy val hasYarnHttpKerberosAuth: Boolean = {
    val yarnHttpAuth: String = InternalConfigHolder.get[String](CommonConfig.STREAMPARK_YARN_AUTH)
    "kerberos".equalsIgnoreCase(yarnHttpAuth)
  }

  lazy val hasYarnHttpSimpleAuth: Boolean = {
    val yarnHttpAuth: String = InternalConfigHolder.get[String](CommonConfig.STREAMPARK_YARN_AUTH)
    "simple".equalsIgnoreCase(yarnHttpAuth)
  }

  /**
   * @param appName
   * @return
   */
  def getAppId(appName: String): JavaList[ApplicationId] = {
    val appStates = util.EnumSet.of(RUNNING, ACCEPTED, SUBMITTED)
    val appIds =
      try {
        HadoopUtils.yarnClient
          .getApplications(appStates)
          .filter(_.getName == appName)
          .map(_.getApplicationId)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          ArrayBuffer.empty[ApplicationId]
      }
    appIds.toList
  }

  /**
   * get yarn application state
   *
   * @param appId
   * @return
   */
  def getState(appId: String): YarnApplicationState = {
    val applicationId = ConverterUtils.toApplicationId(appId)
    val state =
      try {
        val applicationReport = HadoopUtils.yarnClient.getApplicationReport(applicationId)
        applicationReport.getYarnApplicationState
      } catch {
        case e: Exception =>
          e.printStackTrace()
          null
      }
    state
  }

  /**
   * Determine if the task named appName is running in yarn
   *
   * @return
   *   boolean
   * @param appName
   * @return
   */
  def isContains(appName: String): Boolean = {
    val runningApps = HadoopUtils.yarnClient.getApplications(util.EnumSet.of(RUNNING))
    if (runningApps != null) {
      runningApps.exists(_.getName == appName)
    } else {
      false
    }
  }

  def getRMWebAppProxyURL: String = {
    if (StringUtils.isNotBlank(PROXY_YARN_URL)) PROXY_YARN_URL else getRMWebAppURL()
  }

  /**
   * <pre>
   *
   * @return
   *   </pre>
   */
  def getRMWebAppURL(getLatest: Boolean = false): String = {
    if (!getLatest && rmHttpURL != null) {
      return rmHttpURL
    }
    synchronized {
      val conf = HadoopUtils.hadoopConf
      val (webConfKey, defaultPort, protocol) = {
        if (YarnConfiguration.useHttps(conf)) {
          (
            YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT,
            "https://")
        } else {
          (YarnConfiguration.RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_PORT, "http://")
        }
      }

      def findActiveRMId(yarnConf: YarnConfiguration): String = {
        Option(RMHAUtils.findActiveRMHAId(yarnConf)) match {
          case Some(rmId) =>
            logInfo("findActiveRMHAId successful")
            rmId
          case _ =>
            // if you don't know why, don't modify it
            logWarn(
              s"findActiveRMHAId is null, config yarn.acl.enable:${yarnConf.get("yarn.acl.enable")},now http try it."
            )

            val rpcTimeoutForChecks = yarnConf.getInt(
              CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
              CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT
            )

            HAUtil
              .getRMHAIds(conf)
              .find {
                id =>
                  val url =
                    protocol + Option(conf.get(HAUtil.addSuffix(webConfKey, id))).getOrElse {
                      val hostname = conf.get(HAUtil.addSuffix("yarn.resourcemanager.hostname", id))
                      s"$hostname:$defaultPort"
                    }
                  // try check yarn url
                  HttpClientUtils.tryCheckUrl(url, rpcTimeoutForChecks)
              }
              .getOrElse(throw new IOException(
                "[StreamPark] YarnUtils.getRMWebAppURL: can not found yarn active node"))
        }
      }

      def getAddressConfKey: String = {
        val yarnConf = new YarnConfiguration(conf)
        val activeRMId = findActiveRMId(yarnConf)
        logInfo(s"current activeRMHAId: $activeRMId")
        val activeRMKey = HAUtil.addSuffix(webConfKey, activeRMId)
        val hostRMKey = HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, activeRMId)
        (activeRMKey, hostRMKey) match {
          case (key, host)
              if HAUtil.getConfValueForRMInstance(key, yarnConf) == null &&
                HAUtil.getConfValueForRMInstance(host, yarnConf) != null =>
            logInfo(s"Find rm web address by : $host")
            host
          case (key, _) =>
            logInfo(s"Find rm web address by : $key")
            key
        }
      }

      rmHttpURL = protocol + Option(conf.get("yarn.web-proxy.address", null)).getOrElse {
        val addrKey = if (!HAUtil.isHAEnabled(conf)) webConfKey else getAddressConfKey
        val socketAddr = conf.getSocketAddr(addrKey, s"0.0.0.0:$defaultPort", defaultPort)
        val address = NetUtils.getConnectAddress(socketAddr)
        val resolved = address.getAddress

        val hostName = {
          if (resolved != null && !resolved.isAnyLocalAddress && !resolved.isLoopbackAddress) {
            address.getHostName
          } else {
            Try(InetAddress.getLocalHost.getCanonicalHostName) match {
              case Success(value) => value
              case _ => address.getHostName
            }
          }
        }
        s"$hostName:${address.getPort}"
      }
      logInfo(s"yarn resourceManager webapp url:$rmHttpURL")
    }
    rmHttpURL
  }

  /**
   * @param url
   *   url
   * @return
   */
  @throws[IOException]
  def restRequest(url: String, timeout: Int = 5000): String = {
    if (url == null) return null
    url match {
      case u if u.matches("^http(|s)://.*") =>
        Try(request(url, timeout)) match {
          case Success(v) => v
          case Failure(e) =>
            if (hasYarnHttpKerberosAuth) {
              throw new ConnectException(s"yarnUtils authRestRequest error, url: $u, detail: $e")
            } else {
              throw new ConnectException(s"yarnUtils restRequest error, url: $u, detail: $e")
            }
        }
      case _ =>
        Try(request(s"${getRMWebAppURL()}/$url", timeout)) match {
          case Success(v) => v
          case Failure(_) =>
            Utils.retry[String](5)(request(s"${getRMWebAppURL(true)}/$url", timeout)) match {
              case Success(v) => v
              case Failure(e) =>
                throw new ConnectException(
                  s"yarnUtils restRequest retry 5 times all failed. detail: $e")
            }
        }
    }
  }

  private[this] def request(reqUrl: String, timeout: Int): String = {
    val config = RequestConfig.custom.setConnectTimeout(timeout).build
    if (hasYarnHttpKerberosAuth) {
      HadoopUtils
        .getUgi()
        .doAs(new PrivilegedExceptionAction[String] {
          override def run(): String = {
            HttpClientUtils.httpAuthGetRequest(reqUrl, config)
          }
        })
    } else {
      val url =
        if (!hasYarnHttpSimpleAuth) reqUrl
        else s"$reqUrl?user.name=${HadoopConfigUtils.hadoopUserName}"
      HttpClientUtils.httpGetRequest(url, config)
    }
  }

}
