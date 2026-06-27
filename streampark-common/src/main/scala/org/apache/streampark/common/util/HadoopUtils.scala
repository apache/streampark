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
import org.apache.streampark.common.conf.ConfigKeys._
import org.apache.streampark.common.util.Implicits._

import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.service.Service.STATE
import org.apache.hadoop.yarn.api.records.{ApplicationId, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import javax.security.auth.kerberos.KerberosTicket

import java.io.{File, IOException}
import java.security.PrivilegedAction
import java.util
import java.util.{Timer, TimerTask}
import java.util.concurrent._

import scala.util.{Failure, Success, Try}

object HadoopUtils extends Logger {

  private[this] lazy val HADOOP_HOME: String = "HADOOP_HOME"
  private[this] lazy val HADOOP_CONF_DIR: String = "HADOOP_CONF_DIR"
  private[this] lazy val CONF_SUFFIX: String = "/etc/hadoop"

  private[this] var reusableYarnClient: YarnClient = _

  private[this] var ugi: UserGroupInformation = _

  private[this] var reusableConf: Configuration = _

  private[this] var reusableHdfs: FileSystem = _

  private[this] var tgt: KerberosTicket = _

  private[this] val hadoopLock = new Object()

  private[this] lazy val configurationCache: util.Map[String, Configuration] =
    new ConcurrentHashMap[String, Configuration]()

  def getUgi(): UserGroupInformation = hadoopLock.synchronized {
    if (ugi == null) {
      ugi = if (HadoopConfigUtils.kerberosEnable) {
        getKerberosUGI()
      } else {
        UserGroupInformation.createRemoteUser(HadoopConfigUtils.hadoopUserName)
      }
    }
    ugi
  }

  private[this] lazy val hadoopConfDir: String = Try(
    FileUtils.getPathFromEnv(HADOOP_CONF_DIR)) match {
    case Failure(_) => FileUtils.resolvePath(FileUtils.getPathFromEnv(HADOOP_HOME), CONF_SUFFIX)
    case Success(value) => value
  }

  private[this] lazy val tgtRefreshTime: Long = {
    val user = UserGroupInformation.getLoginUser
    val method = classOf[UserGroupInformation].getDeclaredMethod("getTGT")
    method.setAccessible(true)
    tgt = method.invoke(user).asInstanceOf[KerberosTicket]
    Option(tgt) match {
      case Some(value) =>
        val start = value.getStartTime.getTime
        val end = value.getEndTime.getTime
        ((end - start) * 0.90f).toLong
      case _ =>
        logWarn("get kerberos tgtRefreshTime failed, try get kerberos.ttl. ")
        val timeUnit = DateUtils.getTimeUnit(InternalConfigHolder.get(CommonConfig.KERBEROS_TTL))
        timeUnit._2 match {
          case TimeUnit.SECONDS => timeUnit._1 * 1000
          case TimeUnit.MINUTES => timeUnit._1 * 60 * 1000
          case TimeUnit.HOURS => timeUnit._1 * 60 * 60 * 1000
          case TimeUnit.DAYS => timeUnit._1 * 60 * 60 * 24 * 1000
          case _ =>
            throw new IllegalArgumentException(
              s"[StreamPark] parameter:${CommonConfig.KERBEROS_TTL.key} invalided, unit options are [s|m|h|d]")
        }
    }
  }

  def getConfigurationFromHadoopConfDir(confDir: String = hadoopConfDir): Configuration = {
    if (!configurationCache.containsKey(confDir)) {
      FileUtils.exists(confDir)
      val hadoopConfDir = new File(confDir)
      val confName = List("hdfs-default.xml", "core-site.xml", "hdfs-site.xml", "yarn-site.xml")
      val files =
        hadoopConfDir
          .listFiles()
          .filter(x => x.isFile && confName.contains(x.getName))
          .toList
      val conf = new HadoopConfiguration()
      if (CollectionUtils.isNotEmpty(files)) {
        files.foreach(x => conf.addResource(new Path(x.getAbsolutePath)))
      }
      configurationCache.put(confDir, conf)
    }
    configurationCache(confDir)
  }

  /**
   * <pre> Note: There are two ways to load a hadoop configuration file:<br> 1) Copy
   * core-site.xml,hdfs-site.xml,yarn-site.xml of hadoop conf to the resource dir.<br> 2)
   * Automatically goes to $HADOOP_HOME/etc/hadoop to load the configuration.<br> We recommend the
   * second method, without copying the configuration files.<br> </pre>
   */
  def hadoopConf: Configuration = hadoopLock.synchronized {
    Option(reusableConf).getOrElse {
      reusableConf = getConfigurationFromHadoopConfDir(hadoopConfDir)
      // add hadoopConfDir to classpath...you know why???
      ClassLoaderUtils.loadResource(hadoopConfDir)

      if (StringUtils.isBlank(reusableConf.get("hadoop.tmp.dir"))) {
        reusableConf.set("hadoop.tmp.dir", "/tmp")
      }
      if (StringUtils.isBlank(reusableConf.get("hbase.fs.tmp.dir"))) {
        reusableConf.set("hbase.fs.tmp.dir", "/tmp")
      }
      // disable timeline service as we only query yarn app here.
      // Otherwise we may hit this kind of ERROR:
      // java.lang.ClassNotFoundException: com.sun.jersey.api.client.config.ClientConfig
      reusableConf.set("yarn.timeline-service.enabled", "false")
      reusableConf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      reusableConf.set("fs.file.impl", classOf[LocalFileSystem].getName)
      reusableConf.set("fs.hdfs.impl.disable.cache", "true")
      reusableConf
    }
  }

  private[this] def closeHadoop(): Unit = {
    if (reusableHdfs != null) {
      reusableHdfs.close()
      reusableHdfs = null
    }
    if (reusableYarnClient != null) {
      reusableYarnClient.close()
      reusableYarnClient = null
    }
    if (tgt != null && !tgt.isDestroyed) {
      tgt.destroy()
      tgt = null
    }
    reusableConf = null
    ugi = null
  }

  private[this] def getKerberosUGI(): UserGroupInformation = {
    logInfo("kerberos login starting....")

    require(
      HadoopConfigUtils.kerberosPrincipal.nonEmpty && HadoopConfigUtils.kerberosKeytab.nonEmpty,
      s"$KEY_SECURITY_KERBEROS_PRINCIPAL and $KEY_SECURITY_KERBEROS_KEYTAB must not be empty")

    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")

    if (HadoopConfigUtils.kerberosKrb5.nonEmpty) {
      System.setProperty("java.security.krb5.conf", HadoopConfigUtils.kerberosKrb5)
      System.setProperty("java.security.krb5.conf.path", HadoopConfigUtils.kerberosKrb5)
    }

    System.setProperty("sun.security.spnego.debug", HadoopConfigUtils.kerberosDebug)
    System.setProperty("sun.security.krb5.debug", HadoopConfigUtils.kerberosDebug)
    hadoopConf.set(KEY_HADOOP_SECURITY_AUTHENTICATION, KEY_KERBEROS)

    Try {
      UserGroupInformation.setConfiguration(hadoopConf)
      val ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          HadoopConfigUtils.kerberosPrincipal,
          HadoopConfigUtils.kerberosKeytab)
      UserGroupInformation.setLoginUser(ugi)
      logInfo("kerberos authentication successful")
      ugi
    } match {
      case Success(ugi) => ugi
      case Failure(e) => throw e
    }
  }

  def hdfs: FileSystem = hadoopLock.synchronized {
    Option(reusableHdfs).getOrElse {
      reusableHdfs = Try {
        getUgi().doAs[FileSystem](new PrivilegedAction[FileSystem]() {
          // scalastyle:off FileSystemGet
          override def run(): FileSystem = FileSystem.get(hadoopConf)
          // scalastyle:on FileSystemGet
        })
      } match {
        case Success(fs) =>
          if (HadoopConfigUtils.kerberosEnable) {
            // reLogin...
            val timer = new Timer()
            timer.schedule(
              new TimerTask {
                override def run(): Unit = hadoopLock.synchronized {
                  closeHadoop()
                  logInfo(
                    s"Check Kerberos Tgt And reLogin From Keytab Finish:refresh time: ${DateUtils.format()}")
                }
              },
              tgtRefreshTime,
              tgtRefreshTime)
          }
          fs
        case Failure(e) =>
          throw new IllegalArgumentException(s"[StreamPark] access hdfs error: $e")
      }
      reusableHdfs
    }
  }

  def yarnClient: YarnClient = hadoopLock.synchronized {
    if (reusableYarnClient == null || !reusableYarnClient.isInState(STATE.STARTED)) {
      reusableYarnClient = Try {
        getUgi().doAs(new PrivilegedAction[YarnClient]() {
          override def run(): YarnClient = {
            val yarnConf = new YarnConfiguration(hadoopConf);
            val client = YarnClient.createYarnClient;
            client.init(yarnConf);
            client.start();
            client
          }
        })
      } match {
        case Success(client) => client
        case Failure(e) =>
          throw new IllegalArgumentException(s"[StreamPark] access yarnClient error: $e")
      }
    }
    reusableYarnClient
  }

  def toApplicationId(appId: String): ApplicationId = {
    require(
      appId != null,
      "[StreamPark] HadoopUtils.toApplicationId: applicationId muse not be null")
    val timestampAndId = appId.split("_")
    ApplicationId.newInstance(timestampAndId(1).toLong, timestampAndId.last.toInt)
  }

  @throws[IOException]
  def downloadJar(jarOnHdfs: String): String = {
    val tmpDir = FileUtils.createTempDir()
    val fs = FileSystem.get(new Configuration)
    val sourcePath = fs.makeQualified(new Path(jarOnHdfs))
    if (!fs.exists(sourcePath)) {
      throw new IOException(s"jar file: $jarOnHdfs doesn't exist.")
    }
    val destPath = new Path(tmpDir.getAbsolutePath + "/" + sourcePath.getName)
    fs.copyToLocalFile(sourcePath, destPath)
    new File(destPath.toString).getAbsolutePath
  }

  def toYarnState(state: String): YarnApplicationState = {
    YarnApplicationState.values.find(_.name() == state).orNull
  }

  def toYarnFinalStatus(state: String): FinalApplicationStatus = {
    FinalApplicationStatus.values.find(_.name() == state).orNull
  }

  private class HadoopConfiguration extends Configuration {

    private lazy val rewriteNames = List(
      "dfs.blockreport.initialDelay",
      "dfs.datanode.directoryscan.interval",
      "dfs.heartbeat.interval",
      "dfs.namenode.decommission.interval",
      "dfs.namenode.replication.interval",
      "dfs.namenode.checkpoint.period",
      "dfs.namenode.checkpoint.check.period",
      "dfs.client.datanode-restart.timeout",
      "dfs.ha.log-roll.period",
      "dfs.ha.tail-edits.period",
      "dfs.datanode.bp-ready.timeout")

    private def getHexDigits(value: String): String = {
      var negative = false
      var str = value
      var hexString: String = null
      if (value.startsWith("-")) {
        negative = true
        str = value.substring(1)
      }
      if (str.startsWith("0x") || str.startsWith("0X")) {
        hexString = str.substring(2)
        if (negative) hexString = "-" + hexString
        return hexString
      }
      null
    }

    // HDFS default value change (with adding time unit) breaks old version MR tarball work with Hadoop 3.x
    // detail: https://issues.apache.org/jira/browse/HDFS-12920
    private def getSafeValue(name: String): String = {
      val value = getTrimmed(name)
      if (rewriteNames.contains(name)) {
        return value.replaceFirst("s$", "")
      }
      value
    }

    override def getLong(name: String, defaultValue: Long): Long = {
      val valueString = getSafeValue(name)
      valueString match {
        case null => defaultValue
        case v =>
          val hexString = getHexDigits(v)
          if (hexString != null) {
            return java.lang.Long.parseLong(hexString, 16)
          }
          java.lang.Long.parseLong(valueString)
      }
    }
  }
}
