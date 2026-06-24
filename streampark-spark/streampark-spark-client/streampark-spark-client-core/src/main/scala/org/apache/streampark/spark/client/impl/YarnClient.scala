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

package org.apache.streampark.spark.client.impl

import org.apache.streampark.common.conf.ConfigKeys._
import org.apache.streampark.common.enums.SparkDeployMode
import org.apache.streampark.common.util.{HadoopUtils, SparkEnvUtils, YarnUtils}
import org.apache.streampark.common.util.Implicits._
import org.apache.streampark.spark.client.`trait`.SparkClientTrait
import org.apache.streampark.spark.client.bean._

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import scala.util.{Failure, Success, Try}

/** yarn application mode submit */
object YarnClient extends SparkClientTrait {

  private lazy val sparkHandles = new ConcurrentHashMap[String, SparkAppHandle]()

  override def doCancel(cancelRequest: CancelRequest): CancelResponse = {
    val sparkAppHandle = sparkHandles.remove(cancelRequest.appId)
    if (sparkAppHandle != null) {
      Try(sparkAppHandle.stop()) match {
        case Success(_) =>
          logger.info(s"[StreamPark][Spark][YarnClient] spark job: ${cancelRequest.appId} is stopped successfully.")
          CancelResponse(null)
        case Failure(e) =>
          logger.error("[StreamPark][Spark][YarnClient] sparkAppHandle kill failed. Try kill by yarn", e)
          yarnKill(cancelRequest.appId)
          CancelResponse(null)
      }
    } else {
      logger.warn(s"[StreamPark][Spark][YarnClient] spark job: ${cancelRequest.appId} is not existed. Try kill by yarn")
      yarnKill(cancelRequest.appId)
      CancelResponse(null)
    }
  }

  private def yarnKill(appId: String): Unit = {
    Try(HadoopUtils.yarnClient.killApplication(ApplicationId.fromString(appId))) match {
      case Success(_) => logger.info(s"[StreamPark][Spark][YarnClient] spark job: $appId is killed by yarn successfully.")
      case Failure(e) => throw e
    }
  }

  override def setConfig(submitRequest: SubmitRequest): Unit = {}

  override def doSubmit(submitRequest: SubmitRequest): SubmitResponse = {
    // 1) prepare sparkLauncher
    val launcher: SparkLauncher = prepareSparkLauncher(submitRequest)

    // 2) set spark config
    setSparkConfig(submitRequest, launcher)

    // 3) launch
    Try(launch(launcher)) match {
      case Success(handle: SparkAppHandle) =>
        if (handle.getError.isPresent) {
          logger.info(s"[StreamPark][Spark][YarnClient] spark job: ${submitRequest.appName} submit failed.")
          throw handle.getError.get()
        } else {
          logger.info(s"[StreamPark][Spark][YarnClient] spark job: ${submitRequest.appName} submit successfully, " +
            s"appid: ${handle.getAppId}, " +
            s"state: ${handle.getState}")
          sparkHandles += handle.getAppId -> handle
          val trackingUrl = YarnUtils.getYarnAppTrackingUrl(HadoopUtils.toApplicationId(handle.getAppId))
          SubmitResponse(handle.getAppId, trackingUrl, submitRequest.appProperties)
        }
      case Failure(e) => throw e
    }
  }

  private def launch(sparkLauncher: SparkLauncher): SparkAppHandle = {
    logger.info("[StreamPark][Spark][YarnClient] The spark job start submitting")
    val submitFinished: CountDownLatch = new CountDownLatch(1)
    val sparkAppHandle = sparkLauncher.startApplication(new SparkAppHandle.Listener() {
      override def infoChanged(sparkAppHandle: SparkAppHandle): Unit = {}
      override def stateChanged(handle: SparkAppHandle): Unit = {
        if (handle.getAppId != null) {
          logger.info(s"${handle.getAppId} stateChanged : ${handle.getState.toString}")
        } else {
          logger.info("stateChanged : {}", handle.getState.toString)
        }
        if (handle.getAppId != null && submitFinished.getCount != 0) {
          // Task submission succeeded
          submitFinished.countDown()
        }
        if (handle.getState.isFinal) {
          if (StringUtils.isNotBlank(handle.getAppId) && sparkHandles.containsKey(handle.getAppId)) {
            sparkHandles.remove(handle.getAppId)
          }
          if (submitFinished.getCount != 0) {
            // Task submission failed
            submitFinished.countDown()
          }
          logger.info("Task is end, final state : {}", handle.getState.toString)
        }
      }
    })
    submitFinished.await()
    sparkAppHandle
  }

  private def prepareSparkLauncher(submitRequest: SubmitRequest) = {
    val env = new JavaHashMap[String, String]()
    if (StringUtils.isNotBlank(submitRequest.hadoopUser)) {
      env.put("HADOOP_USER_NAME", submitRequest.hadoopUser)
    }
    val sparkLauncher = new SparkLauncher(env)
      .setSparkHome(submitRequest.sparkVersion.sparkHome)
      .setAppResource(submitRequest.userJarPath)
      .setMainClass(submitRequest.appMain)
      .setAppName(submitRequest.appName)
      .setConf("spark.yarn.dist.jars", submitRequest.hdfsWorkspace.sparkLib)
      .setConf("spark.yarn.applicationType", "StreamPark Spark")
      .setVerbose(true)
      .setMaster("yarn")
      .setDeployMode(submitRequest.deployMode match {
        case SparkDeployMode.YARN_CLIENT => "client"
        case SparkDeployMode.YARN_CLUSTER => "cluster"
        case _ =>
          throw new IllegalArgumentException("[StreamPark][Spark][YarnClient] Invalid spark on yarn deployMode, only support \"client\" and \"cluster\".")
      })
    SparkEnvUtils
      .resolveJavaHome(submitRequest.sparkVersion.sparkHome, submitRequest.sparkVersion.version)
      .foreach { javaHome =>
        env.put("JAVA_HOME", javaHome)
        sparkLauncher.setJavaHome(javaHome)
        logger.info(s"[StreamPark][Spark][YarnClient] Using JAVA_HOME: $javaHome")
      }
    sparkLauncher
  }

  private def setSparkConfig(submitRequest: SubmitRequest, sparkLauncher: SparkLauncher): Unit = {
    logger.info("[StreamPark][Spark][YarnClient] set spark configuration.")
    // 1) put yarn queue
    if (SparkDeployMode.isYarnMode(submitRequest.deployMode)) {
      setYarnQueue(submitRequest)
    }

    // 2) set spark conf
    submitRequest.appProperties.foreach(prop => {
      val k = prop._1
      val v = prop._2
      logInfo(s"| $k  : $v")
      sparkLauncher.setConf(k, v)
    })

    // 3) set spark args
    submitRequest.appArgs.foreach(sparkLauncher.addAppArgs(_))
    if (submitRequest.hasExtra("sql")) {
      sparkLauncher.addAppArgs("--sql", submitRequest.getExtra("sql").toString)
    }
  }

  private def setYarnQueue(submitRequest: SubmitRequest): Unit = {
    if (submitRequest.hasExtra(KEY_SPARK_YARN_QUEUE_NAME)) {
      submitRequest.appProperties.put(KEY_SPARK_YARN_QUEUE, submitRequest.getExtra(KEY_SPARK_YARN_QUEUE_NAME).asInstanceOf[String])
    }
    if (submitRequest.hasExtra(KEY_SPARK_YARN_QUEUE_LABEL)) {
      submitRequest.appProperties.put(KEY_SPARK_YARN_AM_NODE_LABEL, submitRequest.getExtra(KEY_SPARK_YARN_QUEUE_LABEL).asInstanceOf[String])
      submitRequest.appProperties.put(KEY_SPARK_YARN_EXECUTOR_NODE_LABEL, submitRequest.getExtra(KEY_SPARK_YARN_QUEUE_LABEL).asInstanceOf[String])
    }
  }
}
