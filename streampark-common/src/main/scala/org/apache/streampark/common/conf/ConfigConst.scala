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
package org.apache.streampark.common.conf

import java.time.LocalDateTime

object ConfigConst {

  /** common const */

  val DEFAULT_DATAMASK_STRING = "********"

  /** about parameter... */

  val KEY_APP_HOME = "app.home"

  val KEY_HOST = "host"

  val KEY_PORT = "port"

  val KEY_DB = "db"

  val KEY_USER = "user"

  val KEY_PASSWORD = "password"

  val KEY_TIMEOUT = "timeout"

  val KEY_SEMANTIC = "semantic"

  /** sign.... */
  val SIGN_COLON = ":"

  val SIGN_SEMICOLON = ";"

  val SIGN_COMMA = ","

  val SIGN_EMPTY = ""

  /** kerberos */
  val KEY_KERBEROS = "kerberos"

  val KEY_KERBEROS_SERVICE_ACCOUNT = "kubernetes.service-account"

  val KEY_HADOOP_USER_NAME = "HADOOP_USER_NAME"

  /** hadoop.security.authentication */
  val KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication"

  val KEY_SECURITY_KERBEROS_ENABLE = "security.kerberos.login.enable"

  val KEY_SECURITY_KERBEROS_DEBUG = "security.kerberos.login.debug"

  val KEY_SECURITY_KERBEROS_KEYTAB = "security.kerberos.login.keytab"

  val KEY_SECURITY_KERBEROS_PRINCIPAL = "security.kerberos.login.principal"

  val KEY_SECURITY_KERBEROS_KRB5_CONF = "security.kerberos.login.krb5"

  val KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf"

  // spark

  val KEY_SPARK_MAIN_CLASS = "spark.main.class"

  val KEY_SPARK_APP_NAME = "spark.app.name"

  val KEY_SPARK_BATCH_DURATION = "spark.batch.duration"

  // flink
  def KEY_APP_CONF(prefix: String = null): String = if (prefix == null) "conf" else s"${prefix}conf"

  def KEY_FLINK_CONF(prefix: String = null): String =
    if (prefix == null) "flink.conf" else s"${prefix}flink.conf"

  def KEY_APP_NAME(prefix: String = null): String =
    if (prefix == null) "app.name" else s"${prefix}app.name"

  def KEY_FLINK_SQL(prefix: String = null): String = if (prefix == null) "sql" else s"${prefix}sql"

  def KEY_FLINK_PARALLELISM(prefix: String = null): String =
    if (prefix == null) "parallelism.default" else s"${prefix}parallelism.default"

  val KEY_FLINK_OPTION_PREFIX = "flink.option."

  val KEY_FLINK_PROPERTY_PREFIX = "flink.property."

  val KEY_FLINK_TABLE_PREFIX = "flink.table."

  val KEY_APP_PREFIX = "app."

  val KEY_SQL_PREFIX = "sql."

  val KEY_FLINK_APP_NAME = "pipeline.name"

  val KEY_YARN_APP_ID = "yarn.application.id"

  val KEY_YARN_APP_NAME = "yarn.application.name"

  val KEY_YARN_APP_QUEUE = "yarn.application.queue"

  val KEY_YARN_APP_NODE_LABEL = "yarn.application.node-label"

  val KEY_K8S_IMAGE_PULL_POLICY = "kubernetes.container.image.pull-policy"

  // ---table---
  val KEY_FLINK_TABLE_PLANNER = "flink.table.planner"

  val KEY_FLINK_TABLE_MODE = "flink.table.mode"

  val KEY_FLINK_TABLE_CATALOG = "flink.table.catalog"

  val KEY_FLINK_TABLE_DATABASE = "flink.table.database"

  /** about config Kafka */

  val KAFKA_SINK_PREFIX = "kafka.sink."

  val KAFKA_SOURCE_PREFIX = "kafka.source."

  val KEY_KAFKA_TOPIC = "topic"

  val KEY_KAFKA_SEMANTIC = "semantic"

  val KEY_KAFKA_PATTERN = "pattern"

  val KEY_KAFKA_START_FROM = "start.from"

  val KEY_KAFKA_START_FROM_OFFSET = "offset"

  val KEY_KAFKA_START_FROM_TIMESTAMP = "timestamp"

  val KEY_ALIAS = "alias"

  /** about config jdbc... */
  val KEY_JDBC_PREFIX = "jdbc."

  val KEY_JDBC_DRIVER = "driverClassName"

  val KEY_JDBC_URL = "jdbcUrl"

  val KEY_JDBC_USER = "username"

  val KEY_JDBC_PASSWORD = "password"

  val KEY_JDBC_INSERT_BATCH = "batch.size"

  val DEFAULT_JDBC_INSERT_BATCH = 1

  val MONGO_PREFIX = "mongodb."

  /** about config HBase */
  val HBASE_PREFIX = "hbase."

  val KEY_HBASE_COMMIT_BATCH = "hbase.commit.batch"

  val KEY_HBASE_WRITE_SIZE = "hbase.client.write.size"

  val DEFAULT_HBASE_COMMIT_BATCH = 1000

  val KEY_HBASE_AUTH_USER = "hbase.auth.user"

  val DEFAULT_HBASE_WRITE_SIZE = 1024 * 1024 * 10

  /** about influx */
  val INFLUX_PREFIX = "influx."

  val KEY_INFLUX_ACTIONS = "actions"

  val KEY_INFLUX_FLUSH_DURATION = "flush.duration"

  /** flink config key */
  val KEY_FLINK_APPLICATION_ARGS = "$internal.application.program-args"

  val KEY_FLINK_APPLICATION_MAIN_CLASS = "$internal.application.main"

  val KEY_FLINK_JM_PROCESS_MEMORY = "jobmanager.memory.process.size"

  val KEY_FLINK_TM_PROCESS_MEMORY = "taskmanager.memory.process.size"

  val STREAMPARK_FLINKSQL_CLIENT_CLASS = "org.apache.streampark.flink.cli.SqlClient"

  def printLogo(info: String): Unit = {
    // scalastyle:off println
    println("\n")
    println("        _____ __                                             __       ")
    println("       / ___// /_________  ____ _____ ___  ____  ____ ______/ /__     ")
    println("       \\__ \\/ __/ ___/ _ \\/ __ `/ __ `__ \\/ __ \\  __ `/ ___/ //_/")
    println("      ___/ / /_/ /  /  __/ /_/ / / / / / / /_/ / /_/ / /  / ,<        ")
    println("     /____/\\__/_/   \\___/\\__,_/_/ /_/ /_/ ____/\\__,_/_/  /_/|_|   ")
    println("                                       /_/                        \n\n")
    println("    Version:  2.1.8                                          ")
    println("    WebSite:  https://streampark.apache.org                           ")
    println("    GitHub :  https://github.com/apache/streampark                    ")
    println(s"    Info   :  $info                                 ")
    println(s"    Time   :  ${LocalDateTime.now}              \n\n")
    // scalastyle:on println
  }

}
