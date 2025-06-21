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

import org.junit.jupiter.api.{Assertions, Test}

class PropertiesUtilsTestCase {

  @Test def testExtractProgramArgs(): Unit = {
    val args =
      "mysql-sync-database " +
        "--database employees " +
        "--mysql-conf hostname=127.0.0.1 " +
        "--mysql-conf port=3306 " +
        "--mysql-conf username=root " +
        "--mysql-conf password=123456 " +
        "--mysql-conf database-name=employees " +
        "--including-tables 'test|test.*' " +
        "--excluding-tables \"emp_*\" " +
        "--query 'select * from employees where age > 20' " +
        "--sink-conf fenodes=127.0.0.1:8030 " +
        "--sink-conf username=root " +
        "--sink-conf password= " +
        "--sink-conf jdbc-url=jdbc:mysql://127.0.0.1:9030 " +
        "--sink-conf sink.label-prefix=label" +
        "--table-conf replication_num=1"
    val programArgs = FlinkConfigurationUtils.extractArguments(args)
    Assertions.assertTrue(programArgs.contains("username=root"))
  }

  @Test def testDynamicProperties(): Unit = {
    val dynamicProperties =
      """
        |-Denv.java.opts1="-Dfile.encoding=UTF-8"
        |-Denv.java.opts2 = "-Dfile.enc\"oding=UTF-8"
        |-Denv.java.opts3 = " -Dfile.encoding=UTF-8"
        |-Dyarn.application.id=123
        |-Dyarn.application.name="streampark job"
        |-Dyarn.application.queue=flink
        |-Ddiy.param.name=apache streampark
        |
        |""".stripMargin

    val map = FlinkConfigurationUtils.extractDynamicProperties(dynamicProperties)
    Assertions.assertEquals(map("env.java.opts1"), "-Dfile.encoding=UTF-8")
    Assertions.assertEquals(map("env.java.opts2"), "-Dfile.enc\\\"oding=UTF-8")
    Assertions.assertEquals(map("env.java.opts3"), " -Dfile.encoding=UTF-8")
    Assertions.assertEquals(map("yarn.application.id"), "123")
    Assertions.assertEquals(map("yarn.application.name"), "streampark job")
    Assertions.assertEquals(map("yarn.application.queue"), "flink")
    Assertions.assertEquals(map("diy.param.name"), "apache streampark")
  }

}
