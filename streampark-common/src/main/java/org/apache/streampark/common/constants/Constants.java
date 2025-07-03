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

package org.apache.streampark.common.constants;

import java.time.Duration;

/**
 * A constant class to hold the constants variables.
 */
public final class Constants {

    private Constants() {
    }

    public static final String DEFAULT = "default";

    public static final String STREAM_PARK = "streampark";

    public static final String HTTP_SCHEMA = "http://";

    public static final String HTTPS_SCHEMA = "https://";

    public static final String JAR_SUFFIX = ".jar";

    public static final String ZIP_SUFFIX = ".zip";

    public static final String EMPTY_STRING = "";

    public static final String PYTHON_SUFFIX = ".py";

    public static final String SEMICOLON = ";";

    public static final String DEFAULT_DATAMASK_STRING = "********";

    public static final String PYTHON_FLINK_DRIVER_CLASS_NAME = "org.apache.flink.client.python.PythonDriver";

    public static final String STREAMPARK_FLINKSQL_CLIENT_CLASS = "org.apache.streampark.flink.cli.SqlClient";

    public static final String STREAMPARK_SPARKSQL_CLIENT_CLASS = "org.apache.streampark.spark.cli.SqlClient";

    public static final String PYTHON_EXECUTABLE = "venv.zip/venv/bin/python3";

    public static final String SINGLE_SLASH = "/";

    public static final String LOCAL_HOST = "localhost";

    /**
     * sleep 1000ms
     */
    public static final long SLEEP_TIME_MILLIS = 1_000L;

    public static final Duration SERVER_CLOSE_WAIT_TIME = Duration.ofSeconds(3);
}
