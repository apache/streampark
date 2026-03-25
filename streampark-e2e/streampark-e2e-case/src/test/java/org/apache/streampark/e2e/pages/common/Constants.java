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

package org.apache.streampark.e2e.pages.common;

import lombok.experimental.UtilityClass;

import java.time.Duration;

@UtilityClass
public class Constants {

    public static final Integer DEFAULT_SLEEP_MILLISECONDS = 2000;

    public static final Duration DEFAULT_WEBDRIVER_WAIT_DURATION = Duration.ofSeconds(10);

    public static final String LINE_SEPARATOR = "\n";

    /** datagen flink sql for test */
    public static final String TEST_FLINK_SQL = "CREATE TABLE datagen (\n"
        + "f_sequence INT,\n"
        + "f_random INT,\n"
        + "f_random_str STRING,\n"
        + "ts AS localtimestamp,\n"
        + "WATERMARK FOR ts AS ts\n"
        + ") WITH (\n"
        + "'connector' = 'datagen',\n"
        + "'rows-per-second'='5',\n"
        + "'fields.f_sequence.kind'='sequence',\n"
        + "'fields.f_sequence.start'='1',\n"
        + "'fields.f_sequence.end'='100',\n"
        + "'fields.f_random.min'='1',\n"
        + "'fields.f_random.max'='100',\n"
        + "'fields.f_random_str.length'='10'\n"
        + ");\n"
        + "\n"
        + "CREATE TABLE print_table (\n"
        + "f_sequence INT,\n"
        + "f_random INT,\n"
        + "f_random_str STRING\n"
        + ") WITH (\n"
        + "'connector' = 'print'\n"
        + ");\n"
        + "\n"
        + "INSERT INTO print_table select f_sequence,f_random,f_random_str from datagen;";
}
