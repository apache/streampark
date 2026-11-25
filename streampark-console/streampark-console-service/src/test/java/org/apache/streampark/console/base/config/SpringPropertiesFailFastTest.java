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

package org.apache.streampark.console.base.config;

import org.apache.streampark.common.configuration.option.CoreOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that fatal {@link SpringProperties} configuration errors print a framed error block
 * to stderr before failing fast, while the original exception types and messages are kept.
 */
class SpringPropertiesFailFastTest {

    private static final String APP_HOME_KEY = CoreOptions.APP_HOME.key();

    @TempDir
    Path appHome;

    private String originalAppHome;

    private PrintStream originalErr;

    private ByteArrayOutputStream capturedErr;

    @BeforeEach
    void setUp() {
        originalAppHome = System.setProperty(APP_HOME_KEY, appHome.toAbsolutePath().toString());
        originalErr = System.err;
        capturedErr = new ByteArrayOutputStream();
        System.setErr(new PrintStream(capturedErr, true));
    }

    @AfterEach
    void tearDown() {
        System.setErr(originalErr);
        if (originalAppHome == null) {
            System.clearProperty(APP_HOME_KEY);
        } else {
            System.setProperty(APP_HOME_KEY, originalAppHome);
        }
    }

    private String capturedErrorOutput() {
        return new String(capturedErr.toByteArray(), StandardCharsets.UTF_8);
    }

    private void writeConfig(String... datasourceEntries) throws IOException {
        Path confDir = Files.createDirectories(appHome.resolve("conf"));
        StringBuilder yaml = new StringBuilder("datasource:\n");
        for (String entry : datasourceEntries) {
            yaml.append("  ").append(entry).append('\n');
        }
        Files.write(confDir.resolve("config.yaml"), yaml.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void missingDialectPrintsFramedErrorBlockToStderr() throws IOException {
        writeConfig("h2-data-dir: /tmp/streampark-h2");

        assertThrows(ExceptionInInitializerError.class, SpringProperties::get);

        String stderr = capturedErrorOutput();
        assertTrue(stderr.contains("[StreamPark startup failure]"));
        assertTrue(stderr.contains("datasource.dialect is required, please check config.yaml"));
        assertTrue(stderr.contains("The console cannot start until this problem is fixed."));
    }

    @Test
    void unsupportedDialectPrintsFramedErrorBlockToStderr() throws IOException {
        writeConfig("dialect: postgre");

        assertThrows(UnsupportedOperationException.class, SpringProperties::get);

        String stderr = capturedErrorOutput();
        assertTrue(stderr.contains("[StreamPark startup failure]"));
        assertTrue(stderr.contains("Unsupported datasource dialect: postgre"));
        assertTrue(stderr.contains("The console cannot start until this problem is fixed."));
    }

    @Test
    void missingConfigFilePrintsFramedErrorBlockToStderr() {
        // the temp app home has no conf directory at all
        assertThrows(ExceptionInInitializerError.class, SpringProperties::get);

        String stderr = capturedErrorOutput();
        assertTrue(stderr.contains("[StreamPark startup failure]"));
        assertTrue(stderr.contains("not found, please check."));
        assertTrue(stderr.contains("The console cannot start until this problem is fixed."));
    }
}
