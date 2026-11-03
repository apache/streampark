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

package org.apache.streampark.common.util;

import org.apache.streampark.shaded.ch.qos.logback.classic.LoggerContext;
import org.apache.streampark.shaded.ch.qos.logback.core.helpers.NOPAppender;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class StreamParkLoggerFactoryTest {

    @TempDir
    Path workspace;

    @Test
    void loadConfigurationFromJarUrl() throws Exception {
        Path jar = workspace.resolve("sql client.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry("logback.xml"));
            output.write(configuration("ch.qos.logback.core.helpers.NOPAppender")
                .getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        verifyConfiguration(new URL("jar:" + jar.toUri().toURL() + "!/logback.xml"));
    }

    @Test
    void loadConfigurationFromEncodedUrl() throws Exception {
        Path file = workspace.resolve("logback config.xml");
        Files.writeString(file, configuration("ch.qos.logback.core.helpers.NOPAppender"));
        verifyConfiguration(file.toUri().toURL());
    }

    @Test
    void preserveAlreadyShadedClassNames() throws Exception {
        Path file = workspace.resolve("logback.xml");
        Files.writeString(file, configuration(
            "org.apache.streampark.shaded.ch.qos.logback.core.helpers.NOPAppender"));
        verifyConfiguration(file.toUri().toURL());
    }

    private void verifyConfiguration(URL url) {
        LoggerContext context = new LoggerContext();
        try {
            new StreamParkLoggerFactory.ShadedContextInitializer(context).configureByResource(url);
            assertThat(context.getLogger("ROOT").getAppender("NOP"))
                .isInstanceOf(NOPAppender.class);
        } finally {
            context.stop();
        }
    }

    private String configuration(String appenderClass) {
        return "<configuration><appender name=\"NOP\" class=\"" + appenderClass
            + "\"/><root level=\"INFO\"><appender-ref ref=\"NOP\"/></root></configuration>";
    }
}
