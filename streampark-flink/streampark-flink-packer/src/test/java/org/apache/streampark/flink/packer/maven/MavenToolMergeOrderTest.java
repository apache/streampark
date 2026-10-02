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

package org.apache.streampark.flink.packer.maven;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;

class MavenToolMergeOrderTest {

    @TempDir
    Path workspace;

    @Test
    void preferTargetShimOverEmbeddedClasses() throws Exception {
        for (int index = 0; index < 10; index++) {
            Path directory = Files.createDirectories(workspace.resolve("app-" + index));
            Path client = createJar(directory, "streampark-flink-sqlclient-3.0.0.jar", (byte) 0);
            Path base = createJar(directory, "streampark-flink-shims-base-3.0.0.jar", (byte) 1);
            Path shim = createJar(directory, "streampark-flink-shims_flink-2.2-3.0.0.jar", (byte) 2);
            Path output = directory.resolve("job.jar");
            MavenTool.buildFatJar("org.apache.streampark.flink.cli.SqlClient",
                new LinkedHashSet<>(List.of(client.toString(), base.toString(), shim.toString())),
                output.toString());
            try (JarFile jar = new JarFile(output.toFile())) {
                assertThat(jar.getInputStream(jar.getJarEntry("target-shim.class")).readAllBytes())
                    .containsExactly((byte) 2);
                assertThat(jar.getManifest().getMainAttributes().getValue("Main-Class"))
                    .isEqualTo("org.apache.streampark.flink.cli.SqlClient");
            }
        }
    }

    private Path createJar(Path directory, String name, byte content) throws Exception {
        Path jar = directory.resolve(name);
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
            output.putNextEntry(new JarEntry("target-shim.class"));
            output.write(new byte[]{content});
            output.closeEntry();
        }
        return jar;
    }
}
