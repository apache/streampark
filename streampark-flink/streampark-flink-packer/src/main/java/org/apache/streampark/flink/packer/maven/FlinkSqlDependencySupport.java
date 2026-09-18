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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Runtime dependencies excluded from the packaged SQL client. */
public final class FlinkSqlDependencySupport {

    private static final Artifact SNAKEYAML = new Artifact("org.yaml", "snakeyaml", "2.0");

    private FlinkSqlDependencySupport() {
    }

    public static DependencyInfo withRuntimeDependencies(DependencyInfo dependencies) {
        return dependencies.merge(Collections.singletonList(SNAKEYAML), null);
    }

    public static Set<String> resolveRuntimeJars() throws Exception {
        List<File> jars = MavenTool.resolveArtifacts(Collections.singleton(SNAKEYAML));
        return jars.stream().map(File::getAbsolutePath).collect(Collectors.toSet());
    }
}
