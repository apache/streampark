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

package org.apache.streampark.flink.packer.docker;

import org.apache.streampark.common.fs.LfsOperator;
import org.apache.streampark.common.util.HadoopConfigUtils;

import javax.annotation.Nullable;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;

/** flink-hadoop integration docker image template. */
public class FlinkHadoopDockerfileTemplate extends FlinkDockerfileTemplateTrait {

    private final String workspacePath;
    private final String flinkBaseImage;
    private final String flinkMainJarPath;
    private final Set<String> flinkExtraLibPaths;
    @Nullable
    private final String hadoopConfDirPath;
    @Nullable
    private final String hiveConfDirPath;
    private final String hadoopConfDir;
    private final String hiveConfDir;

    public FlinkHadoopDockerfileTemplate(
                                         String workspacePath,
                                         String flinkBaseImage,
                                         String flinkMainJarPath,
                                         Set<String> flinkExtraLibPaths,
                                         @Nullable String hadoopConfDirPath,
                                         @Nullable String hiveConfDirPath) {
        this.workspacePath = workspacePath;
        this.flinkBaseImage = flinkBaseImage;
        this.flinkMainJarPath = flinkMainJarPath;
        this.flinkExtraLibPaths = flinkExtraLibPaths;
        this.hadoopConfDirPath = hadoopConfDirPath;
        this.hiveConfDirPath = hiveConfDirPath;
        this.hadoopConfDir = hadoopConfDirPath == null || hadoopConfDirPath.isEmpty()
            ? ""
            : workspace().relativize(Paths.get(hadoopConfDirPath).toAbsolutePath()).toString();
        this.hiveConfDir = hiveConfDirPath == null || hiveConfDirPath.isEmpty()
            ? ""
            : workspace().relativize(Paths.get(hiveConfDirPath).toAbsolutePath()).toString();
    }

    @Override
    public String workspacePath() {
        return workspacePath;
    }

    @Override
    public String flinkBaseImage() {
        return flinkBaseImage;
    }

    @Override
    public String flinkMainJarPath() {
        return flinkMainJarPath;
    }

    @Override
    public Set<String> flinkExtraLibPaths() {
        return flinkExtraLibPaths;
    }

    @Override
    public String offerDockerfileContent() {
        StringBuilder dockerfile =
            new StringBuilder(
                "FROM " + flinkBaseImage + "\n"
                    + "RUN mkdir -p " + FLINK_HOME + "/usrlib\n");
        if (!hadoopConfDir.isEmpty()) {
            dockerfile
                .append("COPY ")
                .append(hadoopConfDir)
                .append(" /opt/hadoop-conf\n")
                .append("ENV HADOOP_CONF_DIR /opt/hadoop-conf\n");
        }
        if (!hiveConfDir.isEmpty()) {
            dockerfile
                .append("COPY ")
                .append(hiveConfDir)
                .append(" /opt/hive-conf\n")
                .append("ENV HIVE_CONF_DIR /opt/hive-conf\n");
        }
        dockerfile
            .append("COPY ")
            .append(extraLibName())
            .append(" ")
            .append(FLINK_HOME)
            .append("/lib/\n")
            .append("COPY ")
            .append(mainJarName())
            .append(" ")
            .append(FLINK_HOME)
            .append("/usrlib/")
            .append(mainJarName())
            .append("\n");
        return dockerfile.toString();
    }

    /** Use relevant system variables as the value of hadoopConfDirPath, hiveConfDirPath. */
    public static FlinkHadoopDockerfileTemplate fromSystemHadoopConf(
                                                                     String workspacePath,
                                                                     String flinkBaseImage,
                                                                     String flinkMainJarPath,
                                                                     Set<String> flinkExtraLibPaths) {
        String hadoopConfDir = "";
        Optional<String> systemHadoopConf = HadoopConfigUtils.getSystemHadoopConfDirAsJava();
        if (systemHadoopConf.isPresent() && LfsOperator.exists(systemHadoopConf.get())) {
            hadoopConfDir = workspacePath + "/hadoop-conf";
            LfsOperator.mkCleanDirs(hadoopConfDir);
            LfsOperator.copyDir(systemHadoopConf.get(), hadoopConfDir);
        }
        String hiveConfDir = "";
        Optional<String> systemHiveConf = HadoopConfigUtils.getSystemHiveConfDirAsJava();
        if (systemHiveConf.isPresent() && LfsOperator.exists(systemHiveConf.get())) {
            hiveConfDir = workspacePath + "/hive-conf";
            LfsOperator.mkCleanDirs(hiveConfDir);
            LfsOperator.copyDir(systemHiveConf.get(), hiveConfDir);
        }
        return new FlinkHadoopDockerfileTemplate(
            workspacePath,
            flinkBaseImage,
            flinkMainJarPath,
            flinkExtraLibPaths,
            hadoopConfDir,
            hiveConfDir);
    }
}
