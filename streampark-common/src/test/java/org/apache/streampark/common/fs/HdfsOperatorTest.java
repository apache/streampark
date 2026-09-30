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

package org.apache.streampark.common.fs;

import org.apache.streampark.common.util.HadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class HdfsOperatorTest {

    private static final String CHECKPOINT_PATH = "/tmp/checkpoints/chk-1";

    @ParameterizedTest
    @MethodSource("checkpointPaths")
    void deleteCheckpointPath(String defaultFS, String path) throws Exception {
        Configuration configuration = HadoopUtils.hadoopConf();
        String previousDefault = configuration.get(FileSystem.FS_DEFAULT_NAME_KEY);
        Field cachedFileSystem = HadoopUtils.class.getDeclaredField("reusableHdfs");
        cachedFileSystem.setAccessible(true);
        Object previousFileSystem = cachedFileSystem.get(null);
        RecordingFileSystem fileSystem = new RecordingFileSystem(URI.create(defaultFS));
        fileSystem.setConf(configuration);
        try {
            configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, defaultFS);
            cachedFileSystem.set(null, fileSystem);

            HdfsOperator.delete(path);

            assertThat(fileSystem.deletedPath)
                .isEqualTo(new Path(CHECKPOINT_PATH).makeQualified(fileSystem.getUri(), new Path("/")));
        } finally {
            cachedFileSystem.set(null, previousFileSystem);
            if (previousDefault == null) {
                configuration.unset(FileSystem.FS_DEFAULT_NAME_KEY);
            } else {
                configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, previousDefault);
            }
        }
    }

    private static Stream<Arguments> checkpointPaths() {
        return Stream.of("hdfs://namenode:8020", "hdfs://nameservice1", "hdfs://nameservice1/")
            .flatMap(defaultFS -> Stream.of(
                "hdfs:" + CHECKPOINT_PATH,
                "hdfs://" + CHECKPOINT_PATH,
                new Path(CHECKPOINT_PATH).makeQualified(URI.create(defaultFS), new Path("/")).toString(),
                CHECKPOINT_PATH)
                .map(path -> Arguments.of(defaultFS, path)));
    }

    /** Captures filesystem operations without accessing local files or an HDFS cluster. */
    private static final class RecordingFileSystem extends RawLocalFileSystem {

        private final URI uri;
        private Path deletedPath;

        private RecordingFileSystem(URI uri) {
            this.uri = uri;
        }

        @Override
        public URI getUri() {
            return uri;
        }

        @Override
        protected Path getInitialWorkingDirectory() {
            return new Path("/");
        }

        @Override
        public FileStatus getFileStatus(Path path) {
            checkPath(path);
            return new FileStatus(0, true, 1, 0, 0, path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            checkPath(path);
            deletedPath = makeQualified(path);
            return true;
        }
    }
}
