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

import org.apache.streampark.common.util.HdfsUtils;

import java.io.IOException;

/** Hadoop File System (aka HDFS) Operator */
public final class HdfsOperator {

    private HdfsOperator() {
    }

    public static boolean exists(String path) {
        try {
            return HdfsUtils.exists(toHdfsPath(path));
        } catch (IOException e) {
            throw new IllegalStateException("[StreamPark] Failed to check hdfs path: " + path, e);
        }
    }

    public static void mkdirs(String path) {
        try {
            HdfsUtils.mkdirs(toHdfsPath(path));
        } catch (IOException e) {
            throw new IllegalStateException("[StreamPark] Failed to mkdirs hdfs path: " + path, e);
        }
    }

    public static void delete(String path) {
        try {
            HdfsUtils.delete(toHdfsPath(path));
        } catch (IOException e) {
            throw new IllegalStateException("[StreamPark] Failed to delete hdfs path: " + path, e);
        }
    }

    public static void move(String srcPath, String dstPath) {
        try {
            HdfsUtils.move(toHdfsPath(srcPath), toHdfsPath(dstPath));
        } catch (IOException e) {
            throw new IllegalStateException(
                "[StreamPark] Failed to move " + srcPath + " to " + dstPath, e);
        }
    }

    public static void upload(String srcPath, String dstPath) {
        upload(srcPath, dstPath, false, true);
    }

    public static void upload(String srcPath, String dstPath, boolean delSrc) {
        upload(srcPath, dstPath, delSrc, true);
    }

    public static void upload(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
        try {
            HdfsUtils.upload(srcPath, toHdfsPath(dstPath), delSrc, overwrite);
        } catch (IOException e) {
            throw new IllegalStateException(
                "[StreamPark] Failed to upload " + srcPath + " to " + dstPath, e);
        }
    }

    public static void copy(String srcPath, String dstPath) {
        copy(srcPath, dstPath, false, true);
    }

    public static void copy(String srcPath, String dstPath, boolean delSrc) {
        copy(srcPath, dstPath, delSrc, true);
    }

    public static void copy(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
        try {
            HdfsUtils.copyHdfs(toHdfsPath(srcPath), toHdfsPath(dstPath), delSrc, overwrite);
        } catch (IOException e) {
            throw new IllegalStateException(
                "[StreamPark] Failed to copy " + srcPath + " to " + dstPath, e);
        }
    }

    public static void copyDir(String srcPath, String dstPath) {
        copyDir(srcPath, dstPath, false, true);
    }

    public static void copyDir(String srcPath, String dstPath, boolean delSrc) {
        copyDir(srcPath, dstPath, delSrc, true);
    }

    public static void copyDir(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
        try {
            HdfsUtils.copyHdfsDir(toHdfsPath(srcPath), toHdfsPath(dstPath), delSrc, overwrite);
        } catch (IOException e) {
            throw new IllegalStateException(
                "[StreamPark] Failed to copyDir " + srcPath + " to " + dstPath, e);
        }
    }

    public static void mkCleanDirs(String path) {
        delete(path);
        mkdirs(path);
    }

    /**
     * Returns the SHA-256 digest of an HDFS file.
     *
     * @param path HDFS file path
     * @return lowercase hexadecimal SHA-256 digest
     */
    public static String fileSha256(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException(
                "[StreamPark] HdfsOperator.fileSha256: file must not be null.");
        }
        try {
            return HdfsUtils.fileSha256(toHdfsPath(path));
        } catch (IOException e) {
            throw new IllegalStateException(
                "[StreamPark] Failed to compute SHA-256 for: " + path, e);
        }
    }

    private static String toHdfsPath(String path) {
        // Hadoop also represents HDFS paths without an authority as hdfs:/path.
        if (path.startsWith("hdfs:/")) {
            return path;
        }
        return HdfsUtils.getDefaultFS().concat(path);
    }
}
