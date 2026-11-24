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
import org.apache.streampark.common.util.HdfsUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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

    public static InputStream open(String path) throws IOException {
        canonicalPath(path);
        Path hdfsPath = toPath(path);
        FileStatus status = HadoopUtils.hdfs().getFileStatus(hdfsPath);
        if (!status.isFile() || status.isSymlink()) {
            throw new IOException("Not a regular HDFS file: " + path);
        }
        return HadoopUtils.hdfs().open(hdfsPath);
    }

    public static List<String> list(String path) throws IOException {
        canonicalPath(path);
        Path hdfsPath = toPath(path);
        FileStatus status = HadoopUtils.hdfs().getFileStatus(hdfsPath);
        if (!status.isDirectory()) {
            throw new IOException("Not an HDFS directory: " + path);
        }
        List<String> entries = new ArrayList<>();
        for (FileStatus child : HadoopUtils.hdfs().listStatus(hdfsPath)) {
            entries.add(child.getPath().toString());
        }
        return entries;
    }

    public static boolean isFile(String path) throws IOException {
        try {
            canonicalPath(path);
        } catch (java.io.FileNotFoundException e) {
            return false;
        }
        FileStatus status = status(path);
        return status != null && status.isFile() && !status.isSymlink();
    }

    public static boolean isDirectory(String path) throws IOException {
        try {
            canonicalPath(path);
        } catch (java.io.FileNotFoundException e) {
            return false;
        }
        FileStatus status = status(path);
        return status != null && status.isDirectory();
    }

    private static FileStatus status(String path) throws IOException {
        try {
            return HadoopUtils.hdfs().getFileStatus(toPath(path));
        } catch (java.io.FileNotFoundException e) {
            return null;
        }
    }

    public static String canonicalPath(String path) throws IOException {
        Path expected = toPath(path);
        FileStatus linkStatus = HadoopUtils.hdfs().getFileLinkStatus(expected);
        if (linkStatus.isSymlink()) {
            throw new IOException("Symbolic links are not allowed: " + path);
        }
        FileStatus status = HadoopUtils.hdfs().getFileStatus(expected);
        if (status.isSymlink()) {
            throw new IOException("Symbolic links are not allowed: " + path);
        }
        Path qualified = HadoopUtils.hdfs().makeQualified(expected);
        Path resolved = HadoopUtils.hdfs().resolvePath(expected);
        if (!qualified.equals(resolved)) {
            throw new IOException("Non-canonical HDFS path: " + path);
        }
        return qualified.toString();
    }

    private static String toHdfsPath(String path) {
        if (path.startsWith("hdfs://")) {
            return path;
        }
        return HdfsUtils.getDefaultFS().concat(path);
    }

    private static Path toPath(String path) {
        return new Path(toHdfsPath(path));
    }
}
