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

import org.apache.streampark.common.enums.StorageType;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/** File system operator factory and shared operations. */
public abstract class FsOperator {

    /** Scala-friendly local file system operator singleton. */
    public static final FsOperator lfs = new FsOperator() {

        @Override
        public boolean exists(String path) {
            return LfsOperator.exists(path);
        }

        @Override
        public void mkdirs(String path) {
            LfsOperator.mkdirs(path);
        }

        @Override
        public void delete(String path) {
            LfsOperator.delete(path);
        }

        @Override
        public void mkCleanDirs(String path) {
            LfsOperator.mkCleanDirs(path);
        }

        @Override
        public void upload(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            LfsOperator.upload(srcPath, dstPath, delSrc, overwrite);
        }

        @Override
        public void copy(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            LfsOperator.copy(srcPath, dstPath, delSrc, overwrite);
        }

        @Override
        public void copyDir(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            LfsOperator.copyDir(srcPath, dstPath, delSrc, overwrite);
        }

        @Override
        public void move(String srcPath, String dstPath) {
            LfsOperator.move(srcPath, dstPath);
        }

        @Override
        public String fileSha256(String path) {
            return LfsOperator.fileSha256(path);
        }

        @Override
        public InputStream open(String path) throws IOException {
            return LfsOperator.open(path);
        }

        @Override
        public List<String> list(String path) throws IOException {
            return LfsOperator.list(path);
        }

        @Override
        public boolean isFile(String path) {
            return LfsOperator.isFile(path);
        }

        @Override
        public boolean isDirectory(String path) {
            return LfsOperator.isDirectory(path);
        }

        @Override
        public String canonicalPath(String path) throws IOException {
            return LfsOperator.canonicalPath(path);
        }
    };

    /** Scala-friendly HDFS operator singleton. */
    public static final FsOperator hdfs = new FsOperator() {

        @Override
        public boolean exists(String path) {
            return HdfsOperator.exists(path);
        }

        @Override
        public void mkdirs(String path) {
            HdfsOperator.mkdirs(path);
        }

        @Override
        public void delete(String path) {
            HdfsOperator.delete(path);
        }

        @Override
        public void mkCleanDirs(String path) {
            HdfsOperator.mkCleanDirs(path);
        }

        @Override
        public void upload(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            HdfsOperator.upload(srcPath, dstPath, delSrc, overwrite);
        }

        @Override
        public void copy(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            HdfsOperator.copy(srcPath, dstPath, delSrc, overwrite);
        }

        @Override
        public void copyDir(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
            HdfsOperator.copyDir(srcPath, dstPath, delSrc, overwrite);
        }

        @Override
        public void move(String srcPath, String dstPath) {
            HdfsOperator.move(srcPath, dstPath);
        }

        @Override
        public String fileSha256(String path) {
            return HdfsOperator.fileSha256(path);
        }

        @Override
        public InputStream open(String path) throws IOException {
            return HdfsOperator.open(path);
        }

        @Override
        public List<String> list(String path) throws IOException {
            return HdfsOperator.list(path);
        }

        @Override
        public boolean isFile(String path) throws IOException {
            return HdfsOperator.isFile(path);
        }

        @Override
        public boolean isDirectory(String path) throws IOException {
            return HdfsOperator.isDirectory(path);
        }

        @Override
        public String canonicalPath(String path) throws IOException {
            return HdfsOperator.canonicalPath(path);
        }
    };

    protected FsOperator() {
    }

    public static FsOperator lfs() {
        return lfs;
    }

    public static FsOperator hdfs() {
        return hdfs;
    }

    public static FsOperator of(StorageType storageType) {
        switch (storageType) {
            case HDFS:
                return hdfs;
            case LFS:
                return lfs;
            default:
                throw new UnsupportedOperationException("Unsupported storageType:" + storageType);
        }
    }

    public abstract boolean exists(String path);

    public abstract void mkdirs(String path);

    public void mkdirsIfNotExists(String path) {
        if (!exists(path)) {
            mkdirs(path);
        }
    }

    public abstract void delete(String path);

    public abstract void mkCleanDirs(String path);

    public void upload(String srcPath, String dstPath) {
        upload(srcPath, dstPath, false, true);
    }

    public void copy(String srcPath, String dstPath) {
        copy(srcPath, dstPath, false, true);
    }

    public void copyDir(String srcPath, String dstPath) {
        copyDir(srcPath, dstPath, false, true);
    }

    public abstract void upload(String srcPath, String dstPath, boolean delSrc, boolean overwrite);

    public abstract void copy(String srcPath, String dstPath, boolean delSrc, boolean overwrite);

    public abstract void copyDir(String srcPath, String dstPath, boolean delSrc, boolean overwrite);

    public abstract void move(String srcPath, String dstPath);

    /**
     * Returns the SHA-256 digest of a file for collision-resistant content comparison.
     *
     * @param path file path understood by this operator
     * @return lowercase hexadecimal SHA-256 digest
     * @throws UnsupportedOperationException when a custom operator does not provide digest support
     */
    public String fileSha256(String path) {
        throw new UnsupportedOperationException("SHA-256 is not supported by this file system");
    }

    /** Opens a regular file for reading without following local symbolic links. */
    public InputStream open(String path) throws IOException {
        throw new UnsupportedOperationException("Opening files is not supported by this file system");
    }

    /** Lists direct children of a directory. */
    public List<String> list(String path) throws IOException {
        throw new UnsupportedOperationException("Listing files is not supported by this file system");
    }

    /** Returns whether the path is a regular file. */
    public boolean isFile(String path) throws IOException {
        throw new UnsupportedOperationException("File checks are not supported by this file system");
    }

    /** Returns whether the path is a directory. */
    public boolean isDirectory(String path) throws IOException {
        throw new UnsupportedOperationException("Directory checks are not supported by this file system");
    }

    /** Resolves a path after rejecting symbolic links and non-canonical HDFS paths. */
    public String canonicalPath(String path) throws IOException {
        throw new UnsupportedOperationException("Path resolution is not supported by this file system");
    }
}
