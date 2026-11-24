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

import org.apache.streampark.common.util.Utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Local File System (aka LFS) Operator */
public final class LfsOperator {

    private LfsOperator() {
    }

    public static boolean exists(String path) {
        return StringUtils.isNotBlank(path) && new File(path).exists();
    }

    public static void mkdirs(String path) {
        if (!Utils.isAnyBank(path)) {
            try {
                FileUtils.forceMkdir(new File(path));
            } catch (IOException e) {
                throw new IllegalStateException("[StreamPark] Failed to mkdirs: " + path, e);
            }
        }
    }

    public static void delete(String path) {
        if (Utils.isNotEmpty(path)) {
            File file = new File(path);
            if (file.exists()) {
                try {
                    FileUtils.forceDelete(file);
                } catch (IOException e) {
                    throw new IllegalStateException("[StreamPark] Failed to delete: " + path, e);
                }
            }
        }
    }

    public static void move(String srcPath, String dstPath) {
        if (Utils.isAnyBank(srcPath, dstPath)) {
            return;
        }
        File srcFile = new File(srcPath);
        File dstFile = new File(dstPath);
        if (!srcFile.exists()) {
            return;
        }
        try {
            if (srcFile.getCanonicalPath().equals(dstFile.getCanonicalPath())) {
                return;
            }
            FileUtils.moveToDirectory(srcFile, dstFile, true);
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
        if (new File(srcPath).isDirectory()) {
            copyDir(srcPath, dstPath, delSrc, overwrite);
        } else {
            copy(srcPath, dstPath, delSrc, overwrite);
        }
    }

    /**
     * When the suffixes of srcPath and dstPath are the same, or the file names are the same, copy to
     * the file, otherwise copy to the directory.
     */
    public static void copy(String srcPath, String dstPath) {
        copy(srcPath, dstPath, false, true);
    }

    public static void copy(String srcPath, String dstPath, boolean delSrc) {
        copy(srcPath, dstPath, delSrc, true);
    }

    public static void copy(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
        if (Utils.isAnyBank(srcPath, dstPath)) {
            return;
        }
        File srcFile = new File(srcPath);
        if (!srcFile.exists()) {
            return;
        }
        if (!srcFile.isFile()) {
            throw new IllegalArgumentException("[StreamPark] " + srcPath + " must be a file.");
        }
        File dstFile = resolveCopyDestination(srcFile, dstPath);
        try {
            if (srcFile.getCanonicalPath().equals(dstFile.getCanonicalPath())) {
                throw new IllegalArgumentException(
                    "[StreamPark] src and dst must not be the same path: " + srcPath);
            }
        } catch (IOException e) {
            throw new IllegalStateException("[StreamPark] Failed to resolve copy paths", e);
        }
        boolean shouldCopy =
            overwrite || !dstFile.exists() || !dstFile.getName().equals(srcFile.getName());
        if (shouldCopy) {
            try {
                FileUtils.copyFile(srcFile, dstFile);
                if (delSrc) {
                    FileUtils.forceDelete(srcFile);
                }
            } catch (IOException e) {
                throw new IllegalStateException(
                    "[StreamPark] Failed to copy " + srcPath + " to " + dstPath, e);
            }
        }
    }

    public static void copyDir(String srcPath, String dstPath) {
        copyDir(srcPath, dstPath, false, true);
    }

    public static void copyDir(String srcPath, String dstPath, boolean delSrc) {
        copyDir(srcPath, dstPath, delSrc, true);
    }

    public static void copyDir(String srcPath, String dstPath, boolean delSrc, boolean overwrite) {
        if (Utils.isAnyBank(srcPath, dstPath)) {
            return;
        }
        File srcFile = new File(srcPath);
        if (!srcFile.exists()) {
            return;
        }
        if (!srcFile.isDirectory()) {
            throw new IllegalArgumentException("[StreamPark] " + srcPath + " must be a directory.");
        }
        File dstFile = new File(dstPath);
        boolean shouldCopy;
        try {
            shouldCopy = overwrite || !dstFile.exists()
                || !srcFile.getCanonicalPath().equals(dstFile.getCanonicalPath());
        } catch (IOException e) {
            throw new IllegalStateException("[StreamPark] Failed to resolve copyDir paths", e);
        }
        if (shouldCopy) {
            try {
                FileUtils.copyDirectory(srcFile, dstFile);
                if (delSrc) {
                    FileUtils.deleteDirectory(srcFile);
                }
            } catch (IOException e) {
                throw new IllegalStateException(
                    "[StreamPark] Failed to copyDir " + srcPath + " to " + dstPath, e);
            }
        }
    }

    /**
     * Returns the SHA-256 digest of a local file.
     *
     * @param path local file path
     * @return lowercase hexadecimal SHA-256 digest
     */
    public static String fileSha256(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException(
                "[StreamPark] LFsOperator.fileSha256: file must not be null.");
        }
        File file = new File(path);
        if (!file.exists()) {
            throw new IllegalArgumentException(
                "[StreamPark] LFsOperator.fileSha256: file must exist.");
        }
        try (FileInputStream inputStream = new FileInputStream(file)) {
            return DigestUtils.sha256Hex(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException(
                "[StreamPark] Failed to compute SHA-256 for: " + path, e);
        }
    }

    /** Opens a regular local file without following symbolic links. */
    public static InputStream open(String path) throws IOException {
        Path file = safePath(path);
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Not a regular file: " + path);
        }
        return Channels.newInputStream(
            Files.newByteChannel(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
    }

    /** Lists direct children of a local directory without following symbolic links. */
    public static List<String> list(String path) throws IOException {
        Path directory = safePath(path);
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Not a directory: " + path);
        }
        List<String> entries = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                entries.add(entry.toString());
            }
        }
        return entries;
    }

    public static boolean isFile(String path) {
        try {
            return Files.isRegularFile(safePath(path), LinkOption.NOFOLLOW_LINKS);
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    public static boolean isDirectory(String path) {
        try {
            return Files.isDirectory(safePath(path), LinkOption.NOFOLLOW_LINKS);
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    public static String canonicalPath(String path) throws IOException {
        return safePath(path).toRealPath().toString();
    }

    private static Path safePath(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            throw new IOException("Path must not be empty");
        }
        Path absolute = Paths.get(path).toAbsolutePath().normalize();
        Path current = absolute.getRoot();
        for (Path segment : absolute) {
            current = current.resolve(segment);
            if (Files.isSymbolicLink(current)) {
                throw new IOException("Symbolic links are not allowed: " + path);
            }
        }
        return absolute;
    }

    /** Force delete directory and recreate it. */
    public static void mkCleanDirs(String path) {
        delete(path);
        mkdirs(path);
    }

    /** list file under directory, one level of traversal only */
    public static File[] listDir(String path) {
        if (path == null || path.trim().isEmpty()) {
            return new File[0];
        }
        File file = new File(path);
        if (!file.exists()) {
            return new File[0];
        }
        if (file.isFile()) {
            return new File[]{file};
        }
        File[] files = file.listFiles();
        return files != null ? files : new File[0];
    }

    private static File resolveCopyDestination(File srcFile, String dstPath) {
        File dstFile = new File(dstPath);
        if (dstFile.exists()) {
            if (dstFile.isDirectory()) {
                return new File(dstFile, srcFile.getName());
            }
            return dstFile;
        }
        if (!dstFile.getParentFile().exists()) {
            throw new IllegalArgumentException(
                "[StreamPark] dstPath is invalid and does not exist. Please check");
        }
        return dstFile;
    }
}
