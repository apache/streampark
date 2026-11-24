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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.streampark.common.fs.LfsOperatorTestSupport.DirWithFiles;
import static org.apache.streampark.common.fs.LfsOperatorTestSupport.genRandomDir;
import static org.apache.streampark.common.fs.LfsOperatorTestSupport.genRandomFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LfsOperatorTest {

    private static void withTempDir(TempDirConsumer block) throws Exception {
        java.nio.file.Path tempDirPath = Files.createTempDirectory("LfsOperatorTest-output");
        try {
            block.accept(tempDirPath.toAbsolutePath().toString());
        } finally {
            FileUtils.deleteQuietly(tempDirPath.toFile());
        }
    }

    @FunctionalInterface
    private interface TempDirConsumer {

        void accept(String outputDir) throws Exception;
    }

    @Test
    void testMkdirs() throws Exception {
        withTempDir(
            outputDir -> {
                assertDoesNotThrow(() -> LfsOperator.mkdirs(null));
                assertDoesNotThrow(() -> LfsOperator.mkdirs(""));
                assertTrue(LfsOperator.exists(outputDir));

                assertDoesNotThrow(
                    () -> {
                        LfsOperator.mkdirs(outputDir + "/test");
                        LfsOperator.mkdirs(outputDir + "/test");
                    });
            });
    }

    @Test
    void testExists() throws Exception {
        withTempDir(
            outputDir -> {
                assertDoesNotThrow(
                    () -> {
                        String dir = outputDir + "/tmp";
                        File f = new File(dir);
                        f.mkdirs();
                        assertTrue(LfsOperator.exists(f.getAbsolutePath()));
                        assertTrue(LfsOperator.exists(dir));
                    });

                assertFalse(LfsOperator.exists(null));
                assertFalse(LfsOperator.exists(""));
                assertFalse(LfsOperator.exists(outputDir + "/233"));
            });
    }

    @Test
    void testMkCleanDirs() throws Exception {
        withTempDir(
            outputDir -> {
                assertDoesNotThrow(
                    () -> {
                        for (int i = 0; i < 5; i++) {
                            genRandomFile(outputDir);
                        }
                        assertEquals(5, new File(outputDir).list().length);
                        LfsOperator.mkCleanDirs(outputDir);
                        File dir = new File(outputDir);
                        assertTrue(dir.exists());
                        assertTrue(dir.isDirectory());
                        assertEquals(0, dir.list().length);
                    });

                assertDoesNotThrow(() -> LfsOperator.mkdirs(null));
                assertDoesNotThrow(() -> LfsOperator.mkdirs(""));

                assertTrue(
                    () -> {
                        LfsOperator.mkCleanDirs(outputDir + "/114514");
                        return new File(outputDir + "/114514").exists();
                    });
            });
    }

    @Test
    void listDir() throws Exception {
        withTempDir(
            outputDir -> {
                assertTrue(
                    () -> {
                        DirWithFiles randomDir = genRandomDir(outputDir);
                        File[] expectFs = randomDir.files();
                        File[] actualFs = LfsOperator.listDir(outputDir);
                        return sameFileNames(expectFs, actualFs);
                    });

                assertTrue(
                    () -> {
                        File file = genRandomFile(outputDir);
                        File[] listFile = LfsOperator.listDir(file.getAbsolutePath());
                        return listFile.length == 1 && listFile[0].getName().equals(file.getName());
                    });

                assertTrue(LfsOperator.listDir("").length == 0);
                assertTrue(LfsOperator.listDir(null).length == 0);
                assertTrue(LfsOperator.listDir(outputDir + "/114514").length == 0);
            });
    }

    @Test
    void testDelete() throws Exception {
        withTempDir(
            outputDir -> {
                assertFalse(
                    () -> {
                        String dir = outputDir + "/tmp";
                        genRandomDir(dir);
                        LfsOperator.delete(dir);
                        return new File(dir).exists();
                    });

                assertFalse(
                    () -> {
                        File file = genRandomFile(outputDir);
                        LfsOperator.delete(file.getAbsolutePath());
                        return file.exists();
                    });

                assertDoesNotThrow(() -> LfsOperator.delete(null));
                assertDoesNotThrow(() -> LfsOperator.delete(""));
                assertDoesNotThrow(() -> LfsOperator.delete(outputDir + "/114514"));
            });
    }

    private String sha256Hex(File file) throws Exception {
        try (FileInputStream input = new FileInputStream(file)) {
            return DigestUtils.sha256Hex(IOUtils.toByteArray(input));
        }
    }

    private boolean sameFilesHex(File[] f1, File[] f2) {
        List<String> names1 = sortedNames(f1);
        List<String> names2 = sortedNames(f2);
        if (!names1.equals(names2)) {
            return false;
        }
        List<String> digests1 =
            Arrays.stream(f1).map(this::sha256HexSafe).sorted().collect(Collectors.toList());
        List<String> digests2 =
            Arrays.stream(f2).map(this::sha256HexSafe).sorted().collect(Collectors.toList());
        return digests1.equals(digests2);
    }

    private String sha256HexSafe(File file) {
        try {
            return sha256Hex(file);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean sameFileNames(File[] f1, File[] f2) {
        return sortedNames(f1).equals(sortedNames(f2));
    }

    private List<String> sortedNames(File[] files) {
        return Arrays.stream(files)
            .map(File::getName)
            .sorted()
            .collect(Collectors.toList());
    }

    @Test
    void testCopy() throws Exception {
        withTempDir(
            outputDir -> {
                assertDoesNotThrow(
                    () -> {
                        File file = genRandomFile(outputDir);

                        Files.createDirectory(Paths.get(outputDir, "out-1"));
                        assertCopy(file, outputDir + "/out-1", outputDir + "/out-1/" + file.getName());

                        Files.createDirectory(Paths.get(outputDir, "out-2"));
                        assertCopy(
                            file,
                            outputDir + "/out-2/" + file.getName(),
                            outputDir + "/out-2/" + file.getName());

                        Files.createDirectory(Paths.get(outputDir, "out-3"));
                        assertCopy(
                            file, outputDir + "/out-3/114514.dat", outputDir + "/out-3/114514.dat");
                    });

                assertDoesNotThrow(
                    () -> {
                        LfsOperator.copy(outputDir + "/nobody.dat", outputDir + "/out-5/nobody.dat");
                        assertFalse(new File(outputDir + "/out-5/nobody.dat").exists());
                    });

                DirWithFiles randomDir = genRandomDir(outputDir + "/in-1");
                File dir = randomDir.dir();
                assertThrows(
                    IllegalArgumentException.class,
                    () -> LfsOperator.copy(dir.getAbsolutePath(), outputDir + "/out-6"));

                assertDoesNotThrow(
                    () -> {
                        File file = genRandomFile(outputDir);
                        LfsOperator.copy(file.getAbsolutePath(), outputDir + "/out-7", false, true);
                        assertTrue(file.exists());
                        LfsOperator.copy(file.getAbsolutePath(), outputDir + "/out-8", true, true);
                        assertFalse(file.exists());
                    });

                File file = genRandomFile(outputDir, "114514-233.dat");

                assertDoesNotThrow(
                    () -> {
                        File out = genRandomFile(outputDir + "/out-9", "114514-233.dat");
                        String digestBefore = sha256Hex(out);
                        LfsOperator.copy(file.getAbsolutePath(), out.getAbsolutePath(), false, true);
                        String digestAfter = sha256Hex(new File(out.getAbsolutePath()));
                        assertNotEquals(digestBefore, digestAfter);
                        assertEquals(digestAfter, sha256Hex(file));
                    });

                assertDoesNotThrow(
                    () -> {
                        File out = genRandomFile(outputDir + "/out-10", "114514-233.dat");
                        String digestBefore = sha256Hex(out);
                        LfsOperator.copy(file.getAbsolutePath(), out.getAbsolutePath(), false, false);
                        String digestAfter = sha256Hex(new File(out.getAbsolutePath()));
                        assertEquals(digestBefore, digestAfter);
                        assertNotEquals(digestAfter, sha256Hex(file));
                    });
            });
    }

    private void assertCopy(File file, String to, String expectedOut) {
        LfsOperator.copy(file.getAbsolutePath(), to);
        File output = new File(expectedOut);
        assertTrue(output.exists());
        assertEquals(file.length(), output.length());
    }

    @Test
    void testCopyDir() throws Exception {
        withTempDir(
            outputDir -> {
                assertDoesNotThrow(
                    () -> {
                        DirWithFiles source = genRandomDir(outputDir + "/in-1");
                        File sourceDir = source.dir();
                        String target = outputDir + "/out-1";
                        LfsOperator.copyDir(sourceDir.getAbsolutePath(), target);
                        File targetDir = new File(target);
                        assertTrue(targetDir.exists());
                        assertTrue(targetDir.isDirectory());
                        assertTrue(sameFilesHex(sourceDir.listFiles(), targetDir.listFiles()));
                    });

                assertDoesNotThrow(
                    () -> {
                        LfsOperator.copyDir(outputDir + "/in-2", outputDir + "/out-2");
                        assertFalse(new File(outputDir + "/out-2").exists());
                        LfsOperator.copyDir("", outputDir + "/out-2");
                        LfsOperator.copyDir(null, outputDir + "/out-2");
                    });

                assertThrows(
                    IllegalArgumentException.class,
                    () -> LfsOperator.copyDir(
                        genRandomFile(outputDir).getAbsolutePath(), outputDir + "/out-3"));

                assertDoesNotThrow(
                    () -> {
                        DirWithFiles source = genRandomDir(outputDir + "/in-4");
                        File sourceDir = source.dir();
                        LfsOperator.copyDir(sourceDir.getAbsolutePath(), outputDir + "/out-4", false, true);
                        assertTrue(sourceDir.exists());
                        LfsOperator.copyDir(sourceDir.getAbsolutePath(), outputDir + "/out-5", true, true);
                        assertFalse(sourceDir.exists());
                    });
            });
    }

    @Test
    void testMove() throws Exception {
        withTempDir(
            outputDir -> {
                assertDoesNotThrow(
                    () -> {
                        File sourceFile = genRandomFile(outputDir);
                        String sourceDigest;
                        try (FileInputStream input = new FileInputStream(sourceFile)) {
                            sourceDigest = DigestUtils.sha256Hex(IOUtils.toByteArray(input));
                        }
                        String targetPath = outputDir + "/target-1";
                        LfsOperator.move(sourceFile.getAbsolutePath(), targetPath);

                        File targetFile = new File(targetPath, sourceFile.getName());
                        assertTrue(targetFile.exists());
                        try (FileInputStream input = new FileInputStream(targetFile)) {
                            assertEquals(
                                sourceDigest,
                                DigestUtils.sha256Hex(IOUtils.toByteArray(input)));
                        }
                    });

                assertDoesNotThrow(
                    () -> {
                        DirWithFiles source = genRandomDir(outputDir + "/tmp");
                        File sourceDir = source.dir();
                        File[] sourceFiles = source.files();
                        String targetPath = outputDir + "/target-2";
                        LfsOperator.move(sourceDir.getAbsolutePath(), targetPath);
                        File targetDir = new File(targetPath + "/tmp");
                        assertTrue(targetDir.exists());
                        assertEquals(
                            sortedNames(sourceFiles), sortedNames(targetDir.listFiles()));
                    });

                assertFalse(
                    () -> {
                        LfsOperator.move(outputDir + "/aha.dat", outputDir + "/target-3");
                        return new File(outputDir + "/target-3/aha.dat").exists();
                    });

                assertDoesNotThrow(
                    () -> {
                        File file = genRandomFile(outputDir);
                        assertDoesNotThrow(() -> LfsOperator.move(file.getAbsolutePath(), null));
                        assertDoesNotThrow(() -> LfsOperator.move(file.getAbsolutePath(), ""));
                    });

                assertDoesNotThrow(
                    () -> {
                        File file = genRandomFile(outputDir);
                        String target = outputDir + "/target-4";
                        LfsOperator.move(file.getAbsolutePath(), target);
                        LfsOperator.move(file.getAbsolutePath(), target);
                    });

                assertDoesNotThrow(
                    () -> {
                        DirWithFiles randomDir = genRandomDir(outputDir + "/tmp-5", 3);
                        File dir = randomDir.dir();
                        String target = outputDir + "/target-5";
                        LfsOperator.move(dir.getAbsolutePath(), target);
                        LfsOperator.move(dir.getAbsolutePath(), target);
                        assertTrue(new File(target + "/tmp-5").exists());
                        assertEquals(3, new File(target + "/tmp-5").listFiles().length);
                    });
            });
    }

    @Test
    void rejectInvalidDigestPaths() {
        assertThrows(IllegalArgumentException.class, () -> LfsOperator.fileSha256(null));
        assertThrows(IllegalArgumentException.class, () -> LfsOperator.fileSha256(""));
        assertThrows(IllegalArgumentException.class, () -> LfsOperator.fileSha256("ttt/144514.dat"));
    }

    @Test
    void computeSha256() throws Exception {
        withTempDir(
            outputDir -> {
                byte[] content = "streampark".getBytes(StandardCharsets.UTF_8);
                Path file = Paths.get(outputDir, "artifact.jar");
                Files.write(file, content);

                assertEquals(DigestUtils.sha256Hex(content), LfsOperator.fileSha256(file.toString()));
            });
    }

    @Test
    void readAndListWithoutFollowingSymbolicLinks() throws Exception {
        withTempDir(
            outputDir -> {
                Path realOutputDir = Paths.get(outputDir).toRealPath();
                Path workspace = Files.createDirectories(realOutputDir.resolve("workspace"));
                Path file = Files.writeString(workspace.resolve("artifact.jar"), "artifact");
                assertThat(LfsOperator.isFile(file.toString())).isTrue();
                try (InputStream input = LfsOperator.open(file.toString())) {
                    assertThat(new String(input.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("artifact");
                }
                assertThat(LfsOperator.list(workspace.toString())).hasSize(1);
                assertThat(LfsOperator.canonicalPath(workspace.toString()))
                    .isEqualTo(workspace.toRealPath().toString());

                Path outside = Files.writeString(realOutputDir.resolve("outside.jar"), "outside");
                Path link = workspace.resolve("link.jar");
                Path linkedDirectory = realOutputDir.resolve("linked-directory");
                try {
                    Files.createSymbolicLink(link, outside);
                    Files.createSymbolicLink(linkedDirectory, workspace);
                } catch (UnsupportedOperationException | IOException e) {
                    Assumptions.assumeTrue(false, "symbolic links are unavailable");
                }
                assertThat(LfsOperator.isFile(link.toString())).isFalse();
                assertThatThrownBy(() -> LfsOperator.open(link.toString())).isInstanceOf(IOException.class);
                assertThatThrownBy(() -> LfsOperator.canonicalPath(link.toString())).isInstanceOf(IOException.class);
                assertThatThrownBy(() -> LfsOperator.canonicalPath(linkedDirectory.resolve("artifact.jar").toString()))
                    .isInstanceOf(IOException.class);
            });
    }
}
