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

package org.apache.streampark.console.system.authentication;

import org.apache.streampark.common.util.FileUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Base64;

@Slf4j
public class JWTSecret {

    private static final int KEY_LENGTH = 32;

    public static byte[] getJWTSecret() {
        Path keyPath = Paths.get(System.getProperty("user.home"), "streampark.jwt.key");
        File keyFile = keyPath.toFile();

        // Try to load existing key
        byte[] keyBytes = loadExistingKey(keyFile);
        if (keyBytes != null) {
            return keyBytes;
        }

        // Generate new key
        keyBytes = generateNewKey();
        saveNewKey(keyBytes, keyPath);
        return keyBytes;
    }

    private static byte[] loadExistingKey(File keyFile) {
        if (!keyFile.exists()) {
            return null;
        }

        try {
            String secret = FileUtils.readFile(keyFile).trim();
            byte[] keyBytes = Base64.getDecoder().decode(secret);

            if (keyBytes.length != KEY_LENGTH) {
                log.error("Invalid HMAC key length: {} bytes (expected {} bytes)", keyBytes.length, KEY_LENGTH);
                return null;
            }
            return keyBytes;
        } catch (Exception e) {
            log.error("Failed to read JWT key file", e);
        }
        // Clean up invalid file
        safelyDeleteFile(keyFile);
        return null;
    }

    private static byte[] generateNewKey() {
        byte[] key = new byte[KEY_LENGTH];
        new SecureRandom().nextBytes(key);
        return key;
    }

    private static void saveNewKey(byte[] keyBytes, Path keyPath) {
        String encodedKey = Base64.getEncoder().encodeToString(keyBytes);
        try {
            // Ensure the directory exists
            Files.createDirectories(keyPath.getParent());
            // Safely write to a temporary file before renaming
            Path tempFile = Files.createTempFile(keyPath.getParent(), "streampark", ".tmp");
            Files.write(tempFile, encodedKey.getBytes(StandardCharsets.UTF_8));

            // Atomically move after setting permissions
            setStrictPermissions(tempFile);
            Files.move(tempFile, keyPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        } catch (Exception e) {
            throw new SecurityException("Failed to generate JWT key", e);
        }
    }

    private static void setStrictPermissions(Path path) {
        try {
            Files.setPosixFilePermissions(path,
                PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException e) {
            log.warn("POSIX permissions not supported for {}", path);
        } catch (IOException e) {
            log.error("Failed to set permissions for {}", path, e);
        }
    }

    private static void safelyDeleteFile(File keyFile) {
        try {
            if (keyFile.exists() && !keyFile.delete()) {
                log.warn("Failed to delete invalid key file: {}", keyFile.getAbsolutePath());
            }
        } catch (SecurityException e) {
            log.error("Security exception when deleting key file", e);
        }
    }

}
