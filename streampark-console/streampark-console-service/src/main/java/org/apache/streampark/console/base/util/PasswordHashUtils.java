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

package org.apache.streampark.console.base.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.crypto.hash.Sha256Hash;
import org.apache.shiro.util.ByteSource;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.regex.Pattern;

public final class PasswordHashUtils {

  public static final String PASSWORD_SALT_NOT_REQUIRED = "";

  private static final String HASH_PREFIX = "$sp$pbkdf2-sha256$";
  private static final String PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA256";
  private static final int PBKDF2_ITERATIONS = 600000;
  private static final int PBKDF2_SALT_BYTES = 16;
  private static final int PBKDF2_HASH_BITS = 256;
  private static final String RANDOM_CHARS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  private static final Pattern PBKDF2_PATTERN =
      Pattern.compile(
          "\\A\\$sp\\$pbkdf2-sha256\\$\\d+\\$[A-Za-z0-9+/]+={0,2}\\$[A-Za-z0-9+/]+={0,2}\\z");
  private static final Pattern LEGACY_SHA256_PATTERN = Pattern.compile("\\A[0-9a-f]{64}\\z");
  private static final SecureRandom RANDOM = new SecureRandom();

  private PasswordHashUtils() {}

  public static String encrypt(String password) {
    byte[] salt = new byte[PBKDF2_SALT_BYTES];
    RANDOM.nextBytes(salt);
    byte[] hash = pbkdf2(password, salt, PBKDF2_ITERATIONS);
    return HASH_PREFIX
        + PBKDF2_ITERATIONS
        + "$"
        + Base64.getEncoder().encodeToString(salt)
        + "$"
        + Base64.getEncoder().encodeToString(hash);
  }

  public static boolean matches(String rawPassword, String salt, String encodedPassword) {
    if (rawPassword == null || StringUtils.isBlank(encodedPassword)) {
      return false;
    }

    if (isPbkdf2Hash(encodedPassword)) {
      return matchesPbkdf2(rawPassword, encodedPassword);
    }

    return isLegacySha256Hash(encodedPassword)
        && StringUtils.isNotBlank(salt)
        && StringUtils.equals(encrypt(salt, rawPassword), encodedPassword);
  }

  public static boolean needsRehash(String encodedPassword) {
    return !isPbkdf2Hash(encodedPassword)
        || getPbkdf2Iterations(encodedPassword) < PBKDF2_ITERATIONS;
  }

  public static String getRandomPassword(int length) {
    if (length <= 0) {
      throw new IllegalArgumentException("password length must be greater than 0");
    }
    StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append(RANDOM_CHARS.charAt(RANDOM.nextInt(RANDOM_CHARS.length())));
    }
    return builder.toString();
  }

  static boolean isPbkdf2Hash(String encodedPassword) {
    return encodedPassword != null && PBKDF2_PATTERN.matcher(encodedPassword).matches();
  }

  private static boolean isLegacySha256Hash(String encodedPassword) {
    return encodedPassword != null && LEGACY_SHA256_PATTERN.matcher(encodedPassword).matches();
  }

  @Deprecated
  private static String encrypt(String salt, String password) {
    return new Sha256Hash(password, ByteSource.Util.bytes(salt), 1024).toHex();
  }

  private static boolean matchesPbkdf2(String rawPassword, String encodedPassword) {
    String[] parts = encodedPassword.split("\\$");
    if (parts.length != 6) {
      return false;
    }
    int iterations = parseIterations(parts[3]);
    if (iterations <= 0) {
      return false;
    }
    try {
      byte[] salt = Base64.getDecoder().decode(parts[4]);
      byte[] expectedHash = Base64.getDecoder().decode(parts[5]);
      byte[] actualHash = pbkdf2(rawPassword, salt, iterations);
      return MessageDigest.isEqual(expectedHash, actualHash);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static int getPbkdf2Iterations(String encodedPassword) {
    if (!isPbkdf2Hash(encodedPassword)) {
      return -1;
    }
    return parseIterations(encodedPassword.split("\\$")[3]);
  }

  private static int parseIterations(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static byte[] pbkdf2(String password, byte[] salt, int iterations) {
    PBEKeySpec keySpec = null;
    try {
      keySpec = new PBEKeySpec(password.toCharArray(), salt, iterations, PBKDF2_HASH_BITS);
      return SecretKeyFactory.getInstance(PBKDF2_ALGORITHM).generateSecret(keySpec).getEncoded();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to hash password.", e);
    } finally {
      if (keySpec != null) {
        keySpec.clearPassword();
      }
    }
  }
}
