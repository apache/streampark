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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PasswordHashUtilsTest {

  @Test
  void testEncryptUsesPbkdf2() {
    String encodedPassword = PasswordHashUtils.encrypt("streampark");

    Assertions.assertTrue(PasswordHashUtils.isPbkdf2Hash(encodedPassword));
    Assertions.assertNotEquals(
        "2513f3748847298ea324dffbf67fe68681dd92315bda830065facd8efe08f54f", encodedPassword);
    Assertions.assertTrue(
        PasswordHashUtils.matches(
            "streampark", PasswordHashUtils.PASSWORD_SALT_NOT_REQUIRED, encodedPassword));
    Assertions.assertFalse(PasswordHashUtils.needsRehash(encodedPassword));
  }

  @Test
  void testMatchesLegacySha256Hash() {
    String salt = "rh8b1ojwog777yrg0daesf04gk";
    String legacyPassword = "2513f3748847298ea324dffbf67fe68681dd92315bda830065facd8efe08f54f";

    Assertions.assertTrue(PasswordHashUtils.matches("streampark", salt, legacyPassword));
    Assertions.assertTrue(PasswordHashUtils.needsRehash(legacyPassword));
  }

  @Test
  void testRejectsWrongPassword() {
    String encodedPassword = PasswordHashUtils.encrypt("streampark");

    Assertions.assertFalse(
        PasswordHashUtils.matches(
            "wrong-password", PasswordHashUtils.PASSWORD_SALT_NOT_REQUIRED, encodedPassword));
  }

  @Test
  void testNeedsRehashForLowerPbkdf2WorkFactor() {
    String lowerWorkFactorPassword =
        "$sp$pbkdf2-sha256$310000$tNRfIZqKBy9b5/sEnn2wjw==$"
            + "VFKdWwdlVviXJ5mTanNYLeDcCCYk0Dhw5x6XjLC/Hg0=";

    Assertions.assertTrue(
        PasswordHashUtils.matches(
            "streampark", PasswordHashUtils.PASSWORD_SALT_NOT_REQUIRED, lowerWorkFactorPassword));
    Assertions.assertTrue(PasswordHashUtils.needsRehash(lowerWorkFactorPassword));
  }

  @Test
  void testRandomPasswordLength() {
    Assertions.assertEquals(8, PasswordHashUtils.getRandomPassword(8).length());
  }
}
