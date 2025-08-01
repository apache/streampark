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
import org.apache.streampark.console.core.enums.AuthenticationType;
import org.apache.streampark.console.system.entity.User;

import org.apache.commons.lang3.StringUtils;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Date;
import java.util.regex.Pattern;

@Slf4j
public class JWTUtil {

    private static Long ttlOfSecond;

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 128;
    private static final int GCM_IV_LENGTH = 12;

    private static final String JWT_USERID = "userId";
    private static final String JWT_USERNAME = "userName";
    private static final String JWT_TYPE = "type";
    private static final String JWT_TIMESTAMP = "timestamp";

    private static byte[] JWT_KEY = loadSigningKey(); // 用于 HMAC256

    private static byte[] loadSigningKey() {
        String userHome = System.getProperty("user.home");
        File keyFile = new File(userHome, "streampark.jwt.key");
        String secret = null;
        if (keyFile.exists()) {
            try {
                secret = FileUtils.readFile(keyFile).trim();
            } catch (Exception e) {
                log.error("Failed to read JWT key file", e);
            }
        }

        if (StringUtils.isEmpty(secret)) {
            throw new ExceptionInInitializerError("JWT secret initialization failed.");
        }
        try {
            byte[] key = Base64.getDecoder().decode(secret);
            if (key.length != 32) {
                throw new SecurityException("HMAC key must be 32 bytes");
            }
            return key;
        } catch (Exception e) {
            throw new SecurityException("Invalid JWT secret format", e);
        }
    }

    /** get username from token */
    public static String getUserName(String token) {
        DecodedJWT jwt = decode(token);
        return jwt != null ? jwt.getClaim(JWT_USERNAME).asString() : null;
    }

    public static Long getUserId(String token) {
        DecodedJWT jwt = decode(token);
        return jwt != null ? jwt.getClaim(JWT_USERID).asLong() : null;
    }

    /**
     * @param token
     * @return
     */
    public static Long getTimestamp(String token) {
        DecodedJWT jwt = decode(token);
        return jwt != null ? jwt.getClaim(JWT_TIMESTAMP).asLong() : 0L;
    }

    /**
     * @param token
     * @return
     */
    public static AuthenticationType getAuthType(String token) {
        DecodedJWT jwt = decode(token);
        if (jwt == null) {
            return null;
        }
        int type = jwt.getClaim(JWT_TYPE).asInt();
        return AuthenticationType.of(type);
    }

    /**
     * @param user
     * @param authType
     * @return
     * @throws Exception
     */
    public static String sign(User user, AuthenticationType authType) throws Exception {
        long second = getTTLOfSecond() * 1000;
        Long ttl = System.currentTimeMillis() + second;
        return sign(user, authType, ttl);
    }

    /**
     * @param user
     * @param authType
     * @param expireTime
     * @return
     * @throws Exception
     */
    public static String sign(User user, AuthenticationType authType, Long expireTime) throws Exception {
        Date date = new Date(expireTime);
        Algorithm algorithm = Algorithm.HMAC256(JWT_KEY);

        JWTCreator.Builder builder =
            JWT.create()
                .withClaim(JWT_USERID, user.getUserId())
                .withClaim(JWT_USERNAME, user.getUsername())
                .withClaim(JWT_TYPE, authType.get())
                .withExpiresAt(date);

        if (authType == AuthenticationType.SIGN) {
            builder.withClaim(JWT_TIMESTAMP, System.currentTimeMillis());
        }

        String token = builder.sign(algorithm);
        return encrypt(token);
    }

    public static Long getTTLOfSecond() {
        if (ttlOfSecond == null) {
            String ttl = System.getProperty("server.session.ttl", "24h").trim();
            String regexp = "^\\d+([smhd])$";
            Pattern pattern = Pattern.compile(regexp);
            if (!pattern.matcher(ttl).matches()) {
                throw new IllegalArgumentException(
                    "server.session.ttl is invalid, Time units must be [s|m|h|d], e.g: 24h, 2d... please check config.yaml ");
            }
            String unit = ttl.substring(ttl.length() - 1);
            String time = ttl.substring(0, ttl.length() - 1);
            long second = Long.parseLong(time);
            switch (unit) {
                case "m":
                    return ttlOfSecond = second * 60;
                case "h":
                    return ttlOfSecond = second * 60 * 60;
                case "d":
                    return ttlOfSecond = second * 24 * 60 * 60;
                default:
                    return ttlOfSecond = second;
            }
        }
        return ttlOfSecond;
    }

    private static DecodedJWT decode(String token) {
        try {
            Algorithm algorithm = Algorithm.HMAC256(JWT_KEY);
            JWTVerifier verifier = JWT.require(algorithm).build();
            return verifier.verify(token);
        } catch (Exception e) {
            return null;
        }
    }

    public static boolean verify(String token) {
        try {
            // Base64解码签名密钥
            Algorithm algorithm = Algorithm.HMAC256(JWT_KEY);
            JWTVerifier verifier = JWT.require(algorithm).build();
            verifier.verify(decrypt(token));
            return true;
        } catch (Exception e) {
            log.warn("Invalid JWT: {}", e.getMessage());
            return false;
        }
    }

    // 修复加密方法
    public static String encrypt(String content) throws Exception {
        // 生成随机IV
        byte[] iv = new byte[GCM_IV_LENGTH];
        SecureRandom.getInstanceStrong().nextBytes(iv);

        SecretKeySpec keySpec = new SecretKeySpec(JWT_KEY, "AES");

        // 初始化加密器
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH, iv));

        // 加密数据
        byte[] encrypted = cipher.doFinal(content.getBytes(StandardCharsets.UTF_8));

        // 组合IV + 密文
        ByteBuffer buffer = ByteBuffer.allocate(iv.length + encrypted.length);
        buffer.put(iv);
        buffer.put(encrypted);

        return Base64.getEncoder().encodeToString(buffer.array());
    }

    public static String decrypt(String content) throws Exception {
        byte[] data = Base64.getDecoder().decode(content);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] iv = new byte[GCM_IV_LENGTH];
        buffer.get(iv);
        byte[] encrypted = new byte[buffer.remaining()];
        buffer.get(encrypted);

        SecretKeySpec keySpec = new SecretKeySpec(JWT_KEY, "AES");

        Cipher cipher = Cipher.getInstance(ALGORITHM);
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, spec);

        return new String(cipher.doFinal(encrypted), StandardCharsets.UTF_8);
    }

}
