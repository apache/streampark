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

package org.apache.streampark.common.util;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class DeflaterUtils {

    private static final int BUFFER_SIZE = 256;
    private static final int MAX_NESTED_COMPRESSION_DEPTH = 16;

    private DeflaterUtils() {
    }

    /** Compresses UTF-8 text and returns its Base64 representation. */
    public static String zipString(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        try {
            deflater.setInput(text.getBytes(StandardCharsets.UTF_8));
            deflater.finish();
            byte[] buffer = new byte[BUFFER_SIZE];
            ByteArrayOutputStream output = new ByteArrayOutputStream(BUFFER_SIZE);
            while (!deflater.finished()) {
                int length = deflater.deflate(buffer);
                output.write(buffer, 0, length);
            }
            return Base64.getEncoder().encodeToString(output.toByteArray());
        } finally {
            deflater.end();
        }
    }

    /** Decompresses Base64-encoded UTF-8 text, or returns {@code null} for invalid input. */
    public static String unzipString(String compressedText) {
        if (compressedText == null) {
            return null;
        }
        if (compressedText.isEmpty()) {
            return "";
        }

        byte[] compressedBytes;
        try {
            compressedBytes = Base64.getDecoder().decode(compressedText);
        } catch (IllegalArgumentException e) {
            LOG.warn("Failed to decode base64 string: {}", e.getMessage());
            return null;
        }
        Inflater inflater = new Inflater();
        inflater.setInput(compressedBytes);
        byte[] buffer = new byte[BUFFER_SIZE];
        ByteArrayOutputStream output = new ByteArrayOutputStream(BUFFER_SIZE);
        try {
            while (!inflater.finished()) {
                int length = inflater.inflate(buffer);
                if (length > 0) {
                    output.write(buffer, 0, length);
                    continue;
                }
                if (inflater.finished()) {
                    break;
                }
                if (inflater.needsInput() || inflater.needsDictionary()) {
                    return null;
                }
                // Inflater made no progress and cannot request more input. Treat it as corrupt.
                return null;
            }
        } catch (DataFormatException e) {
            return null;
        } finally {
            inflater.end();
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Returns plain text while accepting legacy values that were stored without compression or
     * compressed more than once.
     */
    public static String toPlainText(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String current = text;
        for (int depth = 0; depth < MAX_NESTED_COMPRESSION_DEPTH; depth++) {
            String decoded = unzipString(current);
            if (decoded == null || decoded.equals(current)) {
                return current;
            }
            current = decoded;
        }
        return current;
    }

    /** Normalizes plain or legacy nested-compressed text to one compressed representation. */
    public static String compressForStorage(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        return zipString(toPlainText(text));
    }
}
