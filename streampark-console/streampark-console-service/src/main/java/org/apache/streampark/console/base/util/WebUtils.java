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

import org.apache.streampark.common.conf.ConfigKeys;
import org.apache.streampark.common.util.SystemPropertyUtils;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.toolkit.StringPool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.stream.IntStream;

/** Web Utils */
@Slf4j
public final class WebUtils {

    private static final String TEMP = "temp";
    private static final String LIB = "lib";
    private static final String PLUGINS = "plugins";
    private static final String CLIENT = "client";
    private static final String CONF = "conf";

    private WebUtils() {
    }

    /**
     * camel to underscore
     *
     * @param value value
     * @return underscore
     */
    public static String camelToUnderscore(String value) {
        if (StringUtils.isBlank(value) || value.contains("_")) {
            return value;
        }
        String[] arr = StringUtils.splitByCharacterTypeCamelCase(value);
        if (arr.length == 0) {
            return value;
        }
        StringBuilder result = new StringBuilder();
        IntStream.range(0, arr.length)
            .forEach(
                i -> {
                    if (i != arr.length - 1) {
                        result.append(arr[i]).append(StringPool.UNDERSCORE);
                    } else {
                        result.append(arr[i]);
                    }
                });
        return StringUtils.lowerCase(result.toString());
    }

    public static String getAppHome() {
        return System.getProperty(ConfigKeys.KEY_APP_HOME());
    }

    public static File getAppDir(String dir) {
        return new File(getAppHome(), dir);
    }

    public static File getAppTempDir() {
        return getAppDir(TEMP);
    }

    public static File getAppLibDir() {
        return getAppDir(LIB);
    }

    public static File getAppClientDir() {
        return getAppDir(CLIENT);
    }

    public static File getPluginDir() {
        return getAppDir(PLUGINS);
    }

}
