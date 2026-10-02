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

package org.apache.streampark.console.base.config;

import org.apache.streampark.common.configuration.ConfigSource;
import org.apache.streampark.common.configuration.Configuration;
import org.apache.streampark.common.configuration.ConfigurationLoader;
import org.apache.streampark.common.configuration.ConfigurationParser;
import org.apache.streampark.common.configuration.GlobalConfiguration;
import org.apache.streampark.common.configuration.option.CoreOptions;
import org.apache.streampark.console.base.util.WebUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Loads the console configuration file and derives Spring Boot properties.
 *
 * <p>The method also installs the initial StreamPark configuration snapshot. It does not copy
 * application values into JVM system properties: Spring and StreamPark receive the same source
 * values, while each framework retains its own explicit precedence rules.
 */
public final class SpringProperties {

    /** Returns Spring Boot defaults combined with the external StreamPark configuration file. */
    public static Properties get() {
        Properties springConfig = getSpringConfig();
        Configuration userConfiguration = getUserConfig();
        Properties userConfig = new Properties();
        userConfig.putAll(userConfiguration.toMap());
        mergeConfig(userConfig, springConfig);
        dataSourceConfig(userConfig, springConfig);

        Map<String, Object> defaults = new LinkedHashMap<>();
        springConfig.forEach((key, value) -> defaults.put(key.toString(), value));
        Configuration configuration = new ConfigurationLoader()
            .add(ConfigSource.DEFAULTS, "console defaults", defaults)
            .add(ConfigSource.FILE, "conf/config.yaml", userConfiguration.toMap())
            .addSystemProperties()
            .load();
        GlobalConfiguration.update(configuration);
        return springConfig;
    }

    private static void dataSourceConfig(Properties userConfig, Properties springConfig) {
        String dialect = userConfig.getProperty("datasource.dialect", "");
        if (StringUtils.isBlank(dialect)) {
            throw new ExceptionInInitializerError(
                "datasource.dialect is required, please check config.yaml");
        }
        switch (dialect.toLowerCase()) {
            case "mysql":
                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    springConfig.put("spring.datasource.driver-class-name", "com.mysql.cj.jdbc.Driver");
                } catch (ClassNotFoundException e) {
                    try {
                        Class.forName("com.mysql.jdbc.Driver");
                        springConfig.put("spring.datasource.driver-class-name", "com.mysql.jdbc.Driver");
                    } catch (ClassNotFoundException e1) {
                        throw new ExceptionInInitializerError(
                            "datasource.dialect is mysql, \"com.mysql.cj.jdbc.Driver\" and \"com.mysql.jdbc.Driver\" classes not found, Please ensure that the MySQL Connector/J can be found under $streampark/lib,\n"
                                + "Notice: The MySQL Connector/J is incompatible with the Apache 2.0 license, You need to download and put it into $streampark/lib");
                    }
                }
                springConfig.put("spring.sql.init.schema-locations", "classpath:db/schema-mysql.sql");
                springConfig.put("spring.sql.init.data-locations", "classpath:db/data-mysql.sql");
                springConfig.put("spring.sql.init.continue-on-error", "true");
                springConfig.put("spring.sql.init.mode", "always");
                break;
            case "postgresql":
            case "pgsql":
                springConfig.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
                springConfig.put("spring.sql.init.schema-locations", "classpath:db/schema-pgsql.sql");
                springConfig.put("spring.sql.init.data-locations", "classpath:db/data-pgsql.sql");
                springConfig.put("spring.sql.init.continue-on-error", "true");
                springConfig.put("spring.sql.init.mode", "always");
                break;
            case "h2":
                String h2DataDir = userConfig.getProperty("datasource.h2-data-dir", null);
                if (StringUtils.isBlank(h2DataDir)) {
                    h2DataDir = System.getProperty("user.home", "~") + "/streampark/h2-data/metadata";
                } else {
                    h2DataDir += h2DataDir.endsWith("/") ? "metadata" : "/metadata";
                }

                springConfig.put(
                    "spring.datasource.url",
                    String.format(
                        "jdbc:h2:file:%s;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=true;INIT=runscript from 'classpath:db/schema-h2.sql'",
                        h2DataDir));

                String userName = userConfig.getProperty("datasource.username", "admin");
                String password = userConfig.getProperty("datasource.password", "streampark");

                springConfig.put("spring.datasource.driver-class-name", "org.h2.Driver");
                springConfig.put("spring.datasource.username", userName);
                springConfig.put("spring.datasource.password", password);
                springConfig.put("spring.sql.init.data-locations", "classpath:db/data-h2.sql");
                springConfig.put("spring.sql.init.continue-on-error", "true");
                springConfig.put("spring.sql.init.username", userName);
                springConfig.put("spring.sql.init.password", password);
                springConfig.put("spring.sql.init.mode", "always");

                // h2
                springConfig.put("spring.h2.console.path", "/h2-console");
                springConfig.put("spring.h2.console.enabled", true);
                springConfig.put("spring.h2.console.settings.web-allow-others", true);
                springConfig.put("spring.h2.console.settings.trace", true);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported datasource dialect: " + dialect);
        }
    }

    private static void mergeConfig(Properties userConfig, Properties springConfig) {
        Map<String, String> configMapping = new HashMap<>();
        configMapping.put("datasource.username", "spring.datasource.username");
        configMapping.put("datasource.password", "spring.datasource.password");
        configMapping.put("datasource.url", "spring.datasource.url");

        userConfig.forEach(
            (k, v) -> {
                String key = configMapping.get(k);
                if (key != null) {
                    springConfig.put(key, v);
                } else {
                    springConfig.put(k, v);
                }
            });
    }

    private static Configuration getUserConfig() {
        String appHome = WebUtils.getAppHome();
        if (StringUtils.isBlank(appHome)) {
            throw new ExceptionInInitializerError(
                String.format(
                    "[StreamPark] The system initialization check failed. If started local for development and debugging,"
                        + " please ensure the -D%s parameter is clearly specified,"
                        + " more detail: https://streampark.apache.org/docs/development/development/",
                    CoreOptions.APP_HOME.key()));
        }
        File file = new File(appHome, "conf/config.yaml");
        if (file.exists() && file.isFile()) {
            return ConfigurationParser.parse(Path.of(file.getAbsolutePath()));
        } else {
            throw new ExceptionInInitializerError(file.getAbsolutePath() + " not found, please check.");
        }
    }

    private static Properties getSpringConfig() {
        Properties config = new Properties();
        // basic
        config.put("spring.application.name", "Apache StreamPark");
        config.put("spring.main.banner-mode", "false");
        config.put("spring.devtools.restart.enabled", "false");
        config.put("spring.aop.proxy-target-class", "true");
        config.put("spring.messages.encoding", "utf-8");
        config.put("spring.main.allow-circular-references", "true");
        config.put("spring.mvc.converters.preferred-json-mapper", "jackson");

        // jackson
        config.put("spring.jackson.date-format", "yyyy-MM-dd HH:mm:ss");
        config.put("spring.jackson.time-zone", "GMT+8");
        config.put("spring.jackson.deserialization.fail-on-unknown-properties", "false");

        // multipart
        config.put("spring.servlet.multipart.enabled", "true");
        config.put("spring.servlet.multipart.resolve-lazily", "true");
        config.put("spring.servlet.multipart.max-file-size", "-1");
        config.put("spring.servlet.multipart.max-request-size", "-1");

        // metrics
        config.put("management.health.ldap.enabled", "false");
        return config;
    }

    private SpringProperties() {
    }
}
