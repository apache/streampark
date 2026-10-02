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

package org.apache.streampark.flink.core.lineage;

import org.apache.streampark.common.util.StreamParkLoggerFactory;

import org.apache.streampark.shaded.org.slf4j.Logger;

import java.util.Map;

/**
 * Maps a Flink connector's {@code WITH (...)} options to the OpenLineage dataset identity Gravitino
 * expects — the {@code (namespace, name)} pair must be byte-identical to what other emitters into
 * the same Gravitino graph produce for the same physical table, so these rules are not invented
 * here; they mirror the existing convention already in use for this Gravitino deployment.
 *
 * <p>Fail-open by design: an unknown connector or a connector missing a required option logs a
 * {@code WARN} and returns {@code null} rather than throwing. This runs on the job submission path
 * — a lineage gap must never fail the submission.
 *
 * <p>To support another connector, add a case below with its own {@code (namespace, name)} rule;
 * do not guess a generic fallback.
 */
public final class DatasetIdentityRegistry {

    private static final Logger LOG =
        StreamParkLoggerFactory.loggerFactory().getLogger(DatasetIdentityRegistry.class.getName());

    private DatasetIdentityRegistry() {
    }

    /**
     * Resolves the dataset identity for a table declared via {@code CREATE [TEMPORARY] TABLE ...
     * WITH (...)}. Returns {@code null} (logging why) when the connector is unrecognized or a
     * required option is missing.
     */
    public static LineageDataset resolve(String tableName, Map<String, String> options) {
        String connector = options.get("connector");
        if (connector == null) {
            LOG.warn(
                "[lineage] table `{}` has no 'connector' WITH option, skipping lineage for it",
                tableName);
            return null;
        }
        switch (connector) {
            case "mysql-cdc":
                return resolveMysqlCdc(tableName, options);
            case "doris":
                return resolveDoris(tableName, options);
            default:
                LOG.warn(
                    "[lineage] no dataset-identity rule for connector '{}' on table `{}`, skipping lineage for it",
                    connector,
                    tableName);
                return null;
        }
    }

    private static LineageDataset resolveMysqlCdc(String tableName, Map<String, String> options) {
        String hostname = require(tableName, options, "hostname");
        String port = require(tableName, options, "port");
        String database = require(tableName, options, "database-name");
        String table = require(tableName, options, "table-name");
        if (hostname == null || port == null || database == null || table == null) {
            return null;
        }
        return new LineageDataset("mysql-cdc://" + hostname + ":" + port, database + "." + table);
    }

    private static LineageDataset resolveDoris(String tableName, Map<String, String> options) {
        String fenodes = require(tableName, options, "fenodes");
        String identifier = require(tableName, options, "table.identifier");
        if (fenodes == null || identifier == null) {
            return null;
        }
        return new LineageDataset("doris://" + fenodes, identifier);
    }

    private static String require(String tableName, Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null) {
            LOG.warn(
                "[lineage] table `{}` is missing required WITH option '{}', skipping lineage for it",
                tableName,
                key);
        }
        return value;
    }
}
