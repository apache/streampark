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

package org.apache.streampark.console.core.service;

import org.apache.streampark.common.enums.CatalogType;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.bean.DatabaseParam;
import org.apache.streampark.console.core.bean.FlinkCatalogParams;
import org.apache.streampark.console.core.entity.FlinkCatalog;
import org.apache.streampark.console.core.service.container.MysqlBaseITCASE;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatabaseServiceTest extends MysqlBaseITCASE {

    @Autowired
    private FlinkCatalogService catalogService;

    @Autowired
    private DatabaseService databaseService;

    private FlinkCatalogParams flinkCatalog = new FlinkCatalogParams();

    @BeforeEach
    public void setup() {
        flinkCatalog.setCatalogName("flink-test");
        flinkCatalog.setTeamId(1L);
        flinkCatalog.setUserId(1L);
        flinkCatalog.setCatalogType(CatalogType.JDBC);
        FlinkCatalogParams.FlinkJDBCCatalog flinkJDBCCatalog =
            new FlinkCatalogParams.FlinkJDBCCatalog();
        flinkJDBCCatalog.setBaseUrl(MYSQL_CONTAINER.getJdbcUrl());
        flinkJDBCCatalog.setType(CatalogType.JDBC.name().toLowerCase());
        flinkJDBCCatalog.setDefaultDatabase(MYSQL_CONTAINER.getDatabaseName());
        flinkJDBCCatalog.setUsername(MYSQL_CONTAINER.getUsername());
        flinkJDBCCatalog.setPassword(MYSQL_CONTAINER.getPassword());
        flinkCatalog.setFlinkJDBCCatalog(flinkJDBCCatalog);
        catalogService.create(flinkCatalog, 1L);
        FlinkCatalog fc = catalogService.getCatalog("flink-test");
        flinkCatalog.setId(fc.getId());
    }

    @AfterEach
    void destroy() {
        FlinkCatalog fc = catalogService.getCatalog("flink-test");
        catalogService.remove(flinkCatalog.getId());
    }

    // @Test
    public void testDatabaseExists_Positive() {
        DatabaseParam dbParam = new DatabaseParam();
        dbParam.setCatalogId(flinkCatalog.getId());
        dbParam.setCatalogName(flinkCatalog.getCatalogName());
        dbParam.setName(MYSQL_CONTAINER.getDatabaseName());

        assertTrue(databaseService.databaseExists(dbParam));
    }

    // @Test
    public void testCreateDatabase_Negative() {
        DatabaseParam dbParam = new DatabaseParam();
        dbParam.setCatalogId(flinkCatalog.getId());
        dbParam.setName("new_db");
        dbParam.setIgnoreIfExits(false);

        Exception exception =
            assertThrows(
                UnsupportedOperationException.class,
                () -> {
                    databaseService.createDatabase(dbParam);
                });
        assertNull(exception.getMessage());
    }

    // @Test
    public void testListDatabases_Positive() {
        List<DatabaseParam> databaseParamList = databaseService.listDatabases(flinkCatalog.getId());
        assertNotNull(databaseParamList);
    }

    // @Test
    public void testListDatabases_Negative_NoDatabases() {

        Exception exception =
            assertThrows(
                ApiAlertException.class,
                () -> {
                    databaseService.listDatabases(null);
                });

        assertEquals(
            "The catalog can't be null. get catalog from database failed.", exception.getMessage());
    }

    // @Test
    public void testDropDatabase_Positive() {
        DatabaseParam dbParam = new DatabaseParam();
        dbParam.setCatalogId(flinkCatalog.getId());
        dbParam.setCatalogName(flinkCatalog.getCatalogName());
        dbParam.setName(MYSQL_CONTAINER.getDatabaseName());
        dbParam.setCascade(true);
        dbParam.setIgnoreIfExits(true);
        Exception exception =
            assertThrows(
                UnsupportedOperationException.class,
                () -> {
                    databaseService.dropDatabase(dbParam);
                });
        assertEquals(null, exception.getMessage());
    }

    // @Test
    public void testDropDatabase_Negative_NullDatabaseName() {
        DatabaseParam dbParam = new DatabaseParam();
        dbParam.setCatalogId(1L);
        dbParam.setName(null);

        Exception exception =
            assertThrows(
                ApiAlertException.class,
                () -> {
                    databaseService.dropDatabase(dbParam);
                });

        assertEquals("Database name can not be null.", exception.getMessage());
    }
}
