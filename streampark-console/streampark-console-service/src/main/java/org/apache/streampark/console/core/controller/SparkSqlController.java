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

package org.apache.streampark.console.core.controller;

import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponseBody;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.base.web.FormOrJson;
import org.apache.streampark.console.core.assembler.SparkSqlAssembler;
import org.apache.streampark.console.core.entity.SparkSql;
import org.apache.streampark.console.core.request.spark.SparkSqlCompleteRequest;
import org.apache.streampark.console.core.request.spark.SparkSqlDeleteRequest;
import org.apache.streampark.console.core.request.spark.SparkSqlGetRequest;
import org.apache.streampark.console.core.request.spark.SparkSqlHistoryRequest;
import org.apache.streampark.console.core.request.spark.SparkSqlListQueryRequest;
import org.apache.streampark.console.core.request.spark.SparkSqlVerifyRequest;
import org.apache.streampark.console.core.response.spark.SparkSqlResponse;
import org.apache.streampark.console.core.response.sql.SqlCompleteResponse;
import org.apache.streampark.console.core.service.SparkSqlService;
import org.apache.streampark.console.core.service.SqlCompleteService;
import org.apache.streampark.console.core.service.VariableService;
import org.apache.streampark.spark.core.util.SparkSqlValidationResult;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequestMapping("spark/sql")
public class SparkSqlController {

    public static final String TYPE = "type";
    public static final String START = "start";
    public static final String END = "end";

    @Autowired
    private SparkSqlService sparkSqlService;

    @Autowired
    private VariableService variableService;

    @Autowired
    private SqlCompleteService sqlComplete;

    @PostMapping("verify")
    public RestResponseBody<Boolean> verify(@Valid SparkSqlVerifyRequest request) {
        String sql = variableService.replaceVariable(request.getTeamId(), request.getSql());
        SparkSqlValidationResult sparkSqlValidationResult = sparkSqlService.verifySql(sql, request.getVersionId());
        if (!sparkSqlValidationResult.success()) {
            String exception = sparkSqlValidationResult.exception();
            RestResponseBody<Boolean> response = RestResponseBody.success(false).message(exception);
            response.extra(TYPE, sparkSqlValidationResult.failedType().getFailedType());
            response.extra(START, sparkSqlValidationResult.lineStart());
            response.extra(END, sparkSqlValidationResult.lineEnd());
            if (sparkSqlValidationResult.errorLine() > 0) {
                response.extra(START, sparkSqlValidationResult.errorLine());
                response.extra(END, sparkSqlValidationResult.errorLine() + 1);
            }
            return response;
        }
        return RestResponseBody.success(true);
    }

    @PostMapping("list")
    public RestResponseBody<IPage<SparkSqlResponse>> list(@Valid SparkSqlListQueryRequest request,
                                                          RestRequest restRequest) {
        IPage<SparkSql> page = sparkSqlService.getPage(request.getAppId(), restRequest);
        return RestResponseBody.success(SparkSqlAssembler.toPageResponse(page));
    }

    @PostMapping("delete")
    @RequiresPermissions("sql:delete")
    public RestResponseBody<Boolean> delete(@Valid @FormOrJson SparkSqlDeleteRequest request) {
        SparkSql sparkSql = SparkSqlAssembler.toDeleteEntity(request);
        ApiAlertException.throwIfNull(sparkSql, "Spark SQL delete request cannot be null.");
        Boolean deleted = sparkSqlService.removeById(sparkSql.getSql());
        return RestResponseBody.success(deleted);
    }

    /** {@code data} is {@link SparkSqlResponse} for one id, or {@link SparkSqlResponse}{@code []} for two ids (legacy compare). */
    @SuppressWarnings("java:S1452")
    @PostMapping("get")
    public RestResponseBody<?> get(@Valid SparkSqlGetRequest request) throws InternalException {
        ApiAlertException.throwIfTrue(
            request.getAppId() == null || request.getTeamId() == null,
            "Permission denied, appId and teamId cannot be null");
        String[] array = request.getId().split(",");
        SparkSql sparkSql1 = sparkSqlService.getById(array[0]);
        ApiAlertException.throwIfNull(sparkSql1, "Spark SQL not found.");
        sparkSql1.base64Encode();
        if (array.length == 1) {
            return RestResponseBody.success(SparkSqlAssembler.toResponse(sparkSql1));
        }
        SparkSql sparkSql2 = sparkSqlService.getById(array[1]);
        ApiAlertException.throwIfNull(sparkSql2, "Spark SQL not found.");
        sparkSql2.base64Encode();
        return RestResponseBody.success(SparkSqlAssembler.toResponseArray(new SparkSql[]{sparkSql1, sparkSql2}));
    }

    @PostMapping("history")
    public RestResponseBody<List<SparkSqlResponse>> history(@Valid SparkSqlHistoryRequest request) {
        List<SparkSql> sqlList = sparkSqlService.listSparkSqlHistory(request.getId());
        return RestResponseBody.success(SparkSqlAssembler.toListResponse(sqlList));
    }

    @PostMapping("sqlComplete")
    public RestResponseBody<SqlCompleteResponse> getSqlComplete(@Valid SparkSqlCompleteRequest request) {
        SqlCompleteResponse response = new SqlCompleteResponse();
        response.setWord(sqlComplete.getComplete(request.getSql()));
        return RestResponseBody.success(response);
    }
}
