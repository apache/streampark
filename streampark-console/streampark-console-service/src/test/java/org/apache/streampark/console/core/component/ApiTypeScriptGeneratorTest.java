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

package org.apache.streampark.console.core.component;

import org.apache.streampark.console.core.bean.ApiContractDocument;
import org.apache.streampark.console.core.bean.ApiContractDocument.ApiEndpointDescriptor;
import org.apache.streampark.console.core.bean.OpenAPISchema;
import org.apache.streampark.console.core.request.common.IdRequest;
import org.apache.streampark.console.system.request.user.UserCreateRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class ApiTypeScriptGeneratorTest {

    @Test
    void shouldGenerateInterfacesFromContractDocument() {
        ApiContractDocument document = new ApiContractDocument();
        ApiEndpointDescriptor endpoint = new ApiEndpointDescriptor();
        endpoint.setController("UserController");
        endpoint.setHandler("addUser");
        endpoint.setPath("/user/post");
        endpoint.setHttpMethod("POST");
        endpoint.setRequestType("UserCreateRequest");
        endpoint.setResponseDataType("Void");
        document.setEndpoints(Collections.singletonList(endpoint));

        Map<String, List<OpenAPISchema.Schema>> dtoSchemas = new LinkedHashMap<>();
        dtoSchemas.put("UserCreateRequest", RequestDtoSchemaBuilder.build(UserCreateRequest.class, null,
            RequestDtoSchemaBuilder.defaultTypeNames()));
        dtoSchemas.put("IdRequest", RequestDtoSchemaBuilder.build(IdRequest.class, null,
            RequestDtoSchemaBuilder.defaultTypeNames()));
        document.setDtoSchemas(dtoSchemas);

        String typescript = ApiTypeScriptGenerator.generate(document);

        Assertions.assertTrue(typescript.contains("export interface UserCreateRequest"));
        Assertions.assertTrue(typescript.contains("export interface RestResponseBody"));
        Assertions.assertTrue(typescript.contains("export const apiEndpoints"));
    }
}
