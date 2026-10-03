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

package org.apache.streampark.console.base.web;

import org.apache.streampark.console.system.request.user.UserCreateRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.ServletWebRequest;

import javax.validation.Validation;
import javax.validation.Validator;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FormOrJsonArgumentResolverTest {

    private FormOrJsonArgumentResolver resolver;

    @BeforeEach
    void setUp() {
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        resolver = new FormOrJsonArgumentResolver(new ObjectMapper(), validator);
    }

    @Test
    void shouldBindFromJsonBody() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setContentType("application/json");
        request.setContent("{\"username\":\"editor\",\"status\":\"1\",\"sex\":\"2\"}".getBytes());

        org.springframework.core.MethodParameter parameter =
            new org.springframework.core.MethodParameter(FormOrJsonArgumentResolverTest.class.getDeclaredMethod(
                "sample", UserCreateRequest.class), 0);

        Object target = resolver.resolveArgument(
            parameter, null, new ServletWebRequest(request), null);
        UserCreateRequest dto = (UserCreateRequest) target;
        assertEquals("editor", dto.getUsername());
    }

    @Test
    void shouldBindFromFormFields() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setContentType("application/x-www-form-urlencoded");
        request.addParameter("username", "editor");
        request.addParameter("status", "1");
        request.addParameter("sex", "2");

        org.springframework.core.MethodParameter parameter =
            new org.springframework.core.MethodParameter(FormOrJsonArgumentResolverTest.class.getDeclaredMethod(
                "sample", UserCreateRequest.class), 0);

        Object target = resolver.resolveArgument(
            parameter, null, new ServletWebRequest(request), null);
        UserCreateRequest dto = (UserCreateRequest) target;
        assertEquals("editor", dto.getUsername());
    }

    @SuppressWarnings({"java:S1144", "java:S1172"})
    private void sample(@FormOrJson UserCreateRequest request) {
        // referenced reflectively by resolveArgument tests
    }
}
