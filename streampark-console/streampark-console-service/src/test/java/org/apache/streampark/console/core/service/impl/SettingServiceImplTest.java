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

package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.console.core.bean.SenderEmail;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SettingServiceImplTest {

    @Test
    void buildEmailPropertiesUsesSslOnConnectWhenSslEnabled() {
        SenderEmail senderEmail = senderEmail(true);

        Properties properties = SettingServiceImpl.buildEmailProperties(senderEmail);

        assertEquals("true", properties.getProperty("mail.smtp.auth"));
        assertEquals("smtp.126.com", properties.getProperty("mail.smtp.host"));
        assertEquals("994", properties.getProperty("mail.smtp.port"));
        assertEquals("true", properties.getProperty("mail.smtp.ssl.enable"));
        assertNull(properties.getProperty("mail.smtp.starttls.enable"));
    }

    @Test
    void buildEmailPropertiesKeepsPlainSmtpWhenSslDisabled() {
        SenderEmail senderEmail = senderEmail(false);

        Properties properties = SettingServiceImpl.buildEmailProperties(senderEmail);

        assertEquals("true", properties.getProperty("mail.smtp.auth"));
        assertEquals("smtp.126.com", properties.getProperty("mail.smtp.host"));
        assertEquals("994", properties.getProperty("mail.smtp.port"));
        assertNull(properties.getProperty("mail.smtp.ssl.enable"));
        assertNull(properties.getProperty("mail.smtp.starttls.enable"));
    }

    private static SenderEmail senderEmail(boolean ssl) {
        SenderEmail senderEmail = new SenderEmail();
        senderEmail.setHost("smtp.126.com");
        senderEmail.setPort(994);
        senderEmail.setSsl(ssl);
        return senderEmail;
    }
}
