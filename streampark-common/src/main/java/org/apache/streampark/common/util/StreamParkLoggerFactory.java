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

import org.apache.streampark.shaded.ch.qos.logback.classic.LoggerContext;
import org.apache.streampark.shaded.ch.qos.logback.classic.joran.JoranConfigurator;
import org.apache.streampark.shaded.ch.qos.logback.classic.util.ContextInitializer;
import org.apache.streampark.shaded.ch.qos.logback.classic.util.ContextSelectorStaticBinder;
import org.apache.streampark.shaded.ch.qos.logback.core.CoreConstants;
import org.apache.streampark.shaded.ch.qos.logback.core.LogbackException;
import org.apache.streampark.shaded.ch.qos.logback.core.status.StatusUtil;
import org.apache.streampark.shaded.ch.qos.logback.core.util.StatusPrinter;
import org.apache.streampark.shaded.org.slf4j.ILoggerFactory;
import org.apache.streampark.shaded.org.slf4j.spi.LoggerFactoryBinder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/** Shaded logback SLF4J logger factory binder. */
public final class StreamParkLoggerFactory implements LoggerFactoryBinder {

    private static final StreamParkLoggerFactory INSTANCE = new StreamParkLoggerFactory();

    private static final ContextSelectorStaticBinder CONTEXT_SELECTOR_BINDER;

    static {
        LoggerContext defaultLoggerContext = new LoggerContext();
        try {
            new ShadedContextInitializer(defaultLoggerContext).autoConfig();
        } catch (Exception e) {
            System.err.println("Failed to auto configure default logger context: " + e);
        }
        if (!StatusUtil.contextHasStatusListener(defaultLoggerContext)) {
            StatusPrinter.printInCaseOfErrorsOrWarnings(defaultLoggerContext);
        }
        ContextSelectorStaticBinder selectorBinder = new ContextSelectorStaticBinder();
        try {
            selectorBinder.init(defaultLoggerContext, new Object());
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
        CONTEXT_SELECTOR_BINDER = selectorBinder;
    }

    private StreamParkLoggerFactory() {
    }

    /** Returns the SLF4J logger factory for StreamPark shaded logging. */
    public static ILoggerFactory loggerFactory() {
        return INSTANCE.getLoggerFactory();
    }

    @Override
    public ILoggerFactory getLoggerFactory() {
        if (CONTEXT_SELECTOR_BINDER.getContextSelector() == null) {
            throw new IllegalStateException(
                "'contextSelector' cannot be null. See also "
                    + CoreConstants.CODES_URL
                    + "#null_CS");
        }
        return CONTEXT_SELECTOR_BINDER.getContextSelector().getLoggerContext();
    }

    @Override
    public String getLoggerFactoryClassStr() {
        return CONTEXT_SELECTOR_BINDER.getClass().getName();
    }

    static final class ShadedContextInitializer extends ContextInitializer {

        private static final String SHADED_PACKAGE = "org.apache.streampark.shaded";
        private static final Pattern LOGGING_PACKAGE = Pattern.compile(
            "(?<!org\\.apache\\.streampark\\.shaded\\.)"
                + "(org\\.slf4j|ch\\.qos\\.logback|org\\.apache\\.log4j)(?=\\.)");
        private final LoggerContext loggerContext;

        ShadedContextInitializer(LoggerContext loggerContext) {
            super(loggerContext);
            this.loggerContext = loggerContext;
        }

        @Override
        public void configureByResource(URL url) {
            AssertUtils.notNull(url, "URL argument cannot be null");
            String path = url.getPath();
            if (path.endsWith("xml")) {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(loggerContext);
                try (InputStream resource = url.openStream()) {
                    String text =
                        LOGGING_PACKAGE.matcher(new String(resource.readAllBytes(), StandardCharsets.UTF_8))
                            .replaceAll(SHADED_PACKAGE + ".$1");
                    try (
                        ByteArrayInputStream input =
                            new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8))) {
                        configurator.doConfigure(input);
                    }
                } catch (Exception e) {
                    throw new LogbackException("Failed to configure logger context from " + url, e);
                }
            } else {
                throw new LogbackException(
                    "Unexpected filename extension of file ["
                        + url
                        + "]. Should be .xml");
            }
        }
    }
}
