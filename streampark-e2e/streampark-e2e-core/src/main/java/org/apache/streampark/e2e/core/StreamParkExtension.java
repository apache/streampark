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

package org.apache.streampark.e2e.core;

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.BrowserWebDriverContainer;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testcontainers.containers.BrowserWebDriverContainer.VncRecordingMode.RECORD_ALL;
import static org.testcontainers.containers.VncRecordingContainer.VncRecordingFormat.MP4;

@Slf4j
final class StreamParkExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {

    private final boolean LOCAL_MODE = Objects.equals(System.getProperty("local"), "true");

    private final boolean M1_CHIP_FLAG = Objects.equals(System.getProperty("m1_chip"), "true");

    private final boolean LOCAL_BROWSER = Objects.equals(System.getProperty("local_browser"), "true");

    private final int LOCAL_PORT = 10001;

    private final int DOCKER_PORT = 10000;

    private RemoteWebDriver driver;
    private ComposeContainer compose;
    private BrowserWebDriverContainer<?> browser;
    private HostAndPort address;
    private String rootPath;

    private Path record;

    private final String serviceName = "streampark";

    @Override
    @SuppressWarnings("UnstableApiUsage")
    public void beforeAll(ExtensionContext context) throws IOException {
        Awaitility.setDefaultTimeout(Duration.ofSeconds(120));
        Awaitility.setDefaultPollInterval(Duration.ofSeconds(2));

        setRecordPath();

        if (LOCAL_MODE) {
            runInLocal();
        } else {
            runInDockerContainer(context);
        }

        if (LOCAL_BROWSER) {
            driver = new ChromeDriver(new ChromeOptions());
        } else {
            setBrowserContainerByOsName();

            if (compose != null) {
                Testcontainers.exposeHostPorts(compose.getServicePort(serviceName, DOCKER_PORT));
                browser.withAccessToHost(true);
            }
            browser.start();

            driver = new RemoteWebDriver(browser.getSeleniumAddress(), new ChromeOptions());
        }

        driver
            .manage()
            .timeouts()
            .implicitlyWait(Duration.ofSeconds(2))
            .pageLoadTimeout(Duration.ofSeconds(5));
        driver.manage().window().maximize();

        driver.get(new URL("http", address.getHost(), address.getPort(), rootPath).toString());

        if (!LOCAL_BROWSER) {
            browser.beforeTest(new TestDescription(context));
        }

        final Class<?> clazz = context.getRequiredTestClass();
        Stream.of(clazz.getDeclaredFields())
            .filter(it -> Modifier.isStatic(it.getModifiers()))
            .filter(f -> WebDriver.class.isAssignableFrom(f.getType()))
            .forEach(it -> setDriver(clazz, it));
    }

    private void runInLocal() {
        Testcontainers.exposeHostPorts(LOCAL_PORT);
        address = HostAndPort.fromParts(browserHost(), LOCAL_PORT);
        rootPath = "/";
    }

    private void runInDockerContainer(ExtensionContext context) {
        compose = createDockerCompose(context);
        compose.start();

        address = HostAndPort.fromParts(
            browserHost(), compose.getServicePort(serviceName, DOCKER_PORT));
        rootPath = "/";
    }

    private String browserHost() {
        return LOCAL_BROWSER ? "localhost" : "host.testcontainers.internal";
    }

    private void setBrowserContainerByOsName() {
        DockerImageName imageName;

        if (M1_CHIP_FLAG) {
            imageName = DockerImageName.parse("seleniarm/standalone-chromium:124.0-chromedriver-124.0")
                .asCompatibleSubstituteFor("selenium/standalone-chrome");

            browser = new BrowserWebDriverContainer<>(imageName)
                .withCapabilities(new ChromeOptions())
                .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
                .withFileSystemBind(
                    Constants.HOST_CHROME_DOWNLOAD_PATH.toFile().getAbsolutePath(),
                    Constants.SELENIUM_CONTAINER_CHROME_DOWNLOAD_PATH)
                .withRecordingMode(RECORD_ALL, record.toFile(), MP4)
                .withStartupTimeout(Duration.ofSeconds(300));
        } else {
            browser = new BrowserWebDriverContainer<>()
                .withCapabilities(new ChromeOptions())
                .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
                .withFileSystemBind(
                    Constants.HOST_CHROME_DOWNLOAD_PATH.toFile().getAbsolutePath(),
                    Constants.SELENIUM_CONTAINER_CHROME_DOWNLOAD_PATH)
                .withRecordingMode(RECORD_ALL, record.toFile(), MP4)
                .withStartupTimeout(Duration.ofSeconds(300));
        }
    }

    private void setRecordPath() throws IOException {
        if (!Strings.isNullOrEmpty(System.getenv("RECORDING_PATH"))) {
            record = Paths.get(System.getenv("RECORDING_PATH"));
            if (!record.toFile().exists()) {
                if (!record.toFile().mkdir()) {
                    throw new IOException("Failed to create recording directory: " + record.toAbsolutePath());
                }
            }
        } else {
            record = Files.createTempDirectory("record-");
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (driver != null) {
            driver.quit();
        }
        if (!LOCAL_BROWSER && browser != null) {
            browser.afterTest(new TestDescription(context), Optional.empty());
            browser.stop();
        }
        if (compose != null) {
            compose.stop();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        final Object instance = context.getRequiredTestInstance();
        Stream.of(instance.getClass().getDeclaredFields())
            .filter(f -> WebDriver.class.isAssignableFrom(f.getType()))
            .forEach(it -> setDriver(instance, it));
    }

    private void setDriver(Object object, Field field) {
        try {
            field.setAccessible(true);
            field.set(object, driver);
        } catch (IllegalAccessException e) {
            log.error("Failed to inject web driver to field: {}", field.getName(), e);
        }
    }

    private ComposeContainer createDockerCompose(ExtensionContext context) {
        final Class<?> clazz = context.getRequiredTestClass();
        final StreamPark annotation = clazz.getAnnotation(StreamPark.class);
        final List<File> files = Stream.of(annotation.composeFiles())
            .map(it -> StreamPark.class.getClassLoader().getResource(it))
            .filter(Objects::nonNull)
            .map(URL::getPath)
            .map(File::new)
            .collect(Collectors.toList());

        ComposeContainer compose = new ComposeContainer(files)
            .withPull(true)
            .withTailChildContainers(true)
            .withLocalCompose(true)
            .withExposedService(
                serviceName,
                DOCKER_PORT,
                Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(300)))
            .withLogConsumer(serviceName, outputFrame -> log.info(outputFrame.getUtf8String()))
            .waitingFor(
                serviceName, Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(300)));

        return compose;
    }
}
