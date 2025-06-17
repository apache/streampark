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

import org.apache.streampark.console.core.bean.DockerConfig;
import org.apache.streampark.console.core.bean.MavenConfig;
import org.apache.streampark.console.core.bean.ResponseResult;
import org.apache.streampark.console.core.bean.SenderEmail;
import org.apache.streampark.console.core.entity.Setting;
import org.apache.streampark.console.core.enums.EngineTypeEnum;
import org.apache.streampark.console.core.mapper.SettingMapper;
import org.apache.streampark.console.core.service.SettingService;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.AuthConfig;
import com.github.dockerjava.api.model.AuthResponse;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class SettingServiceImpl extends ServiceImpl<SettingMapper, Setting>
    implements
        SettingService {

    private final Setting emptySetting = new Setting();

    @PostConstruct
    public void loadSettings() {
        list().forEach(x -> SETTINGS.put(x.getSettingKey(), x));
    }

    @Override
    public Setting get(String key) {
        return this.lambdaQuery().eq(Setting::getSettingKey, key).one();
    }

    @Override
    public boolean update(Setting setting) {
        try {
            String value = StringUtils.trimToNull(setting.getSettingValue());
            this.lambdaUpdate().eq(Setting::getSettingKey, setting.getSettingKey())
                .set(Setting::getSettingValue, value)
                .update();

            Optional<Setting> optional = Optional.ofNullable(SETTINGS.get(setting.getSettingKey()));
            optional.ifPresent(x -> x.setSettingValue(value));

            getMavenConfig().updateConfig();

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public MavenConfig getMavenConfig() {
        return MavenConfig.fromSetting();
    }

    @Override
    public DockerConfig getDockerConfig() {
        List<Setting> settingList = baseMapper.querySettingByKeys(Arrays.asList(
            SettingService.KEY_DOCKER_REGISTER_ADDRESS,
            SettingService.KEY_DOCKER_REGISTER_USER,
            SettingService.KEY_DOCKER_REGISTER_PASSWORD,
            SettingService.KEY_DOCKER_REGISTER_NAMESPACE));
        DockerConfig dockerConfig = new DockerConfig();
        settingList.forEach(setting -> {
            switch (setting.getSettingKey()) {
                case SettingService.KEY_DOCKER_REGISTER_ADDRESS:
                    dockerConfig.setAddress(setting.getSettingValue());
                    break;
                case SettingService.KEY_DOCKER_REGISTER_USER:
                    dockerConfig.setUsername(setting.getSettingValue());
                    break;
                case SettingService.KEY_DOCKER_REGISTER_PASSWORD:
                    dockerConfig.setPassword(setting.getSettingValue());
                    break;
                case SettingService.KEY_DOCKER_REGISTER_NAMESPACE:
                    dockerConfig.setNamespace(setting.getSettingValue());
                    break;
                default:
                    break;
            }
        });
        return dockerConfig;
    }

    @Override
    public String getStreamParkAddress() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_STREAMPARK_ADDRESS, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getIngressModeDefault() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_INGRESS_MODE_DEFAULT, emptySetting)
            .getSettingValue();
    }

    @Override
    public EngineTypeEnum getEngine() {
        return EngineTypeEnum.valueOf(SETTINGS
            .getOrDefault(SettingService.KEY_DEFAULT_ENGINE, emptySetting)
            .getSettingValue().toUpperCase());
    }

    @Override
    public ResponseResult checkDocker(DockerConfig dockerConfig) {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
            .withRegistryUrl(dockerConfig.getAddress())
            .build();

        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder().dockerHost(config.getDockerHost()).build();

        ResponseResult result = new ResponseResult();

        try (DockerClient dockerClient = DockerClientImpl.getInstance(config, httpClient)) {
            AuthConfig authConfig = new AuthConfig()
                .withUsername(dockerConfig.getUsername())
                .withPassword(dockerConfig.getPassword())
                .withRegistryAddress(dockerConfig.getAddress());
            AuthResponse response = dockerClient.authCmd().withAuthConfig(authConfig).exec();
            if (response.getStatus().equals("Login Succeeded")) {
                result.setStatus(200);
            } else {
                result.setStatus(500);
                result.setMsg("docker login failed, status: " + response.getStatus());
            }
        } catch (Exception e) {
            if (e.getMessage().contains("LastErrorException")) {
                result.setStatus(400);
            } else if (e.getMessage().contains("Status 401")) {
                result.setStatus(500);
                result.setMsg(
                    "Failed to validate Docker registry, unauthorized: incorrect username or password ");
            } else {
                result.setStatus(500);
                result.setMsg("Failed to validate Docker registry, error: " + e.getMessage());
            }
            log.warn("Failed to validate Docker registry, error:", e);
        }
        return result;
    }

    @Override
    public boolean updateDocker(DockerConfig dockerConfig) {
        List<Setting> settings = DockerConfig.toSettings(dockerConfig);
        for (Setting each : settings) {
            if (!update(each)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public SenderEmail getSenderEmail() {
        try {
            String host = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_HOST).getSettingValue();
            String port = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_PORT).getSettingValue();
            String from = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_FROM).getSettingValue();
            String userName = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_USERNAME).getSettingValue();
            String password = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_PASSWORD).getSettingValue();
            String ssl = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_SSL).getSettingValue();

            SenderEmail senderEmail = new SenderEmail();
            senderEmail.setHost(host);
            if (StringUtils.isNotBlank(port)) {
                senderEmail.setPort(Integer.parseInt(port));
            }
            senderEmail.setFrom(from);
            senderEmail.setUserName(userName);
            senderEmail.setPassword(password);
            if (StringUtils.isNotBlank(ssl)) {
                senderEmail.setSsl(Boolean.parseBoolean(ssl));
            }
            return senderEmail;
        } catch (Exception e) {
            log.error("Fault Alert Email is not set.");
        }
        return null;
    }

    @Override
    public ResponseResult checkEmail(SenderEmail senderEmail) {
        ResponseResult result = new ResponseResult();
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        if (senderEmail.isSsl()) {
            props.put("mail.smtp.starttls.enable", "true");
        }
        props.put("mail.smtp.host", senderEmail.getHost());
        props.put("mail.smtp.port", senderEmail.getPort());

        Session session = Session.getInstance(props);
        try {
            Transport transport = session.getTransport("smtp");
            transport.connect(
                senderEmail.getHost(), senderEmail.getUserName(), senderEmail.getPassword());
            transport.close();
            result.setStatus(200);
        } catch (MessagingException e) {
            result.setStatus(500);
            result.setMsg("connect to target mail server failed: " + e.getMessage());
        }
        return result;
    }

    @Override
    public boolean updateEmail(SenderEmail senderEmail) {
        List<Setting> settings = SenderEmail.toSettings(senderEmail);
        for (Setting each : settings) {
            if (!update(each)) {
                return false;
            }
        }
        return true;
    }
}
