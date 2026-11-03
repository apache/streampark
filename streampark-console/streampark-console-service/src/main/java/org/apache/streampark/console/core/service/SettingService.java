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

import org.apache.streampark.console.core.bean.DockerConfig;
import org.apache.streampark.console.core.bean.LineageConfig;
import org.apache.streampark.console.core.bean.MavenConfig;
import org.apache.streampark.console.core.bean.ResponseResult;
import org.apache.streampark.console.core.bean.SenderEmail;
import org.apache.streampark.console.core.entity.Setting;
import org.apache.streampark.console.core.enums.EngineTypeEnum;

import com.baomidou.mybatisplus.extension.service.IService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** System setting service */
public interface SettingService extends IService<Setting> {

    Map<String, Setting> SETTINGS = new ConcurrentHashMap<>();

    // docker
    String KEY_DOCKER_REGISTER_ADDRESS = "docker.register.address";
    String KEY_DOCKER_REGISTER_USER = "docker.register.user";
    String KEY_DOCKER_REGISTER_PASSWORD = "docker.register.password";
    String KEY_DOCKER_REGISTER_NAMESPACE = "docker.register.namespace";

    // alert.
    String KEY_ALERT_EMAIL_HOST = "alert.email.host";
    String KEY_ALERT_EMAIL_PORT = "alert.email.port";
    String KEY_ALERT_EMAIL_FROM = "alert.email.from";
    String KEY_ALERT_EMAIL_USERNAME = "alert.email.userName";
    String KEY_ALERT_EMAIL_PASSWORD = "alert.email.password";
    String KEY_ALERT_EMAIL_SSL = "alert.email.ssl";

    String KEY_STREAMPARK_ADDRESS = "streampark.console.webapp.address";

    String KEY_INGRESS_MODE_DEFAULT = "ingress.mode.default";

    String KEY_DEFAULT_ENGINE = "engine.default";

    // lineage
    String KEY_LINEAGE_GRAVITINO_ADDRESS = "lineage.gravitino.address";
    String KEY_LINEAGE_GRAVITINO_TOKEN = "lineage.gravitino.token";
    String KEY_LINEAGE_GRAVITINO_NAMESPACE = "lineage.gravitino.namespace";
    String KEY_LINEAGE_FLINK_NATIVE_LISTENER_ENABLE = "lineage.flink.native.listener.enable";

    /**
     * Retrieves the value of the setting associated with the specified key.
     *
     * @param key the key of the setting to retrieve
     * @return the value of the setting if found, null otherwise
     */
    Setting get(String key);

    /**
     * Updates the specified Setting.
     *
     * @param setting the Setting object to update
     * @return true if the update is successful, false otherwise
     */
    boolean update(Setting setting);

    /**
     * Retrieves the Maven configuration settings.
     *
     * @return The MavenConfig object containing the Maven configuration settings.
     */
    MavenConfig getMavenConfig();

    /**
     * Retrieves the Docker configuration settings.
     *
     * @return The DockerConfig object representing the configuration for Docker.
     */
    DockerConfig getDockerConfig();

    /**
     * Retrieves the Gravitino lineage configuration settings.
     *
     * @return The LineageConfig object containing the Gravitino lineage configuration settings.
     */
    LineageConfig getLineageConfig();

    /**
     * Retrieves the StreamPark address.
     *
     * @return a String representing the StreamPark address.
     */
    String getStreamParkAddress();

    /**
     * Retrieves the default ingress mode.
     *
     * @return The default ingress mode.
     */
    String getIngressModeDefault();

    /**
     * Before updating the Docker settings, verify that the parameters are filled in correctly, We
     * check whether it is correct or not by concatenating the given registration URL.
     *
     * @param dockerConfig Docker config to be checked
     * @return Server response value
     */
    ResponseResult checkDocker(DockerConfig dockerConfig);

    /**
     * update docker config
     *
     * @param dockerConfig Docker config needs to be updated
     * @return Whether the update operation was successful
     */
    boolean updateDocker(DockerConfig dockerConfig);

    /**
     * Retrieves the senderEmail
     *
     * @return
     */
    SenderEmail getSenderEmail();

    /**
     * Before updating the email settings, verify that the parameters are filled in correctly, We
     * check whether it is correct or not by concatenating the given host.
     *
     * @param senderEmail email config to be checked
     * @return Server response value
     */
    ResponseResult checkEmail(SenderEmail senderEmail);

    /**
     * update docker config
     *
     * @param senderEmail email config needs to be updated
     * @return Whether the update operation was successful
     */
    boolean updateEmail(SenderEmail senderEmail);

    EngineTypeEnum getEngine();
}
