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

package org.apache.streampark.console.core.entity;

import org.apache.streampark.common.conf.ConfigKeys;
import org.apache.streampark.common.enums.ClusterState;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.enums.FlinkK8sRestExposedType;
import org.apache.streampark.common.enums.ResolveOrder;
import org.apache.streampark.common.util.FlinkConfigurationUtils;
import org.apache.streampark.common.util.HttpClientUtils;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.core.util.YarnQueueLabelExpression;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.hc.client5.http.config.RequestConfig;

import com.baomidou.mybatisplus.annotation.FieldStrategy;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Getter
@Setter
@TableName("t_flink_cluster")
public class FlinkCluster implements Serializable {

    @TableId(type = IdType.AUTO)
    private Long id;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String address;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String jobManagerUrl;

    private String clusterId;

    private String clusterName;

    private Integer deployMode;

    /** flink version */
    private Long versionId;

    private String k8sNamespace;

    private String serviceAccount;

    private String description;

    private Long userId;

    private String flinkImage;

    private String options;

    private String yarnQueue;

    private Boolean k8sHadoopIntegration;

    private String dynamicProperties;

    private Integer k8sRestExposedType;

    private String k8sConf;

    private Integer resolveOrder;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private String exception;

    private Integer clusterState;

    private Date createTime;

    private Date startTime;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Date endTime;

    @TableField(updateStrategy = FieldStrategy.IGNORED)
    private Long alertId;

    private transient Integer allJobs = 0;

    private transient Integer affectedJobs = 0;

    @JsonIgnore
    public FlinkK8sRestExposedType getK8sRestExposedTypeEnum() {
        return FlinkK8sRestExposedType.of(this.k8sRestExposedType);
    }

    @JsonIgnore
    public FlinkDeployMode getFlinkDeployModeEnum() {
        return FlinkDeployMode.of(this.deployMode);
    }

    @JsonIgnore
    public ClusterState getClusterStateEnum() {
        return ClusterState.of(this.clusterState);
    }

    @JsonIgnore
    @SneakyThrows
    public Map<String, Object> getOptionMap() {
        if (StringUtils.isBlank(this.options)) {
            return new HashMap<>();
        }
        Map<String, Object> optionMap = JacksonUtils.read(this.options, Map.class);
        if (FlinkDeployMode.YARN_SESSION == getFlinkDeployModeEnum()) {
            optionMap.put(ConfigKeys.KEY_YARN_APP_NAME(), this.clusterName);
            optionMap.putAll(YarnQueueLabelExpression.getQueueLabelMap(yarnQueue));
        }
        optionMap.entrySet().removeIf(entry -> entry.getValue() == null);
        return optionMap;
    }

    @JsonIgnore
    public URI getRemoteURI() {
        try {
            HttpClientUtils.httpGetRequest(
                this.address,
                RequestConfig.custom().setConnectTimeout(2000, TimeUnit.MILLISECONDS).build());
            return new URI(address);
        } catch (Exception e) {
            log.error("FlinkCluster getRemoteURI error", e);
        }
        return null;
    }

    @JsonIgnore
    public Map<String, String> getFlinkConfig() throws JsonProcessingException {
        String restUrl = this.address + "/jobmanager/config";
        String json = HttpClientUtils.httpGetRequest(
            restUrl, RequestConfig.custom().setConnectTimeout(2000, TimeUnit.MILLISECONDS).build());
        if (StringUtils.isBlank(json)) {
            return new HashMap<>();
        }
        List<Map<String, String>> confList = JacksonUtils.read(json, new TypeReference<List<Map<String, String>>>() {
        });
        Map<String, String> config = new HashMap<>(0);
        confList.forEach(k -> config.put(k.get("key"), k.get("value")));
        return config;
    }

    @JsonIgnore
    public Map<String, Object> getProperties() {
        Map<String, Object> propertyMap = new HashMap<>();
        Map<String, String> dynamicPropertyMap =
            FlinkConfigurationUtils.extractDynamicPropertiesAsJava(this.getDynamicProperties());
        propertyMap.putAll(this.getOptionMap());
        propertyMap.putAll(dynamicPropertyMap);
        ResolveOrder resolveOrder = ResolveOrder.of(this.getResolveOrder());
        if (resolveOrder != null) {
            propertyMap.put(CoreOptions.CLASSLOADER_RESOLVE_ORDER.key(), resolveOrder.getName());
        }
        return propertyMap;
    }

}
