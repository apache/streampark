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

import org.apache.streampark.common.conf.FlinkVersion;
import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.common.util.PropertiesUtils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.ApiDetailException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

@Getter
@Setter
@TableName("t_flink_env")
public class FlinkEnv implements Serializable {

  @TableId(type = IdType.AUTO)
  private Long id;

  private String flinkName;

  private String flinkHome;

  private String flinkConf;

  private String description;

  private String scalaVersion;

  private String version;

  /** is default */
  private Boolean isDefault;

  private Date createTime;

  private transient FlinkVersion flinkVersion;

  private transient String streamParkScalaVersion = scala.util.Properties.versionNumberString();

  private static final Float FLINK_CONFIG_CHANGE_VERSION = 1.19f;
  private static final String LEGACY_CONFIG_FILE = "flink-conf.yaml";
  private static final String NEW_CONFIG_FILE = "config.yaml";
  private static final String CONF_DIR = "/conf/";

  public void doSetFlinkConf() throws ApiDetailException {
    Float version = getVersionNumber();
    File configFile = resolveConfigFile(version);

    try {
      String flinkConf = FileUtils.readFileToString(configFile, StandardCharsets.UTF_8);
      this.flinkConf = DeflaterUtils.zipString(flinkConf);
    } catch (Exception e) {
      throw new ApiDetailException(
          "Failed to read Flink configuration file: " + configFile.getAbsolutePath(), e);
    }
  }

  private File resolveConfigFile(Float version) throws ApiAlertException {
    String confDir = this.flinkHome + CONF_DIR;

    if (version < FLINK_CONFIG_CHANGE_VERSION) {
      // For Flink < 1.19, use flink-conf.yaml
      File configFile = new File(confDir + LEGACY_CONFIG_FILE);
      if (!configFile.exists()) {
        throw new ApiAlertException(
            String.format(
                "Cannot find %s in %s for Flink version %s",
                LEGACY_CONFIG_FILE, confDir, this.version));
      }
      return configFile;
    } else if (version.equals(FLINK_CONFIG_CHANGE_VERSION)) {
      // For Flink 1.19, try both config files (backward compatibility)
      File legacyConfigFile = new File(confDir + LEGACY_CONFIG_FILE);
      if (legacyConfigFile.exists()) {
        return legacyConfigFile;
      }
      File newConfigFile = new File(confDir + NEW_CONFIG_FILE);
      if (newConfigFile.exists()) {
        return newConfigFile;
      }
      throw new ApiAlertException(
          String.format(
              "Cannot find either %s or %s in %s for Flink version %s",
              LEGACY_CONFIG_FILE, NEW_CONFIG_FILE, confDir, this.version));
    } else {
      // For Flink > 1.19, use config.yaml
      File configFile = new File(confDir + NEW_CONFIG_FILE);
      if (!configFile.exists()) {
        throw new ApiAlertException(
            String.format(
                "Cannot find %s in %s for Flink version %s",
                NEW_CONFIG_FILE, confDir, this.version));
      }
      return configFile;
    }
  }

  public void doSetVersion() {
    this.setVersion(this.getFlinkVersion().version());
    this.setScalaVersion(this.getFlinkVersion().scalaVersion());
    if (!streamParkScalaVersion.startsWith(this.getFlinkVersion().scalaVersion())) {
      throw new UnsupportedOperationException(
          String.format(
              "The current Scala version of StreamPark is %s, but the scala version of Flink to be added is %s, which does not match, Please check",
              streamParkScalaVersion, this.getFlinkVersion().scalaVersion()));
    }
  }

  public Map<String, String> convertFlinkYamlAsMap() {
    String flinkYamlString = DeflaterUtils.unzipString(flinkConf);
    return PropertiesUtils.loadFlinkConfYaml(flinkYamlString);
  }

  @JsonIgnore
  public FlinkVersion getFlinkVersion() {
    if (this.flinkVersion == null) {
      this.flinkVersion = new FlinkVersion(this.flinkHome);
    }
    return this.flinkVersion;
  }

  public void unzipFlinkConf() {
    this.flinkConf = DeflaterUtils.unzipString(this.flinkConf);
  }

  @JsonIgnore
  public String getLargeVersion() {
    return this.version.substring(0, this.version.lastIndexOf("."));
  }

  @JsonIgnore
  public String getVersionOfFirst() {
    return this.version.split("\\.")[0];
  }

  @JsonIgnore
  public String getVersionOfMiddle() {
    return this.version.split("\\.")[1];
  }

  @JsonIgnore
  public String getVersionOfLast() {
    return this.version.split("\\.")[2];
  }

  @JsonIgnore
  private Float getVersionNumber() {
    if (StringUtils.isNotBlank(this.version)) {
      return Float.parseFloat(getVersionOfFirst() + "." + getVersionOfMiddle());
    }
    throw new RuntimeException("Flink version is null");
  }

  @JsonIgnore
  public boolean isLegacyFlinkConf() {
    return getVersionNumber() < FLINK_CONFIG_CHANGE_VERSION;
  }
}
