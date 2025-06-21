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

import org.apache.streampark.common.conf.SparkVersion;
import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.console.base.exception.ApiDetailException;

import org.apache.commons.io.FileUtils;

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

@Getter
@Setter
@TableName("t_spark_env")
public class SparkEnv implements Serializable {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String sparkName;

    private String sparkHome;

    private String sparkConf;

    private String description;

    private String scalaVersion;

    private String version;

    /** is default */
    private Boolean isDefault;

    private Date createTime;

    private transient SparkVersion sparkVersion;

    private transient String versionOfLarge;

    private transient String versionOfMiddle;

    private transient String versionOfLast;

    private transient String streamParkScalaVersion = scala.util.Properties.versionNumberString();

    public void doSetSparkConf() throws ApiDetailException {
        try {
            File yaml = new File(this.sparkHome.concat("/conf/spark-defaults.conf"));
            if (yaml.exists()) {
                String sparkConf = FileUtils.readFileToString(yaml, StandardCharsets.UTF_8);
                this.sparkConf = DeflaterUtils.zipString(sparkConf);
            }
        } catch (Exception e) {
            throw new ApiDetailException(e);
        }
    }

    public void doSetVersion() {
        this.setVersion(this.getSparkVersion().version());
        this.setScalaVersion(this.getSparkVersion().scalaVersion());
    }

    @JsonIgnore
    public SparkVersion getSparkVersion() {
        if (this.sparkVersion == null) {
            this.sparkVersion = new SparkVersion(this.sparkHome);
        }
        return this.sparkVersion;
    }

    public void unzipSparkConf() {
        if (sparkConf != null) {
            this.sparkConf = DeflaterUtils.unzipString(this.sparkConf);
        }
    }

}
