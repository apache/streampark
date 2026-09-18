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

package org.apache.streampark.flink.packer.pipeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/** Building pipeline type. */
public enum PipelineTypeEnum {

    /** unknown type */
    UNKNOWN(0, "", ImmutableMap.of(), null),

    /** flink native kubernetes session mode */
    FLINK_NATIVE_K8S_SESSION(
        1,
        "flink native kubernetes session mode task building pipeline",
        ImmutableMap.<Integer, String>builder()
            .put(1, "Create building workspace")
            .put(2, "Build shaded flink app jar")
            .build(),
        ShadedBuildResponse.class),

    /** flink native kubernetes application mode */
    FLINK_NATIVE_K8S_APPLICATION(
        2,
        "flink native kubernetes application mode task building pipeline",
        ImmutableMap.<Integer, String>builder()
            .put(1, "Create building workspace")
            .put(2, "Export kubernetes pod template")
            .put(3, "Build shaded flink app jar")
            .put(4, "Export flink app dockerfile")
            .put(5, "Pull flink app base docker image")
            .put(6, "Build flink app docker image")
            .put(7, "Push flink app docker image")
            .build(),
        DockerImageBuildResponse.class),
    FLINK_STANDALONE(
        3,
        "flink standalone session mode task building pipeline",
        ImmutableMap.<Integer, String>builder()
            .put(1, "Create building workspace")
            .put(2, "Build shaded flink app jar")
            .build(),
        ShadedBuildResponse.class),

    FLINK_YARN_APPLICATION(
        4,
        "flink yarn application mode task building pipeline",
        ImmutableMap.<Integer, String>builder()
            .put(1, "Prepare hadoop yarn environment and building workspace")
            .put(2, "Resolve dependencies and build application jar")
            .put(3, "Upload application dependencies")
            .build(),
        ShadedBuildResponse.class),

    FLINK_K8S_APPLICATION_V2(
        5,
        "flink kubernetes application mode task building pipeline v2",
        ImmutableMap.<Integer, String>builder()
            .put(1, "Create building workspace")
            .put(2, "Build shaded flink app jar")
            .build(),
        K8sAppModeBuildResponse.class),

    SPARK_CLUSTER(
        6,
        "spark yarn cluster mode task building pipeline",
        ImmutableMap.<Integer, String>builder()
            .put(1, "Prepare hadoop yarn environment and building workspace")
            .put(2, "Resolve maven dependencies")
            .put(3, "upload jar to yarn.provided.lib.dirs")
            .build(),
        SimpleBuildResponse.class),

    SPARK_NATIVE_K8S_APPLICATION(
        7,
        "spark native kubernetes application mode task building pipeline",
        ImmutableMap.<Integer, String>builder()
            .put(1, "Create building workspace")
            .put(2, "Export kubernetes pod template")
            .put(3, "Prepare spark job jar")
            .put(4, "Export spark app dockerfile")
            .put(5, "Pull spark app base docker image")
            .put(6, "Build spark app docker image")
            .put(7, "Push spark app docker image")
            .build(),
        DockerImageBuildResponse.class);

    private final Integer code;
    /** short description of pipeline type. */
    private final String desc;
    /** building steps of pipeline, element => [sorted seq -> step desc]. */
    private final Map<Integer, String> steps;
    /** type of result */
    private final Class<? extends BuildResult> resultType;

    PipelineTypeEnum(
                     Integer code,
                     String desc,
                     Map<Integer, String> steps,
                     Class<? extends BuildResult> resultType) {
        this.code = code;
        this.desc = desc;
        this.steps = steps;
        this.resultType = resultType;
    }

    @JsonCreator
    public static PipelineTypeEnum of(Integer code) {
        for (PipelineTypeEnum type : PipelineTypeEnum.values()) {
            if (type.getCode().equals(code)) {
                return type;
            }
        }
        return UNKNOWN;
    }

    public static PipelineTypeEnum ofName(String name) {
        return PipelineTypeEnum.valueOf(name);
    }

    @JsonValue
    public Integer getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public Map<Integer, String> getSteps() {
        return steps;
    }

    public Class<? extends BuildResult> getResultType() {
        return resultType;
    }

    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
