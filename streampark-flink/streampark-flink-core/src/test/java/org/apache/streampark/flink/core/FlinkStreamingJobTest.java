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

package org.apache.streampark.flink.core;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkStreamingJobTest {

    @Test
    void lifecycleExecutesInOrder() throws Exception {
        List<String> trace = new ArrayList<>();

        FlinkStreamingJob job =
            new FlinkStreamingJob() {

                @Override
                protected void configure(StreamExecutionEnvironment env, ParameterTool parameter) {
                    trace.add("configure");
                }

                @Override
                protected void ready() {
                    trace.add("ready");
                }

                @Override
                protected void handle() {
                    trace.add("handle");
                    env.fromElements(1, 2, 3).map(i -> i * 2).print();
                }

                @Override
                protected void destroy() {
                    trace.add("destroy");
                }
            };

        job.run(new String[0]);

        assertThat(trace).containsExactly("configure", "ready", "handle", "destroy");
        assertThat(job.jobExecutionResult).isNotNull();
    }

    static class NoOpJob extends FlinkStreamingJob {

        @Override
        protected void handle() throws FlinkJobException {
            env.fromElements(1).print();
        }
    }

    @Test
    void appNameDefaultsToClassName() throws Exception {
        NoOpJob job = new NoOpJob();
        job.run(new String[0]);
        assertThat(job.getClass().getSimpleName()).isEqualTo("NoOpJob");
        assertThat(job.jobExecutionResult).isNotNull();
    }

    @Test
    void parameterToolPropagatedToEnv() throws Exception {
        FlinkStreamingJob job =
            new FlinkStreamingJob() {

                @Override
                protected void handle() {
                    env.fromElements(1).print();
                }
            };

        job.run(new String[]{"--app.name", "test-job"});

        assertThat(job.parameter.get("app.name")).isEqualTo("test-job");
    }
}
