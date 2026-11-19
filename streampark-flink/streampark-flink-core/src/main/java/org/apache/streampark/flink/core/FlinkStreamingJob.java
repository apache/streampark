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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class FlinkStreamingJob {

    protected StreamExecutionEnvironment env;
    protected ParameterTool parameter;
    protected JobExecutionResult jobExecutionResult;

    public final void run(String[] args) throws FlinkJobException {
        try {
            init(args);
            ready();
            handle();
            jobExecutionResult =
                env.execute(parameter.get("app.name", getClass().getSimpleName()));
            destroy();
        } catch (Exception e) {
            throw new FlinkJobException("Failed to run Flink job: " + getClass().getSimpleName(), e);
        }
    }

    private void init(String[] args) {
        this.parameter = ParameterTool.fromArgs(args);
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        configure(env, parameter);
        env.getConfig().setGlobalJobParameters(parameter);
    }

    protected void configure(StreamExecutionEnvironment env, ParameterTool parameter) {
    }

    protected void ready() {
    }

    protected abstract void handle() throws FlinkJobException;

    protected void destroy() {
    }
}
