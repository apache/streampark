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

package org.apache.streampark.console.base.config;

import org.apache.streampark.common.util.CpuUtils;
import org.apache.streampark.common.util.ThreadUtils;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Create asynchronous thread pools for different services */
@Configuration
public class AsyncExecutorPoolConfig extends AsyncConfigurerSupport {

    private final int CPU_CORES = CpuUtils.getCpuCores();

    /**
     * Create a ThreadPoolTaskExecutor for SavePointService.
     *
     * @return Executor
     */
    @Bean("triggerSavepointExecutor")
    public Executor savepointExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("trigger-savepoint-executor-"),
            new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Create a ThreadPoolTaskExecutor for FlinkAppHttpWatcher.
     *
     * @return Executor
     */
    @Bean("flinkRestAPIWatchingExecutor")
    public Executor restAPIWatchingExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("flink-restapi-watching-executor-"));
    }

    /**
     * Create a ThreadPoolTaskExecutor for SparkAppHttpWatcher.
     *
     * @return Executor
     */
    @Bean("sparkRestAPIWatchingExecutor")
    public Executor sparkRestAPIWatchingExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("spark-cluster-watching-executor-"));
    }

    /**
     * Create a ThreadPoolTaskExecutor for FlinkClusterWatcher.
     *
     * @return Executor
     */
    @Bean("flinkClusterWatchingExecutor")
    public Executor clusterWatchingExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("flink-cluster-watching-executor-"));
    }

    /**
     * Create a ThreadPoolExecutor for AppBuildPipeService.
     *
     * @return ExecutorService
     */
    @Bean("streamparkBuildPipelineExecutor")
    public ExecutorService pipelineExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("streampark-build-pipeline-executor"),
            new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Create a ThreadPoolExecutor for FlinkClusterService.
     *
     * @return ExecutorService
     */
    @Bean("streamparkClusterExecutor")
    public ExecutorService clusterExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("streampark-cluster-executor"),
            new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Create a ThreadPoolTaskExecutor for FlinkK8sChangeEventListener.
     *
     * @return Executor
     */
    @Bean("streamparkNotifyExecutor")
    public Executor notifyExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            20L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("streampark-notify-executor-"),
            new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Create a ThreadPoolTaskExecutor for ApplicationActionService.
     *
     * @return Executor
     */
    @Bean("streamparkDeployExecutor")
    public Executor deployExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("streampark-deploy-executor-"),
            new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Create a ThreadPoolTaskExecutor for ProjectService.
     *
     * @return Executor
     */
    @Bean("streamparkBuildExecutor")
    public Executor buildExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("streampark-build-executor-"),
            new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Create a ThreadPoolTaskExecutor for DistributedTask.
     *
     * @return Executor
     */
    @Bean("streamparkDistributedTaskExecutor")
    public Executor distributedTaskExecutor() {
        return new ThreadPoolExecutor(
            CPU_CORES * 5,
            CPU_CORES * 10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1024),
            ThreadUtils.threadFactory("streampark-distributed-task-"));
    }
}
