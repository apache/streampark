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

package org.apache.streampark.console.core.controller;

import org.apache.streampark.common.enums.ClusterState;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.core.bean.ResponseResult;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.util.ServiceHelper;

import org.apache.shiro.authz.annotation.Logical;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequestMapping("flink/cluster")
public class FlinkClusterController {

    @Autowired
    private FlinkClusterService flinkClusterService;

    @PostMapping("page")
    @RequiresPermissions(value = {"cluster:create", "cluster:update"}, logical = Logical.OR)
    public RestResponse findPage(FlinkCluster flinkCluster, RestRequest restRequest) {
        IPage<FlinkCluster> flinkClusters = flinkClusterService.findPage(flinkCluster, restRequest);
        return RestResponse.success(flinkClusters);
    }

    @PostMapping("alive")
    @RequiresPermissions(value = {"cluster:create", "cluster:update", "app:add", "app:update"}, logical = Logical.OR)
    public RestResponse listAvailableCluster() {
        List<FlinkCluster> flinkClusters = flinkClusterService.listAvailableCluster();
        return RestResponse.success(flinkClusters);
    }

    @PostMapping("list")
    @RequiresPermissions(value = {"cluster:create", "cluster:update", "app:add", "app:update"}, logical = Logical.OR)
    public RestResponse list() {
        List<FlinkCluster> flinkClusters = flinkClusterService.list();
        return RestResponse.success(flinkClusters);
    }

    @PostMapping("remote_url")
    @RequiresPermissions(value = {"cluster:create", "cluster:update"}, logical = Logical.OR)
    public RestResponse remoteUrl(Long id) {
        FlinkCluster cluster = flinkClusterService.getById(id);
        return RestResponse.success(cluster.getAddress());
    }

    @PostMapping("check")
    @RequiresPermissions(value = {"cluster:create", "cluster:update"}, logical = Logical.OR)
    public RestResponse check(FlinkCluster cluster) {
        ResponseResult checkResult = flinkClusterService.check(cluster);
        return RestResponse.success(checkResult);
    }

    @PostMapping("create")
    @RequiresPermissions("cluster:create")
    public RestResponse create(FlinkCluster cluster) {
        Long userId = ServiceHelper.getUserId();
        Boolean success = flinkClusterService.create(cluster, userId);
        return RestResponse.success(success);
    }

    @PostMapping("update")
    @RequiresPermissions("cluster:update")
    public RestResponse update(FlinkCluster cluster) {
        flinkClusterService.update(cluster);
        return RestResponse.success();
    }

    @PostMapping("get")
    @RequiresPermissions(value = {"cluster:create", "cluster:update"}, logical = Logical.OR)
    public RestResponse get(Long id) throws InternalException {
        FlinkCluster cluster = flinkClusterService.getById(id);
        return RestResponse.success(cluster);
    }

    @PostMapping("start")
    @RequiresPermissions("cluster:update")
    public RestResponse start(FlinkCluster cluster) {
        flinkClusterService.updateClusterState(cluster.getId(), ClusterState.STARTING);
        flinkClusterService.start(cluster);
        return RestResponse.success();
    }

    @PostMapping("shutdown")
    @RequiresPermissions("cluster:update")
    public RestResponse shutdown(FlinkCluster cluster) {
        if (flinkClusterService.allowShutdownCluster(cluster)) {
            flinkClusterService.updateClusterState(cluster.getId(), ClusterState.CANCELLING);
            flinkClusterService.shutdown(cluster);
        }
        return RestResponse.success();
    }

    @PostMapping("delete")
    @RequiresPermissions("cluster:update")
    public RestResponse delete(FlinkCluster cluster) {
        flinkClusterService.remove(cluster.getId());
        return RestResponse.success();
    }
}
