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

package org.apache.streampark.console.core.mapper;

import org.apache.streampark.console.core.entity.FlinkApplication;

import org.apache.ibatis.annotations.Param;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import java.util.List;

public interface FlinkApplicationMapper extends BaseMapper<FlinkApplication> {

    IPage<FlinkApplication> selectPage(Page<FlinkApplication> page, @Param("app") FlinkApplication application);

    FlinkApplication selectApp(@Param("id") Long id);

    void persistMetrics(
                        @Param("app") FlinkApplication application,
                        @Param("cancellingState") int cancellingState,
                        @Param("canceledState") int canceledState,
                        @Param("cancellingOptionState") int cancellingOptionState,
                        @Param("savepointingOptionState") int savepointingOptionState);

    List<FlinkApplication> selectAppsByTeamId(@Param("teamId") Long teamId);

    List<String> selectRecentK8sNamespaces(@Param("limitSize") Integer limit);

    List<String> selectRecentK8sClusterIds(
                                           @Param("deployMode") Integer deployMode,
                                           @Param("limitSize") Integer limit);

    List<String> selectRecentFlinkBaseImages(@Param("limitSize") Integer limit);

    List<String> selectRecentK8sPodTemplates(@Param("limitSize") Integer limit);

    List<String> selectRecentK8sJmPodTemplates(@Param("limitSize") Integer limit);

    List<String> selectRecentK8sTmPodTemplates(@Param("limitSize") Integer limit);

    Integer countAffectedByClusterId(
                                     @Param("clusterId") Long clusterId, @Param("dbType") String dbType);
}
