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

package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.core.bean.ResponseResult;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.YarnQueue;
import org.apache.streampark.console.core.mapper.YarnQueueMapper;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.YarnQueueService;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.streampark.console.core.utils.YarnQueueLabelExpression.ERR_FORMAT_HINTS;
import static org.apache.streampark.console.core.utils.YarnQueueLabelExpression.isValid;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class YarnQueueServiceImpl extends ServiceImpl<YarnQueueMapper, YarnQueue>
    implements YarnQueueService {

  public static final String DEFAULT_QUEUE = "default";
  public static final String QUEUE_USED_FORMAT =
      "Please remove the yarn queue for '%s' referenced it before '%s'.";
  public static final String QUEUE_EXISTED_IN_TEAM_HINT =
      "The queue label existed already. Try on a new queue label, please.";
  public static final String QUEUE_EMPTY_HINT = "Yarn queue label mustn't be empty.";
  public static final String QUEUE_AVAILABLE_HINT = "The queue label is available.";

  @Autowired private ApplicationService applicationService;
  @Autowired private FlinkClusterService flinkClusterService;

  @Override
  public IPage<YarnQueue> page(YarnQueue yarnQueue, RestRequest request) {
    AssertUtils.notNull(yarnQueue, "Yarn queue query params mustn't be null.");
    AssertUtils.notNull(
        yarnQueue.getTeamId(), "Team id of yarn queue query params mustn't be null.");
    Page<YarnQueue> page = MybatisPager.getPage(request);
    return this.baseMapper.findQueues(page, yarnQueue);
  }

  /**
   * Check for the yarn queue if exists in the team or the queue name and label format is invalid.
   * status msg 0 Success 1 The queue already existed in the team, 2 The queue name and label format
   * is invalid, 3 The queue name and label is empty.
   */
  @Override
  public ResponseResult<String> checkYarnQueue(YarnQueue yarnQueue) {

    AssertUtils.notNull(yarnQueue, "Yarn queue mustn't be empty.");
    AssertUtils.notNull(yarnQueue.getTeamId(), "Team id mustn't be null.");

    ResponseResult<String> responseResult = new ResponseResult<>();

    if (StringUtils.isEmpty(yarnQueue.getQueueLabel())) {
      responseResult.setStatus(3);
      responseResult.setMsg(QUEUE_EMPTY_HINT);
      return responseResult;
    }

    boolean valid = isValid(yarnQueue.getQueueLabel());
    if (!valid) {
      responseResult.setStatus(2);
      responseResult.setMsg(ERR_FORMAT_HINTS);
      return responseResult;
    }

    boolean existed = this.baseMapper.existsByQueueLabel(yarnQueue);

    if (existed) {
      responseResult.setStatus(1);
      responseResult.setMsg(QUEUE_EXISTED_IN_TEAM_HINT);
      return responseResult;
    }
    responseResult.setStatus(0);
    responseResult.setMsg("The queue label is available.");
    return responseResult;
  }

  @Override
  public boolean createYarnQueue(YarnQueue yarnQueue) {
    ResponseResult<String> checkResponse = checkYarnQueue(yarnQueue);
    ApiAlertException.throwIfFalse(checkResponse.getStatus() == 0, checkResponse.getMsg());
    Date date = new Date();
    yarnQueue.setCreateTime(date);
    yarnQueue.setModifyTime(date);
    return save(yarnQueue);
  }

  @Override
  public void updateYarnQueue(YarnQueue yarnQueue) {

    YarnQueue queueFromDB = getYarnQueueByIdWithPreconditions(yarnQueue);

    // 1) no data to update
    if (StringUtils.equals(yarnQueue.getQueueLabel(), queueFromDB.getQueueLabel())
        && StringUtils.equals(yarnQueue.getDescription(), queueFromDB.getDescription())) {
      return;
    }

    // 2 update description
    if (StringUtils.equals(yarnQueue.getQueueLabel(), queueFromDB.getQueueLabel())) {
      queueFromDB.setDescription(yarnQueue.getDescription());
      updateById(queueFromDB);
      return;
    }

    // 3 update yarnQueue
    ApiAlertException.throwIfFalse(isValid(yarnQueue.getQueueLabel()), ERR_FORMAT_HINTS);

    checkNotReferencedByApplications(
        queueFromDB.getTeamId(), queueFromDB.getQueueLabel(), "updating");

    checkNotReferencedByFlinkClusters(queueFromDB.getQueueLabel(), "updating");

    queueFromDB.setModifyTime(new Date());
    queueFromDB.setDescription(yarnQueue.getDescription());
    queueFromDB.setQueueLabel(yarnQueue.getQueueLabel());

    updateById(queueFromDB);
  }

  @Override
  public void deleteYarnQueue(YarnQueue yarnQueue) {
    YarnQueue queueFromDB = getYarnQueueByIdWithPreconditions(yarnQueue);

    checkNotReferencedByApplications(
        queueFromDB.getTeamId(), queueFromDB.getQueueLabel(), "deleting");

    checkNotReferencedByFlinkClusters(queueFromDB.getQueueLabel(), "deleting");

    removeById(yarnQueue.getId());
  }

  /**
   * Only check the validation of queue-labelExpression when using yarn application or yarn-session
   * mode or yarn-perjob mode.
   *
   * @param executionMode execution mode.
   * @param queueLabel queueLabel expression.
   */
  @Override
  public void checkQueueLabel(ExecutionMode executionMode, String queueLabel) {
    if (ExecutionMode.isYarnMode(executionMode)) {
      ApiAlertException.throwIfFalse(isValid(queueLabel, true), ERR_FORMAT_HINTS);
    }
  }

  @Override
  public boolean isDefaultQueue(String queueLabel) {
    return StringUtils.equals(DEFAULT_QUEUE, queueLabel) || StringUtils.isEmpty(queueLabel);
  }

  @Override
  public boolean existByQueueLabel(String queueLabel) {
    return getBaseMapper()
        .exists(new LambdaQueryWrapper<YarnQueue>().eq(YarnQueue::getQueueLabel, queueLabel));
  }

  @Override
  public boolean existByTeamIdQueueLabel(Long teamId, String queueLabel) {
    return getBaseMapper()
        .exists(
            new LambdaQueryWrapper<YarnQueue>()
                .eq(YarnQueue::getTeamId, teamId)
                .eq(YarnQueue::getQueueLabel, queueLabel));
  }

  // --------- private methods------------

  @VisibleForTesting
  public YarnQueue getYarnQueueByIdWithPreconditions(YarnQueue yarnQueue) {
    AssertUtils.notNull(yarnQueue, "Yarn queue mustn't be null.");
    AssertUtils.notNull(yarnQueue.getId(), "Yarn queue id mustn't be null.");
    YarnQueue queueFromDB = getById(yarnQueue.getId());
    ApiAlertException.throwIfNull(queueFromDB, "The queue doesn't exist.");
    return queueFromDB;
  }

  @VisibleForTesting
  public void checkNotReferencedByFlinkClusters(
      @Nonnull String queueLabel, @Nonnull String operation) {
    List<FlinkCluster> clustersReferenceYarnQueueLabel =
        flinkClusterService.getByExecutionModes(Sets.newHashSet(ExecutionMode.YARN_SESSION))
            .stream()
            .filter(flinkCluster -> StringUtils.equals(flinkCluster.getYarnQueue(), queueLabel))
            .collect(Collectors.toList());
    ApiAlertException.throwIfFalse(
        CollectionUtils.isEmpty(clustersReferenceYarnQueueLabel),
        String.format(QUEUE_USED_FORMAT, "flink clusters", operation));
  }

  @VisibleForTesting
  public void checkNotReferencedByApplications(
      @Nonnull Long teamId, @Nonnull String queueLabel, @Nonnull String operation) {
    List<Application> appsReferenceQueueLabel =
        applicationService
            .getByTeamIdAndExecutionModes(
                teamId, Sets.newHashSet(ExecutionMode.YARN_APPLICATION, ExecutionMode.YARN_PER_JOB))
            .stream()
            .filter(
                application -> {
                  application.setByHotParams();
                  return StringUtils.equals(application.getYarnQueue(), queueLabel);
                })
            .collect(Collectors.toList());
    ApiAlertException.throwIfFalse(
        CollectionUtils.isEmpty(appsReferenceQueueLabel),
        String.format(QUEUE_USED_FORMAT, "applications", operation));
  }
}
