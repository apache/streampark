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

import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.enums.CandidateType;
import org.apache.streampark.console.core.enums.EffectiveType;
import org.apache.streampark.console.core.mapper.FlinkSqlMapper;
import org.apache.streampark.console.core.service.ApplicationBackUpService;
import org.apache.streampark.console.core.service.EffectiveService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.flink.core.FlinkSqlValidationResult;
import org.apache.streampark.flink.proxy.FlinkShimsProxy;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.Method;
import java.util.List;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class FlinkSqlServiceImpl extends ServiceImpl<FlinkSqlMapper, FlinkSql>
    implements FlinkSqlService {

  @Autowired private EffectiveService effectiveService;

  @Autowired private ApplicationBackUpService backUpService;

  @Autowired private FlinkEnvService flinkEnvService;

  private static final String FLINKSQL_VALIDATOR_CLASS =
      "org.apache.streampark.flink.core.FlinkSqlValidator";

  @Override
  public FlinkSql getEffective(Long appId, boolean decode) {
    FlinkSql flinkSql = baseMapper.getEffective(appId);
    if (flinkSql != null && decode) {
      flinkSql.setSql(DeflaterUtils.unzipString(flinkSql.getSql()));
    }
    return flinkSql;
  }

  @Override
  public FlinkSql getLatestFlinkSql(Long appId, boolean decode) {
    Page<FlinkSql> page = new Page<>();
    page.setCurrent(0).setSize(1).setSearchCount(false);
    LambdaQueryWrapper<FlinkSql> queryWrapper =
        new LambdaQueryWrapper<FlinkSql>()
            .eq(FlinkSql::getAppId, appId)
            .orderByDesc(FlinkSql::getVersion);

    Page<FlinkSql> flinkSqlPage = baseMapper.selectPage(page, queryWrapper);
    if (!flinkSqlPage.getRecords().isEmpty()) {
      FlinkSql flinkSql = flinkSqlPage.getRecords().get(0);
      if (decode) {
        flinkSql.setSql(DeflaterUtils.unzipString(flinkSql.getSql()));
      }
      return flinkSql;
    }
    return null;
  }

  @Override
  @Transactional(rollbackFor = {Exception.class})
  public void create(FlinkSql flinkSql) {
    Integer version = this.baseMapper.getLatestVersion(flinkSql.getAppId());
    flinkSql.setVersion(version == null ? 1 : version + 1);
    String sql = DeflaterUtils.zipString(flinkSql.getSql());
    flinkSql.setSql(sql);
    this.save(flinkSql);
    this.setCandidate(CandidateType.NEW, flinkSql.getAppId(), flinkSql.getId());
  }

  @Override
  public void setCandidate(CandidateType candidateType, Long appId, Long sqlId) {
    this.update(
        new LambdaUpdateWrapper<FlinkSql>()
            .eq(FlinkSql::getAppId, appId)
            .set(FlinkSql::getCandidate, CandidateType.NONE.get()));

    this.update(
        new LambdaUpdateWrapper<FlinkSql>()
            .eq(FlinkSql::getId, sqlId)
            .set(FlinkSql::getCandidate, candidateType.get()));
  }

  @Override
  public List<FlinkSql> history(Application application) {
    LambdaQueryWrapper<FlinkSql> queryWrapper =
        new LambdaQueryWrapper<FlinkSql>()
            .eq(FlinkSql::getAppId, application.getId())
            .orderByDesc(FlinkSql::getVersion);

    List<FlinkSql> sqlList = this.baseMapper.selectList(queryWrapper);
    FlinkSql effective = getEffective(application.getId(), false);
    if (effective != null && !sqlList.isEmpty()) {
      for (FlinkSql sql : sqlList) {
        if (sql.getId().equals(effective.getId())) {
          sql.setEffective(true);
          break;
        }
      }
    }
    return sqlList;
  }

  @Override
  public FlinkSql getCandidate(Long appId, CandidateType candidateType) {
    LambdaQueryWrapper<FlinkSql> queryWrapper =
        new LambdaQueryWrapper<FlinkSql>().eq(FlinkSql::getAppId, appId);
    if (candidateType == null) {
      queryWrapper.gt(FlinkSql::getCandidate, CandidateType.NONE.get());
    } else {
      queryWrapper.eq(FlinkSql::getCandidate, candidateType.get());
    }
    return baseMapper.selectOne(queryWrapper);
  }

  @Override
  public void toEffective(Long appId, Long sqlId) {
    effectiveService.saveOrUpdate(appId, EffectiveType.FLINKSQL, sqlId);
  }

  @Override
  public void cleanCandidate(Long id) {
    this.update(
        new LambdaUpdateWrapper<FlinkSql>()
            .eq(FlinkSql::getId, id)
            .set(FlinkSql::getCandidate, CandidateType.NONE.get()));
  }

  @Override
  public void removeApp(Long appId) {
    LambdaQueryWrapper<FlinkSql> queryWrapper =
        new LambdaQueryWrapper<FlinkSql>().eq(FlinkSql::getAppId, appId);
    baseMapper.delete(queryWrapper);
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
  public void rollback(Application application) {
    FlinkSql sql = getCandidate(application.getId(), CandidateType.HISTORY);
    AssertUtils.notNull(sql);
    try {
      // check and backup current job
      FlinkSql effectiveSql = getEffective(application.getId(), false);
      AssertUtils.notNull(effectiveSql);
      // rollback history sql
      backUpService.rollbackFlinkSql(application, sql);
    } catch (Exception e) {
      log.error("Backup and Roll back FlinkSql before start failed.");
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public FlinkSqlValidationResult verifySql(String sql, Long versionId) {
    FlinkEnv flinkEnv = flinkEnvService.getById(versionId);
    return FlinkShimsProxy.proxyVerifySql(
        flinkEnv.getFlinkVersion(),
        classLoader -> {
          try {
            Class<?> clazz = classLoader.loadClass(FLINKSQL_VALIDATOR_CLASS);
            Method method = clazz.getDeclaredMethod("verifySql", String.class);
            method.setAccessible(true);
            Object result = method.invoke(null, sql);
            if (result == null) {
              return null;
            }
            return FlinkShimsProxy.getObject(this.getClass().getClassLoader(), result);
          } catch (Throwable e) {
            log.error("verifySql invocationTargetException: {}", Utils.stringifyException(e));
          }
          return null;
        });
  }

  @Override
  public List<FlinkSql> getByTeamId(Long teamId) {
    return this.baseMapper.getByTeamId(teamId);
  }

  @Override
  public IPage<FlinkSql> page(Long appId, RestRequest request) {
    request.setSortField("version");
    Page<FlinkSql> page = MybatisPager.getPage(request);
    LambdaQueryWrapper<FlinkSql> queryWrapper =
        new LambdaQueryWrapper<FlinkSql>().eq(FlinkSql::getAppId, appId);
    IPage<FlinkSql> sqlList = this.baseMapper.selectPage(page, queryWrapper);
    FlinkSql effectiveSql = baseMapper.getEffective(appId);
    if (effectiveSql != null) {
      for (FlinkSql sql : sqlList.getRecords()) {
        if (sql.getId().equals(effectiveSql.getId())) {
          sql.setEffective(true);
          break;
        }
      }
    }
    return sqlList;
  }
}
