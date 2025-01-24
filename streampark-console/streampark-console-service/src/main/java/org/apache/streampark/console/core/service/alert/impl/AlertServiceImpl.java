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

package org.apache.streampark.console.core.service.alert.impl;

import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.ThreadUtils;
import org.apache.streampark.console.base.exception.AlertException;
import org.apache.streampark.console.base.util.SpringContextUtils;
import org.apache.streampark.console.core.bean.AlertConfigWithParams;
import org.apache.streampark.console.core.bean.AlertTemplate;
import org.apache.streampark.console.core.entity.AlertConfig;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.enums.AlertType;
import org.apache.streampark.console.core.enums.CheckPointStatus;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.service.alert.AlertConfigService;
import org.apache.streampark.console.core.service.alert.AlertNotifyService;
import org.apache.streampark.console.core.service.alert.AlertService;

import org.apache.flink.api.java.tuple.Tuple2;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class AlertServiceImpl implements AlertService {
  @Autowired private AlertConfigService alertConfigService;

  private final ExecutorService notifyExecutor =
      new ThreadPoolExecutor(
          1,
          2,
          20L,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(),
          ThreadUtils.threadFactory("streampark-notify"));

  @Override
  public void alert(Application application, CheckPointStatus checkPointStatus) {
    AlertTemplate alertTemplate = AlertTemplate.of(application, checkPointStatus);
    notifyExecutor.submit(() -> alert(application, alertTemplate));
  }

  @Override
  public void alert(Application application, FlinkAppState appState) {
    AlertTemplate alertTemplate = AlertTemplate.of(application, appState);
    notifyExecutor.submit(() -> alert(application, alertTemplate));
  }

  private void alert(Application application, AlertTemplate alertTemplate) {
    Integer alertId = application.getAlertId();
    if (alertId == null) {
      return;
    }
    AlertConfig alertConfig = alertConfigService.getById(alertId);
    try {
      alert(AlertConfigWithParams.of(alertConfig), alertTemplate);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  @Override
  public boolean alert(AlertConfigWithParams params, AlertTemplate alertTemplate)
      throws AlertException {

    List<AlertType> alertTypes = AlertType.decode(params.getAlertType());
    if (CollectionUtils.isEmpty(alertTypes)) {
      return true;
    }
    // No use thread pool, ensure that the alarm can be sent successfully
    Tuple2<Boolean, AlertException> reduce =
        alertTypes.stream()
            .map(
                alertType -> {
                  try {
                    Class<? extends AlertNotifyService> notifyServiceClass =
                        getAlertServiceImpl(alertType);
                    AssertUtils.notNull(notifyServiceClass);
                    boolean alertRes =
                        SpringContextUtils.getBean(notifyServiceClass)
                            .doAlert(params, alertTemplate);
                    return new Tuple2<Boolean, AlertException>(alertRes, null);
                  } catch (AlertException e) {
                    return new Tuple2<>(false, e);
                  }
                })
            .reduce(
                new Tuple2<>(true, null),
                (tp1, tp2) -> {
                  boolean alertResult = tp1.f0 & tp2.f0;
                  if (tp1.f1 == null && tp2.f1 == null) {
                    return new Tuple2<>(tp1.f0 & tp2.f0, null);
                  }
                  if (tp1.f1 != null && tp2.f1 != null) {
                    // merge multiple exception, and keep the details of the first exception
                    AlertException alertException =
                        new AlertException(
                            tp1.f1.getMessage() + "\n" + tp2.f1.getMessage(), tp1.f1);
                    return new Tuple2<>(alertResult, alertException);
                  }
                  return new Tuple2<>(alertResult, tp1.f1 == null ? tp2.f1 : tp1.f1);
                });
    if (reduce.f1 != null) {
      throw reduce.f1;
    }

    return reduce.f0;
  }

  private Class<? extends AlertNotifyService> getAlertServiceImpl(AlertType alertType) {
    switch (alertType) {
      case EMAIL:
        return EmailAlertNotifyServiceImpl.class;
      case DING_TALK:
        return DingTalkAlertNotifyServiceImpl.class;
      case WE_COM:
        return WeComAlertNotifyServiceImpl.class;
      case LARK:
        return LarkAlertNotifyServiceImpl.class;
      case HTTP_CALLBACK:
        return HttpCallbackAlertNotifyServiceImpl.class;
      default:
        return null;
    }
  }
}
