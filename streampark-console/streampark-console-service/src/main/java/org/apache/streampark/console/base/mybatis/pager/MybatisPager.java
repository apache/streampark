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

package org.apache.streampark.console.base.mybatis.pager;

import org.apache.streampark.console.base.domain.Constant;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.util.WebUtils;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@SuppressWarnings("unchecked")
public final class MybatisPager {

  private static final Pattern SORT_FIELD_PATTERN = Pattern.compile("^[A-Za-z][A-Za-z0-9_]*$");

  public static <T> Page<T> getPage(RestRequest request) {
    String sortField = WebUtils.camelToUnderscore(StringUtils.trimToEmpty(request.getSortField()));
    if (!SORT_FIELD_PATTERN.matcher(sortField).matches()) {
      throw new IllegalArgumentException(
          String.format("Invalid argument sortField: %s", request.getSortField()));
    }

    if (request.getSortOrder() == null) {
      request.setSortOrder(Constant.ORDER_DESC);
    }

    Page<T> page = new Page<>();
    page.setCurrent(request.getPageNum());
    page.setSize(request.getPageSize());

    List<OrderItem> orderItems = new ArrayList<>(2);
    if (StringUtils.equalsIgnoreCase(request.getSortOrder(), Constant.ORDER_DESC)) {
      orderItems.add(OrderItem.desc(sortField));
    } else if (StringUtils.equalsIgnoreCase(request.getSortOrder(), Constant.ORDER_ASC)) {
      orderItems.add(OrderItem.asc(sortField));
    } else {
      throw new IllegalArgumentException("Invalid argument sortOrder: " + request.getSortOrder());
    }
    page.setOrders(orderItems);
    return page;
  }
}
