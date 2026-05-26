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

import org.apache.streampark.console.base.domain.RestRequest;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MybatisPagerTest {

  @Test
  void getPageShouldRejectSqlExpressionSortField() {
    Arrays.asList(
            "create_time,CEIL(VARIANT)--",
            "create_time,SLEEP(10)--",
            "create_time,PG_SLEEP(10)",
            "create_time;select 1",
            "create_time desc",
            "user.name",
            "`create_time`",
            "create_time'--",
            "create_time/**/desc")
        .forEach(
            sortField -> {
              RestRequest request = new RestRequest();
              request.setSortField(sortField);

              assertThatThrownBy(() -> MybatisPager.getPage(request))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("Invalid argument sortField");
            });
  }

  @Test
  void getPageShouldAcceptColumnNameSortField() {
    RestRequest request = new RestRequest();
    request.setSortField("createTime");

    Page<?> page = MybatisPager.getPage(request);

    assertThat(page.orders()).hasSize(1);
    assertThat(page.orders().get(0).getColumn()).isEqualTo("create_time");
  }
}
