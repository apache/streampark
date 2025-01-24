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

import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.core.entity.ExternalLink;
import org.apache.streampark.console.core.service.ExternalLinkService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequestMapping("/flink/externalLink")
public class ExternalLinkController {

  @Autowired private ExternalLinkService externalLinkService;

  @PostMapping("/list")
  @RequiresPermissions("externalLink:view")
  public RestResponse list() {
    List<ExternalLink> externalLink = externalLinkService.list();
    return RestResponse.success(externalLink);
  }

  @PostMapping("/render")
  public RestResponse render(
      @NotNull(message = "The flink app id cannot be null") @RequestParam("appId") Long appId) {
    List<ExternalLink> renderedExternalLink = externalLinkService.render(appId);
    return RestResponse.success(renderedExternalLink);
  }

  @PostMapping("/create")
  @RequiresPermissions("externalLink:create")
  public RestResponse create(@Valid ExternalLink externalLink) {
    externalLinkService.create(externalLink);
    return RestResponse.success();
  }

  @PostMapping("/update")
  @RequiresPermissions("externalLink:update")
  public RestResponse update(@Valid ExternalLink externalLink) {
    AssertUtils.notNull(externalLink.getId(), "The link id cannot be null");
    externalLinkService.update(externalLink);
    return RestResponse.success();
  }

  @DeleteMapping("/delete")
  @RequiresPermissions("externalLink:delete")
  public RestResponse delete(
      @NotNull(message = "The link id cannot be null") @RequestParam("id") Long id) {
    externalLinkService.delete(id);
    return RestResponse.success();
  }
}
