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

package org.apache.streampark.console.core.service;

import org.apache.streampark.console.SpringTestBase;
import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.enums.GitAuthorizedError;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class ProjectServiceTest extends SpringTestBase {

  private final Project project = new Project();

  @Autowired private ProjectService projectService;

  @BeforeEach
  void before() {
    project.setUrl("git@github.com:apache/streampark.git");
  }

  @Disabled("This test case can't be runnable due to external service is not available.")
  @Test
  void testGitBranches() {
    List<String> branches = projectService.getAllBranches(project);
    branches.forEach(System.out::println);
  }

  @Disabled("This test case can't be runnable due to external service is not available.")
  @Test
  void testGitTags() {
    List<String> branches = projectService.getAllTags(project);
    branches.forEach(System.out::println);
  }

  @Disabled("This test case can't be runnable due to external service is not available.")
  @Test
  void testGitCheckAuth() {
    GitAuthorizedError error = projectService.gitCheck(project);
    System.out.println(error);
  }
}
