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

package org.apache.streampark.console.core.entity;

import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.console.base.util.MavenUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.annotation.FieldStrategy;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.lib.Constants;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

@Slf4j
@Getter
@Setter
@TableName("t_flink_project")
public class Project implements Serializable {
  @TableId(type = IdType.AUTO)
  private Long id;

  private Long teamId;

  private String name;

  private String url;

  /** git branch or tag */
  private String refs;

  private Date lastBuild;

  @TableField(updateStrategy = FieldStrategy.IGNORED)
  private String userName;

  @TableField(updateStrategy = FieldStrategy.IGNORED)
  private String password;

  @TableField(updateStrategy = FieldStrategy.IGNORED)
  private String prvkeyPath;

  /** 1:git 2:svn */
  private Integer repository;

  private String pom;

  private String buildArgs;

  private String description;
  /**
   * Build status: -2: Changed, need to rebuild -1: Not built 0: Building 1: Build successful 2:
   * Build failed
   */
  private Integer buildState;

  /** 1) flink 2) spark */
  private Integer type;

  private Date createTime;

  private Date modifyTime;

  private transient String module;

  private transient String dateFrom;

  private transient String dateTo;

  /** get project source */
  @JsonIgnore
  public File getAppSource() {
    File sourcePath = new File(Workspace.PROJECT_LOCAL_PATH());
    if (!sourcePath.exists() && !sourcePath.mkdirs()) {
      throw new IllegalStateException(
          "Failed to create project source path: " + sourcePath.getAbsolutePath());
    } else if (sourcePath.isFile()) {
      throw new IllegalArgumentException(
          "[StreamPark] project source base path: "
              + sourcePath.getAbsolutePath()
              + " must be directory");
    }

    String sourceDir = getSourceDirName();
    String newPath = String.format("%s/%s", sourcePath.getAbsolutePath(), id);
    File file = new File(newPath, sourceDir);
    if (!file.exists() && !file.mkdirs()) {
      throw new IllegalStateException("Failed to create directory: " + file.getAbsolutePath());
    }
    return file;
  }

  private String getSourceDirName() {
    String branches = "main";
    if (StringUtils.isNotBlank(this.refs)) {
      if (this.refs.startsWith(Constants.R_HEADS)) {
        branches = this.refs.replace(Constants.R_HEADS, "");
      } else if (this.refs.startsWith(Constants.R_TAGS)) {
        branches = this.refs.replace(Constants.R_TAGS, "");
      } else {
        branches = this.refs;
      }
    }
    String rootName = url.replaceAll(".*/|\\.git|\\.svn", "");
    return rootName.concat("-").concat(branches);
  }

  @JsonIgnore
  public File getDistHome() {
    return new File(Workspace.APP_LOCAL_DIST(), id.toString());
  }

  @JsonIgnore
  public File getGitRepository() {
    File home = getAppSource();
    return new File(home, Constants.DOT_GIT);
  }

  public void delete() throws IOException {
    FileUtils.deleteDirectory(getAppSource());
    FileUtils.deleteDirectory(getDistHome());
  }

  @JsonIgnore
  public boolean isCloned() {
    File repository = getGitRepository();
    return repository.exists();
  }

  /**
   * If you check that the project already exists and has been cloned, delete it first, Mainly to
   * solve: if the latest pulling code in the file deletion, etc., the local will not automatically
   * delete, may cause unpredictable errors.
   */
  public void cleanCloned() throws IOException {
    if (isCloned()) {
      this.delete();
    }
  }

  @JsonIgnore
  public String getMavenWorkHome() {
    String buildHome = this.getAppSource().getAbsolutePath();
    if (StringUtils.isBlank(this.getPom())) {
      return buildHome;
    }
    return new File(buildHome.concat("/").concat(this.getPom())).getParentFile().getAbsolutePath();
  }

  @JsonIgnore
  public String getLog4BuildStart() {
    return String.format(
        "%sproject : %s\nrefs: %s\ncommand : %s\n\n",
        getLogHeader("maven install"), getName(), getRefs(), this.getMavenBuildArgs());
  }

  @JsonIgnore
  public String getLog4CloneStart() {
    return String.format(
        "%sproject  : %s\nrefs : %s\nworkspace: %s\n\n",
        getLogHeader("git clone"), getName(), getRefs(), getAppSource());
  }

  @JsonIgnore
  private String getLogHeader(String header) {
    return "---------------------------------[ " + header + " ]---------------------------------\n";
  }

  @JsonIgnore
  public String getMavenBuildArgs() {
    return MavenUtils.getMavenArgs(this.buildArgs);
  }
}
