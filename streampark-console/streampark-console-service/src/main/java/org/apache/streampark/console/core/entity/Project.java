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

import org.apache.streampark.common.conf.CommonConfig;
import org.apache.streampark.common.conf.InternalConfigHolder;
import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.util.CommonUtils;
import org.apache.streampark.console.base.util.WebUtils;

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
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    if (!sourcePath.exists()) {
      sourcePath.mkdirs();
    } else if (sourcePath.isFile()) {
      throw new IllegalArgumentException(
          "[StreamPark] project source base path: "
              + sourcePath.getAbsolutePath()
              + " must be directory");
    }

    String sourceDir = getSourceDirName();
    File srcFile =
        new File(String.format("%s/%s/%s", sourcePath.getAbsolutePath(), name, sourceDir));
    String newPath = String.format("%s/%s", sourcePath.getAbsolutePath(), id);
    if (srcFile.exists()) {
      File newFile = new File(newPath);
      if (!newFile.exists()) {
        newFile.mkdirs();
      }
      // old project path move to new path
      srcFile.getParentFile().renameTo(newFile);
    }
    return new File(newPath, sourceDir);
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
  public String getMavenArgs() {
    StringBuilder mvnArgBuffer = new StringBuilder(" clean package -DskipTests ");
    if (StringUtils.isNotBlank(this.buildArgs)) {
      mvnArgBuffer.append(this.buildArgs.trim());
    }

    // --settings
    String setting = InternalConfigHolder.get(CommonConfig.MAVEN_SETTINGS_PATH());
    if (StringUtils.isNotBlank(setting)) {
      File file = new File(setting);
      if (file.exists() && file.isFile()) {
        mvnArgBuffer.append(" --settings ").append(setting.trim());
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Invalid maven-setting file path \"%s\", the path not exist or is not file",
                setting));
      }
    }

    // check maven args
    String mvnArgs = mvnArgBuffer.toString();
    if (mvnArgs.contains("\n")) {
      throw new IllegalArgumentException(
          String.format(
              "Illegal argument: newline character in maven build parameters: \"%s\"", mvnArgs));
    }

    String args = getIllegalArgs(mvnArgs);
    if (args != null) {
      throw new IllegalArgumentException(
          String.format("Illegal argument: \"%s\" in maven build parameters: %s", args, mvnArgs));
    }

    // find mvn
    boolean windows = Utils.isWindows();
    String mvn = windows ? "mvn.cmd" : "mvn";

    String mavenHome = System.getenv("M2_HOME");
    if (mavenHome == null) {
      mavenHome = System.getenv("MAVEN_HOME");
    }

    boolean useWrapper = true;
    if (mavenHome != null) {
      mvn = mavenHome + "/bin/" + mvn;
      try {
        Process process = Runtime.getRuntime().exec(mvn + " --version");
        process.waitFor();
        AssertUtils.required(process.exitValue() == 0);
        useWrapper = false;
      } catch (Exception ignored) {
        log.warn("try using user-installed maven failed, now use maven-wrapper.");
      }
    }

    if (useWrapper) {
      if (windows) {
        mvn = WebUtils.getAppHome().concat("/bin/mvnw.cmd");
      } else {
        mvn = WebUtils.getAppHome().concat("/bin/mvnw");
      }
    }
    return mvn.concat(mvnArgs);
  }

  private String getIllegalArgs(String param) {
    Pattern pattern = Pattern.compile("(`(.?|\\s)*`)|(\\$\\((.?|\\s)*\\))");
    Matcher matcher = pattern.matcher(param);
    if (matcher.find()) {
      return matcher.group(1) == null ? matcher.group(3) : matcher.group(1);
    }

    Iterator<String> iterator = Arrays.asList(";", "|", "&", ">", "<").iterator();
    String[] argsList = param.split("\\s+");
    while (iterator.hasNext()) {
      String chr = iterator.next();
      for (String arg : argsList) {
        if (arg.contains(chr)) {
          return arg;
        }
      }
    }
    return null;
  }

  @JsonIgnore
  public String getMavenWorkHome() {
    String buildHome = this.getAppSource().getAbsolutePath();
    if (CommonUtils.notEmpty(this.getPom())) {
      buildHome =
          new File(buildHome.concat("/").concat(this.getPom())).getParentFile().getAbsolutePath();
    }
    return buildHome;
  }

  @JsonIgnore
  public String getLog4BuildStart() {
    return String.format(
        "%sproject : %s\nrefs: %s\ncommand : %s\n\n",
        getLogHeader("maven install"), getName(), getRefs(), getMavenArgs());
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
}
