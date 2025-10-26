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

package org.apache.streampark.console.core.task;

import org.apache.streampark.common.util.CommandUtils;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.util.GitUtils;
import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.enums.BuildState;

import org.apache.commons.lang3.StringUtils;

import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.StoredConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * This class handles the complete lifecycle of project building including: - Git repository
 * cloning/pulling - Maven/Gradle build execution - Artifact deployment and validation -
 * Comprehensive logging and monitoring
 */
@Slf4j
public class ProjectBuildTask extends AbstractLogFileTask {

  // ========== Constants ==========

  private static final Duration CLONE_TIMEOUT = Duration.ofMinutes(10);
  private static final String BUILD_START_MARKER = "=== BUILD STARTED ===";
  private static final String BUILD_END_MARKER = "=== BUILD COMPLETED ===";
  private static final Pattern SENSITIVE_INFO_PATTERN =
      Pattern.compile("password|token|key|secret", Pattern.CASE_INSENSITIVE);

  // ========== Fields ==========

  private final Project project;
  private final Consumer<BuildState> stateUpdateConsumer;
  private final Consumer<Logger> notifyReleaseConsumer;
  private final LocalDateTime startTime = LocalDateTime.now();

  // Build statistics
  private LocalDateTime cloneStartTime;
  private LocalDateTime buildStartTime;
  private LocalDateTime deployStartTime;

  /**
   * Creates a new ProjectBuildTask with enhanced validation and configuration.
   *
   * @param logPath log file path for build output
   * @param project project to build (must not be null)
   * @param stateUpdateConsumer callback for build state updates (must not be null)
   * @param notifyReleaseConsumer callback for release notifications (must not be null)
   * @throws IllegalArgumentException if any parameter is null or invalid
   */
  public ProjectBuildTask(
      String logPath,
      Project project,
      Consumer<BuildState> stateUpdateConsumer,
      Consumer<Logger> notifyReleaseConsumer) {
    super(logPath, true);

    // Enhanced parameter validation
    this.project = validateProject(project);
    this.stateUpdateConsumer =
        Objects.requireNonNull(stateUpdateConsumer, "State update consumer cannot be null");
    this.notifyReleaseConsumer =
        Objects.requireNonNull(notifyReleaseConsumer, "Notify release consumer cannot be null");

    // Validate log path
    validateLogPath(logPath);

    log.info(
        "ProjectBuildTask initialized for project: {} (ID: {})",
        project.getName(),
        project.getId());
  }

  // ========== Main Execution Flow ==========

  @Override
  protected void doRun() throws Throwable {
    try {
      logBuildStart();
      updateBuildState(BuildState.BUILDING);

      // Phase 1: Clone source code with retry mechanism
      if (!execute("clone", this::performClone)) {
        handleBuildFailure("Failed to clone source code");
        return;
      }

      // Phase 2: Execute build with timeout and monitoring
      if (!execute("build", this::performBuild)) {
        handleBuildFailure("Failed to build project");
        return;
      }

      // Phase 3: Deploy artifacts
      if (!execute("deploy", this::performDeploy)) {
        handleBuildFailure("Failed to deploy artifacts");
        return;
      }

      // Success
      handleBuildSuccess();

    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        handleBuildInterruption();
      } else {
        handleBuildFailure("Unexpected error during build: " + e.getMessage(), e);
      }
    }
  }

  @Override
  protected void processException(Throwable t) {
    log.error("Build task exception for project: {}", project.getName(), t);
    updateBuildState(BuildState.FAILED);
    if (t instanceof Exception) {
      logBuildError("Build failed with exception", (Exception) t);
    } else {
      fileLogger.error("Build failed with throwable: {}", t.getMessage(), t);
    }
  }

  @Override
  protected void doFinally() {
    try {
      cleanupResources();
      logBuildSummary();
    } catch (Exception e) {
      log.warn("Error during cleanup for project: {}", project.getName(), e);
    }
  }

  // ========== Clone Phase Implementation ==========

  private boolean performClone() throws Exception {
    cloneStartTime = LocalDateTime.now();

    try {
      validatePreCloneConditions();
      prepareCloneDirectory();

      boolean success = executeGitClone();
      if (success) {
        logCloneSuccess();
      }

      return success;

    } catch (Exception e) {
      logCloneError("Git clone operation failed", e);
      throw e;
    }
  }

  private void validatePreCloneConditions() throws IllegalStateException {
    if (StringUtils.isBlank(project.getUrl())) {
      throw new IllegalStateException("Project URL cannot be blank");
    }

    if (project.getAppSource() == null || !project.getAppSource().exists()) {
      throw new IllegalStateException("Project source directory does not exist");
    }

    // Validate URL format (basic validation)
    if (!project.getUrl().startsWith("http") && !project.getUrl().startsWith("git@")) {
      throw new IllegalStateException("Invalid Git URL format: " + sanitizeUrl(project.getUrl()));
    }
  }

  private void prepareCloneDirectory() throws IOException {
    try {
      project.cleanCloned();

      // Ensure parent directory exists
      Path sourcePath = project.getAppSource().toPath();
      Path parentDir = sourcePath.getParent();
      if (parentDir != null && !Files.exists(parentDir)) {
        Files.createDirectories(parentDir);
        fileLogger.info("Created parent directory: {}", parentDir);
      }

    } catch (Exception e) {
      throw new IOException("Failed to prepare clone directory: " + e.getMessage(), e);
    }
  }

  private boolean executeGitClone() {
    Git git = null;
    try {
      fileLogger.info("Starting Git clone for project: {}", project.getName());
      fileLogger.info("Repository URL: {}", sanitizeUrl(project.getUrl()));
      fileLogger.info("Target directory: {}", project.getAppSource());
      fileLogger.info("Branch/Tag: {}", StringUtils.defaultIfBlank(project.getRefs(), "default"));
      fileLogger.info(project.getLog4CloneStart());

      GitUtils.GitCloneRequest request = buildGitCloneRequest();
      git = GitUtils.clone(request);

      configureGitRepository(git);
      validateCloneContent(git);

      return true;

    } catch (Exception e) {
      logCloneError("Git clone failed", e);
      return false;
    } finally {
      if (git != null) {
        try {
          git.close();
        } catch (Exception e) {
          log.warn("Failed to close Git repository: {}", e.getMessage());
        }
      }
    }
  }

  private GitUtils.GitCloneRequest buildGitCloneRequest() {
    GitUtils.GitCloneRequest request = new GitUtils.GitCloneRequest();
    request.setUrl(project.getUrl());
    request.setRefs(project.getRefs());
    request.setStoreDir(project.getAppSource());
    request.setUsername(project.getUserName());
    request.setPassword(project.getPassword());
    request.setPrivateKey(project.getPrvkeyPath());
    return request;
  }

  private void configureGitRepository(Git git) throws Exception {
    StoredConfig config = git.getRepository().getConfig();
    String url = project.getUrl();

    // Disable SSL verification for HTTP/HTTPS URLs
    config.setBoolean("http", url, "sslVerify", false);
    config.setBoolean("https", url, "sslVerify", false);

    // Set timeout configurations
    config.setInt("http", url, "timeout", (int) CLONE_TIMEOUT.getSeconds());
    config.setInt("https", url, "timeout", (int) CLONE_TIMEOUT.getSeconds());

    config.save();
    fileLogger.info("Git repository configuration updated successfully");
  }

  private void validateCloneContent(Git git) {
    File workTree = git.getRepository().getWorkTree();

    if (!workTree.exists() || !workTree.isDirectory()) {
      throw new IllegalStateException("Clone directory does not exist or is not a directory");
    }

    File[] files = workTree.listFiles();
    if (files == null || files.length == 0) {
      throw new IllegalStateException("Cloned repository is empty");
    }

    // Log directory structure (limited depth for security)
    logDirectoryStructure(workTree, "", 0, 3);

    fileLogger.info("Clone validation completed. Found {} files/directories", files.length);
  }

  // ========== Build Phase Implementation ==========

  private boolean performBuild() throws Exception {
    buildStartTime = LocalDateTime.now();

    try {
      validatePreBuildConditions();
      boolean success = executeMavenBuild();

      if (success) {
        Duration buildDuration = Duration.between(buildStartTime, LocalDateTime.now());
        fileLogger.info("Maven build completed successfully in {}", formatDuration(buildDuration));
      }

      return success;

    } catch (Exception e) {
      logBuildError("Maven build failed", e);
      throw e;
    }
  }

  private void validatePreBuildConditions() throws IllegalStateException {
    String mavenWorkHome = project.getMavenWorkHome();
    if (StringUtils.isBlank(mavenWorkHome)) {
      throw new IllegalStateException("Maven work home cannot be blank");
    }

    File workDir = new File(mavenWorkHome);
    if (!workDir.exists()) {
      throw new IllegalStateException("Maven work directory does not exist: " + mavenWorkHome);
    }

    // Check for pom.xml or build.gradle
    File pomFile = new File(workDir, "pom.xml");
    File gradleFile = new File(workDir, "build.gradle");
    if (!pomFile.exists() && !gradleFile.exists()) {
      throw new IllegalStateException(
          "No build file (pom.xml or build.gradle) found in: " + mavenWorkHome);
    }

    String mavenArgs = project.getMavenBuildArgs();
    if (StringUtils.isBlank(mavenArgs)) {
      throw new IllegalStateException("Maven arguments cannot be blank");
    }
  }

  private boolean executeMavenBuild() {
    try {
      fileLogger.info(BUILD_START_MARKER);
      fileLogger.info("🔨 Starting Maven build for project: {}", project.getName());
      fileLogger.info("   📂 Working directory: {}", project.getMavenWorkHome());
      fileLogger.info(
          "   ⚙️  Build command: {}", sanitizeBuildCommand(project.getMavenBuildArgs()));

      int exitCode =
          CommandUtils.execute(
              project.getMavenWorkHome(),
              Collections.singletonList(project.getMavenBuildArgs()),
              this::logBuildOutput);

      fileLogger.info("Maven build completed with exit code: {}", exitCode);
      fileLogger.info(BUILD_END_MARKER);

      return exitCode == 0;

    } catch (Exception e) {
      fileLogger.error("Maven build execution failed: {}", e.getMessage());
      return false;
    }
  }

  private void logBuildOutput(String line) {
    if (StringUtils.isNotBlank(line)) {
      // Filter sensitive information
      String sanitizedLine = sanitizeBuildOutput(line);
      fileLogger.info(sanitizedLine);
    }
  }

  // ========== Deploy Phase Implementation ==========

  private boolean performDeploy() throws Exception {
    deployStartTime = LocalDateTime.now();

    try {
      validatePreDeployConditions();
      deployBuildArtifacts();
      validateDeploymentResult();
      logDeploySuccess();

      return true;

    } catch (Exception e) {
      logDeployError("Deployment failed", e);
      throw e;
    }
  }

  private void validatePreDeployConditions() throws IllegalStateException {
    File sourceDir = project.getAppSource();
    if (sourceDir == null || !sourceDir.exists()) {
      throw new IllegalStateException("Source directory does not exist");
    }

    File distHome = project.getDistHome();
    if (distHome == null) {
      throw new IllegalStateException("Distribution home directory is not configured");
    }
  }

  private void deployBuildArtifacts() throws Exception {
    File sourcePath = project.getAppSource();
    List<File> artifacts = new ArrayList<>();

    // Find build artifacts with improved algorithm
    findBuildArtifacts(artifacts, sourcePath, 0, 5); // Limit depth to 5

    if (artifacts.isEmpty()) {
      throw new RuntimeException(
          "No deployable artifacts (*.tar.gz or *.jar) found in project directory: "
              + sourcePath.getAbsolutePath());
    }

    fileLogger.info("🔍 Found {} deployable artifact(s)", artifacts.size());

    for (File artifact : artifacts) {
      deployArtifact(artifact);
    }
  }

  private void deployArtifact(File artifact) throws Exception {
    String artifactPath = artifact.getAbsolutePath();
    fileLogger.info("📦 Deploying artifact: {}", artifactPath);

    if (artifactPath.endsWith(".tar.gz")) {
      deployTarGzArtifact(artifact);
    } else if (artifactPath.endsWith(".jar")) {
      deployJarArtifact(artifact);
    } else {
      throw new IllegalArgumentException("Unsupported artifact type: " + artifactPath);
    }

    fileLogger.info("✅ Successfully deployed artifact: {}", artifact.getName());
  }

  private void deployTarGzArtifact(File tarGzFile) throws Exception {
    File deployPath = project.getDistHome();
    ensureDirectoryExists(deployPath);

    if (!tarGzFile.exists()) {
      throw new IllegalStateException("Tar.gz file does not exist: " + tarGzFile.getAbsolutePath());
    }

    String extractCommand =
        String.format(
            "tar -xzf %s -C %s", tarGzFile.getAbsolutePath(), deployPath.getAbsolutePath());

    fileLogger.info("📦 Extracting tar.gz: {}", extractCommand);

    CommandUtils.execute(extractCommand);
  }

  private void deployJarArtifact(File jarFile) throws Exception {
    // Validate JAR file integrity
    Utils.checkJarFile(jarFile.toURI().toURL());

    String moduleName = jarFile.getName().replace(".jar", "");
    File distHome = project.getDistHome();
    File targetDir = new File(distHome, moduleName);

    ensureDirectoryExists(targetDir);

    File targetJar = new File(targetDir, jarFile.getName());

    // Use Files.move for atomic operation and better error handling
    try {
      Files.move(jarFile.toPath(), targetJar.toPath());
      fileLogger.info("📦 JAR artifact moved to: {}", targetJar.getAbsolutePath());
    } catch (IOException e) {
      throw new IOException("Failed to move JAR artifact: " + e.getMessage(), e);
    }
  }

  private void validateDeploymentResult() throws Exception {
    File distHome = project.getDistHome();
    if (!distHome.exists()) {
      throw new IllegalStateException("Deployment directory was not created");
    }

    File[] deployedFiles = distHome.listFiles();
    if (deployedFiles == null || deployedFiles.length == 0) {
      throw new IllegalStateException("No files were deployed");
    }

    fileLogger.info("✅ Deployment validation completed. {} items deployed", deployedFiles.length);
  }

  // ========== Utility Methods ==========

  private boolean execute(String operationName, ExecuteOperation operation) {
    try {
      boolean success = operation.execute();
      if (success) {
        fileLogger.info("{} operation succeeded", operationName);
        return true;
      }
      fileLogger.warn("{} operation failed", operationName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fileLogger.error("{} operation interrupted", operationName);
      return false;
    } catch (Exception e) {
      fileLogger.error("{} operation failed: {}", operationName, e.getMessage());
    }
    return false;
  }

  private void findBuildArtifacts(
      List<File> artifacts, File directory, int currentDepth, int maxDepth) {
    if (currentDepth >= maxDepth || !directory.isDirectory()) {
      return;
    }

    File[] files = directory.listFiles();
    if (files == null) {
      return;
    }

    for (File file : files) {
      if (file.isDirectory()) {
        if ("target".equals(file.getName())) {
          findArtifactsInTargetDirectory(artifacts, file);
        } else if (!file.getName().startsWith(".")) {
          // Recursive search in non-hidden directories
          findBuildArtifacts(artifacts, file, currentDepth + 1, maxDepth);
        }
      }
    }
  }

  private void findArtifactsInTargetDirectory(List<File> artifacts, File targetDir) {
    File[] files = targetDir.listFiles();
    if (files == null) {
      return;
    }

    File tarGzFile = null;
    File jarFile = null;

    for (File file : files) {
      String fileName = file.getName();

      // Priority 1: tar.gz files
      if (fileName.endsWith(".tar.gz")) {
        tarGzFile = file;
        break; // tar.gz has highest priority
      }

      // Priority 2: JAR files (excluding sources and original)
      if (fileName.endsWith(".jar")
          && !fileName.startsWith("original-")
          && !fileName.endsWith("-sources.jar")
          && !fileName.endsWith("-javadoc.jar")) {

        if (jarFile == null || file.length() > jarFile.length()) {
          jarFile = file; // Keep the largest JAR
        }
      }
    }

    File selectedArtifact = tarGzFile != null ? tarGzFile : jarFile;
    if (selectedArtifact != null) {
      artifacts.add(selectedArtifact);
      fileLogger.info(
          "📄 Found build artifact: {} ({})",
          selectedArtifact.getName(),
          formatFileSize(selectedArtifact.length()));
    }
  }

  private void logDirectoryStructure(
      File directory, String indent, int currentDepth, int maxDepth) {
    if (currentDepth >= maxDepth || !directory.isDirectory()) {
      return;
    }

    File[] files = directory.listFiles();
    if (files == null) {
      return;
    }

    for (File file : files) {
      // Only show git-related files and important build files
      if (file.getName().startsWith(".git")
          || file.getName().equals("pom.xml")
          || file.getName().equals("build.gradle")
          || file.getName().equals("target")
          || file.getName().equals("src")) {

        String type = file.isDirectory() ? "/" : "";
        fileLogger.info("{}├── {}{}", indent, file.getName(), type);

        if (file.isDirectory() && currentDepth < maxDepth - 1) {
          logDirectoryStructure(file, indent + "│   ", currentDepth + 1, maxDepth);
        }
      }
    }
  }

  // ========== State Management ==========

  private void updateBuildState(BuildState state) {
    try {
      stateUpdateConsumer.accept(state);
    } catch (Exception e) {
      log.warn("Failed to update build state to {}: {}", state, e.getMessage());
    }
  }

  private void handleBuildSuccess() {
    updateBuildState(BuildState.SUCCESSFUL);
    fileLogger.info("=== BUILD SUCCESSFUL ===");
    fileLogger.info(
        "Project {} built successfully in {}",
        project.getName(),
        formatDuration(Duration.between(startTime, LocalDateTime.now())));

    try {
      notifyReleaseConsumer.accept(fileLogger);
    } catch (Exception e) {
      log.warn("Failed to notify release consumer: {}", e.getMessage());
    }
  }

  private void handleBuildFailure(String message) {
    handleBuildFailure(message, null);
  }

  private void handleBuildFailure(String message, Throwable cause) {
    updateBuildState(BuildState.FAILED);
    fileLogger.error("=== BUILD FAILED ===");
    fileLogger.error("Project {} build failed: {}", project.getName(), message);

    if (cause != null) {
      fileLogger.error("Cause: {}", cause.getMessage());
    }
  }

  private void handleBuildInterruption() {
    updateBuildState(BuildState.FAILED);
    fileLogger.warn("=== BUILD INTERRUPTED ===");
    fileLogger.warn("Project {} build was interrupted", project.getName());
  }

  // ========== Logging Methods ==========

  private void logBuildStart() {
    fileLogger.info("===============================================");
    fileLogger.info("StreamPark Project Build Started");
    fileLogger.info("===============================================");
    fileLogger.info("Project: {}", project.getName());
    fileLogger.info("ID: {}", project.getId());
    fileLogger.info("Start Time: {}", startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    fileLogger.info("Repository: {}", sanitizeUrl(project.getUrl()));
    fileLogger.info("Branch/Tag: {}", StringUtils.defaultIfBlank(project.getRefs(), "default"));
    fileLogger.info("Build Args: {}", sanitizeBuildCommand(project.getMavenBuildArgs()));
    fileLogger.info("===============================================");
    fileLogger.info(project.getLog4BuildStart());
  }

  private void logBuildSummary() {
    LocalDateTime endTime = LocalDateTime.now();
    Duration totalDuration = Duration.between(startTime, endTime);

    fileLogger.info("===============================================");
    fileLogger.info("Build Summary for Project: {}", project.getName());
    fileLogger.info("===============================================");
    fileLogger.info("Total Duration: {}", formatDuration(totalDuration));
    fileLogger.info("End Time: {}", endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    fileLogger.info("===============================================");
  }

  private void logCloneSuccess() {
    if (cloneStartTime != null) {
      Duration cloneDuration = Duration.between(cloneStartTime, LocalDateTime.now());
      fileLogger.info("Git clone completed successfully in {}", formatDuration(cloneDuration));
      fileLogger.info(
          String.format("[StreamPark] project [%s] git clone successful!", project.getName()));
    }
  }

  private void logCloneError(String message, Exception e) {
    fileLogger.error("[StreamPark] {}: {}", message, e.getMessage());
    fileLogger.error(
        "Project: {}, Refs: {}, URL: {}",
        project.getName(),
        project.getRefs(),
        sanitizeUrl(project.getUrl()));
  }

  private void logBuildSuccess() {
    if (buildStartTime != null) {}
  }

  private void logBuildError(String message, Exception e) {
    fileLogger.error("[StreamPark] {}: {}", message, e.getMessage());
    fileLogger.error(
        "Project: {}, Working Directory: {}", project.getName(), project.getMavenWorkHome());
  }

  private void logDeploySuccess() {
    if (deployStartTime != null) {
      Duration deployDuration = Duration.between(deployStartTime, LocalDateTime.now());
      fileLogger.info("Deployment completed successfully in {}", formatDuration(deployDuration));
    }
  }

  private void logDeployError(String message, Exception e) {
    fileLogger.error("[StreamPark] {}: {}", message, e.getMessage());
    fileLogger.error(
        "Project: {}, Dist Home: {}", project.getName(), project.getDistHome().getAbsolutePath());
  }

  // ========== Validation Methods ==========

  private Project validateProject(Project project) {
    Objects.requireNonNull(project, "Project cannot be null");

    if (project.getId() == null) {
      throw new IllegalArgumentException("Project ID cannot be null");
    }

    if (StringUtils.isBlank(project.getName())) {
      throw new IllegalArgumentException("Project name cannot be blank");
    }

    return project;
  }

  private void validateLogPath(String logPath) {
    if (StringUtils.isBlank(logPath)) {
      throw new IllegalArgumentException("Log path cannot be blank");
    }

    try {
      Path path = Paths.get(logPath);
      Path parent = path.getParent();
      if (parent != null && !Files.exists(parent)) {
        Files.createDirectories(parent);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid log path: " + logPath, e);
    }
  }

  // ========== Utility Helper Methods ==========

  private void ensureDirectoryExists(File directory) throws IOException {
    if (!directory.exists() && !directory.mkdirs()) {
      throw new IOException("Failed to create directory: " + directory.getAbsolutePath());
    }
  }

  private void cleanupResources() {
    // Cleanup any temporary files or resources
    try {
      // Force garbage collection to clean up any file handles
      System.gc();
    } catch (Exception e) {
      log.debug("Error during resource cleanup: {}", e.getMessage());
    }
  }

  private String sanitizeUrl(String url) {
    if (StringUtils.isBlank(url)) {
      return "[BLANK_URL]";
    }

    // Remove credentials from URL for logging
    return url.replaceAll("://[^@/]+@", "://***@");
  }

  private String sanitizeBuildCommand(String command) {
    if (StringUtils.isBlank(command)) {
      return "[NO_COMMAND]";
    }

    // Remove sensitive information from build commands
    return SENSITIVE_INFO_PATTERN.matcher(command).replaceAll("***");
  }

  private String sanitizeBuildOutput(String output) {
    if (StringUtils.isBlank(output)) {
      return "";
    }

    // Remove sensitive information from build output
    return SENSITIVE_INFO_PATTERN.matcher(output).replaceAll("***");
  }

  private String formatDuration(Duration duration) {
    long seconds = duration.getSeconds();
    long minutes = seconds / 60;
    seconds = seconds % 60;

    if (minutes > 0) {
      return String.format("%dm %ds", minutes, seconds);
    } else {
      return String.format("%ds", seconds);
    }
  }

  private String formatFileSize(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.1f KB", bytes / 1024.0);
    } else {
      return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }
  }

  /**
   * Truncates a string to the specified length, adding ellipsis if necessary.
   *
   * @param str the string to truncate
   * @param maxLength maximum length allowed
   * @return truncated string
   */
  private String truncateString(String str, int maxLength) {
    if (str == null) {
      return "";
    }
    if (str.length() <= maxLength) {
      return str;
    }
    return str.substring(0, Math.max(0, maxLength - 3)) + "...";
  }

  /** Functional interface for retryable operations. */
  @FunctionalInterface
  private interface ExecuteOperation {

    boolean execute() throws Exception;
  }
}
