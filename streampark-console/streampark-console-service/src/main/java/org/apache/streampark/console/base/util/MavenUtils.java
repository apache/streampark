package org.apache.streampark.console.base.util;

import org.apache.streampark.common.conf.CommonConfig;
import org.apache.streampark.common.conf.InternalConfigHolder;
import org.apache.streampark.common.util.Utils;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class MavenUtils {

  // Security constants for Maven parameter validation
  private static final int MAX_BUILD_ARGS_LENGTH = 2048;
  private static final int MAX_MAVEN_ARG_LENGTH = 512;
  private static final Pattern COMMAND_INJECTION_PATTERN =
      Pattern.compile(
          "(`[^`]*`)|"
              + // Backticks
              "(\\$\\([^)]*\\))|"
              + // $() command substitution
              "(\\$\\{[^}]*})|"
              + // ${} variable substitution
              "([;&|><])|"
              + // Command separators and redirects
              "(\\\\[nrt])|"
              + // Escaped newlines/tabs
              "([\\r\\n\\t])|"
              + // Actual control characters
              "(\\\\x[0-9a-fA-F]{2})|"
              + // Hex encoded characters
              "(%[0-9a-fA-F]{2})|"
              + // URL encoded characters
              "(\\\\u[0-9a-fA-F]{4})", // Unicode encoded characters
          Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

  // Whitelist of allowed Maven arguments
  private static final Set<String> ALLOWED_MAVEN_ARGS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "-D",
                  "--define",
                  "-P",
                  "--activate-profiles",
                  "-q",
                  "--quiet",
                  "-X",
                  "--debug",
                  "-e",
                  "--errors",
                  "-f",
                  "--file",
                  "-s",
                  "--settings",
                  "-t",
                  "--toolchains",
                  "-T",
                  "--threads",
                  "-B",
                  "--batch-mode",
                  "-V",
                  "--show-version",
                  "-U",
                  "--update-snapshots",
                  "-N",
                  "--non-recursive",
                  "-C",
                  "--strict-checksums",
                  "-c",
                  "--lax-checksums",
                  "-o",
                  "--offline",
                  "--no-snapshot-updates",
                  "--fail-at-end",
                  "--fail-fast",
                  "--fail-never",
                  "--resume-from",
                  "--projects",
                  "--also-make",
                  "--also-make-dependents",
                  "clean",
                  "compile",
                  "test",
                  "package",
                  "install",
                  "deploy",
                  "validate",
                  "initialize",
                  "generate-sources",
                  "process-sources",
                  "generate-resources",
                  "process-resources",
                  "process-classes",
                  "generate-test-sources",
                  "process-test-sources",
                  "generate-test-resources",
                  "process-test-resources",
                  "test-compile",
                  "process-test-classes",
                  "prepare-package",
                  "pre-integration-test",
                  "integration-test",
                  "post-integration-test",
                  "verify",
                  "install",
                  "deploy")));

  // Allowed system properties (commonly used in Maven builds)
  private static final Set<String> ALLOWED_SYSTEM_PROPERTIES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "skipTests",
                  "maven.test.skip",
                  "maven.javadoc.skip",
                  "maven.source.skip",
                  "project.build.sourceEncoding",
                  "project.reporting.outputEncoding",
                  "maven.compiler.source",
                  "maven.compiler.target",
                  "maven.compiler.release",
                  "flink.version",
                  "scala.version",
                  "hadoop.version",
                  "kafka.version",
                  "java.version",
                  "encoding",
                  "file.encoding")));

  @JsonIgnore
  public static String getMavenArgs(String buildArgs) {
    try {
      StringBuilder mvnArgBuffer = new StringBuilder(" clean package -DskipTests ");

      // Apply security validation to build arguments
      if (StringUtils.isNotBlank(buildArgs)) {
        String validatedBuildArgs = validateAndSanitizeBuildArgs(buildArgs.trim());
        if (StringUtils.isNotBlank(validatedBuildArgs)) {
          mvnArgBuffer.append(validatedBuildArgs);
        }
      }

      // Enhanced --settings validation with path security
      String setting = InternalConfigHolder.get(CommonConfig.MAVEN_SETTINGS_PATH());
      if (StringUtils.isNotBlank(setting)) {
        String validatedSettingsPath = validateAndSanitizeSettingsPath(setting);
        mvnArgBuffer.append(" --settings ").append(validatedSettingsPath);
      }

      // Get final Maven arguments string
      String mvnArgs = mvnArgBuffer.toString();

      // Enhanced security checks
      validateFinalMavenCommand(mvnArgs);

      // Find and validate Maven executable with enhanced security
      String mvn = getSecureMavenExecutable();

      log.info(
          "🔧 Secure Maven command prepared: {} {}",
          sanitizeForLogging(mvn),
          sanitizeForLogging(mvnArgs));
      return mvn.concat(mvnArgs);

    } catch (SecurityException e) {
      log.error("🚨 Security validation failed for Maven build: {}", e.getMessage());
      throw new IllegalArgumentException(
          "Maven build security validation failed: " + e.getMessage(), e);
    } catch (Exception e) {
      log.error("❌ Failed to prepare Maven command: {}", e.getMessage());
      throw new IllegalArgumentException("Failed to prepare Maven command: " + e.getMessage(), e);
    }
  }

  /**
   * Validates and sanitizes Maven settings file path to prevent path traversal attacks.
   *
   * @param settingsPath the Maven settings file path
   * @return validated and normalized settings path
   * @throws SecurityException if path validation fails
   */
  private static String validateAndSanitizeSettingsPath(String settingsPath) {
    if (StringUtils.isBlank(settingsPath)) {
      logSecurityEvent("ERROR", "PATH_VALIDATION", "Settings path is blank", "");
      throw new SecurityException("Settings path cannot be blank");
    }

    logSecurityEvent(
        "INFO", "PATH_VALIDATION", "Starting Maven settings path validation", settingsPath);

    try {
      // Normalize the path to resolve any relative components
      Path normalizedPath = Paths.get(settingsPath).normalize();
      String pathString = normalizedPath.toString();

      // Security checks for path traversal
      if (pathString.contains("..") || pathString.contains("~")) {
        logSecurityEvent(
            "VIOLATION",
            "PATH_TRAVERSAL_DETECTION",
            "Path traversal attempt detected in settings path",
            settingsPath);
        throw new SecurityException("Path traversal detected in settings path");
      }

      // Verify the file exists and is readable
      File file = normalizedPath.toFile();
      if (!file.exists()) {
        throw new SecurityException(
            String.format(
                "Maven settings file does not exist: %s", sanitizeForLogging(pathString)));
      }

      if (!file.isFile()) {
        throw new SecurityException(
            String.format("Maven settings path is not a file: %s", sanitizeForLogging(pathString)));
      }

      if (!file.canRead()) {
        throw new SecurityException(
            String.format(
                "Maven settings file is not readable: %s", sanitizeForLogging(pathString)));
      }

      // Additional security: check file size to prevent DoS
      if (file.length() > 10 * 1024 * 1024) { // 10MB limit
        throw new SecurityException("Maven settings file is too large (>10MB)");
      }

      logSecurityEvent(
          "SUCCESS", "PATH_VALIDATION", "Maven settings path validation completed", pathString);
      return pathString;

    } catch (InvalidPathException e) {
      logSecurityEvent(
          "VIOLATION", "PATH_SYNTAX_ERROR", "Invalid path syntax in settings path", settingsPath);
      throw new SecurityException("Invalid path syntax in settings path", e);
    }
  }

  /**
   * Performs final validation on the complete Maven command string.
   *
   * @param mvnArgs the complete Maven arguments string
   * @throws SecurityException if validation fails
   */
  private static void validateFinalMavenCommand(String mvnArgs) {
    logSecurityEvent(
        "INFO", "FINAL_COMMAND_VALIDATION", "Starting final Maven command validation", mvnArgs);

    // Check for newline characters (existing validation)
    if (mvnArgs.contains("\n") || mvnArgs.contains("\r")) {
      logSecurityEvent(
          "VIOLATION",
          "CONTROL_CHARACTER_DETECTION",
          "Control characters detected in maven command",
          mvnArgs);
      throw new SecurityException("Control characters detected in maven build parameters");
    }

    // Additional validation using enhanced pattern matching
    Matcher dangerousMatcher = COMMAND_INJECTION_PATTERN.matcher(mvnArgs);
    if (dangerousMatcher.find()) {
      String dangerousPattern = dangerousMatcher.group();
      logSecurityEvent(
          "VIOLATION",
          "INJECTION_DETECTION",
          "Command injection pattern in final Maven command",
          dangerousPattern);
      throw new SecurityException("Command injection pattern detected in final Maven command");
    }

    // Validate total command length
    if (mvnArgs.length() > MAX_BUILD_ARGS_LENGTH + 100) { // Allow some buffer for default args
      logSecurityEvent(
          "VIOLATION",
          "COMMAND_LENGTH_EXCEEDED",
          String.format("Maven command exceeds maximum length: %d", mvnArgs.length()),
          mvnArgs);
      throw new SecurityException("Maven command exceeds maximum allowed length");
    }

    logSecurityEvent(
        "SUCCESS", "FINAL_COMMAND_VALIDATION", "Final Maven command validation completed", mvnArgs);
  }

  /**
   * Securely determines the Maven executable with enhanced validation.
   *
   * @return validated Maven executable path
   * @throws SecurityException if Maven executable validation fails
   */
  private static String getSecureMavenExecutable() {
    boolean windows = Utils.isWindows();
    String mvn = windows ? "mvn.cmd" : "mvn";

    // Validate environment variables for potential injection
    String mavenHome = validateMavenHomeEnvironment();

    boolean useWrapper = true;
    if (mavenHome != null) {
      mvn = mavenHome + "/bin/" + mvn;
      try {
        // Test Maven installation with security considerations
        ProcessBuilder pb = new ProcessBuilder(mvn, "--version");
        pb.environment().clear(); // Clear environment to prevent injection
        Process process = pb.start();
        boolean finished = process.waitFor(10, TimeUnit.SECONDS);

        if (finished && process.exitValue() == 0) {
          useWrapper = false;
          log.info("✅ Validated system Maven installation: {}", sanitizeForLogging(mvn));
        } else {
          log.warn("⚠️  System Maven validation failed, using wrapper");
        }
      } catch (Exception e) {
        log.warn("⚠️  Maven validation error: {}, using wrapper", e.getMessage());
      }
    }

    if (useWrapper) {
      String wrapperPath = WebUtils.getAppHome().concat(windows ? "/bin/mvnw.cmd" : "/bin/mvnw");

      // Validate wrapper executable exists and is secure
      File wrapperFile = new File(wrapperPath);
      if (!wrapperFile.exists() || !wrapperFile.canExecute()) {
        throw new SecurityException("Maven wrapper not found or not executable: " + wrapperPath);
      }

      mvn = wrapperPath;
      log.info("✅ Using secure Maven wrapper: {}", sanitizeForLogging(mvn));
    }

    return mvn;
  }

  /**
   * Validates Maven home environment variables for security issues.
   *
   * @return validated Maven home path or null if not set/invalid
   */
  private static String validateMavenHomeEnvironment() {
    String mavenHome = System.getenv("M2_HOME");
    if (mavenHome == null) {
      mavenHome = System.getenv("MAVEN_HOME");
    }

    if (mavenHome == null) {
      return null;
    }

    try {
      // Validate and normalize the Maven home path
      Path normalizedPath = Paths.get(mavenHome).normalize();
      String pathString = normalizedPath.toString();

      // Security checks
      if (pathString.contains("..") || pathString.contains("~")) {
        log.warn("⚠️  Suspicious Maven home path, ignoring: {}", sanitizeForLogging(mavenHome));
        return null;
      }

      File mavenDir = normalizedPath.toFile();
      if (!mavenDir.exists() || !mavenDir.isDirectory()) {
        log.warn("⚠️  Invalid Maven home directory, ignoring: {}", sanitizeForLogging(pathString));
        return null;
      }

      return pathString;

    } catch (InvalidPathException e) {
      log.warn("⚠️  Invalid Maven home path syntax, ignoring: {}", sanitizeForLogging(mavenHome));
      return null;
    }
  }

  /**
   * Logs security events for auditing and monitoring purposes. This method provides comprehensive
   * security logging for Maven build operations.
   *
   * @param eventType the type of security event (SUCCESS, WARNING, VIOLATION, etc.)
   * @param operation the operation being performed (BUILD_VALIDATION, PATH_VALIDATION, etc.)
   * @param details additional details about the event
   * @param sensitiveData any sensitive data that should be sanitized for logging
   */
  private static void logSecurityEvent(
      String eventType, String operation, String details, String sensitiveData) {
    String sanitizedData = sanitizeForLogging(sensitiveData);

    switch (eventType.toUpperCase()) {
      case "SUCCESS":
        log.info(
            "✅ [SECURITY-AUDIT] {} - {}: {} | Data: {}",
            eventType,
            operation,
            details,
            sanitizedData);
        break;
      case "WARNING":
        log.warn(
            "⚠️  [SECURITY-AUDIT] {} - {}: {} | Data: {}",
            eventType,
            operation,
            details,
            sanitizedData);
        break;
      case "VIOLATION":
      case "ERROR":
        log.error(
            "🚨 [SECURITY-AUDIT] {} - {}: {} | Data: {}",
            eventType,
            operation,
            details,
            sanitizedData);
        break;
      default:
        log.info(
            "ℹ️  [SECURITY-AUDIT] {} - {}: {} | Data: {}",
            eventType,
            operation,
            details,
            sanitizedData);
    }

    // Additional structured logging for security monitoring systems
    log.info(
        "SECURITY_EVENT_JSON: {{\"timestamp\":\"{}\",\"event_type\":\"{}\",\"operation\":\"{}\",\"details\":\"{}\",\"source\":\"Project.getMavenArgs\"}}",
        Instant.now().toString(),
        eventType,
        operation,
        details.replace("\"", "\\\""));
  }

  /**
   * Comprehensive security validation for Maven build arguments. Implements multiple layers of
   * security checks to prevent command injection attacks.
   *
   * @param buildArgs the raw build arguments string
   * @return sanitized and validated build arguments
   * @throws SecurityException if dangerous patterns or parameters are detected
   */
  private static String validateAndSanitizeBuildArgs(String buildArgs) {
    if (StringUtils.isBlank(buildArgs)) {
      logSecurityEvent("SUCCESS", "BUILD_VALIDATION", "Empty build arguments processed", "");
      return "";
    }

    logSecurityEvent(
        "INFO", "BUILD_VALIDATION", "Starting validation of build arguments", buildArgs);

    // 1. Length validation to prevent DoS attacks
    if (buildArgs.length() > MAX_BUILD_ARGS_LENGTH) {
      logSecurityEvent(
          "VIOLATION",
          "LENGTH_VALIDATION",
          String.format(
              "Build arguments exceed maximum length. Length: %d, Limit: %d",
              buildArgs.length(), MAX_BUILD_ARGS_LENGTH),
          buildArgs);
      throw new SecurityException(
          "Build arguments exceed maximum allowed length: " + MAX_BUILD_ARGS_LENGTH);
    }

    // 2. Normalize Unicode and decode potential encoding attacks
    String normalizedArgs = normalizeAndDecode(buildArgs);
    if (!normalizedArgs.equals(buildArgs)) {
      logSecurityEvent(
          "WARNING",
          "ENCODING_DETECTION",
          "Encoding or Unicode normalization applied to build arguments",
          String.format("Original: %s | Normalized: %s", buildArgs, normalizedArgs));
    }

    // 3. Advanced command injection pattern detection
    Matcher dangerousMatcher = COMMAND_INJECTION_PATTERN.matcher(normalizedArgs);
    if (dangerousMatcher.find()) {
      String dangerousPattern = dangerousMatcher.group();
      logSecurityEvent(
          "VIOLATION",
          "INJECTION_DETECTION",
          "Command injection pattern detected in build arguments",
          String.format("Pattern: %s | Full args: %s", dangerousPattern, normalizedArgs));
      throw new SecurityException(
          "Dangerous command injection pattern detected: " + sanitizeForLogging(dangerousPattern));
    }

    // 4. Whitelist-based argument validation
    String validatedArgs = validateArgumentsAgainstWhitelist(normalizedArgs);
    logSecurityEvent(
        "SUCCESS",
        "BUILD_VALIDATION",
        "Build arguments validation completed successfully",
        validatedArgs);
    return validatedArgs;
  }

  /**
   * Normalizes Unicode characters and decodes potential encoding attacks.
   *
   * @param input the input string to normalize
   * @return normalized string
   */
  private static String normalizeAndDecode(String input) {
    // Normalize Unicode characters to prevent Unicode-based attacks
    String normalized = Normalizer.normalize(input, Normalizer.Form.NFC);

    // Basic URL decode to catch simple encoding attempts
    normalized =
        normalized
            .replace("%20", " ")
            .replace("%3B", ";")
            .replace("%7C", "|")
            .replace("%26", "&")
            .replace("%3E", ">")
            .replace("%3C", "<")
            .replace("%24", "$")
            .replace("%60", "`");

    return normalized;
  }

  /**
   * Validates Maven arguments against predefined whitelists.
   *
   * @param args the normalized arguments string
   * @return validated arguments string
   * @throws SecurityException if invalid arguments are found
   */
  private static String validateArgumentsAgainstWhitelist(String args) {
    String[] argArray = args.trim().split("\\s+");
    StringBuilder validatedArgs = new StringBuilder();

    for (int i = 0; i < argArray.length; i++) {
      String arg = argArray[i].trim();

      if (StringUtils.isBlank(arg)) {
        continue;
      }

      // Validate individual argument length
      if (arg.length() > MAX_MAVEN_ARG_LENGTH) {
        log.error(
            "🚨 Security Alert: Individual argument exceeds length limit: {}",
            sanitizeForLogging(arg));
        throw new SecurityException(
            "Individual argument exceeds maximum length: " + MAX_MAVEN_ARG_LENGTH);
      }

      if (arg.startsWith("-D")) {
        // Handle system property definitions
        validateSystemProperty(arg, i < argArray.length - 1 ? argArray[i + 1] : null);
        validatedArgs.append(arg).append(" ");

        // Skip next argument if it's the value for this -D parameter
        if (i < argArray.length - 1 && !argArray[i + 1].startsWith("-")) {
          i++; // Skip the value part
          validatedArgs.append(argArray[i]).append(" ");
        }
      } else if (arg.startsWith("--define")) {
        // Handle long form system property definitions
        validateSystemProperty(arg, null);
        validatedArgs.append(arg).append(" ");
      } else if (ALLOWED_MAVEN_ARGS.contains(arg)) {
        // Standard Maven argument
        validatedArgs.append(arg).append(" ");
      } else {
        // Check if it's a Maven lifecycle phase or goal
        if (isValidMavenPhaseOrGoal(arg)) {
          validatedArgs.append(arg).append(" ");
        } else {
          logSecurityEvent(
              "VIOLATION", "WHITELIST_VALIDATION", "Unauthorized Maven argument detected", arg);
          throw new SecurityException("Unauthorized Maven argument: " + sanitizeForLogging(arg));
        }
      }
    }

    return validatedArgs.toString().trim();
  }

  /**
   * Validates system property arguments (-D parameters).
   *
   * @param arg the system property argument
   * @param nextArg the next argument (value) if applicable
   * @throws SecurityException if invalid system property is detected
   */
  private static void validateSystemProperty(String arg, String nextArg) {
    String propertyDefinition = arg;

    if (arg.equals("-D") && nextArg != null) {
      propertyDefinition = nextArg;
    } else if (arg.startsWith("-D")) {
      propertyDefinition = arg.substring(2);
    } else if (arg.startsWith("--define=")) {
      propertyDefinition = arg.substring(9);
    }

    // Extract property name (before = sign)
    String propertyName = propertyDefinition.split("=")[0];

    if (!ALLOWED_SYSTEM_PROPERTIES.contains(propertyName)) {
      // Allow some common patterns but be restrictive
      if (!isValidSystemPropertyPattern(propertyName)) {
        logSecurityEvent(
            "VIOLATION",
            "SYSTEM_PROPERTY_VALIDATION",
            "Unauthorized system property detected",
            propertyName);
        throw new SecurityException(
            "Unauthorized system property: " + sanitizeForLogging(propertyName));
      }
    }
  }

  /**
   * Checks if the property name follows safe patterns.
   *
   * @param propertyName the property name to validate
   * @return true if the property pattern is considered safe
   */
  private static boolean isValidSystemPropertyPattern(String propertyName) {
    // Allow properties that start with safe prefixes
    List<String> safePatterns =
        Arrays.asList(
            "maven.",
            "project.",
            "flink.",
            "scala.",
            "hadoop.",
            "kafka.",
            "java.",
            "user.",
            "file.",
            "encoding");

    return safePatterns.stream().anyMatch(propertyName::startsWith)
        && propertyName.matches("^[a-zA-Z0-9._-]+$"); // Only allow safe characters
  }

  /**
   * Checks if an argument is a valid Maven lifecycle phase or goal.
   *
   * @param arg the argument to check
   * @return true if it's a valid Maven phase or goal
   */
  private static boolean isValidMavenPhaseOrGoal(String arg) {
    // Basic validation: only allow alphanumeric, hyphens, and colons (for plugin goals)
    return arg.matches("^[a-zA-Z0-9:._-]+$") && arg.length() <= 50;
  }

  /**
   * Sanitizes sensitive information for safe logging.
   *
   * @param input the input to sanitize
   * @return sanitized string safe for logging
   */
  private static String sanitizeForLogging(String input) {
    if (input == null) {
      return "null";
    }

    // Mask potential sensitive patterns and limit length
    String sanitized =
        input
            .replaceAll("(password|pwd|secret|token|key)=\\S*", "$1=***")
            .replaceAll("(`[^`]*`)", "***BACKTICK_COMMAND***")
            .replaceAll("(\\$\\([^)]*\\))", "***COMMAND_SUBSTITUTION***")
            .replaceAll("(\\$\\{[^}]*})", "***VARIABLE_SUBSTITUTION***");

    // Limit length for logging
    if (sanitized.length() > 100) {
      sanitized = sanitized.substring(0, 100) + "...";
    }

    return sanitized;
  }
}
