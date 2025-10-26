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

import org.apache.streampark.common.util.HadoopConfigUtils;
import org.apache.streampark.common.util.HadoopUtils;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.exception.PermissionDeniedException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.enums.UserType;
import org.apache.streampark.console.core.service.ApplicationLogService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.ProxyService;
import org.apache.streampark.console.core.service.ServiceHelper;
import org.apache.streampark.console.core.task.FlinkK8sWatcherWrapper;
import org.apache.streampark.console.system.authentication.JWTUtil;
import org.apache.streampark.console.system.entity.Member;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.MemberService;
import org.apache.streampark.console.system.service.UserService;
import org.apache.streampark.flink.kubernetes.FlinkK8sWatcher;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;

@Slf4j
@Service
public class ProxyServiceImpl implements ProxyService {

  @Autowired private ServiceHelper serviceHelper;

  @Autowired private ApplicationService applicationService;

  @Autowired private FlinkClusterService flinkClusterService;

  @Autowired private MemberService memberService;

  @Autowired private ApplicationLogService logService;

  @Autowired private UserService userService;

  @Autowired private FlinkK8sWatcher flinkK8sWatcher;

  @Autowired private FlinkK8sWatcherWrapper k8sWatcherWrapper;

  private final RestTemplate proxyRestTemplate;

  private String httpAuthUsername = "";

  public ProxyServiceImpl(RestTemplateBuilder restTemplateBuilder) {
    this.proxyRestTemplate =
        restTemplateBuilder
            .errorHandler(
                new DefaultResponseErrorHandler() {
                  @Override
                  public void handleError(@Nonnull ClientHttpResponse response) {
                    // Ignore errors in the Flink Web UI itself, such as 404 errors.
                  }
                })
            .build();
  }

  @Override
  public ResponseEntity<?> proxyFlink(HttpServletRequest request, Long appId) throws Exception {
    ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);

    Application app = applicationService.getById(appId);
    checkProxyApp(app);
    String url = null;
    switch (app.getExecutionModeEnum()) {
      case YARN_PER_JOB:
      case YARN_APPLICATION:
      case YARN_SESSION:
        String yarnURL = YarnUtils.getRMWebAppProxyURL();
        url = yarnURL + "/proxy/" + app.getClusterId();
        url += getRequestURL(request, "/proxy/flink/" + appId);
        return proxyYarnRequest(request, url);
      case REMOTE:
        FlinkCluster cluster = flinkClusterService.getById(app.getFlinkClusterId());
        url = cluster.getAddress();
        break;
      case KUBERNETES_NATIVE_APPLICATION:
      case KUBERNETES_NATIVE_SESSION:
        url = flinkK8sWatcher.getRemoteRestUrl(k8sWatcherWrapper.toTrackId(app));
        break;
    }

    if (url == null) {
      ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
      builder.body("The flink job manager url is not ready");
      return builder.build();
    }

    url += getRequestURL(request, "/proxy/flink/" + appId);
    return proxyRequest(request, url);
  }

  @Override
  public ResponseEntity<?> proxyYarn(HttpServletRequest request, Long logId) throws Exception {
    ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
    ApplicationLog log = logService.getById(logId);
    if (log == null) {
      return builder.body("The application log not found.");
    }
    checkProxyAppLog(log);
    String yarnId = log.getYarnAppId();
    String yarnURL = YarnUtils.getRMWebAppProxyURL();
    String url = yarnURL + "/proxy/" + yarnId + "/";
    url += getRequestURL(request, "/proxy/yarn/" + logId);
    return proxyYarnRequest(request, url);
  }

  @Override
  public ResponseEntity<?> proxyHistory(HttpServletRequest request, Long logId) throws Exception {
    ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);

    ApplicationLog log = logService.getById(logId);
    if (log == null) {
      return builder.body("The application log not found.");
    }
    checkProxyAppLog(log);
    String url = log.getJobManagerUrl();
    if (StringUtils.isBlank(url)) {
      return builder.body("The jobManager url is null.");
    }
    url += getRequestURL(request, "/proxy/history/" + logId);
    return proxyRequest(request, url);
  }

  @Override
  public ResponseEntity<?> proxyCluster(HttpServletRequest request, Long clusterId)
      throws Exception {
    ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
    FlinkCluster cluster = flinkClusterService.getById(clusterId);
    if (cluster == null) {
      return builder.body("The cluster not found.");
    }
    String url = cluster.getAddress();
    if (StringUtils.isBlank(url)) {
      return builder.body("The cluster address is invalid.");
    }
    url += getRequestURL(request, "/proxy/cluster/" + clusterId);
    return proxyRequest(request, url);
  }

  public void checkProxyApp(Application app) {
    if (app == null) {
      throw new PermissionDeniedException("Invalid operation, application is invalid.");
    }
    String token = serviceHelper.getAuthorization();
    if (token != null) {
      Long userId = JWTUtil.getUserId(token);
      if (userId != null && !userId.equals(app.getUserId())) {
        User user = userService.getById(userId);
        if (user != null && user.getUserType() != UserType.ADMIN) {
          Member member = memberService.findByUserId(app.getTeamId(), userId);
          if (member == null) {
            throw new PermissionDeniedException(
                "Permission denied, this job not created by the current user, And the job cannot be found in the current user's team.");
          }
        }
      }
    }
  }

  public void checkProxyAppLog(ApplicationLog log) {
    if (log == null) {
      throw new PermissionDeniedException("Invalid operation, The application log not found.");
    }
    Application app = applicationService.getById(log.getAppId());
    checkProxyApp(app);
  }

  private HttpEntity<?> getRequestEntity(HttpServletRequest request, String url) throws Exception {
    HttpHeaders headers = new HttpHeaders();
    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String headerName = headerNames.nextElement();
      headers.set(headerName, request.getHeader(headerName));
    }

    // Ensure the Host header is set correctly.
    URI uri = new URI(url);
    headers.set("Host", uri.getHost());
    byte[] body = null;
    if (request.getInputStream().available() > 0) {
      InputStream inputStream = request.getInputStream();
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      IOUtils.copy(inputStream, byteArrayOutputStream);
      body = byteArrayOutputStream.toByteArray();
    }
    return new HttpEntity<>(body, headers);
  }

  private ResponseEntity<?> proxyRequest(HttpServletRequest request, String url) throws Exception {
    return proxy(request, url, getRequestEntity(request, url));
  }

  private ResponseEntity<?> proxyYarnRequest(HttpServletRequest request, String url)
      throws Exception {
    if (YarnUtils.hasYarnHttpKerberosAuth()) {
      UserGroupInformation ugi = HadoopUtils.getUgi();
      HttpEntity<?> requestEntity = getRequestEntity(request, url);
      setRestTemplateCredentials(ugi.getShortUserName());
      return ugi.doAs(
          (PrivilegedExceptionAction<ResponseEntity<?>>) () -> proxy(request, url, requestEntity));
    }

    if (YarnUtils.hasYarnHttpSimpleAuth()) {
      String urlTemplate =
          StringUtils.isNotBlank(request.getQueryString()) ? "%s&user.name=%s" : "%s?user.name=%s";
      return proxyRequest(
          request, String.format(urlTemplate, url, HadoopConfigUtils.hadoopUserName()));
    }
    return proxyRequest(request, url);
  }

  private ResponseEntity<?> proxy(
      HttpServletRequest request, String url, HttpEntity<?> requestEntity) {
    try {
      return proxyRestTemplate.exchange(
          url, HttpMethod.valueOf(request.getMethod()), requestEntity, byte[].class);
    } catch (RestClientException e) {
      log.error("Proxy url: {} failed. ", url, e);
      return new ResponseEntity<>(HttpStatus.BAD_GATEWAY);
    }
  }

  private String getRequestURL(HttpServletRequest request, String replaceString) {
    String url =
        request.getRequestURI()
            + (request.getQueryString() != null ? "?" + request.getQueryString() : "");
    return url.replace(replaceString, "");
  }

  /**
   * Configures the RestTemplate's HttpClient connector. This method is primarily used to configure
   * the HttpClient authentication information and SSL certificate validation policies.
   *
   * @param username The username for HTTP basic authentication.
   */
  private void setRestTemplateCredentials(String username) {
    // Check if the username is not null and has changed since the last configuration
    if (username != null && !this.httpAuthUsername.equals(username)) {
      // Create a new credentials provider
      BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      // Add the username and password for HTTP basic authentication
      credentialsProvider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(username, null));
      // Customize the HttpClient with the credentials provider
      CloseableHttpClient httpClient =
          HttpClients.custom().setDefaultCredentialsProvider(credentialsProvider).build();
      // Set the HttpClient request factory for the RestTemplate
      this.proxyRestTemplate.setRequestFactory(
          new HttpComponentsClientHttpRequestFactory(httpClient));
      // Update the last known username
      this.httpAuthUsername = username;
    }
  }
}
