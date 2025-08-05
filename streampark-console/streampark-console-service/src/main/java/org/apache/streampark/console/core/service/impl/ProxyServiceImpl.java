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
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.entity.SparkApplication;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.service.ProxyService;
import org.apache.streampark.console.core.watcher.FlinkK8sWatcherWrapper;
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

    @Autowired
    private FlinkClusterService flinkClusterService;

    @Autowired
    private FlinkK8sWatcher flinkK8sWatcher;

    @Autowired
    private FlinkK8sWatcherWrapper k8sWatcherWrapper;

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
    public ResponseEntity<?> proxyFlink(HttpServletRequest request, FlinkApplication app) throws Exception {
        ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
        String url = null;
        switch (app.getDeployModeEnum()) {
            case YARN_PER_JOB:
            case YARN_APPLICATION:
            case YARN_SESSION:
                String yarnURL = YarnUtils.getRMWebAppProxyURL();
                url = yarnURL + "/proxy/" + app.getClusterId();
                url += getRequestURL(request, "/proxy/flink/" + app.getId());
                return proxyYarnRequest(request, url);
            case REMOTE:
                FlinkCluster cluster = flinkClusterService.getById(app.getFlinkClusterId());
                url = cluster.getAddress();
                break;
            case KUBERNETES_NATIVE_APPLICATION:
            case KUBERNETES_NATIVE_SESSION:
                url = flinkK8sWatcher.getRemoteRestUrl(k8sWatcherWrapper.toTrackId(app));
                break;
            default:
                throw new UnsupportedOperationException(
                    "unsupported deployMode ".concat(app.getDeployModeEnum().getName()));
        }

        if (url == null) {
            ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
            builder.body("The flink job manager url is not ready");
            return builder.build();
        }

        url += getRequestURL(request, "/proxy/flink/" + app.getId());
        return proxyRequest(request, url);
    }

    @Override
    public ResponseEntity<?> proxySpark(HttpServletRequest request, SparkApplication app) throws Exception {
        ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
        switch (app.getDeployModeEnum()) {
            case YARN_CLIENT:
            case YARN_CLUSTER:
                String yarnURL = YarnUtils.getRMWebAppProxyURL();
                String url = yarnURL + "/proxy/" + app.getClusterId();
                url += getRequestURL(request, "/proxy/spark/" + app.getId());
                return proxyYarnRequest(request, url);
            default:
                throw new UnsupportedOperationException(
                    "unsupported deployMode ".concat(app.getDeployModeEnum().getName()));
        }
    }

    @Override
    public ResponseEntity<?> proxyYarn(HttpServletRequest request, ApplicationLog log) throws Exception {
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
        String yarnId = log.getClusterId();
        if (StringUtils.isBlank(yarnId)) {
            return builder.body("The yarn application id is null.");
        }
        String yarnURL = YarnUtils.getRMWebAppProxyURL();
        String url = yarnURL + "/proxy/" + yarnId + "/";
        url += getRequestURL(request, "/proxy/yarn/" + log.getId());
        return proxyYarnRequest(request, url);
    }

    @Override
    public ResponseEntity<?> proxyHistory(HttpServletRequest request, ApplicationLog log) throws Exception {
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);

        String url = log.getTrackingUrl();
        if (StringUtils.isBlank(url)) {
            return builder.body("The jobManager url is null.");
        }
        url += getRequestURL(request, "/proxy/history/" + log.getId());
        return proxyRequest(request, url);
    }

    @Override
    public ResponseEntity<?> proxyFlinkCluster(HttpServletRequest request, Long clusterId) throws Exception {
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE);
        FlinkCluster cluster = flinkClusterService.getById(clusterId);
        if (cluster == null) {
            return builder.body("The cluster not found.");
        }
        String url = cluster.getAddress();
        if (StringUtils.isBlank(url)) {
            return builder.body("The cluster address is invalid.");
        }

        url += getRequestURL(request, "/proxy/flink_cluster/" + clusterId);
        switch (cluster.getFlinkDeployModeEnum()) {
            case YARN_PER_JOB:
            case YARN_APPLICATION:
            case YARN_SESSION:
                return proxyYarnRequest(request, url);
            case REMOTE:
            case KUBERNETES_NATIVE_APPLICATION:
            case KUBERNETES_NATIVE_SESSION:
                return proxyRequest(request, url);
            default:
                throw new UnsupportedOperationException(
                    "unsupported deployMode ".concat(cluster.getFlinkDeployModeEnum().getName()));
        }
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

    private ResponseEntity<?> proxyYarnRequest(HttpServletRequest request, String url) throws Exception {
        if (YarnUtils.hasYarnHttpKerberosAuth()) {
            UserGroupInformation ugi = HadoopUtils.getUgi();
            HttpEntity<?> requestEntity = getRequestEntity(request, url);
            setRestTemplateCredentials(ugi.getShortUserName());
            return ugi.doAs(
                (PrivilegedExceptionAction<ResponseEntity<?>>) () -> proxy(request, url, requestEntity));
        }
        if (YarnUtils.hasYarnHttpSimpleAuth()) {
            return proxyRequest(request, String.format("%s?user.name=%s", url, HadoopConfigUtils.hadoopUserName()));
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
