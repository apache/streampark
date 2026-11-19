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

package org.apache.streampark.common.util;

import org.apache.streampark.common.configuration.Constants;
import org.apache.streampark.common.configuration.GlobalConfiguration;
import org.apache.streampark.common.configuration.option.YarnOptions;

import org.apache.streampark.shaded.org.slf4j.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** YARN cluster utilities. */
public final class YarnUtils {

    private static final Logger LOG =
        StreamParkLoggerFactory.loggerFactory().getLogger(YarnUtils.class.getName());

    private static String rmHttpURL;

    private YarnUtils() {
    }

    public static boolean hasYarnHttpKerberosAuth() {
        return "kerberos"
            .equalsIgnoreCase(GlobalConfiguration.current().get(YarnOptions.HTTP_AUTHENTICATION));
    }

    public static boolean hasYarnHttpSimpleAuth() {
        return "simple"
            .equalsIgnoreCase(GlobalConfiguration.current().get(YarnOptions.HTTP_AUTHENTICATION));
    }

    public static List<ApplicationId> getAppId(String appName) {
        EnumSet<YarnApplicationState> appStates =
            EnumSet.of(
                YarnApplicationState.RUNNING,
                YarnApplicationState.ACCEPTED,
                YarnApplicationState.SUBMITTED);
        try {
            List<ApplicationId> appIds = new ArrayList<>();
            for (ApplicationReport report : HadoopUtils.yarnClient().getApplications(appStates)) {
                if (appName.equals(report.getName())) {
                    appIds.add(report.getApplicationId());
                }
            }
            return appIds;
        } catch (Exception e) {
            LOG.warn("Failed to list YARN applications for appName={}", appName, e);
            return new ArrayList<>();
        }
    }

    public static YarnApplicationState getState(String appId) {
        ApplicationId applicationId = ApplicationId.fromString(appId);
        try {
            ApplicationReport applicationReport =
                HadoopUtils.yarnClient().getApplicationReport(applicationId);
            return applicationReport.getYarnApplicationState();
        } catch (Exception e) {
            LOG.warn("Failed to get YARN application state for appId={}", appId, e);
            return null;
        }
    }

    public static boolean isContains(String appName) {
        try {
            List<ApplicationReport> runningApps =
                HadoopUtils.yarnClient().getApplications(EnumSet.of(YarnApplicationState.RUNNING));
            if (runningApps != null) {
                for (ApplicationReport app : runningApps) {
                    if (appName.equals(app.getName())) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to check whether YARN contains appName={}", appName, e);
        }
        return false;
    }

    public static String getRMWebAppProxyURL() {
        String proxyUrl = GlobalConfiguration.current().get(YarnOptions.PROXY_URL);
        if (StringUtils.isNotBlank(proxyUrl)) {
            return proxyUrl;
        }
        return getRMWebAppURL();
    }

    public static String getRMWebAppURL() {
        return getRMWebAppURL(false);
    }

    /**
     * Resolve the base URL for ResourceManager REST API access.
     *
     * <p>Unlike {@link #getRMWebAppURL()}, this always targets the ResourceManager's own web app
     * address (yarn.resourcemanager.webapp.address) instead of preferring yarn.web-proxy.address.
     * That is because the web proxy only serves /proxy/... paths and does not expose the RM-only
     * REST endpoints such as ws/v1/cluster/apps/...
     */
    public static String getRMWebAppRestURL() {
        Configuration conf = HadoopUtils.hadoopConf();
        boolean useHttps = YarnConfiguration.useHttps(conf);
        String addressKey =
            useHttps ? YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS : YarnConfiguration.RM_WEBAPP_ADDRESS;
        int defaultPort =
            useHttps
                ? YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT
                : YarnConfiguration.DEFAULT_RM_WEBAPP_PORT;
        String protocol = useHttps ? Constants.HTTPS_SCHEMA : Constants.HTTP_SCHEMA;
        java.net.InetSocketAddress inetSocketAddress =
            conf.getSocketAddr(
                addressKey, "0.0.0.0:" + defaultPort, defaultPort);
        java.net.InetSocketAddress address = NetUtils.getConnectAddress(inetSocketAddress);
        return protocol + address.getHostName() + ":" + address.getPort();
    }

    public static String getRMWebAppURL(boolean getLatest) {
        if (rmHttpURL == null || getLatest) {
            synchronized (YarnUtils.class) {
                Configuration conf = HadoopUtils.hadoopConf();
                boolean useHttps = YarnConfiguration.useHttps(conf);
                String addressPrefix;
                String defaultPort;
                String protocol;
                if (useHttps) {
                    addressPrefix = YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS;
                    defaultPort = "8090";
                    protocol = Constants.HTTPS_SCHEMA;
                } else {
                    addressPrefix = YarnConfiguration.RM_WEBAPP_ADDRESS;
                    defaultPort = "8088";
                    protocol = Constants.HTTP_SCHEMA;
                }

                String proxy = conf.get("yarn.web-proxy.address", null);
                if (proxy != null) {
                    rmHttpURL = protocol + proxy;
                } else {
                    String name;
                    if (!HAUtil.isHAEnabled(conf)) {
                        name = addressPrefix;
                    } else {
                        YarnConfiguration yarnConf = new YarnConfiguration(conf);
                        String activeRMId = RMHAUtils.findActiveRMHAId(yarnConf);
                        if (activeRMId == null) {
                            LOG.warn(
                                "[StreamPark] 'findActiveRMHAId' is null,config yarn.acl.enable:{},now http try it.",
                                yarnConf.get("yarn.acl.enable"));
                            Map<String, String> idUrlMap = new HashMap<>();
                            for (String id : HAUtil.getRMHAIds(conf)) {
                                String address = conf.get(HAUtil.addSuffix(addressPrefix, id));
                                if (address == null) {
                                    String hostname =
                                        conf.get(HAUtil.addSuffix("yarn.resourcemanager.hostname", id));
                                    address = hostname + ":" + defaultPort;
                                }
                                idUrlMap.put(protocol + address, id);
                            }
                            activeRMId = null;
                            int rpcTimeoutForChecks =
                                yarnConf.getInt(
                                    CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
                                    CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);
                            for (Map.Entry<String, String> entry : idUrlMap.entrySet()) {
                                String activeUrl = httpTestYarnRMUrl(entry.getKey(), rpcTimeoutForChecks);
                                if (activeUrl != null) {
                                    activeRMId = idUrlMap.get(activeUrl);
                                    break;
                                }
                            }
                        } else {
                            LOG.info("[StreamPark] 'findActiveRMHAId' successful");
                        }
                        if (activeRMId == null) {
                            throw new IllegalArgumentException(
                                "[StreamPark] YarnUtils.getRMWebAppURL: can not found yarn active node");
                        }
                        LOG.info("[StreamPark] Current activeRMHAId: {}", activeRMId);
                        String appActiveRMKey = HAUtil.addSuffix(addressPrefix, activeRMId);
                        String hostnameActiveRMKey =
                            HAUtil.addSuffix(YarnConfiguration.RM_HOSTNAME, activeRMId);
                        if (HAUtil.getConfValueForRMInstance(appActiveRMKey, yarnConf) == null
                            && HAUtil.getConfValueForRMInstance(hostnameActiveRMKey, yarnConf) != null) {
                            LOG.info("[StreamPark] Find rm web address by : {}", hostnameActiveRMKey);
                            name = hostnameActiveRMKey;
                        } else {
                            LOG.info("[StreamPark] Find rm web address by : {}", appActiveRMKey);
                            name = appActiveRMKey;
                        }
                    }

                    java.net.InetSocketAddress inetSocketAddress =
                        conf.getSocketAddr(name, "0.0.0.0:" + defaultPort, Integer.parseInt(defaultPort));

                    java.net.InetSocketAddress address = NetUtils.getConnectAddress(inetSocketAddress);

                    StringBuilder buffer = new StringBuilder(protocol);
                    java.net.InetAddress resolved = address.getAddress();
                    if (resolved != null
                        && !resolved.isAnyLocalAddress()
                        && !resolved.isLoopbackAddress()) {
                        buffer.append(address.getHostName());
                    } else {
                        try {
                            buffer.append(InetAddress.getLocalHost().getCanonicalHostName());
                        } catch (Exception e) {
                            buffer.append(address.getHostName());
                        }
                    }
                    buffer.append(':').append(address.getPort());
                    rmHttpURL = buffer.toString();
                }
                LOG.info("[StreamPark] Yarn resourceManager webapp url:{}", rmHttpURL);
            }
        }
        return rmHttpURL;
    }

    private static String httpTestYarnRMUrl(String url, int timeout) {
        RequestConfig config =
            RequestConfig.custom().setConnectTimeout(timeout, TimeUnit.MILLISECONDS).build();
        return HttpClientUtils.httpGetRequest(url, config);
    }

    public static String getYarnAppTrackingUrl(ApplicationId applicationId) {
        try {
            return HadoopUtils.yarnClient().getApplicationReport(applicationId).getTrackingUrl();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to get Yarn application tracking URL for " + applicationId, e);
        }
    }

    public static String restRequest(String url, Timeout timeout) throws IOException {
        if (url == null) {
            return null;
        }
        if (url.matches("^http(|s)://.*")) {
            try {
                return request(url, timeout);
            } catch (Exception e) {
                if (hasYarnHttpKerberosAuth()) {
                    throw new IOException("yarnUtils authRestRequest error, url: " + url + ", detail: " + e, e);
                }
                throw new IOException("yarnUtils restRequest error, url: " + url + ", detail: " + e, e);
            }
        }
        // RM-only REST endpoints (e.g. ws/v1/cluster/apps/...) are served by the ResourceManager
        // itself, not the web proxy. The web proxy only exposes /proxy/... paths.
        java.util.function.BooleanSupplier isRmRest = () -> url.startsWith("ws/v1");
        try {
            String baseUrl = isRmRest.getAsBoolean() ? getRMWebAppRestURL() : getRMWebAppURL();
            return request(baseUrl + "/" + url, timeout);
        } catch (Exception first) {
            Optional<String> retried =
                Utils.retry(
                    5,
                    Duration.ofSeconds(5),
                    () -> {
                        String retryBaseUrl =
                            isRmRest.getAsBoolean() ? getRMWebAppRestURL() : getRMWebAppURL(true);
                        return request(retryBaseUrl + "/" + url, timeout);
                    });
            if (retried.isPresent()) {
                return retried.get();
            }
            throw new IOException(
                "yarnUtils restRequest retry 5 times all failed. detail: " + first, first);
        }
    }

    private static String request(String reqUrl, Timeout timeout) throws Exception {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout).build();
        if (hasYarnHttpKerberosAuth()) {
            return HadoopUtils.getUgi()
                .doAs(
                    (PrivilegedExceptionAction<String>) () -> HttpClientUtils.httpAuthGetRequest(reqUrl, config));
        }
        String url;
        if (!hasYarnHttpSimpleAuth()) {
            url = reqUrl;
        } else {
            url = reqUrl + "?user.name=" + HadoopConfigUtils.hadoopUserName();
        }
        return HttpClientUtils.httpGetRequest(url, config);
    }
}
