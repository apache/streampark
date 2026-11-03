/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
export default {
  systemSetting: 'System Setting',
  systemSettingItems: {
    mavenSetting: {
      name: 'Maven Setting',
    },
    dockerSetting: {
      name: 'Docker Setting',
    },
    emailSetting: {
      name: 'Sender Email Setting',
    },
    consoleSetting: {
      name: 'Console Setting',
    },
    ingressSetting: {
      name: 'Ingress Setting',
    },
    lineageSetting: {
      name: 'Gravitino Lineage Setting',
    },
  },
  update: {
    success: 'Setting updated successfully',
    dockerNotStart: 'Please make sure your Docker service has been started. 🙂',
  },
  title: {
    mavenSettings: 'Maven Settings File Path',
    mavenRepository: 'Maven Central Repository',
    mavenUser: 'Maven Central Repository Auth User',
    mavenPassword: 'Maven Central Repository Auth Password',
    docker: 'Docker Setting',
    email: 'Alert Mailbox Setting',
    ingress: 'Ingress domain address Setting',
  },
  desc: {
    mavenSettings: 'Specify the full path to the local maven settings.xml',
    mavenRepository: 'Maven central repository, Specify a repository address for maven.',
    mavenUser: 'Maven central repository authentication username',
    mavenPassword: 'Maven central repository authentication password',
    docker: 'Docker container service settings',
    email: 'Alert Mailbox settings used to sent alert email',
    ingress: 'Automatically generate an nginx-based ingress by passing in a domain name',
  },
  docker: {
    address: {
      label: 'Docker Address',
      desc: 'Docker container service address',
    },
    namespace: {
      label: 'Docker namespace',
      desc: 'Namespace for docker image used in docker building env and target image register',
    },
    username: {
      label: 'Docker Username',
      desc: 'Docker container service authentication username',
    },
    password: {
      label: 'Docker Password',
      desc: 'Docker container service authentication password',
    },
  },
  email: {
    host: {
      label: 'Smtp Host',
      desc: 'Alert Mailbox Smtp Host',
    },
    port: {
      label: 'Smtp Port',
      desc: 'Smtp Port of the mailbox',
    },
    from: {
      label: 'Email Address',
      desc: 'Your email address used to send alert email',
    },
    userName: {
      label: 'Email Username',
      desc: 'Authentication username used to send email，generally: your email address',
    },
    password: {
      label: 'Email Password',
      desc: 'Authentication password used to send alert email',
    },
    ssl: {
      label: 'SSL enable',
      desc: 'Whether to enable SSL in the mailbox that sends the alert',
    },
  },
  lineage: {
    address: {
      label: 'Gravitino Address',
      desc: 'Base URL of the Gravitino server lineage events are reported to, e.g. http://host:8090',
    },
    token: {
      label: 'Gravitino Auth Token',
      desc: 'Bearer token forwarded to Gravitino, required once Gravitino oauth authentication is enabled',
    },
    namespace: {
      label: 'Gravitino Lineage Namespace',
      desc: 'OpenLineage job/dataset namespace StreamPark reports under',
    },
    nativeListenerEnable: {
      label: 'Enable Flink Native Lineage Listener',
      desc: 'Whether to also inject the official openlineage-flink listener config; only takes effect once Gravitino Address is set',
    },
  },
};
