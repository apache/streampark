<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  ~
  -->

Apache StreamPark
==========

<img src="https://streampark.apache.org/image/logo_name.png" alt="StreamPark logo" height="90px" align="right" />

**StreamPark**: a streaming application development framework and cloud-native real-time computing platform.

[![GitHub stars](https://img.shields.io/github/stars/apache/streampark?style=for-the-badge&label=stars)](https://github.com/apache/streampark/stargazers)
[![Latest release](https://img.shields.io/github/v/release/apache/streampark.svg?style=for-the-badge&label=release)](https://github.com/apache/streampark/releases)
[![X Follow](https://img.shields.io/badge/2K%2B-follow?style=for-the-badge&logo=X&label=%40ASFStreamPark)](https://x.com/ASFStreamPark)

## 📊 Overview

**StreamPark** is a streaming application development framework and cloud-native real-time computing platform. Designed to simplify the development and management of streaming applications, StreamPark provides a development framework for constructing stream processing applications using Apache Flink and Apache Spark, along with a professional streaming application management platform. The platform encompasses application development, debugging, interactive querying, deployment, operation, maintenance, and more. Originally named StreamX, the project was renamed StreamPark in August 2022 and officially graduated as an Apache Top-Level Project (TLP) in January 2025.

* **Streaming Application Development Framework**
    * StreamPark provides a Streaming application framework to simplify Apache Flink and Apache Spark application development, offering plug-and-play connectors to reduce the learning curve and development complexity.
* **Real-Time Computing Platform**
    * StreamPark delivers a one-stop real-time computing platform with core capabilities including application development, deployment, management and monitoring, etc.
* **Supports Batch & Streaming**
    * Supports both Apache Flink and Apache Spark, enabling seamless integration of streaming and batch processing, with multi-engine/multi-version support on a single platform.
* **Supports multi-engine/multi-version**
    * StreamPark supports multiple versions of Apache Flink and Apache Spark, enabling users to develop and manage applications for different versions of these streaming engines within a single framework.
* **Multi-environment Compatibility**
    * Compatible with various cluster environments, users can submit Flink and Spark applications to standalone clusters, YARN (Hadoop 2.x/3.x), and Kubernetes.
* **Rich Ecosystems**
    * Compatible with mainstream open-source technologies (e.g., Apache Flink, Apache Spark, Apache Paimon, Apache Doris) and ML/AI ecosystems, ensuring flexible technology adoption.
* **Easy to use**
    * StreamPark is designed to lower the learning curve and entry barrier. Only one service, deployment easy, allowing even beginners to get started within minutes.

<img src="https://streampark.apache.org/image/dashboard-preview.png"/>

## 🚀 QuickStart

#### 🐳 Play StreamPark in Docker

```shell 
  docker run -d -p 10000:10000 apache/streampark:latest
```

---

#### 🖥️ Local Quick Installation Experience

```shell
  curl -L https://streampark.apache.org/quickstart.sh | sh
```
https://github.com/user-attachments/assets/dd7d5a89-bc28-4ccc-9ad5-258925fc4f34

## 🔨 How to Build

```shell
 ./build.sh
```

🗄 how to [Development](https://streampark.apache.org/docs/development/development)

## ⬇️ Downloads

Please head to the [releases page](https://streampark.apache.org/download) to download a release of Apache StreamPark.

## 📚 Docs

[Official Documentation](https://streampark.apache.org/docs/get-started)

## 💋 Our users

Various companies and organizations use Apache StreamPark for research, production and commercial products. Are you using this project? [Welcome to add your company](https://github.com/apache/streampark/issues/163)!

![Our users](https://streampark.apache.org/image/users.png?20250214)


## 🤝 Contribution

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](https://github.com/apache/streampark/pulls)

### 🙋 Submit Pull Request and Issues

You can submit any ideas as [pull requests](https://github.com/apache/streampark/pulls) or as [issues](https://github.com/apache/streampark/issues/new/choose).

> If you're new to posting issues, we ask that you read [*How To Ask Questions The Smart Way*](http://www.catb.org/~esr/faqs/smart-questions.html) (**This guide does not provide actual support services for this project!**), [How to Report Bugs Effectively](http://www.chiark.greenend.org.uk/~sgtatham/bugs.html) prior to posting. Well written bug reports help us help you!

### 🍻 How to Contribute

We welcome your suggestions, comments (including criticisms), comments and contributions. See [How to Contribute](https://streampark.apache.org/community/submit_guide/submit_code) and [Code Submission Guide](https://streampark.apache.org/community/submit_guide/code_style_and_quality_guide)

Thank you to all the people who already contributed to Apache StreamPark!

## 💬 Contact Us

Contact us through the following mailing list.

| Name                                                          | Mailing list                                                                                                                                                                               | 
|:--------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [dev@streampark.apache.org](mailto:dev@streampark.apache.org) | [Subscribe](mailto:dev-subscribe@streampark.apache.org)、[Unsubscribe](mailto:dev-unsubscribe@streampark.apache.org) 、[Archives](http://mail-archives.apache.org/mod_mbox/streampark-dev/) |


## 💿 Social Media

- [X (Twitter)](https://twitter.com/ASFStreamPark)
- [Zhihu](https://www.zhihu.com/people/streampark) (in Chinese)
- [bilibili](https://space.bilibili.com/455330087) (in Chinese)
- WeChat Official Account (in Chinese, scan the QR code to follow)

<img src="https://streampark.apache.org/image/wx_qr.png" alt="Join the Group" height="300px"><br>


## 📜 License

Licensed under the [Apache License, Version 2.0](LICENSE)
