#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM ubuntu:22.04

USER root

ENV LANG=C.UTF-8 \
    JAVA_HOME=/usr/lib/jvm/jdk8

# Build arguments for version management
ARG JAVA_MAJOR_VERSION=8
ARG TINI_VERSION=v0.19.0

# Base system setup
RUN apt update && \
    apt install -y \
        software-properties-common \
        ca-certificates \
        curl \
        gnupg \
        iputils-ping \
        net-tools \
        vim \
        wget && \
    add-apt-repository -y ppa:openjdk-r/ppa && \
    apt update && \
    apt install -y "openjdk-${JAVA_MAJOR_VERSION}-jdk" && \
    rm -rf /var/lib/apt/lists/*

# Configure Java environment
RUN ARCH=$(dpkg --print-architecture); \
    ln -sv "/usr/lib/jvm/java-${JAVA_MAJOR_VERSION}-openjdk-${ARCH}" "${JAVA_HOME}"

# Install docker
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    # Add the repository to Apt sources:
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    tee /etc/apt/sources.list.d/docker.list >/dev/null && \
    apt update && \
    # Install the Docker packages.
    apt install -y \
        docker-ce \
        docker-ce-cli \
        containerd.io \
        docker-buildx-plugin \
        docker-compose-plugin && \
    rm -rf /var/lib/apt/lists/*

# Install Tini
ARG TARGETPLATFORM
RUN case "${TARGETPLATFORM}" in \
        "linux/amd64") ARCH=amd64 ;; \
        "linux/arm64") ARCH=arm64 ;; \
        *) echo "Unsupported platform: ${TARGETPLATFORM}"; exit 1 ;; \
    esac; \
    wget --no-check-certificate \
        -O /usr/sbin/tini \
        "https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH}" && \
    chmod +x /usr/sbin/tini

# Install StreamPark
COPY dist/apache-streampark*-*-bin.tar.gz /
RUN tar -zxvf apache-streampark*-*-bin.tar.gz \
    && mv apache-streampark*-*-bin streampark \
    && rm -f apache-streampark*-*-bin.tar.gz

ENTRYPOINT ["/usr/sbin/tini", "--", "/streampark/bin/streampark.sh", "start_docker"]
