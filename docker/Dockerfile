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

# Install basic tools and dependencies
RUN apt update && \
    apt install -y wget curl vim net-tools iputils-ping software-properties-common ca-certificates gnupg && \
    add-apt-repository ppa:openjdk-r/ppa -y && \
    apt update && \
    apt install -y openjdk-8-jdk && \
    # Set up Java 8 symlink
    JAVA_PATH=$(ls -d /usr/lib/jvm/java-8-openjdk-*) && \
    ln -s "$JAVA_PATH" /usr/lib/jvm/jdk8 && \
    # Clean up to reduce image size
    apt clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/jdk8

# Install docker
RUN install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    # Add the repository to Apt sources:
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt update && \
    # Install the Docker packages.
    apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin && \
    apt clean && rm -rf /var/lib/apt/lists/* \


# Install Tini
ARG TARGETPLATFORM
ENV TINI_VERSION=v0.19.0

RUN echo "TARGET PLATFORM: $TARGETPLATFORM"

RUN case "$TARGETPLATFORM" in \
        "linux/amd64") TINI_PLATFORM=amd64 ;; \
        "linux/arm64") TINI_PLATFORM=arm64 ;; \
        *) echo "Unknown TARGETPLATFORM: $TARGETPLATFORM" && exit 2 ;; \
    esac && \
    wget --no-check-certificate -O /usr/sbin/tini "https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TINI_PLATFORM}" && \
    chmod +x /usr/sbin/tini

# Install StreamPark
COPY dist/apache-streampark*-*-bin.tar.gz /tmp/streampark.tar.gz
RUN tar -zxvf /tmp/streampark.tar.gz -C /opt && \
    mv /opt/apache-streampark*-*-bin /opt/streampark && \
    rm -f /tmp/streampark.tar.gz

ENTRYPOINT ["/usr/sbin/tini", "--", "/streampark/bin/streampark.sh", "start_docker"]
