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

FROM apache/streampark:ci as base-image
FROM flink:1.20.1-scala_2.12-java8 as flink-image

# Install streampark
FROM sbloodys/hadoop:3.3.6
COPY --from=base-image /streampark /streampark
RUN sudo chown -R hadoop.hadoop /streampark \
    && sed -i "s/hadoop-user-name: hdfs$/hadoop-user-name: hadoop/g" /streampark/conf/config.yaml

# Install Flink
COPY --from=flink-image /opt/flink /flink-1.20.1
RUN sudo chown -R hadoop.hadoop /flink-1.20.1

# Install javac
ARG TARGETPLATFORM
RUN echo "TARGETPLATFORM: $TARGETPLATFORM"
RUN \
    if [ "$TARGETPLATFORM" = "linux/arm64" ];then \
        sudo rm -f /etc/yum.repos.d/*.repo; \
        sudo wget http://mirrors.aliyun.com/repo/Centos-altarch-7.repo -O /etc/yum.repos.d/CentOS-Base.repo; \
        sudo sed -i "s/http:\/\//https:\/\//g" /etc/yum.repos.d/CentOS-Base.repo; \
        sudo yum install -y java-1.8.0-openjdk-devel; \
    elif [ "$TARGETPLATFORM" = "linux/amd64" ];then \
        sudo yum install -y java-1.8.0-openjdk-devel; \
    else \
        echo "unknown TARGETPLATFORM: $TARGETPLATFORM"; \
        exit 2;  \
    fi
