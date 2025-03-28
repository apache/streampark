FROM docker:28.0.4

RUN apk update && apk add openjdk8 bash && \
    echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk' >> /etc/profile && \

    mkdir -p /apache-streampark_2.12-2.1.5 && \
    mkdir -p /streampark_workspace && \
    mkdir -p ~/.kube && \

    wget https://downloads.apache.org/streampark/2.1.5/apache-streampark_2.12-2.1.5-incubating-bin.tar.gz && \
    tar xf apache-streampark_2.12-2.1.5-incubating-bin.tar.gz -C /apache-streampark_2.12-2.1.5 --strip-components=1 && \
    rm -rf apache-streampark_2.12-2.1.5-incubating-bin.tar.gz && \

    wget -O /apache-streampark_2.12-2.1.5/lib/mysql-connector-java-8.0.28.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar

RUN echo "\
sed -i 's|/tmp/streampark|/streampark_workspace|g' /apache-streampark_2.12-2.1.5/conf/config.yaml && \
sed -i '/^datasource:/,/^streampark:/ s/^  dialect:.*/  dialect: mysql/' /apache-streampark_2.12-2.1.5/conf/config.yaml && \
sed -i '/^datasource:/,/^streampark:/ s/^  username:.*/  username: root/' /apache-streampark_2.12-2.1.5/conf/config.yaml && \
sed -i '/^datasource:/,/^streampark:/ s/^  password:.*/  password: Admin@streampark/' /apache-streampark_2.12-2.1.5/conf/config.yaml && \
sed -i '/^datasource:/,/^streampark:/ s|^  url:.*|  url: jdbc:mysql://streampark-mysql-headless:3306/streampark?useUnicode=true\&characterEncoding=UTF-8\&useJDBCCompliantTimezoneShift=true\&useLegacyDatetimeCode=false\&serverTimezone=GMT%2B8|' /apache-streampark_2.12-2.1.5/conf/config.yaml && \
sed -i '/  url:/a\  driver-class-name: com.mysql.cj.jdbc.Driver' /apache-streampark_2.12-2.1.5/conf/config.yaml && \
sed -i '/  h2-data-dir:/d' /apache-streampark_2.12-2.1.5/conf/config.yaml" > 0.sh && sh 0.sh && rm -rf 0.sh
