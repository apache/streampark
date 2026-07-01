#!/usr/bin/env bash

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

base=$(cd $(dirname $0);pwd)


__cmd__() {
	if which ${1:-"cmd"} >/dev/null 2>&1;then echo 1;else echo 0;fi
}

__get__() {
	local i=$1
	test "x$i" == "x" && read -p "Please enter $2: " -t 30 i
	test "x$i" == "x" && { echo "0x0F";return; }
	echo $i
}
__exit__() {
	test "x$1" == "x0x0F" && { echo "parameter error.">&2;exit; }
}

create_demo_code(){
	local gourp_id=$1
	local module=$2
	local code_dir=$3

cat > $code_dir/Demo.scala <<EOF
package ${group_id}.$module

import org.apache.spark.streaming.StreamingContext
import org.apache.streampark.spark.core.Streaming
import org.apache.streampark.spark.core.support.kafka.KafkaDirectSource

object HelloStreamParkApp extends Streaming {
    override def handle(ssc : StreamingContext): Unit = {
        val source = new KafkaDirectSource[String,String](ssc)
        //val conf = ssc.sparkContext.getConf
        source.getDStream[(String,String)](x => (x.topic,x.value)).foreachRDD((rdd,time) => {
            rdd.take(10).foreach(println)
            source.updateOffsets(time.milliseconds)
        })
    }
}
EOF
}

create_module() {
	local module=$(__get__ "$1" "module name")
	__exit__ "$module"
	local src_dir=src/main/scala
	local code_dir=${src_dir}/${group_id//./\/}/${module}
	local deploy_dir=assembly
	local conf_dir=${deploy_dir}/conf/prod
	local log_dir=${deploy_dir}/logs
	local temp_dir=${deploy_dir}/temp

	mkdir -p ${name}/${module}
	test ! -d ${name}/${module} && { echo "create modulue dir failed.">&2;exit; }

	mkdir -p ${name}/${module}/${code_dir}
	mkdir -p $name/${module}/${src_dir/main/test}
	mkdir -p ${name}/${module}/${conf_dir}
	mkdir -p ${name}/${module}/${log_dir}
	mkdir -p ${name}/${module}/${temp_dir}

    cp -r ${base}/bin ${name}/${module}/assembly/bin
	cp ${name}/default.properties ${name}/${module}/${conf_dir}/${module}.properties

	echo "# $module module documentation" >${name}/${module}/README.md
	cat > ${name}/${module}/assembly.xml <<EOF

<assembly>
    <id>bin</id>
    <formats>
        <format>tar.gz</format>
    </formats>
    <dependencySets>
        <dependencySet>
            <useProjectArtifact>true</useProjectArtifact>
            <outputDirectory>lib</outputDirectory>
            <useProjectAttachments>true</useProjectAttachments>
            <includes>
                <include>org.apache.streampark:streampark-spark</include>
            </includes>
        </dependencySet>
        <dependencySet>
            <!--
               Do not use the artifact of the project, do not decompress the third-party jar
            -->
            <useProjectArtifact>false</useProjectArtifact>
            <outputDirectory>lib</outputDirectory>
            <useProjectAttachments>true</useProjectAttachments>
            <scope>provided</scope>
        </dependencySet>
    </dependencySets>
    <fileSets>
        <fileSet>
            <outputDirectory>/</outputDirectory>
            <includes>
                <include>README.md</include>
            </includes>
        </fileSet>
        <fileSet>
            <directory>\${project.basedir}/assembly</directory>
            <outputDirectory>/</outputDirectory>
        </fileSet>
        <!-- Package the jar file compiled by the project itself into the lib directory of the gz file -->
        <fileSet>
            <directory>\${project.build.directory}</directory>
            <outputDirectory>lib</outputDirectory>
            <includes>
                <include>*.jar</include>
            </includes>
        </fileSet>
    </fileSets>
	<fileSets>
        <fileSet>
            <directory>\${project.build.directory}</directory>
            <outputDirectory>lib</outputDirectory>
            <includes>
                <include>*.jar</include>
            </includes>
        </fileSet>
        <fileSet>
            <directory>assembly/bin</directory>
            <outputDirectory>bin</outputDirectory>
            <fileMode>0755</fileMode>
        </fileSet>
        <fileSet>
            <directory>assembly/conf</directory>
            <outputDirectory>conf</outputDirectory>
            <fileMode>0755</fileMode>
        </fileSet>
        <fileSet>
            <directory>assembly/logs</directory>
            <outputDirectory>logs</outputDirectory>
            <fileMode>0755</fileMode>
        </fileSet>
        <fileSet>
            <directory>assembly/logs</directory>
            <outputDirectory>temp</outputDirectory>
            <fileMode>0755</fileMode>
        </fileSet>
    </fileSets>
</assembly>
EOF

	cat >$name/$module/pom.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>$artifact_id</artifactId>
        <groupId>$group_id</groupId>
        <version>$version</version>
		<relativePath>../pom.xml</relativePath>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>$module</artifactId>

    <dependencies>
        <dependency>
            <groupId>org.apache.streampark</groupId>
            <artifactId>streampark-spark</artifactId>
        </dependency>
    </dependencies>


    <build>
        <plugins>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
            </plugin>
        </plugins>
    </build>

</project>

EOF
create_demo_code $group_id $module $name/$module/$code_dir
}

create_pom() {
	local group_id=$(__get__ "$1" "group id")
	__exit__ "$group_id"
	local artifact_id=$(__get__ "$2" "artifact id")
	__exit__ "$artifact_id"
	local module=$(__get__ "$3" "module name")
	__exit__ "$module"
	local version=$(__get__ "${4:-"1.0"}" "version")
	__exit__ "$version"

cat > $name/pom.xml <<EOF

<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>$group_id</groupId>
    <artifactId>$artifact_id</artifactId>
    <packaging>pom</packaging>
    <version>$version</version>

    <profiles>
        <profile>
            <!--
            mvn clean package
            Direct packaging, only the assembly.xml file will be
            <includes>
                <include>org.apache.streampark:streampark-spark</include>
            </includes>
            The included Jar package is packaged in
             -->
            <id>default</id>
            <properties>
                <scope>compile</scope>
            </properties>
            <activation>
                <activeByDefault>true</activeByDefault>
            </activation>
        </profile>
        <profile>
            <!--
             mvn clean package -Pwithjar -Dmaven.test.skip=true
            Including dependent jar packaging, will be included in the assembly.xml file
            <includes>
                <include>org.apache.streampark.spark:spark-core</include>
            </includes>
            The included Jar is packaged with the jar set in the pom <scope>\${project.scope}</scope>,
            Such a crappy design is mainly because I don't know how to elegantly extract the JarA that depends on running and compiling.
             -->
            <id>withjar</id>
            <properties>
                <scope>provided</scope>
            </properties>
        </profile>
    </profiles>
    <modules>
        <module>$module</module>
    </modules>

    <!-- 定义统一版本号-->
    <properties>
        <streampark.spark.version>1.0.0</streampark.spark.version>

        <spark.version>2.2.0</spark.version>
        <hadoop.version>2.10.1</hadoop.version>
        <hbase.version>1.2.0-cdh5.12.1</hbase.version>
        <hive.version>1.1.0-cdh5.12.1</hive.version>

        <scala.version>2.11.12</scala.version>
        <scala.binary.version>2.11</scala.binary.version>
        <redis.version>2.8.2</redis.version>
        <mysql.version>5.1.6</mysql.version>
        <kafka.version>0.10.2.0</kafka.version>
        <es.version>5.6.3</es.version>
        <protobuf.version>2.5.0</protobuf.version>

        <json4s.version>3.2.10</json4s.version>
        <spray.version>1.3.3</spray.version>
        <akka.version>2.3.9</akka.version>

        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
        <project.build.jdk>11</project.build.jdk>


        <PermGen>64m</PermGen>
        <MaxPermGen>512m</MaxPermGen>
        <CodeCacheSize>512m</CodeCacheSize>

    </properties>


    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.apache.streampark</groupId>
                <artifactId>streampark-spark-core</artifactId>
                <version>\${streampark.spark.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>


    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>

        <resources>
            <resource>
                <directory>src/main/resources</directory>
            </resource>
        </resources>

        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>\${project.build.jdk}</source>
                    <target>\${project.build.jdk}</target>
                </configuration>
            </plugin>

            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                 <version>4.3.0</version>
                <executions>
                    <execution>
                        <id>compile-scala</id>
                        <phase>compile</phase>
                        <goals>
                            <goal>add-source</goal>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>test-compile-scala</id>
                        <phase>test-compile</phase>
                        <goals>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <scalaVersion>\${scala.version}</scalaVersion>
                </configuration>
            </plugin>
        </plugins>

        <pluginManagement>
            <plugins>
                <plugin>
                    <artifactId>maven-assembly-plugin</artifactId>
                    <version>3.0.0</version>
                    <executions>
                        <execution>
                            <id>distro-assembly</id>
                            <phase>package</phase>
                            <goals>
                                <goal>single</goal>
                            </goals>
                        </execution>
                    </executions>
                    <configuration>
                        <appendAssemblyId>false</appendAssemblyId>
                        <descriptors>
                            <descriptor>assembly.xml</descriptor>
                        </descriptors>
                    </configuration>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>

</project>

EOF
	create_module "$module"
}

create_git_filter() {
	echo -e "# Createdby .ignore support plugin (hsz.mobi)\n.idea/\n*.iml\ntarget/\n.DS_Store" >$name/.gitignore
}

create_readme() {
	echo -e "# $name project instruction" >$name/README.md
}

create_default_conf() {
	cp $base/create_conf.sh $name
	bash $base/create_conf.sh > $name/default.properties
}

create_project() {
	local name=$1
	test "x$name" == "x" && read -p "Please enter name: " -t 30 name
	test "x$name" == "x" && { echo "The name of the project can not be empty" >&2;exit; }
	mkdir -p $name
	test ! -d $name && { echo "project create failed." >&2;exit; }
	create_git_filter
	create_readme
	create_default_conf
	create_pom "$2" "$3" "$4" "$5"
	echo "procject $name create successful."
}
test $# -eq 0 && {
	echo -e "Usage:\n\tbash $0 [procject_name]\nEx:\n\tbash $0 ~/code/test_project" >&2;
	exit;
}
create_project "$@"
